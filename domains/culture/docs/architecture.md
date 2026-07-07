# 아키텍처 — 오케스트레이션 전략 + 코드 지도

culture bronze 수집이 **어떻게(오케스트레이션)** 돌고 **어디에(코드)** 로직이 있는지.

## 1. 오케스트레이션 전략 (Airflow 레벨)

DAG [`culture_bronze`](../culture_bronze.py) (스케줄 `@daily`). 태스크 흐름:

```text
plan ──▶ fetch_raw (12개 동적 매핑 · 병렬) ──▶ load_bronze ──▶ report (all_done)
```

- **plan** — 적재할 데이터셋 목록과 공유 `ingest_ts`를 계산. 여기서 `target`을 검증(fail-fast).
- **fetch_raw** — 데이터셋마다 매핑 태스크 1개. 원본 API 응답을 **R2 raw에 박제까지만**.
  하나 실패해도 격리·재시도.
- **load_bronze** — fetch가 박제한 raw를 다시 읽어 bronze Iceberg에 **멱등 적재**(API 재호출
  없음). `all_done`이라 일부 fetch가 실패해도 성공분은 적재. 성공 시 Asset outlet 갱신 →
  culture_transform(dbt, 후속 #103) 기동.
- **report** — 모든 결과를 모아 정량 run 리포트 적재(`all_done`로 항상 실행).

### 설계 결정 (왜)

- **데이터셋당 매핑 태스크 1개 (동적 매핑)** — `plan`이 낸 목록을 `.expand()`로 펼쳐 12개
  태스크를 병렬 실행. 한 데이터셋의 API 오류가 **run 전체를 실패시키지 않고**(격리), 그 태스크만
  독립 재시도(`retries=2`)되며, Airflow 그리드에서 어느 데이터셋이 깨졌는지 바로 보인다.
  단일 루프 태스크였다면 all-or-nothing이라 부분 실패·개별 재시도·가시성을 모두 잃는다.
- **fetch/load 분리 (재현 불가/가능 경계)** — 실시간 API 응답은 지나가면 재현 불가라 **raw
  박제까지가 fetch_raw**의 몫. bronze Iceberg는 raw에서 언제든 재생 가능하므로 **load_bronze**가
  raw만 다시 읽어 적재한다(API 재호출 없음) → bronze만 깨진 run은 load_bronze **단독
  재시도/backfill**로 복구(population #94와 동일 패턴). load_bronze 성공 시 Asset outlet으로
  culture_transform(후속 #103)을 기동. → [operations.md](operations.md)
- **멱등성 (`ingest_ts` 파티션)** — `ingest_ts`는 `plan`에서 **한 번** 계산해 12개 태스크가
  공유한다. 한 run의 모든 적재가 같은 파티션에 떨어지고, 재시도/부분 재실행이 같은 파티션을
  덮어쓰므로(Iceberg는 delete-then-insert) 중복·오염이 없다. → [storage.md](storage.md)
- **target fail-closed** — 수동 트리거의 `target`은 자유 입력이라 오타(`prd`)가 prod로 샐 수
  있다. `_plan`이 `normalize_target`으로 `{dev,prod}` 외 값을 **즉시 실패**시켜, 12개 태스크가
  뜨기 전에 run을 멈춘다. → [change-log #41](../change-log.md)
- **coverage 분모 = plan** — 실패한 매핑 태스크는 예외를 던져 XCom에 결과를 안 남긴다. 성공
  summary만 세면 실패가 분모에서도 사라져 coverage가 늘 ~100%로 보인다. `report`는 `plan`
  출력에서 기대 수를 잡는다. → [reliability.md](reliability.md) · [change-log #39](../change-log.md)
- **롤링 날짜창 / lookback** — `date_from/to`를 안 주면 `[end - lookback_days, end]` 창을 자동
  사용(일배치가 매일 최근 창을 집는다). 날짜창을 받는 엔드포인트는 `pblprfr`·`prffest`·`boxoffice`
  뿐이고, **boxoffice는 ≤ 31일**(초과 시 `returncode 05`)이라 `lookback_days` 기본 31.
- **계약 위반 게이트 opt-in (`fail_on_violation`)** — 기본 off. 계약 v0 안정화 전 거짓 경보를
  피하려고 위반은 **surface만** 하고 run은 실패시키지 않는다(수집 자체 실패는 항상 태스크가 빨갛게
  실패). `True`면 위반 시 run 실패. → [reliability.md](reliability.md)
- **`report`는 `all_done`** — 일부 데이터셋이 실패해도 리포트는 항상 돌아 커버리지·SLO 스냅샷을 남긴다.

### 변환 오케스트레이션 (culture_transform)

DAG [`culture_transform`](../culture_transform.py) — bronze → silver/gold dbt 변환. 태스크 흐름:

```text
dbt_source_freshness ──▶ dbt_seed ──▶ dbt_run ──▶ dbt_test
```

- **왜 Asset 트리거인가** — cron이 아니라 `culture_bronze`의 **load_bronze outlet**
  (`Asset("iceberg://culture/bronze")`, #102)을 **구독**해 기동한다(`schedule=[Asset(...)]`).
  bronze가 **실제로 갱신됐을 때만** 변환이 돈다 — 수집이 실패·지연된 날에도 시간표대로 도는
  cron의 **우연 결합**이 사라져, 낡은 bronze 위에서 헛도는 run이 없다. 논리 Asset URI는
  target(dev/prod)과 무관하며 [`config.py`](../culture_ingest/common/config.py)의
  `CULTURE_BRONZE_ASSET`이 outlet·schedule의 **단일 진실 원천**.
- **freshness 게이트를 맨 앞에** — `dbt source freshness`가 `sources.yml` 계약(경고 30h/에러 48h)을
  실측한다. **error만 실패**시켜, bronze가 48h 넘게 낡았으면 seed/run/test로 나아가지 않고 **여기서
  멈춘다** → 낡은 입력으로 silver/gold를 오염시키는 대신 **수집부터 고치게** 신호를 준다.
- **seed → run → test** — seed(`sema_branch_gu`, 시립미술관 분관→자치구 매핑)는 작아서 매 run 멱등
  갱신. run이 silver 9종 + gold 3종을 dbt `ref()` 순서로 빌드하고, test가 계약을 검증한다.
- **target 파라미터** — 트리거 시 덮어쓸 수 있고 기본 dev. dbt target이 카탈로그(iceberg_dev/iceberg)를
  가른다. → [change-log #103](../change-log.md)

### 데이터 흐름 (오케스트레이션 관점)

한 데이터셋이 거치는 경로:

```text
소스 API (KOPIS XML / 서울 JSON)
  │  clients.KopisClient / SeoulClient   — 원본 bytes만 받음(파싱 X)
  ▼ [fetch_raw]
landing.write_page()      ─▶ R2 raw: raw/culture/<source>/<dataset>/load_date=/ingest_ts=/page-NNNN.{xml,json}
landing.write_manifest()  ─▶ 같은 prefix에 _manifest.json (엔드포인트·행수·요청 파라미터·checks)
  │
  ├─ checks.evaluate_landing()   — 완전성·드리프트·freshness (계약 v0)
  │
  ▼ [load_bronze]  R2 raw만 다시 읽음 — API 재호출 없음
warehouse.load()          ─▶ bronze Iceberg: iceberg[_dev].culture.bronze_<dataset>  (레코드 1건 = 1행)
```

raw와 bronze Iceberg를 둘 다 남기는 이유: raw는 재처리용 **원본 보존**, bronze는 Trino/dbt가 SQL로
읽을 수 있는 형태. 파싱·타입화는 하지 않는다(silver 몫). → [storage.md](storage.md)

## 2. 코드 지도 (패키지·모듈)

`culture_bronze.py`(DAG 엔트리)는 얇게 흐름만 잡고, 로직은 `culture_ingest/` 패키지에
위임한다. **common**(도메인 무관 프레임워크) vs **source**(culture 전용)로 나뉜다.

| 파일 | 역할 |
|------|------|
| [`culture_bronze.py`](../culture_bronze.py) | 일배치 DAG. `plan → fetch_raw → load_bronze → report`. |
| [`culture_ingest/common/config.py`](../culture_ingest/common/config.py) | R2 접속정보(dev/prod), `.env` 파싱, `RunContext`, 키 prefix, `normalize_target`. |
| [`culture_ingest/common/http.py`](../culture_ingest/common/http.py) | 페이지 묶음 `Page`. 전송(재시도·rate limit)은 루트 `common/http`(#78) 소비(#152). |
| [`culture_ingest/common/landing.py`](../culture_ingest/common/landing.py) | R2/로컬 싱크, 페이지·`_manifest.json` 기록, 결과 `DatasetResult`. |
| [`culture_ingest/common/checks.py`](../culture_ingest/common/checks.py) | 수집 계약 v0 검증(완전성·드리프트·freshness). → [reliability.md](reliability.md) |
| [`culture_ingest/common/warehouse.py`](../culture_ingest/common/warehouse.py) | bronze Iceberg 테이블 생성/적재(`BronzeWarehouse`, Trino HTTP). → [storage.md](storage.md) |
| [`culture_ingest/source/config.py`](../culture_ingest/source/config.py) | 적재 루트 `raw/culture`, 소스 API 키 로딩. |
| [`culture_ingest/source/clients.py`](../culture_ingest/source/clients.py) | KOPIS(XML)·서울(JSON) 클라이언트 — `common.http` 합성(#152), 원본 bytes만 받음. 페이징·probe(#147)·400 판정(#146)은 여기(도메인 소관). |
| [`culture_ingest/source/datasets.py`](../culture_ingest/source/datasets.py) | 12데이터셋 레지스트리(단일 진실 원천 — 데이터셋 추가 = 여기 한 줄). → [sources.md](sources.md) |
| [`culture_ingest/source/ingest.py`](../culture_ingest/source/ingest.py) | 적재 오케스트레이션 — fetch 계열(`run_batch`/`ingest_one`)·load 계열(`load_bronze`), run 리포트 빌드. |
| [`scripts/run_culture_ingest.py`](../scripts/run_culture_ingest.py) | Airflow 없이 로컬 실행 CLI. → [operations.md](operations.md) |

### 진입점 & 콜그래프

fetch(원본 박제)와 load(bronze 적재) 두 계열. fetch는 진입점 둘이 같은 코어로 수렴한다:

```text
[DAG]  _plan ─▶ (12×) _fetch_raw ─▶ ingest_one(name, ctx=공유, opts, target)
                                        │
[CLI]  run_batch(names, opts, target) ─ for ds in select(names) ─┐
                                        │                          │
                                        ▼                          ▼
                            fetch 코어 (ingest.py — 데이터셋 1개 raw 박제)
                                  ├─ clients.{kopis,seoul}   원본 bytes
                                  └─ landing.write_page / write_manifest   R2 raw

[DAG]  _load_bronze ─▶ load_bronze(ctx, summaries) ─▶ load_bronze_from_raw
                             ├─ sink.get(raw 객체) → parse_records
                             └─ warehouse.load          bronze Iceberg (멱등)

[DAG]  _report ─▶ build_run_report(summaries) ─▶ write_run_report() ─▶ R2 _reports/…/run_report.json
```

- **`ingest_one`** (DAG) — 데이터셋 1개. `ctx`(=`ingest_ts`)를 **상류 `plan`에서 받아** 12개
  태스크가 같은 파티션을 공유.
- **`run_batch`** (CLI) — 여러 데이터셋을 **자체 생성한 `ctx` 하나**로 한 프로세스에서 순차 적재.
- 둘 다 같은 fetch 코어로 수렴 → DAG/CLI 동작 일치. fetch는 **raw 박제까지만** 한다.
- **`load_bronze`** — R2 싱크·Trino 웨어하우스를 만들어 `load_bronze_from_raw`(주입 코어)를
  실행. 입력은 raw뿐(API 재호출 없음), 같은 `ingest_ts` 파티션 delete-then-insert라 멱등.
  CLI `--write-iceberg`도 같은 함수를 fetch 직후 순차 호출한다(DAG/CLI 동작 일치).

### `.airflowignore` — DAG 스캔 vs import

`culture_ingest/`·`scripts/`는 DAG 스캔에서 제외(import만). DAG가 자기 디렉토리를 `sys.path`에
넣고 `culture_ingest.*`를 불러온다.
