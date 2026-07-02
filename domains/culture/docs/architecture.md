# 아키텍처 — 오케스트레이션 전략 + 코드 지도

culture bronze 수집이 **어떻게(오케스트레이션)** 돌고 **어디에(코드)** 로직이 있는지.

## 1. 오케스트레이션 전략 (Airflow 레벨)

DAG [`culture_bronze`](../culture_bronze.py) (스케줄 `@daily`). 태스크 흐름:

```text
plan ──▶ ingest_dataset (12개 동적 매핑 · 병렬) ──▶ report (all_done)
```

- **plan** — 적재할 데이터셋 목록과 공유 `ingest_ts`를 계산. 여기서 `target`을 검증(fail-fast).
- **ingest_dataset** — 데이터셋마다 매핑 태스크 1개. 하나 실패해도 격리·재시도.
- **report** — 모든 결과를 모아 정량 run 리포트 적재(`all_done`로 항상 실행).

### 설계 결정 (왜)

- **데이터셋당 매핑 태스크 1개 (동적 매핑)** — `plan`이 낸 목록을 `.expand()`로 펼쳐 12개
  태스크를 병렬 실행. 한 데이터셋의 API 오류가 **run 전체를 실패시키지 않고**(격리), 그 태스크만
  독립 재시도(`retries=2`)되며, Airflow 그리드에서 어느 데이터셋이 깨졌는지 바로 보인다.
  단일 루프 태스크였다면 all-or-nothing이라 부분 실패·개별 재시도·가시성을 모두 잃는다.
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

### 데이터 흐름 (오케스트레이션 관점)

한 데이터셋이 태스크 안에서 거치는 경로:

```text
소스 API (KOPIS XML / 서울 JSON)
  │  clients.KopisClient / SeoulClient   — 원본 bytes만 받음(파싱 X)
  ▼
landing.write_page()      ─▶ R2 raw: raw/culture/<source>/<dataset>/load_date=/ingest_ts=/page-NNNN.{xml,json}
landing.write_manifest()  ─▶ 같은 prefix에 _manifest.json (엔드포인트·행수·요청 파라미터·checks)
  │
  ├─ checks.evaluate_landing()   — 완전성·드리프트·freshness (계약 v0)
  │
  ▼ (write_iceberg=True 일 때만)
warehouse.load()          ─▶ bronze Iceberg: iceberg[_dev].culture.bronze_<dataset>  (레코드 1건 = 1행)
```

raw와 bronze Iceberg를 둘 다 남기는 이유: raw는 재처리용 **원본 보존**, bronze는 Trino/dbt가 SQL로
읽을 수 있는 형태. 파싱·타입화는 하지 않는다(silver 몫). → [storage.md](storage.md)

## 2. 코드 지도 (패키지·모듈)

`culture_bronze.py`(DAG 엔트리)는 얇게 흐름만 잡고, 로직은 `culture_ingest/` 패키지에
위임한다. **common**(도메인 무관 프레임워크) vs **source**(culture 전용)로 나뉜다.

| 파일 | 역할 |
|------|------|
| [`culture_bronze.py`](../culture_bronze.py) | 일배치 DAG. `plan → ingest_dataset → report`. |
| [`culture_ingest/common/config.py`](../culture_ingest/common/config.py) | R2 접속정보(dev/prod), `.env` 파싱, `RunContext`, 키 prefix, `normalize_target`. |
| [`culture_ingest/common/http.py`](../culture_ingest/common/http.py) | 429/5xx 재시도 `requests` 세션, 페이지 묶음 `Page`. |
| [`culture_ingest/common/landing.py`](../culture_ingest/common/landing.py) | R2/로컬 싱크, 페이지·`_manifest.json` 기록, 결과 `DatasetResult`. |
| [`culture_ingest/common/checks.py`](../culture_ingest/common/checks.py) | 수집 계약 v0 검증(완전성·드리프트·freshness). → [reliability.md](reliability.md) |
| [`culture_ingest/common/warehouse.py`](../culture_ingest/common/warehouse.py) | bronze Iceberg 테이블 생성/적재(`BronzeWarehouse`, Trino HTTP). → [storage.md](storage.md) |
| [`culture_ingest/source/config.py`](../culture_ingest/source/config.py) | 적재 루트 `raw/culture`, 소스 API 키 로딩. |
| [`culture_ingest/source/clients.py`](../culture_ingest/source/clients.py) | KOPIS(XML)·서울(JSON) HTTP 클라이언트. 원본 bytes만 받음. |
| [`culture_ingest/source/datasets.py`](../culture_ingest/source/datasets.py) | 12데이터셋 레지스트리(단일 진실 원천 — 데이터셋 추가 = 여기 한 줄). → [sources.md](sources.md) |
| [`culture_ingest/source/ingest.py`](../culture_ingest/source/ingest.py) | 적재 오케스트레이션(`run_batch`/`ingest_one`), run 리포트 빌드. |
| [`scripts/run_culture_ingest.py`](../scripts/run_culture_ingest.py) | Airflow 없이 로컬 실행 CLI. → [operations.md](operations.md) |

### 진입점 & 콜그래프

`ingest_dataset()`가 공유 코어이고, 진입점 둘이 그 위를 감싼다:

```text
[DAG]  _plan ─▶ (12×) _ingest ─▶ ingest_one(name, ctx=공유, opts, target)
                                        │
[CLI]  run_batch(names, opts, target) ─ for ds in select(names) ─┐
                                        │                          │
                                        ▼                          ▼
                            ingest_dataset(ds, clients, landing, opts, warehouse)
                                  ├─ clients.{kopis,seoul}   원본 bytes
                                  ├─ landing.write_page / write_manifest   R2 raw
                                  └─ warehouse.load          bronze Iceberg (선택)

[DAG]  _report ─▶ build_run_report(summaries) ─▶ write_run_report() ─▶ R2 _reports/…/run_report.json
```

- **`ingest_one`** (DAG) — 데이터셋 1개. `ctx`(=`ingest_ts`)를 **상류 `plan`에서 받아** 12개
  태스크가 같은 파티션을 공유.
- **`run_batch`** (CLI) — 여러 데이터셋을 **자체 생성한 `ctx` 하나**로 한 프로세스에서 순차 적재.
- 둘 다 결국 `ingest_dataset` 하나로 수렴 → DAG/CLI 동작 일치.

### `.airflowignore` — DAG 스캔 vs import

`culture_ingest/`·`scripts/`는 DAG 스캔에서 제외(import만). DAG가 자기 디렉토리를 `sys.path`에
넣고 `culture_ingest.*`를 불러온다.
