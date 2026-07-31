# 운영 — 실행 · 트리거 · 재수집 · 디버깅

## 시크릿 (env)

DAG는 키를 **환경변수에서만** 읽는다. `docker-compose`가 `sample/.env`를 (`env_file:`로) 모든
Airflow 컨테이너에 주입: `KOPIS_SERVICE_KEY`, `SEOUL_API_KEY_CULT`, `PUBLIC_DATA_API_KEY_CULT`(KCISA #196), `KOBIS_SERVICE_KEY`(#197), `R2_DEV_*`.
네 소스 키는 **모두 필수** — `build_clients`가 하나라도 없으면 `RuntimeError`(전 fetch_raw 실패).
**키는 절대 커밋하지 않는다** — `.env`는 상위 레포에서 gitignore.

## 로컬 실행 (CLI · Airflow 없이)

[`scripts/run_culture_ingest.py`](../scripts/run_culture_ingest.py):

```bash
# domains/culture/ 에서 — 로컬 dry-run (R2 안 씀)
python scripts/run_culture_ingest.py --dry-run --local-dir ./_dryrun \
    --env-file ../../../sample/.env --date-from 20260601 --date-to 20260628

# seoul-dev 버킷에 실제 적재
python scripts/run_culture_ingest.py --target dev --env-file ../../../sample/.env \
    --date-from 20260101 --date-to 20261231 --include-detail --max-detail 200
```

- `--write-iceberg` — fetch 성공분을 같은 프로세스에서 이어 bronze Iceberg까지 적재
  (**fetch→load 순차**, DAG의 `load_bronze`와 같은 함수).
- 종료 코드: `0` 성공 · `1` 데이터셋 fetch 실패 **또는** bronze load 실패 · `2` 설정/인증
  실패(사전 점검).

## Airflow 트리거 (DAG 파라미터)

| 파라미터 | 뜻 | 기본 |
|---|---|---|
| `target` | `dev` / `prod` (그 외 값은 **즉시 실패**) | dev |
| `datasets` | 적재할 슬러그(빈 값 = daily 전체 — 시설 상세는 야간 missing top-up 모드로 포함, #466) | [] |
| `date_from`/`date_to` | YYYYMMDD (비면 롤링창) | "" |
| `lookback_days` | 날짜창 크기 (boxoffice ≤ 31) | 31 |
| `include_detail` | KOPIS 상세 엔드포인트 크롤 | True |
| `max_detail` | 상세 크롤당 id 상한 (공연 상세용 — 시설 상세는 주간 DAG가 2000으로 오버라이드) | 200 |
| `detail_mode` | `missing`(야간 top-up: 목록에 있으나 bronze 상세 없는 시설만, #466) / `full`(전수) | missing |
| `kopis_rows` | KOPIS 목록 페이지 크기 | 100 |
| `fail_on_violation` | 계약 위반 시 run 실패 | False |
| `engine` | bronze 적재 엔진 `pyiceberg`(기본) / `trino`(롤백 레버) — 그 외 값은 즉시 실패 | pyiceberg |

bronze Iceberg 적재는 파라미터가 아니라 **`load_bronze` 태스크가 매 run 수행**한다
(옵션으로 켜던 구 파라미터는 #102에서 삭제 → [change-log](../change-log.md)).

## 재수집 (backfill)

`ingest_ts`가 실행을 격리하고 같은 파티션을 덮어쓰므로(멱등), **같은 창을 다시 돌려도 안전**하다.

- **특정 데이터셋만**: 파라미터 `datasets=["kopis_boxoffice", …]`(빈 값=전체). CLI는 `--datasets`.
- **특정 기간**: `date_from`/`date_to`(YYYYMMDD) 명시. 안 주면 `[end - lookback_days, end]` 롤링창.
- **boxoffice 제약**: `stdate~eddate` **≤ 31일**(초과 시 `returncode 05`) → 긴 기간은 31일씩 나눠 재수집.
- **상세(detail)**: `include_detail=True` + `max_detail`로 크롤 id 상한 조정.
- **KOBIS 박스오피스(#197)**: `kobis_boxoffice_*`의 `targetDt`는 **실행 logical date − 1일**로
  고정 계산되며 `date_from`/`date_to` 창을 **쓰지 않는다**(일배치가 창을 항상 당일로
  채우기 때문 — 창을 존중하면 아직 확정 안 된 당일을 조회하게 됨). 과거 특정일 재수집은
  **Airflow 로 그 날짜+1일을 logical date 로 재실행**한다(예: 6/1 박스오피스 = logical
  date 6/2 → load_date 2026-06-02 → targetDt 20260601). CLI `--date-from/--date-to`는
  KOBIS 에 무효(전국/서울 모두 항상 전일분).
- **시설 상세 이원화(#206·#466)**: 야간 일배치는 `detail_mode="missing"`으로
  "목록에 있으나 bronze 상세가 없는" 신규 시설만 top-up 한다(평상시 0건 → API 호출
  없이 skip; 신규 시설 목록↔상세 갭이 하루 내로 닫혀 gold not_null 야간 실패를
  방지). 전수 재크롤은 `culture_facility_refresh`(일 05:30 KST)가 `detail_mode="full"`
  + `max_detail=2000`으로 수행. 수동 전수 크롤:
  `airflow dags trigger culture_bronze --conf '{"datasets": ["kopis_facility", "kopis_facility_detail"], "max_detail": 2000, "detail_mode": "full"}'`
- **공연 상세도 야간 top-up(#518)**: `kopis_performance_detail`도 `missing_only_nightly`.
  상세에서 쓰는 값은 `mt10id`(공연↔공연장) 하나뿐이고 이는 공연 id 에 대해 불변이라
  — 실측 608건 중 변경 0건 — 재크롤 정보량이 0 이었다(200콜/일의 약 92%가 전날 것
  재수집). 시설과 달리 **주기가 아니라 대상을 줄인다**: 신규 공연이 매일 생기므로
  야간 실행은 그대로 두고 안티조인으로 신규분만 긁는다. **전환 직후엔 200건씩 나간다** —
  앞자르기가 한 번도 닿지 않은 목록 후미가 미크롤로 남아 있어서다(전환 시점 목록
  1,260건 중 크롤 이력 608건 = 백로그 652건). cap 만큼 하루 200씩 배수해 3~4일이면
  목록 전량, 그 뒤가 정상 상태(신규 ~15건/일)다. 부수 효과로 상세 커버리지가
  29%→~100%로 올라 `facility_match` 가 이름 폴백에서 `detail_id` 로 옮겨간다. 전수 재크롤:
  `airflow dags trigger culture_bronze --conf '{"datasets": ["kopis_performance", "kopis_performance_detail"], "max_detail": 3000, "detail_mode": "full"}'`
- **top-up known-id 는 데이터셋별 조회(#518)**: plan 이 `load_known_detail_ids`로
  `missing_only_nightly` 데이터셋마다 **자기 `id_field`**로 기존 id 를 읽는다. 한 집합을
  공유하면 공연(`mt20id`)이 시설(`mt10id`) 집합과 안티조인돼 교집합 0 → 목록 앞 200건을
  매일 재크롤하던 종전 동작이 유지된다(cap 이 차집합 뒤라 폭주는 없지만 **절감 0 인 채
  로그만 top-up 처럼 보이는 조용한 no-op**). 조회 실패는 데이터셋 단위 fail-open.

```bash
# 예: boxoffice만 특정 주간 재수집 (dev, 로컬 CLI)
python scripts/run_culture_ingest.py --target dev --env-file ../../../sample/.env \
    --datasets kopis_boxoffice --date-from 20260601 --date-to 20260628
```

## 복구 — bronze Iceberg만 실패한 run

raw 박제(fetch_raw)는 성공했는데 `load_bronze`만 실패한 run은 **API 재호출 없이**
load_bronze만 재실행하면 된다. 같은 `ingest_ts` 파티션을 delete-then-insert 하므로 멱등:

```bash
# <ts> = 해당 run의 logical date (ISO8601, 예: 2026-07-03T00:00:00+09:00)
# -d: downstream(report)도 함께 재실행 — 리포트·Discord의 load_failed 표기까지 갱신
airflow tasks clear culture_bronze -t load_bronze -d -s <ts> -e <ts> --yes
```

`run_report.json`의 `load_failed: true`(리포트/Discord 알림)가 이 케이스의 신호다.

## bronze 적재 엔진 (#203)

bronze 쓰기는 기본 **pyiceberg**(R2 Data Catalog REST 직접 커밋 — 데이터셋당 1회,
03:00 정기런 load_bronze 26분 → 약 3~5분: dev 검증 303s(2026-07-09), 첫 03:00 정기런
실측 load 단독 ~200s·run 전체 297s(2026-07-10), $snapshots 데이터셋당 1커밋 확인)다.
Trino 는 조회·DDL·silver/gold(dbt)에서 그대로 쓴다.

- **롤백**: pyiceberg 경로 장애 시 `{"engine": "trino"}` 로 재트리거(코드 변경 불필요).
  CLI 는 `--engine trino`. 로컬 CLI에서 pyiceberg 미설치 환경이면 `--engine trino`로 실행한다.
- **추가 env**: `R2_DEV_DATA_CATALOG_URI/WAREHOUSE/TOKEN`(dev), `R2_DATA_CATALOG_*`(prod)
  — 없으면 load_bronze 가 이름을 적어 즉시 실패.
- **일몰**: 03:00 정기런 7회 연속 성공 후 trino 쓰기 경로(`BronzeWarehouse.load`)와
  `engine` 파라미터를 제거하는 후속 이슈를 등록한다.
  — 진행: **1/7** (2026-07-10 success, load ~200s·데이터셋당 1스냅샷).

## 메타DB Connection — SLO dag_run enrichment (#411)

`culture_slo` 의 `load_dag_runs` 는 Airflow Connection `airflow_metadb` 로 메타DB
dag_run 을 읽는다(태스크 격리 #303 의 허용 경로 = PostgresHook + 정의된 Connection).
**등록은 CLI 1회** — 컨테이너 안에서 기존 env 를 재사용하므로 접속 문자열이
레포·로그에 노출되지 않는다:

```bash
docker exec elt-infra-airflow-scheduler-1 bash -c \
  'airflow connections add airflow_metadb \
     --conn-uri "${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN/postgresql+psycopg2/postgres}"'
```

- **스택 재구축(메타DB 볼륨 소실) 시 재등록 필요.** 미등록이면 load_dag_runs 가
  스킵 로그를 남기고 0을 반환한다 — 핵심 SLO(run_report 기반)는 영향 없고
  `bronze_culture_dag_runs` 만 안 자란다.
- DBT freshness 와의 결합: 이 테이블의 소스 freshness 는 적재가 살아있을 때만
  켠다(#238 사고 — 빈 테이블 감시 금지). 스킵이 이틀 넘게 지속되면 freshness 가
  다시 error 를 낼 수 있으니 Connection 부터 확인한다.

## D1 서빙 export — `culture_serving_export` (#520)

외부 gold 7종을 팀 공용 D1(`ask-seoul-dev-d1`)에 전량 스냅샷 게시. 도메인 쪽 코드는
`culture_serving_export.py`(factory 호출) 하나이고, 계약·파이프라인은 공통이 정본:
계약 = ASAC-DBT culture `meta.serving`(v1.1) · 파이프라인 = `common/serving`(#505).

- **env 선행 3키** (`sample/.env` → env_file 주입, ASK-Seoul#54):
  `CLOUDFLARE_API_TOKEN`(D1 Edit, 비밀) + `SERVING_CLOUDFLARE_ACCOUNT_ID` ·
  `SERVING_D1_DATABASE_ID`(팀 D1 식별자, 비밀 아님 — 값은 상위 레포
  `serving/wrangler.toml` 과 동일). 없으면 `publish_to_d1` 이 RuntimeError 로 즉시 실패.
  **`.env` 를 고친 뒤에는 컨테이너 재생성 필수** — env_file 은 생성 시점에만 읽힌다.
- **manifest 선행**: `/opt/airflow/dbt/domains/culture/target/manifest.json` 에
  `meta.serving` 이 있어야 한다(없으면 "enabled 계약이 없다" 로 실패). transform 이
  매일 갱신하지만, 계약 yml 만 바꾼 직후엔 컨테이너에서 `dbt parse` 로 재생성:

```bash
docker exec elt-infra-airflow-scheduler-1 bash -c \
  '/home/airflow/dbt-venv/bin/dbt parse --project-dir /opt/airflow/dbt/domains/culture --profiles-dir /opt/airflow/dbt --target dev'
```

- 수동 1회 실행: `airflow dags test culture_serving_export`(스케줄러 컨테이너).
- 스모크는 `SERVING_API_BASE_URL` 미설정 시 no-op pass(공개 Worker 창구는 #476 결정 대기).
- 검증 쿼리(행수·등록): `SELECT name, row_count, serving_status FROM _catalog WHERE
  product_id LIKE 'culture_%'` — 게시 수 == 등록 수가 자기검증(#477)의 통과 조건.

## 디버깅

1. **어느 데이터셋이 깨졌나** — Airflow 그리드에서 red `fetch_raw` 매핑 인덱스 → 태스크 로그.
   데이터셋별로 태스크가 갈려 있어 바로 짚인다. red `load_bronze`는 bronze 적재 실패
   (위 **복구** 절 — load_bronze만 clear).
2. **정량 상태** — `run_report.json`(`_reports/…`)의 `coverage`·`violations`·`failed_datasets`·
   `load_failed`·`slo_passed` 확인. → [reliability.md](reliability.md)
3. **흔한 오류**
   - `Missing culture source keys` / `Missing R2 config` — env 미주입(위 **시크릿 (env)** 절 참고).
   - KOPIS `returncode 05` — 날짜창 31일 초과(특히 boxoffice) → 창을 좁혀 재수집.
   - `target must be one of ('dev', 'prod')` — `target` 오타(`plan`에서 즉시 실패).
   - 서울 `SeoulError`(INFO-000/200 외) — 서비스명·키·쿼터 확인.
