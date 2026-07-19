# Weather 도메인 AI 인덱스

KMA `getVilageFcst` 수집부터 Weather Silver/Gold 변환, 유지보수, 신뢰성 보고까지의 실행 코드다.
변경 범위는 `domains/weather/**`이며 Traffic이나 다른 도메인 모듈을 import하지 않는다.

## 가장 먼저 읽을 파일

1. 수집 흐름: `weather_vilage_fcst_bronze.py`
2. 변환 흐름과 dbt tag: `weather_vilage_fcst_transform.py`
3. landing 계약: `weather_ingest/landing.py`
4. 수집 manifest 계약: `weather_ingest/run_manifest.py`
5. dbt 실행·artifact 계약: `weather_dbt_execution.py`
6. 원천 의미: `docs/source.md`
7. 상세 Bronze 배경: `docs/bronze-pipeline-reference.md`

## DAG entrypoints

| 파일 | 책임 |
|---|---|
| `weather_vilage_fcst_bronze.py` | 서울 KMA grid 수집, raw checkpoint, Bronze 적재·검증, run manifest 상태 전이를 조율한다. |
| `weather_vilage_fcst_transform.py` | root dbt project의 Weather tag를 source→Silver→Gold 순서로 실행한다. |
| `weather_w1_contract_smoke.py` | Weather W1 bridge/tag 계약을 격리 검증한다. |
| `weather_iceberg_maintenance.py` | Weather Iceberg 유지보수 작업을 조율한다. |
| `weather_reliability_report.py` | 매일 09:00 KST에 Weather data plane과 전체 pipeline stage를 분리 수집해 구조화된 Discord 신뢰성 리포트를 보낸다. |

DAG entrypoint는 순서와 Airflow wiring만 소유한다. 도메인 로직은 아래 모듈에 둔다.

## 모듈 책임 지도

| 모듈 | 책임 |
|---|---|
| `weather_ingest/kma.py` | 발표시각, KMA request/response 파싱과 source-native 필드 |
| `weather_ingest/landing.py` | grid/page checkpoint, complete marker, raw object 계약 |
| `weather_ingest/runtime.py` | HTTP·R2 adapter와 landing/manifest 조립 |
| `weather_ingest/bronze.py` | 기존 import 경로를 보존하는 compatibility facade |
| `weather_ingest/bronze_contract.py` | Bronze schema, row validation, canonical record mapping |
| `weather_ingest/bronze_pyiceberg.py` | PyIceberg transaction, retry, stale multipart cleanup |
| `weather_ingest/bronze_trino.py` | Trino SQL fallback write |
| `weather_ingest/bronze_verification.py` | materialized Bronze run 검증 |
| `weather_ingest/bronze_batch.py` | raw page batch 준비와 atomic append 조율 |
| `weather_ingest/bronze_dag_support.py` | Airflow context, schedule, Discord notification |
| `bronze_run_manifest.py` | 과거 `weather.bronze_run_manifest` import를 보존하는 deprecated facade |
| `weather_ingest/weather_traffic_cost_proxy.py` | 기존 CLI와 import 경로를 보존하는 compatibility facade |
| `weather_ingest/cost_proxy/config.py` | immutable benchmark model registry와 실행 환경 계약 |
| `weather_ingest/cost_proxy/compile.py` | 격리 dbt compile과 manifest model 검증 |
| `weather_ingest/cost_proxy/collection.py` | read-only benchmark suite 조율 |
| `weather_ingest/cost_proxy/comparison.py` | fingerprint gate와 metric 비교 |
| `weather_ingest/run_manifest.py` | STARTED/SUCCESS/FAILED와 publishability 기록 |
| `weather_dbt_execution.py` | root dbt 실행, attempt 격리, artifact 보존, 선택 방식 |
| `weather_lineage.py` | 명시적 opt-in일 때만 DAG OpenLineage selective enable |
| `weather_ingest/reliability_report.py` | 기존 import를 보존하는 compatibility facade |
| `weather_ingest/reliability/config.py` | 환경설정, identifier, 상수 |
| `weather_ingest/reliability/trino_repository.py` | Bronze·manifest read-only 요약 |
| `weather_ingest/reliability/lineage.py` | allowlist된 Marquez job의 최신 상태·staleness·p50/p95 요약 |
| `weather_ingest/reliability/history.py` | 날짜가 고정된 R2 snapshot과 관측 기반 7일 trend |
| `weather_ingest/reliability/report.py` | data/control plane 우선순위와 최종 Pipeline Reliability v2 조립 |
| `weather_ingest/reliability/card.py` | 상태 기반 Discord embed card 조립 |
| `weather_ingest/reliability/discord.py` | 기존 text 호환 formatter와 webhook transport |
| `config/seoul_kma_grids.csv` | 서울을 덮는 KMA `nx, ny` grid 계약 |

## Run manifest 소유권과 호환 경로

- 구현과 SQL의 단일 소유자는 `weather_ingest/run_manifest.py`다.
- `bronze_run_manifest.py`는 기존 `weather.bronze_run_manifest` 사용자를 위한 deprecated
  re-export facade다. 새 코드는 canonical 경로를 import한다.
- facade는 SQL이나 lifecycle 로직을 소유하지 않고 Traffic 모듈도 import하지 않는다.
- 다른 도메인은 자기 도메인의 manifest Module을 소유한다. 이 호환 경로를 이유로 새로운
  cross-domain Python dependency를 만들지 않는다.

## 핵심 실행 흐름

```text
Bronze DAG -> KmaLanding -> R2 raw/checkpoint -> PyIceberg append/Trino verify
           -> WeatherRunManifest SUCCESS + is_publishable
Transform DAG -> dbt tag run/test -> invocation 전용 manifest.json/run_results.json
              -> lineage/metrics
Reliability DAG (09:00 KST) -> Trino Bronze/manifest (weather heavy pool)
                               -> Marquez stages + exact-date R2 history (no Trino)
                               -> structured Discord card + daily delivery state
```

## Pipeline Reliability v2 운영 계약

- DAG ID는 `weather_bronze_reliability_report`를 유지하지만 보고 범위는 Weather Bronze,
  source freshness, Silver/Gold transform, Iceberg maintenance까지다.
- `collect_weather_data_plane`만 `trino_weather_heavy`를 사용한다. compose와 deliver는 Trino slot을
  점유하지 않으며 `collect -> compose -> deliver` 순서로 실행한다.
- 최근 24시간 KMA 발표시각, grid slot, raw pagination page, freshness, publishability가 data truth다.
  이 영역의 실패는 Marquez 성공보다 우선해 `FAIL`이다.
- Marquez는 allowlist job의 최신 run, staleness, 회복된 실패, p50/p95 runtime을 보조 관측한다.
  maintenance 미관측은 정보성 `UNKNOWN`이고, 실제 최신 maintenance 실패가 관측되면 `FAIL`이다.
- 7일 추세는 R2 prefix list 없이 이전 날짜의 exact key 7개만 읽는다. 아직 저장되지 않은 날짜는
  0이나 실패가 아니라 `UNKNOWN`이다.
- 정규 리포트는 매일 09:00 KST 한 번이다. 15분 reliability report는 없으며, 정규 DAG/task 실패는
  `problem_failure_callback`을 통해 즉시 별도 Discord 알림을 보낸다.
- daily fingerprint는 Discord 성공 뒤에만 기록하고 history snapshot은 날짜 key에 멱등 overwrite한다.

## dbt 선택·manifest·lineage

- 활성 dbt project는 Weather/Traffic monoproject `${ASK_SEOUL_DBT_PROJECT_DIR:-/opt/airflow/dbt/domains/traffic_weather}`다.
- Weather와 Traffic을 별도 dbt project로 실행하지 않고 이 project의 단일 manifest를 사용한다.
- DAG는 모델명·path·tag를 나열하지 않고 `selectors.yml`의 named selector만 공통 dbt 실행 Module에 전달한다.
- artifact는 `target/<pipeline>/<run_id>/<task_id>/try<n>/execution/` 아래에 invocation별로 둔다.
- 해당 invocation의 `manifest.json`, `run_results.json`, 필요 시 `sources.json`만 읽는다.
- Airflow DAG lineage는 `weather_lineage.py`, dbt OpenLineage 실행은 `weather_dbt_execution.py`가 소유한다.

## 변경 시작점

- KMA 시간·API·grid/page checkpoint: `weather_ingest/kma.py`, `weather_ingest/landing.py`
- Bronze schema·적재: `weather_ingest/bronze.py`
- 수집 정합성·publishability: `weather_ingest/run_manifest.py`
- dbt tag/실행/artifact: `weather_vilage_fcst_transform.py`, `weather_dbt_execution.py`
- 알림·신뢰성: `weather_ingest/reliability/`
- 회귀 검증: `tests/test_weather_*.py`

## 문서 주의

`docs/superpowers/**`와 `docs/retrospectives/**`는 당시 결정·검증 기록이다. 이전 모델 목록이나
pre-monoproject 경로가 남아 있을 수 있으므로 현재 동작의 기준으로 사용하지 않는다.
현재 계약의 우선순위는 실제 코드와 이 README, `docs/source.md`, 그다음 보존 문서 순이다.
