# Weather·Traffic benchmark 반복·실행 지문 계약

## 목적과 실행 경계

이 benchmark는 Cloudflare 청구액이 아닌 Trino·Iceberg 비용 대리 지표를 before/after로 비교하기 위한 Weather 소유 도구다. `collect`는 `ASK_SEOUL_TARGET=dev`에서만 동작하며, `--repeat`은 최소 `3`이어야 한다. 이보다 작은 값은 Trino 연결 전에 `ValueError`로 실패한다.

수집 과정은 dbt `compile`, 컴파일된 SQL의 `EXPLAIN ANALYZE`, 기존 Weather/Traffic reliability report 조회만 수행한다. dbt model run, table write, raw upload, backfill, prod write를 수행하지 않는다. 따라서 이 작업은 Traffic 코드·table·raw object·benchmark 결과에 대한 쓰기를 만들지 않는다.

## 측정 suite

| Suite | Domain | 읽기 전용 workload | 고정 대상/입력 계약 |
| --- | --- | --- | --- |
| `weather_silver` | Weather | dbt compile + `EXPLAIN ANALYZE` | `silver_kma_vilage_fcst` |
| `traffic_silver` | Traffic | dbt compile + `EXPLAIN ANALYZE` | `silver_seoul_traffic_incident`, `seoul_traffic_incident`의 publishable snapshot |
| `weather_gold` | Weather | dbt compile + `EXPLAIN ANALYZE` | `gold_weather_forecast_by_place` |
| `traffic_gold` | Traffic | dbt compile + `EXPLAIN ANALYZE` | `gold_traffic_incident_current_by_admin_dong_hourly`, `traffic_snapshot_dag_run_id` |
| `weather_watchdog` | Weather | 기존 reliability report 조회 | Weather Bronze 및 manifest metadata |
| `traffic_watchdog` | Traffic | 기존 reliability report 조회 | Traffic Bronze 및 manifest metadata |

Weather suite는 `bronze_kma_vilage_fcst`, `bronze_collection_run_manifest`를 입력 source table로 기록한다. Traffic suite는 `bronze_seoul_traffic_incident`, `bronze_seoul_traffic_incident_request_audit`, `bronze_collection_run_manifest`를 기록한다. Traffic model suite는 `seoul_traffic_incident`의 최신 publishable Bronze run을 해석해 `traffic_snapshot_dag_run_id`로 dbt compile에 전달한다.

## 비교 전제: 두 지문

각 suite는 bundle에 source fingerprint와 execution fingerprint를 함께 남긴다.

- **Source fingerprint**는 각 입력 Iceberg table의 최신 `snapshot_id`, commit 시각, file 수·크기, record 수를 담는다. suite 실행 전후가 다르면 해당 suite는 `comparable_within_run=false`가 된다.
- **Execution fingerprint**는 suite 이름, domain, source tables, `target=dev`, catalog, schema, `DBT_BIN`을 담는다. model suite는 dbt project와 `compile_command`(snapshot var를 포함한 `--vars` 포함)를, watchdog suite는 report 이름을 추가로 담는다. 생성 시각과 query metrics는 포함하지 않는다.

`compare`는 execution fingerprint를 먼저, source fingerprint를 다음으로 비교한다. 두 bundle의 두 지문이 모두 같고 각 suite의 실행 중 source fingerprint도 유지됐을 때만 반복별 median을 계산한다.

| 비교 불가 사유 | 의미 | 처리 |
| --- | --- | --- |
| `execution_fingerprint_mismatch` | target/catalog/schema, dbt 실행 경로·project·compile 명령, suite 정의 또는 snapshot var 등 실행 계약이 달라졌다. | 비용 대리 지표를 비교하지 않고 `comparable=false`를 반환한다. |
| `fingerprint_mismatch` | before/after bundle이 동일한 Iceberg source snapshot·파일 상태를 보지 않았다. | 비용 대리 지표를 비교하지 않고 `comparable=false`를 반환한다. |
| `fingerprint_changed_during_run` | 한 bundle의 반복 수집 중 입력 source table metadata가 변했다. | 해당 bundle을 안정된 표본으로 보지 않고 `comparable=false`를 반환한다. |
| `suite_set_mismatch` | before/after의 suite 구성 자체가 다르다. | 공통 median 비교를 하지 않는다. |
| `repeat_count_mismatch` | suite별 반복 횟수가 서로 다르거나 반복 결과가 없다. | 공통 median 비교를 하지 않는다. |

## Traffic transform 동시 작업 시 처리

Traffic transform이 같은 dev schema에서 동작하는 동안 Traffic Bronze/manifest snapshot 또는 publishable run이 바뀔 수 있다. 이때 `fingerprint_mismatch` 또는 `fingerprint_changed_during_run`은 benchmark 오류가 아니라 동일 입력이 아니므로 비교를 차단한 정상 결과다. 실행 계약 자체가 바뀐 경우에는 `execution_fingerprint_mismatch`가 같은 역할을 한다.

동시 작업 중에는 before/after 실측을 확정하거나 성능 개선·저하를 기록하지 않는다. 이 문서는 live benchmark 결과를 포함하지 않으며, Traffic transform이 안정된 뒤 dev 환경에서 `collect --repeat 3`으로 before/after bundle을 수집하고 fingerprint gate를 통과한 경우에만 비교 결과를 남긴다.

## 코드 검증 범위

아래 검증은 live Trino 수집을 호출하지 않는다.

```powershell
python -m pytest -q domains/weather/tests/test_weather_traffic_cost_proxy.py
python -m py_compile domains/weather/weather_ingest/weather_traffic_cost_proxy.py
```

Windows의 `pytest.exe` launcher는 collection 이전 entrypoint 해석 문제가 있었으므로 사용하지 않고 `python -m pytest`로 실행한다.
