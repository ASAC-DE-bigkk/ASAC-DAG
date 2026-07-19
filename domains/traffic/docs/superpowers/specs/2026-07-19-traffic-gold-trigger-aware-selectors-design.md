# Traffic Gold trigger-aware selector 설계

## 목표

`traffic_gold_transform`은 `Traffic Incident Silver OR Traffic Flow Bronze` Asset으로 시작한다.
따라서 정상 운영에서도 두 Asset이 동시에 도착한다고 가정할 수 없다. 각 Gold DagRun이 실제로
pin한 입력에 맞는 dbt node만 실행하게 하여 incident-only run이 Flow 전용 모델 때문에
실패하지 않도록 한다.

## 확인된 실패

- run: `asset_triggered__2026-07-19T08:24:20.558197+00:00_K612gEob`
- Incident Silver Asset만 연결되어 `traffic_flow_snapshot_dag_run_id`가 없었다.
- Incident 및 cross-domain Traffic Gold 모델 17개는 성공했다.
- Flow 전용 incremental 모델 4개는 pinned Flow row가 없다는 기존 fail-closed pre-hook에서
  의도대로 실패했다.
- Trino OOM이나 Citydata 조회가 원인이 아니었다.

## 선택한 구조

ASAC-DBT에 기존 Gold selector를 재사용하는 incident-only named selector를 추가한다.

- `ask_seoul_traffic_transform_flow_gold_scope`: Flow 전용 모델 4개와 그 자식 test node
- `ask_seoul_traffic_transform_gold_incident_models`: 기존 Gold model 21개에서 Flow scope 제외
- `ask_seoul_traffic_transform_gold_incident_gate_tests`
- `ask_seoul_traffic_transform_gold_incident_hourly_tests`
- `ask_seoul_traffic_transform_gold_incident_full_tests`

ASAC-DAG의 `DbtPhaseSpec`은 Flow snapshot이 없을 때 사용할 named selector를 선택적으로
가진다. `run_dbt_phase`는 resolver XCom에 non-empty Flow run ID가 있으면 기존 selector를,
없으면 incident-only selector를 사용한다.

## 실행 의미

### Incident Silver만 도착한 run

1. 최신 publishable Incident Silver와 evidence를 pin한다.
2. Flow identity는 `None`으로 유지한다.
3. Incident 및 cross-domain Traffic Gold 모델 17개만 실행한다.
4. 현재 test tier에 대응하는 incident-only test만 실행한다.
5. `(incident, None, citydata)` identity로 성공 marker를 기록한다.

### compatible Flow Bronze가 도착한 run

1. Silver 성공 marker와 compatible Flow Bronze event를 결합한다.
2. Flow manifest publishability를 기존 방식으로 검증한다.
3. 기존 Gold model 21개와 기존 tier test selector를 그대로 실행한다.
4. `(incident, flow, citydata)` identity로 성공 marker를 기록한다.

## 보존 계약

- Flow 전용 모델의 `traffic_flow_assert_pinned_incremental_rows()`를 수정하지 않는다.
- Flow 모델이 선택됐는데 pinned row가 없으면 계속 fail-closed 한다.
- Incident/Flow event time, ingest lineage, canonical grain, incremental MERGE, idempotency를
  변경하지 않는다.
- 기존 cross-domain Gold는 Traffic schema에만 쓰고 외부 도메인은 source로만 읽는다.
- Commerce, Citydata, Transit, Culture 코드와 파일은 수정하거나 생성하지 않는다.
- maintenance, W2, recovery, backfill은 pause 상태로 유지하고 실행하지 않는다.
- dev만 사용하며 `.env`와 secret은 읽거나 출력하지 않는다.

## 검증

- ASAC-DBT: selector 구조 test, `dbt parse`, `dbt ls` exact node-set 비교
- ASAC-DAG: no-flow/flow-present runtime selector RED/GREEN test와 전체 Traffic regression
- Airflow: import error 0, Weather/Traffic allowlist, 동일 incident-only run 성공
- Flow Bronze 재활성화 후 compatible Flow event run에서 기존 21개 경로 성공
- 동일 identity 재실행은 admission에서 heavy phase를 skip
- PR diff에 금지 도메인 파일이 없을 때만 dev merge 및 최신 revision 재배포
