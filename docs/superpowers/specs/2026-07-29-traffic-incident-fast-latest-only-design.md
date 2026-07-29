# Traffic Incident 최신 우선 변환 구조

## 목표

Traffic Incident Bronze가 5분마다 도착해도 이미 시작한 Silver 변환이 계속
`stale` 실패로 끝나지 않게 한다. 데이터 정합성, 멱등성, Iceberg snapshot
fence와 실패 관측성은 유지하면서 Airflow task와 dbt 프로세스 시작 횟수를
줄인다.

## 보존해야 하는 기존 의도

- Bronze source freshness, availability, generic contract는 Silver 쓰기 전에
  반드시 통과한다.
- snapshot은 긴 preflight 뒤, Silver 쓰기 직전에 선택한다.
- Silver는 선택한 immutable Bronze `dag_run_id`만 읽는다.
- 검증과 success marker가 완료되기 전에는 Silver asset을 발행하지 않는다.
- Iceberg snapshot이 외부 rewrite/compaction으로 바뀌면 fail closed 한다.

## 재검토 결과

Silver 변환 시간이 Bronze 수집 주기보다 길 수 있으므로 완료 시점에
“pin이 여전히 절대 최신인가”를 hard correctness로 요구하면 정상 실행도
영구적으로 실패할 수 있다. correctness는 아래 두 계약으로 제한한다.

1. 선택한 pin은 실행 시작 시 publishable이며 최신이다.
2. 만들어진 current Silver는 그 immutable pin과 정확히 일치한다.

실행 중 새 Bronze가 도착한 경우에는 새 asset-triggered run이 처리한다.
pin이 최신 publishable run보다 뒤처졌는지는 correctness 실패가 아니다.
지연은 기존 `traffic_bronze_reliability_report`가 pipeline stage 성공 시각과
15/30분 임계치로 추적한다.

## 운영 task 구조

논리 단계는 네 개이며 Airflow에서는 관측 가능한 여덟 task로 유지한다.

1. Preflight
   - `validate_dev_runtime`
   - `dbt_deps`
   - `dbt_source_freshness`
   - `dbt_test_traffic_bronze_source_contract`
     - availability singular test와 Bronze generic contract를 한 dbt 호출로 실행
2. Prepare
   - `resolve_traffic_snapshot_run`
     - resolve, admit, latest 확인을 같은 pool 점유 안에서 연속 수행
3. Build and verify
   - `dbt_run_silver`
     - `dbt build --selector ask_seoul_traffic_transform_incident_silver`
     - model과 pin-based tests를 한 parse/invocation으로 실행
     - 실행 전/후 Iceberg snapshot fence 유지
4. Commit publication
   - `publish_traffic_incident_silver_asset`
     - write evidence 재검증, deferred run coalesce, success marker 기록,
       outlet metadata 준비를 한 task 성공 경계로 묶음
   - `publish_dbt_run_metrics` teardown

## 실패 의미

- preflight 실패: source/contract 문제이며 Silver를 쓰지 않는다.
- prepare skip: 같은 결과가 이미 publish됐거나 더 최신 run이 준비된
  정상적인 수렴이다.
- dbt build 실패: Silver asset과 marker를 발행하지 않는다.
- snapshot fence 실패: 외부 writer/compactor race로 fail closed 한다.
- publish/marker 실패: task가 실패하므로 Airflow outlet event도 발행되지 않는다.

## 범위

이번 변경은 `traffic_incident_transform`과 그 공통 spec/runtime 경계, 그리고
Traffic Incident dbt selector/contract에만 적용한다. Flow/Gold는 검증 결과를
확인한 뒤 같은 패턴을 별도 변경으로 확장한다.
