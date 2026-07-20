# Traffic Silver pin 재검증 설계

## 문제

`traffic_incident_transform`은 Asset event에서 선택한 publishable Incident Bronze
`dag_run_id`를 `resolve_traffic_snapshot_run`에서 고정한다. 그러나 task가
`trino_traffic_heavy` 1-slot을 기다리는 동안 더 최신 Bronze batch가 publish되면,
고정된 run은 여전히 publishable이지만 최신 run 순위에서는 밀릴 수 있다.

이 상태에서 `dbt_test_silver`의
`assert_traffic_current_pinned_publishable_run`은 올바르게 stale Silver를 감지하지만,
정상적인 catch-up supersession까지 DAG failure와 즉시 장애 알림으로 표현한다.

## 보존할 계약

- Asset event에서 선택한 `dag_run_id`는 실행 중 다른 값으로 바꾸지 않는다.
- `assert_traffic_current_pinned_publishable_run`과 freshness 기준을 삭제하거나 완화하지 않는다.
- manifest 조회 실패, pin identity 불일치, 실제 dbt model/test 실패는 fail-closed로 유지한다.
- 오래된 Bronze run은 대체 Silver가 durable해진 뒤에만 COALESCED 처리한다.
- `dbt_run_silver`와 `dbt_test_silver`는 같은 `trino_traffic_heavy` 1-slot 안에서 실행한다.

## 대안

1. freshness 허용 순위를 늘린다. stale Silver를 정상으로 인정하므로 제외한다.
2. 실행 중 pin을 최신 run으로 교체한다. Asset lineage와 immutable identity가 달라지므로 제외한다.
3. 각 snapshot-required dbt phase가 pool slot을 획득한 직후 pin을 재검증한다. 계약을 낮추지 않고 정상 supersession만 skip할 수 있어 채택한다.

## 선택 설계

`require_latest_publishable_incident_snapshot(manifest, run_id)`를 Traffic transform
support에 추가한다. 이 함수는 `latest_publishable_run_id()`를 읽어 다음처럼 동작한다.

- latest가 pin과 같으면 pin을 반환한다.
- latest가 다르면 `AirflowSkipException`으로 현재 transform을 `superseded` 처리한다.
- manifest 조회 실패나 반환 identity가 비정상이면 `AirflowFailException`으로 종료한다.

`traffic_incident_transform.run_dbt_phase`는 `snapshot_required=True`인 phase에만
이 검증 함수를 `pre_execution_guard`로 주입한다. guard는 PythonOperator가
`trino_traffic_heavy` slot을 획득한 뒤, 실제 dbt subprocess를 시작하기 직전에 실행된다.

따라서 다음 두 race를 모두 닫는다.

1. resolver 이후 `dbt_run_silver` 시작 전 최신 Bronze가 publish되면 쓰기 전에 skip한다.
2. Silver write 이후 `dbt_test_silver` 시작 전 최신 Bronze가 publish되면 test를 실패시키지 않고 publish/marker를 skip한다. 최신 Bronze Asset event가 만든 다음 run이 정정한다.

guard와 dbt subprocess가 실행되는 동안에는 동일한 1-slot을 보유하므로 Traffic Bronze가
manifest를 갱신할 수 없다. 별도 lock이나 freshness 완화가 필요하지 않다.

## 검증

- helper가 동일 latest pin을 통과시킨다.
- helper가 newer latest pin을 `AirflowSkipException`으로 분류한다.
- manifest I/O 오류는 `AirflowFailException`으로 유지한다.
- stale pin에서는 dbt executor가 호출되지 않는다.
- current pin에서는 기존 dbt phase가 그대로 실행된다.
- DAG phase selector, task 분리, pool/priority, 다른 도메인 파일은 변경되지 않는다.
