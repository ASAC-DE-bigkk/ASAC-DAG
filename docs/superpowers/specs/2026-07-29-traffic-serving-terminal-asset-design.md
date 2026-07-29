# Traffic Serving Export Terminal Asset 설계

## 목적

Traffic export를 수동 pause/drain 또는 cron이 아니라, Gold write와 contract test를 모두 통과한 terminal asset 뒤에 실행한다.

## 설계

- `traffic_gold_transform`의 `mark_traffic_gold_success` task만 `traffic_gold_publication_ready` asset을 emit한다. 이 task는 Gold run/test 성공 뒤에만 도달한다.
- `traffic_serving_export`는 이 asset을 schedule로 사용하고 `max_active_runs=1`을 유지한다.
- manual `safe-trigger-dag.sh` traffic family에는 `traffic_incident_landing`을 추가한다. 자동 asset-triggered run에는 manual guard를 적용하지 않는다.
- asset event는 Gold run id와 marker identity를 metadata로 남긴다. Publisher ledger의 source run과 연결한다.

## 비범위

- Weather 자동 export, cron fallback, 다른 도메인 변경은 하지 않는다.

## 검증

1. Gold 성공 task만 terminal asset outlet을 가진다.
2. export DAG schedule이 terminal asset이며 manual-only가 아니다.
3. Gold 실패 경로에는 terminal asset emit task가 도달하지 않는다.
4. running/queued landing은 manual safe trigger를 차단한다.
