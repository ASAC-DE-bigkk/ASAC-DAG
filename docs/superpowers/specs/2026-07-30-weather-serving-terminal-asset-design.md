# Weather D1 게시 Terminal Asset 설계

## 목적

Weather D1 4종 게시를 수동 실행이 아니라, 해당 제품을 만드는 Gold write와 계약 테스트가 모두 성공한 뒤 자동 실행한다.

## 설계

- `weather_vilage_fcst_transform`의 `dbt_test_gold` 뒤에 정상 경로 전용 `mark_weather_gold_publication_ready` task를 둔다.
- 이 task만 `iceberg://weather/gold/publication-ready` Asset을 발행한다.
- Asset metadata에는 upstream Gold DAG run ID와 그 run이 pin한 Bronze DAG run ID를 기록한다.
- `weather_serving_export`는 위 Asset만 schedule로 구독하며 공통 `build_serving_export_dag`를 그대로 사용한다.
- metrics teardown은 성공 여부와 무관하게 실행될 수 있으므로 Asset을 발행하지 않는다.
- `weather_w2_canonical_transform`은 D1 4종의 terminal publisher가 아니므로 Asset을 발행하지 않는다.
- D1 snapshot 교체, read-back, `_catalog`, API smoke, rollback, `_publication_ledger`는 기존 공통 Publisher 책임으로 유지한다.

## 실패·중복 처리

- Gold write 또는 `dbt_test_gold`가 실패하면 marker task에 도달하지 않아 D1 export가 실행되지 않는다.
- `weather_serving_export`의 `max_active_runs=1`을 유지해 게시 작업을 직렬화한다.
- 이 제품은 Gold run별 exactly-once 게시가 아니라, 트리거 시점의 최신 검증 Gold 상태를 D1 snapshot으로 게시한다.
- 여러 Gold Asset event가 짧은 시간에 도착하면 Airflow가 event를 합치거나 동일 최신 Gold 상태를 다시 게시할 수 있다. 이 경우에도 snapshot 교체이므로 제품 행이 append 중복되지 않으며 D1 Publisher의 last-known-good 및 rollback 계약을 변경하지 않는다.
- `_publication_ledger.source_run_id`는 기존 공통 계약대로 serving export DAG run ID를 사용한다. upstream Gold run ID와 Bronze run ID는 Airflow Asset event metadata에서 추적한다.
- 기존 수동 trigger 가능성은 유지하되 정규 운영 트리거는 terminal Asset으로 제한한다.

## 운영 전환

- 현재 Weather recollect/backfill이 끝나기 전에는 `weather_serving_export`를 pause 상태로 유지한다.
- 마지막 backlog Bronze와 downstream Gold 검증이 성공한 뒤 배포·unpause한다.
- 최초 asset-triggered run에서 Weather 4종의 source/D1 row count, `_catalog`, `_publication_ledger`, API smoke를 확인한다.

## 비범위

- Traffic 및 다른 도메인 변경
- Weather W2 canonical transform 구조 변경
- 공통 D1 Publisher 정책 변경
- Gold run 단위 exactly-once/idempotency key 도입
- `.airflowignore` 변경

## 완료 검증

1. `weather_vilage_fcst_transform`의 Gold 계약 테스트 성공 task 뒤에만 terminal Asset outlet이 존재한다.
2. metrics teardown과 W2 canonical DAG에는 terminal Asset outlet이 없다.
3. `weather_serving_export` schedule이 `iceberg://weather/gold/publication-ready` Asset이다.
4. Weather DAG 단위 테스트와 Airflow import 검증이 통과한다.
5. prod canary에서 Weather D1 4종의 row count·`_catalog`·`_publication_ledger`가 일치한다.
