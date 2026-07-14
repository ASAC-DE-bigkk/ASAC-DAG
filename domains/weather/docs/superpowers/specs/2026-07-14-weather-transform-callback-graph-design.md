# Weather transform callback 그래프 단순화 설계

## 목표

Weather transform DAG의 `ALL_DONE` 메트릭 task와 모든 task에서 이어지는 실패 leaf를 제거해 본 변환 흐름을 단일 직렬 그래프로 유지한다.

## 결정

- `publish_dbt_run_metrics`는 Airflow DAG `on_success_callback`으로 옮긴다.
- Airflow는 DAG callback에 context dict를 단일 positional argument로 전달하므로, 기존 `publish_dbt_run_metrics(**context)` 앞에 context를 풀어 주는 wrapper를 둔다.
- 즉시 Discord 알림과 R2 Problem 기록은 기존 task-level failure callback을 유지한다. DAG-level failure callback은 DagRun이 최종 실패한 뒤에만 실행되므로 즉시 알림 용도로 바꾸지 않는다.
- 성공 callback은 성공한 DagRun에서만 실행되므로 실패한 변환의 메트릭을 적재하지 않는다. 별도 `ALL_DONE` leaf가 없어지면 선형 마지막 task의 failed/upstream_failed 상태가 DagRun 실패로 자연스럽게 집계된다.

## 영향과 검증

- Weather transform DAG·회귀 테스트·Weather 내부 문서만 변경한다.
- Traffic, DBT model, R2, Trino table, prod, backfill에는 변경이나 쓰기가 없다.
- 구조 테스트, callback wrapper 단위 테스트, Python compile, dev의 `airflow tasks test` 성공 경로를 검증한다.
