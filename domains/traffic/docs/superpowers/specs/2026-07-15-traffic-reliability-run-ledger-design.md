# Traffic reliability 실행 원장 설계

## 배경

`traffic_bronze_reliability_report`가 Airflow metadata ORM을 직접 읽어 Airflow 3에서 항상 실패했다. 또한 Trino manifest의 최신 `STARTED` 이벤트를 publication failure로 해석해, 정상 Bronze run이 실행 중인 동안 Discord 경보가 반복됐다.

## 목표

- task 코드에서 Airflow metadata ORM을 완전히 제거한다.
- R2에 run lifecycle `STARTED`/`SUCCESS`/`FAILED`를 기록해 Trino 장애 중에도 terminal failure 근거를 보존한다.
- publishability는 최신 terminal manifest(`SUCCESS` 또는 `FAILED`)로만 판정한다. 최근 `STARTED`는 진행 중으로만 집계한다.
- schedule slot이 원장 시작 이후 stale window를 넘겨 누락되거나 `STARTED` 상태로 멈추면 실패로 보고한다.

## 경계와 저장 계약

`traffic_ingest.run_ledger.TrafficRunLedger`가 R2 저장·조회와 key/JSON 계약을 단독 소유한다.

```text
traffic-run-ledger/observed_date=YYYY-MM-DD/dag_id=<dag_id>/
  <safe-run-id>__STARTED.json
  <safe-run-id>__SUCCESS.json
  <safe-run-id>__FAILED.json
```

각 document는 `dag_id`, 원본 `run_id`, `logical_date`, `status`, `event_at`, `task_id`, `failure_reason`을 가진다. 실패 사유는 예외 메시지가 아닌 예외 타입과 task id만 기록한다. 상태별 key는 결정적이므로 task callback과 DAG callback의 중복 실패 기록은 멱등이다.

## lifecycle

1. Bronze DAG의 첫 task가 best-effort로 `STARTED`를 쓴 뒤 runtime validation을 수행한다.
2. asset publish가 끝나면 best-effort로 `SUCCESS`를 쓴다.
3. task/DAG failure callback은 먼저 best-effort `FAILED`를 쓰고, 기존 Trino manifest failure 및 Problem document 흐름은 유지한다.
4. reliability report는 R2 ledger로 scheduled run success/failed/running/missing/stalled를 계산하고, Trino manifest로 데이터 publishability를 별도로 계산한다.

## bootstrap 및 실패 정책

새 원장은 release 이전의 run을 갖지 않는다. lookback 안에 첫 scheduled ledger event가 없으면 report는 `run_ledger_bootstrapping` 상태의 `WARN`을 내며 성공으로 위장하지 않는다. 첫 event 이후에는 schedule interval과 stale window를 기준으로 missing slot을 계산한다. R2 조회 자체가 실패하면 `run_ledger_query_failed`로 fail-closed 한다.

## 비목표

- Airflow REST API/ORM 도입
- weather 또는 공용 모듈 수정
- Trino manifest schema 변경
- 기존 R2 Problem document 및 task-level Discord 실패 알림 제거
