# Traffic reliability 실행 원장 구현 계획

> 이 계획은 #380의 구현과 검증 순서다. 모든 production 변경은 RED 테스트를 확인한 뒤 적용한다.

**목표:** Airflow 3 ORM 의존성을 제거하고 R2 run lifecycle ledger로 scheduled Bronze 상태를 판정한다.

**구조:** `traffic_ingest.run_ledger`가 write/read 계약을 소유하고, Bronze DAG는 lifecycle callback adapter만 가진다. reliability report는 ledger summary와 Trino manifest summary를 조합한다.

## Task 1 — 원장 계약 RED 테스트

**Files:**
- Add: `domains/traffic/tests/test_traffic_run_ledger.py`
- Modify: `domains/traffic/tests/test_traffic_bronze_module_wiring.py`

- fake storage로 결정적 key, 중복 `FAILED`, scheduled success/failure/running/stalled/missing/bootstrap summary를 검증한다.
- Bronze DAG가 runtime validation 전에 STARTED를 기록하고 terminal callbacks를 연결하는지 검증한다.
- `python -m pytest -q domains/traffic/tests/test_traffic_run_ledger.py domains/traffic/tests/test_traffic_bronze_module_wiring.py`가 구현 전 실패하는지 확인한다.

## Task 2 — 원장과 Bronze lifecycle 구현

**Files:**
- Add: `domains/traffic/traffic_ingest/run_ledger.py`
- Modify: `domains/traffic/traffic_ingest/bronze_dag_support.py`
- Modify: `domains/traffic/traffic_incident_bronze.py`

- R2 ledger의 write/read, safe key, redacted failure reason, bootstrap/stale/missing summary를 구현한다.
- DAG 첫 task에서 best-effort STARTED를 쓰고 asset publish 후 SUCCESS를 쓴다.
- failure callback이 ledger FAILED와 기존 manifest failure를 모두 기록하도록 한다.
- Task 1의 focused test를 다시 실행한다.

## Task 3 — ORM 제거와 report/Discord 정합성

**Files:**
- Delete: `domains/traffic/traffic_ingest/reliability/airflow_evidence.py`
- Modify: `domains/traffic/traffic_ingest/reliability/config.py`
- Modify: `domains/traffic/traffic_ingest/reliability/report.py`
- Modify: `domains/traffic/traffic_ingest/reliability/discord.py`
- Modify: `domains/traffic/traffic_ingest/reliability/trino_repository.py`
- Modify: `domains/traffic/traffic_ingest/reliability_report.py`
- Modify: `domains/traffic/traffic_reliability_report.py`
- Modify: reliability report tests and architecture test

- scheduled status는 ledger summary로 대체하고 ORM/`DagRun`/`create_session` import를 제거한다.
- `STARTED`가 최신이어도 최신 terminal publishable manifest가 SUCCESS이면 publishability PASS를 유지한다.
- fingerprint는 terminal manifest와 ledger failure identity만 포함해 실행 중 run id 변화로 재알림하지 않는다.
- focused reliability tests를 실행한다.

## Task 4 — 검증

1. `python -m pytest -q domains/traffic/tests/test_traffic_run_ledger.py domains/traffic/tests/test_traffic_reliability_*.py domains/traffic/tests/test_traffic_bronze_module_wiring.py`
2. 변경 Python 파일 `py_compile`, `git diff --check`
3. Airflow container DAG import와 local dev report task smoke (Discord 발송 없이)
4. R2 dev ledger object와 report 결과를 확인하고 `LessonRun.md`에 run id/task/row count/object/table을 기록한다.
