# Traffic Airflow metadata 조회 재시도 Implementation Plan

> **For agentic workers:** 이 계획은 현재 세션에서 테스트 우선으로 순서대로 실행한다. 각 단계는 완료 후 검증 결과를 기록한다.

**Goal:** 일시적인 Airflow metadata 조회 오류를 한 번 재시도하고, 최종 실패는 redacted 진단과 함께 기존 fail-closed 상태로 보존한다.

**Architecture:** ORM 조회 본문을 단일 시도 함수로 분리하고 public collector가 최대 두 번 호출한다. 최종 실패 구조는 바꾸지 않으며, DAG context의 task log URL은 metadata 실패 시에만 안전하게 Discord formatter로 전달한다.

**Tech Stack:** Python 3.11, Airflow ORM session, pytest, `common.security` redaction

## Global Constraints

- 변경 파일은 `domains/traffic/**` 안에만 둔다.
- metadata 조회는 read-only이며 재시도 횟수는 총 두 번으로 고정한다.
- Discord와 report 결과에 예외 원문·credential·connection URL을 넣지 않는다.
- 기존 final failure의 `airflow_metadata_query_failed`와 FAIL 판정은 유지한다.

---

### Task 1: 재시도와 redaction의 RED 테스트

**Files:**
- Modify: `domains/traffic/tests/test_traffic_reliability_report.py`
- Modify: `domains/traffic/tests/test_traffic_reliability_dag.py`

**Interfaces:**
- Produces: `_collect_airflow_scheduled_run_summary_once(dag_id, detected_at, lookback_hours)` seam
- Produces: `build_traffic_reliability_report(airflow_metadata_log_url: str | None = None)`

- [ ] 첫 시도 `RuntimeError` 뒤 두 번째 summary가 반환되는 collector 테스트를 추가한다.
- [ ] 두 번의 secret 포함 예외가 final fallback으로 남고 caplog·report·Discord에서 secret이 사라지는 테스트를 추가한다.
- [ ] DAG context의 task log URL이 metadata 실패 report에만 전달되는 테스트를 추가한다.
- [ ] `python -m pytest -q domains/traffic/tests/test_traffic_reliability_report.py domains/traffic/tests/test_traffic_reliability_dag.py`를 실행해 새 seam이 없어 실패하는지 확인한다.

### Task 2: 최소 재시도·진단 구현

**Files:**
- Modify: `domains/traffic/traffic_ingest/reliability_report.py`
- Modify: `domains/traffic/traffic_reliability_report.py`

**Interfaces:**
- Consumes: Task 1의 단일 시도 seam과 optional task log URL
- Produces: 성공 summary 또는 기존 final fail-closed summary

- [ ] 기존 ORM 본문을 `_collect_airflow_scheduled_run_summary_once`로 이동한다.
- [ ] public collector에 총 두 번의 호출, `refresh_env_secrets`, `scrub_exception`, redacted traceback logging을 추가한다.
- [ ] final fallback에만 redacted `diagnostic_log_url`을 넣고 Discord formatter가 이 위치를 표시하게 한다.
- [ ] `collect_and_notify`가 Airflow context의 현재 task log URL을 builder에 전달하게 한다.
- [ ] Task 1 focused test를 다시 실행해 통과를 확인한다.

### Task 3: 회귀·런타임 검증과 문서화

**Files:**
- Modify: `domains/traffic/docs/superpowers/specs/2026-07-14-traffic-airflow-metadata-retry-design.md`
- Modify: `domains/traffic/docs/superpowers/plans/2026-07-14-traffic-airflow-metadata-retry.md`

- [ ] traffic reliability report와 DAG test 전체를 실행한다.
- [ ] 변경 Python 파일을 `py_compile`하고 `git diff --check`를 실행한다.
- [ ] 로컬 Airflow dev runtime에서 reliability report task를 실행해 read-only metadata summary와 Discord 진단 위치를 확인한다. secret은 출력하지 않는다.
- [ ] runtime 결과와 남은 timeout 범위를 설계 문서에 기록한다.

## 검증 결과 (2026-07-14)

- `python -m pytest -q domains/traffic/tests/test_traffic_reliability_report.py domains/traffic/tests/test_traffic_reliability_dag.py`: 28 passed (로컬 Windows Airflow 지원 경고 1건만 발생).
- `python -m py_compile` 대상 Python 파일 2개와 `git diff --check`를 통과했다.
- 로컬 `airflow-scheduler` 컨테이너에서 Discord 전송 없이 report builder를 실행했다. Airflow metadata summary는 `scheduled_run_count=278`, `airflow_metadata_reason=null`로 조회에 성공했다.
- 전체 report의 기존 상태값은 `FAIL`이었지만 metadata 실패 사유는 없었다. 따라서 이번 검증은 metadata DB 읽기 경로만 판정했으며, retry/fail-closed/secret 마스킹과 redaction 계층 실패 시 synthetic 안전 traceback은 단위 테스트로 각각 확인했다.
