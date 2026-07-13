# 교통 신뢰성 리포트 Airflow 실패 가시화 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 실제 Airflow scheduled run 실패를 Discord에 표시하고 하나라도 실패하면 report를 FAIL 처리한다.

**Architecture:** 기존 Trino audit/manifest summary와 별도로 Airflow metadata의 `DagRun`·`TaskInstance`를 read-only 조회한다. scheduled run만 24시간 window에 집계하며 실패 run의 최초 failed task와 안전한 오류 요약을 report payload에 넣고 formatter가 출력한다.

**Tech Stack:** Python 3.11, Airflow ORM (`DagRun`, `TaskInstance`, `create_session`), pytest, Discord webhook formatter.

## Global Constraints

- scheduled run만 집계하며 manual/backfill run은 제외한다.
- failed scheduled run이 하나라도 있으면 overall `FAIL`이다.
- exception 전문·webhook·secret을 Discord 또는 test output에 노출하지 않는다.
- metadata query failure도 FAIL이며 기존 Trino 데이터 품질 gate는 보존한다.

---

### Task 1: Airflow scheduled run summary

**Files:**

- Modify: `domains/traffic/traffic_ingest/reliability_report.py:1-263`
- Test: `domains/traffic/tests/test_traffic_reliability_report.py:1-130`

**Interfaces:**

- Produces: `collect_airflow_scheduled_run_summary(dag_id, detected_at, lookback_hours)` returning `{expected, success, failed, running, failures}`.
- `failures` element: `{logical_date, run_id, task_id, reason}`.

- [ ] **Step 1: Write the failing test**

```python
assert summary["expected"] == 288
assert summary["failed"] == 1
assert summary["failures"][0]["task_id"] == "record_seoul_traffic_run_started"
assert summary["failures"][0]["reason"] == "TrinoConnectionError: trino DNS 이름 해석 실패"
```

- [ ] **Step 2: Run it to verify RED**

Run: `python -m pytest domains/traffic/tests/test_traffic_reliability_report.py -q`

Expected: the Airflow summary helper assertion fails.

- [ ] **Step 3: Write minimal implementation**

Use `create_session()` to filter `DagRun` by DAG id, scheduled run type and the UTC lookback window. For a failed run, read the first failed `TaskInstance`; resolve its existing redacted R2 problem document under the corresponding `errors/observed_date/domain=traffic/dag_id=traffic_incident_bronze` prefix and normalize its title/detail to a first-line, length-limited, secret-free reason. If R2 lookup fails, retain the failed task and use `원인 미확인`.

- [ ] **Step 4: Run focused tests to verify GREEN**

Run: `python -m pytest domains/traffic/tests/test_traffic_reliability_report.py -q`

Expected: PASS.

### Task 2: FAIL gate and Discord failure description

**Files:**

- Modify: `domains/traffic/traffic_ingest/reliability_report.py:223-320`
- Test: `domains/traffic/tests/test_traffic_reliability_report.py:40-150`

**Interfaces:**

- Consumes: Task 1 `airflow_runs` summary.
- Produces: `report["airflow_runs"]`, overall `status="FAIL"` on failed count, Discord failure lines.

- [ ] **Step 1: Write the failing test**

```python
assert result["status"] == "FAIL"
assert "스케줄 수집 상태: 283/288 성공, 5 실패" in message
assert "11:30 KST | task=record_seoul_traffic_run_started" in message
assert "TrinoConnectionError" in message
```

- [ ] **Step 2: Run it to verify RED**

Run: `python -m pytest domains/traffic/tests/test_traffic_reliability_report.py -q`

Expected: status or Discord failure assertion fails.

- [ ] **Step 3: Write minimal implementation**

Add `airflow_runs` to the report. Return PASS only when the existing traffic gate passes, the metadata query succeeds, and scheduled `failed == 0`. Format KST timestamps, failure list, and first-to-last failure window.

- [ ] **Step 4: Run focused tests to verify GREEN**

Run: `python -m pytest domains/traffic/tests/test_traffic_reliability_report.py -q`

Expected: PASS.

- [ ] **Step 5: Run regression verification and commit**

Run: `python -m pytest domains/traffic/tests -q; python -m compileall -q domains/traffic/traffic_ingest/reliability_report.py`

Expected: all tests PASS and compile succeeds.

Commit: `git commit -m "fix(traffic): report scheduled run failures"`
