# Reliability Latest-Run and Notification Delivery Plan

> **For Codex:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task by task.

**Goal:** Make Weather and Traffic reliability publishability depend only on the newest target DAG run, and make Discord deduplication retry-safe with stable failure fingerprints.

**Architecture:** Preserve the existing per-`dag_run_id` latest-event CTE, then select one newest run from that reduced set and carry its identity, status, publishable flag, and event time together into each report. In each reliability DAG, derive a deterministic JSON fingerprint from status and stable failure identity only, compare it to the last successfully delivered fingerprint, and persist it only after Discord confirms delivery.

**Tech Stack:** Python 3, Airflow DAG modules, Trino SQL, pytest.

**Issue:** ASAC-DAG #324

---

### Task 1: Lock the latest-run publishability contract with failing tests

**Files:**
- Modify: `domains/weather/tests/test_weather_reliability_report.py`
- Modify: `domains/traffic/tests/test_traffic_reliability_report.py`

**Step 1: Add Weather RED coverage**

Extend the manifest cursor row to return both historical aggregates and one coherent newest-run tuple:

```python
(
    success,
    failed,
    running,
    expected_raw_objects,
    actual_raw_objects,
    last_success_at,
    last_publishable_at,
    latest_dag_run_id,
    latest_status,
    latest_is_publishable,
    latest_event_at,
)
```

Add a report test where an older run is successful/publishable but the newest run is failed or non-publishable. Assert `publishability_ok is False` and assert all four `latest_*` fields identify that same newest run.

**Step 2: Add equivalent Traffic RED coverage**

Use the Traffic tuple without raw-object totals. Preserve the Airflow metadata stub and assert the same newest-run-only rule.

**Step 3: Run the focused report tests and observe RED**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_reliability_report.py domains/traffic/tests/test_traffic_reliability_report.py -q
```

Expected: FAIL because the collectors do not return a coherent newest-run tuple and report publishability still borrows historical `last_publishable_at`.

### Task 2: Implement newest-run-only publishability

**Files:**
- Modify: `domains/weather/weather_ingest/reliability_report.py`
- Modify: `domains/traffic/traffic_ingest/reliability_report.py`

**Step 1: Extend each manifest aggregation minimally**

Keep the existing `latest` CTE grouped by `dag_run_id`. In the outer aggregate, select newest-run fields with the same deterministic event-time/run-id ordering:

```sql
max_by(dag_run_id, ROW(latest_event_at, dag_run_id)) AS latest_dag_run_id,
max_by(latest_status, ROW(latest_event_at, dag_run_id)) AS latest_status,
max_by(latest_is_publishable, ROW(latest_event_at, dag_run_id)) AS latest_is_publishable,
max_by(latest_event_at, ROW(latest_event_at, dag_run_id)) AS latest_event_at
```

Map these columns into `latest_dag_run_id`, `latest_status`, `latest_is_publishable`, and `latest_event_at` summary keys. Retain existing lookback counts and historical last-success timestamps for observability.

**Step 2: Derive publishability from the coherent newest-run tuple**

Use:

```python
publishability_ok = (
    dag_runs.get("latest_status") == "SUCCESS"
    and dag_runs.get("latest_is_publishable") is True
)
```

Do not fall back to `last_publishable_at`. Preserve query-failure FAIL behavior and Traffic's Airflow failure precedence.

**Step 3: Run the focused report tests and observe GREEN**

Run the Task 1 pytest command. Expected: PASS.

### Task 3: Lock stable, delivery-aware notification behavior with failing tests

**Files:**
- Modify: `domains/weather/tests/test_weather_reliability_dag.py`
- Modify: `domains/traffic/tests/test_traffic_reliability_dag.py`

**Step 1: Add fingerprint stability tests**

For each domain, construct two reports with identical status and failure identity but different `detected_at`, freshness ages, collection times, and newest event timestamps. Assert their fingerprints are equal.

Construct `FAIL(A)` and `FAIL(B)` reports whose stable failure identities differ (for example newest run ID/status or Traffic failed task identity). Assert fingerprints differ and the second failure is notifyable after the first was delivered.

**Step 2: Add delivery ordering and retry tests**

For each `collect_and_notify` function, inject report/formatter/sender/state functions and assert:

- successful send occurs before the delivered fingerprint is written;
- `send_discord_message(...) is False` does not write state, so the next invocation retries;
- a sender exception does not write state, so the next invocation retries;
- unchanged delivered fingerprint suppresses the send;
- Variable read and write exceptions remain fail-open.

**Step 3: Run the focused DAG tests and observe RED**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_reliability_dag.py domains/traffic/tests/test_traffic_reliability_dag.py -q
```

Expected: FAIL because status-only comparison writes Variable state before delivery and cannot distinguish stable failure identities.

### Task 4: Implement stable fingerprints and post-delivery persistence

**Files:**
- Modify: `domains/weather/weather_reliability_report.py`
- Modify: `domains/traffic/traffic_reliability_report.py`

**Step 1: Build a deterministic failure identity**

Serialize a small domain-specific mapping with `json.dumps(..., sort_keys=True, separators=(",", ":"))`. Include report status and stable reasons/identifiers such as domain reason/status, newest manifest run identity/status/publishable flag, and Traffic Airflow failed run/task/reason tuples. Exclude volatile timestamps, ages, counters, and collection times.

**Step 2: Separate decision from persistence**

Read the last delivered fingerprint in a fail-open helper. Add a separate fail-open write helper. Do not write during the decision.

**Step 3: Persist only confirmed delivery**

In `collect_and_notify`, send only when the fingerprint differs. Catch sender exceptions as failed delivery while allowing formatter defects to remain task-visible. Record the fingerprint only when the sender returns exactly `True`; otherwise leave state untouched for retry.

**Step 4: Run the focused DAG tests and observe GREEN**

Run the Task 3 pytest command. Expected: PASS.

### Task 5: Synchronize operator documentation

**Files:**
- Modify if stale: `domains/weather/README.md`
- Modify if stale: `domains/traffic/README.md`

**Step 1: Correct schedules only where stale**

Document Weather's default dev webhook-backed schedule as hourly (`0 * * * *`) and Traffic's as every 15 minutes (`*/15 * * * *`). Retain the existing dev-only and environment override rules.

### Task 6: Verify the bounded change

**Files:**
- Test: `domains/weather/tests/**`
- Test: `domains/traffic/tests/**`

**Step 1: Run reliability suites**

```powershell
python -m pytest domains/weather/tests/test_weather_reliability_report.py domains/weather/tests/test_weather_reliability_dag.py domains/traffic/tests/test_traffic_reliability_report.py domains/traffic/tests/test_traffic_reliability_dag.py -q
```

**Step 2: Compile affected Python modules**

```powershell
python -m py_compile domains/weather/weather_ingest/reliability_report.py domains/weather/weather_reliability_report.py domains/traffic/traffic_ingest/reliability_report.py domains/traffic/traffic_reliability_report.py
```

The fake-Airflow DAG tests serve as the local DAG import check. If a configured Airflow runtime is available, also run its DAG import-error command.

**Step 3: Run boundary and repository hygiene checks**

```powershell
python -m pytest domains/weather/tests/test_domain_boundary.py -q
python domains/weather/domain_boundary.py --base-ref origin/dev
git diff --check
git status --short
```

Confirm every changed path is under `domains/weather/**` or `domains/traffic/**`.

**Step 4: Request code review and address only verified findings**

Review correctness against ASAC-DAG #324, with special attention to coherent newest-run selection, timestamp exclusion, retry behavior, and fail-open Variable access. Re-run affected RED/GREEN tests after any correction.

**Step 5: Commit the verified bounded change**

```powershell
git add domains/weather domains/traffic
git commit -m "fix: harden reliability latest-run alerts (#324)"
```

Do not push or open a pull request.
