# Weather-owned Cross-domain Contracts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `domains/_shared` by moving the unchanged Weather·Traffic manifest contract and single Iceberg maintenance DAG under Weather ownership.

**Architecture:** Weather exposes `weather.bronze_run_manifest` as the single publish-contract implementation consumed by both pipelines. The existing maintenance helper moves into `weather_ingest`, while one Weather-owned DAG keeps the existing DAG ID, schedule, table list, and sequential execution behavior.

**Tech Stack:** Python 3.11, Airflow 3, Trino DB-API, pytest, GitHub CLI

## Global Constraints

- Only `domains/weather/**`, `domains/traffic/**`, and deletion of `domains/_shared/**` may change.
- Do not create or modify `AGENTS.md` or `LessonRun.md`.
- Preserve manifest SQL, schema, constants, idempotency, and failure-reason behavior byte-for-byte except for ownership documentation.
- Preserve maintenance DAG ID, schedule, retries, table order, and maintenance operation order.
- Do not split the maintenance DAG or add Trino concurrency.
- Do not request a named reviewer; 돌발정보와 기상청은 같은 담당 범위다.

---

### Task 1: Establish failing manifest import tests

**Files:**
- Modify: `domains/weather/tests/test_weather_run_manifest.py`
- Modify: `domains/traffic/tests/test_traffic_run_manifest.py`

**Interfaces:**
- Consumes: the current `_shared.bronze_run_manifest` behavior asserted by both tests
- Produces: test imports for `weather.bronze_run_manifest`

- [ ] **Step 1: Change only the test imports**

```python
from weather.bronze_run_manifest import STATUS_SUCCESS, record_bronze_run_event
from weather.bronze_run_manifest import failure_reason_from_context
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_run_manifest.py domains/traffic/tests/test_traffic_run_manifest.py -q
```

Expected: collection fails with `ModuleNotFoundError: No module named 'weather.bronze_run_manifest'`.

### Task 2: Move the manifest implementation and consumers

**Files:**
- Create: `domains/weather/bronze_run_manifest.py`
- Modify: `domains/weather/weather_vilage_fcst_bronze.py`
- Modify: `domains/traffic/traffic_incident_bronze.py`
- Modify: `domains/traffic/traffic_incident_transform.py`
- Delete: `domains/_shared/bronze_run_manifest.py`

**Interfaces:**
- Consumes: `MANIFEST_TABLE`, status constants, `record_bronze_run_event`, and `failure_reason_from_context` from the existing module
- Produces: the same names from `weather.bronze_run_manifest`

- [ ] **Step 1: Add the Weather-owned module with unchanged implementation**

Use the exact content of `domains/_shared/bronze_run_manifest.py` at `domains/weather/bronze_run_manifest.py`. No SQL, signature, constant, or error-handling changes are allowed.

- [ ] **Step 2: Update all three production imports**

```python
from weather.bronze_run_manifest import (
    STATUS_FAILED,
    STATUS_STARTED,
    STATUS_SUCCESS,
    failure_reason_from_context,
    record_bronze_run_event,
)
```

The transform keeps its compact form:

```python
from weather.bronze_run_manifest import MANIFEST_TABLE, STATUS_SUCCESS
```

- [ ] **Step 3: Delete the old manifest module and verify GREEN**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_run_manifest.py domains/traffic/tests/test_traffic_run_manifest.py -q
```

Expected: `2 passed`.

- [ ] **Step 4: Verify no manifest consumer references `_shared`**

Run:

```powershell
rg -n "_shared\.bronze_run_manifest" domains
```

Expected: no matches.

### Task 3: Establish a failing Weather-owned maintenance DAG test

**Files:**
- Create: `domains/weather/tests/test_weather_iceberg_maintenance_dag.py`
- Delete: `domains/_shared/tests/test_iceberg_maintenance_dag.py`

**Interfaces:**
- Consumes: existing FakeDAG/FakePythonOperator test harness and assertions
- Produces: the same assertions loading `domains/weather/weather_iceberg_maintenance.py`

- [ ] **Step 1: Move the existing test and change the module path**

```python
module_path = Path(__file__).resolve().parents[1] / "weather_iceberg_maintenance.py"
spec = importlib.util.spec_from_file_location(
    "weather_iceberg_maintenance_under_test",
    module_path,
)
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_iceberg_maintenance_dag.py -q
```

Expected: both tests fail because `weather_iceberg_maintenance.py` does not exist.

### Task 4: Move the maintenance helper and DAG

**Files:**
- Create: `domains/weather/weather_ingest/iceberg_maintenance.py`
- Create: `domains/weather/weather_iceberg_maintenance.py`
- Delete: `domains/_shared/maintenance.py`
- Delete: `domains/_shared/iceberg_maintenance_dag.py`
- Delete: `domains/_shared/__init__.py`
- Delete: `domains/_shared/tests/` after its test is moved

**Interfaces:**
- Consumes: `_normalize_tables` and `run_maintenance` behavior from the existing helper
- Produces: `weather_ingest.iceberg_maintenance._normalize_tables` and `run_maintenance`; Airflow DAG `ask_seoul_iceberg_maintenance`

- [ ] **Step 1: Add the helper with unchanged executable behavior**

Copy the implementation from `domains/_shared/maintenance.py` to `domains/weather/weather_ingest/iceberg_maintenance.py`. Only the module docstring may change to state Weather ownership.

- [ ] **Step 2: Add the Weather-owned DAG with its existing configuration**

Copy the DAG body from `domains/_shared/iceberg_maintenance_dag.py`, rename the file, and use:

```python
from weather_ingest.iceberg_maintenance import run_maintenance, _normalize_tables
```

Keep `DEFAULT_PARAMS`, `_maintain`, `_default_schedule`, DAG ID, default args, tags, and operator configuration unchanged.

- [ ] **Step 3: Delete the remaining `_shared` files and verify GREEN**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_iceberg_maintenance_dag.py -q
```

Expected: `2 passed`.

- [ ] **Step 4: Verify `_shared` is gone**

Run:

```powershell
rg --files domains/_shared
```

Expected: the path does not exist and no files are returned.

### Task 5: Full regression and boundary verification

**Files:**
- Verify only; no new files

**Interfaces:**
- Consumes: all changed Weather·Traffic modules
- Produces: verification evidence for issue #331 and the PR body

- [ ] **Step 1: Run all relevant tests**

Because this is an explicitly approved cross-domain branch, neutralize only the base-diff input of the unchanged Weather-only harness:

```powershell
$env:WEATHER_DOMAIN_BASE_REF='HEAD'
python -m pytest domains/weather/tests domains/traffic/tests -q
Remove-Item Env:WEATHER_DOMAIN_BASE_REF
```

Expected: all tests pass; the Windows Airflow platform warning and existing Airflow atexit warning may remain.

- [ ] **Step 2: Compile changed Python modules**

```powershell
python -m py_compile domains/weather/bronze_run_manifest.py domains/weather/weather_iceberg_maintenance.py domains/weather/weather_ingest/iceberg_maintenance.py domains/weather/weather_vilage_fcst_bronze.py domains/traffic/traffic_incident_bronze.py domains/traffic/traffic_incident_transform.py
```

Expected: exit code 0.

- [ ] **Step 3: Verify import paths and allowed changed paths**

```powershell
rg -n "_shared" domains/weather domains/traffic
git diff --name-only origin/dev...HEAD
git diff --check origin/dev...HEAD
```

Expected: no runtime `_shared` references; changed paths are limited to Weather, Traffic, and `_shared` deletion; diff check exits 0.

- [ ] **Step 4: Commit implementation**

```powershell
git add domains/weather domains/traffic domains/_shared
git commit -m "refactor(weather): own cross-domain runtime contracts (#331)"
```

### Task 6: Publish the cross-domain PR

**Files:**
- No repository file changes

**Interfaces:**
- Consumes: verified branch commits
- Produces: ready-for-review ASAC-DAG PR targeting `dev`

- [ ] **Step 1: Push the issue branch**

```powershell
git push -u origin feat/331-weather-owned-shared-contracts
```

- [ ] **Step 2: Create a UTF-8 PR body from the shared template**

The body must link `Refs #331`, list the exact changed paths and preserved contracts, include test/compile/import evidence, state that 돌발정보와 기상청 share one owner, and confirm that no named reviewer is requested.

- [ ] **Step 3: Open a ready PR to `dev` and verify its body**

```powershell
gh pr create --repo ASAC-DE-bigkk/ASAC-DAG --base dev --head feat/331-weather-owned-shared-contracts --title "refactor(weather): own Weather·Traffic shared runtime contracts" --body-file domains/weather/.pr-331-body.md
gh pr view --repo ASAC-DE-bigkk/ASAC-DAG --json number,title,body,state,baseRefName,headRefName,url,reviewRequests
```

Expected: open ready PR, base `dev`, no requested reviewers, UTF-8 body intact.
