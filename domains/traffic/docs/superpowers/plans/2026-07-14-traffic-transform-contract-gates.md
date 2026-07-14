# Traffic Transform Contract Gates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add exact-set-gated Bronze source and admin-axis seed contract tasks before the Traffic Silver phase.

**Architecture:** Extend the existing `dbt_task`/`run_dbt_phase` path with an optional `dbt ls --output json` exact-set preflight. Add two independent pre-Silver tasks, preserve existing snapshot/failure/artifact behavior, and cover dependency and normalization contracts in the existing Traffic DAG unit test module.

**Tech Stack:** Python 3.11, Airflow `PythonOperator`, dbt CLI, pytest, JSONL.

## Global Constraints

- Modify or create files only under `dags/domains/traffic/**`.
- Keep `dbt_source_freshness`, incident availability, common admin dimension materialization/test, canonical Gold, same-target fresh parse, and stale Gold exclusions intact.
- Use dev target only; do not add full-refresh, backfill, destructive delete, or repair parameterization.
- Bronze source allowlist has exactly 40 `(resource, column, test_name)` tuples.
- Admin-axis seed allowlist has exactly 7 `(resource, column, test_name)` tuples.
- Silver remains the first persisted downstream phase after all gates; all gate failures must block Silver and Gold.

---

### Task 1: Add exact-set normalization tests

**Files:**
- Modify: `dags/domains/traffic/tests/test_traffic_transform_dbt_selection.py`

**Interfaces:**
- Consumes: a dbt `ls --output json` node dictionary.
- Produces: expected behavior for `normalize_dbt_test_tuples(nodes)` and exact-set mismatch errors.

- [ ] **Step 1: Write the failing tests**

Add tests for attached source/seed nodes with `test_metadata.name`, `column_name`, and `test_metadata.kwargs.column_name`; assert the normalized tuple uses the final two attached-node segments as the resource. Add a mismatch test asserting the validation helper raises with expected/actual details.

- [ ] **Step 2: Run the focused tests to verify failure**

Run from `C:\Users\Dell3571\Desktop\Projects\ask-seoul-sample\dags`:

```powershell
python -m pytest domains/traffic/tests/test_traffic_transform_dbt_selection.py -k "normalize_dbt_test or exact_test_set" -q
```

Expected: FAIL because the normalization and exact-set helpers do not exist.

- [ ] **Step 3: Implement the minimal normalization helpers**

Add a helper that converts each JSON node into `(resource, column, test_name)` and a helper that compares `actual` and `expected` sets, raising `AirflowFailException` on drift.

- [ ] **Step 4: Run the focused tests to verify success**

Run the same pytest command and expect PASS.

### Task 2: Add the source and seed allowlists and preflight execution

**Files:**
- Modify: `dags/domains/traffic/traffic_incident_transform.py`
- Test: `dags/domains/traffic/tests/test_traffic_transform_dbt_selection.py`

**Interfaces:**
- Consumes: `normalize_dbt_test_tuples` and exact-set validation from Task 1.
- Produces: `dbt_task(..., contract_selector=..., expected_test_tuples=...)` and preflight validation inside `run_dbt_phase`.

- [ ] **Step 1: Write the failing task contract tests**

Assert that the source gate uses `test --select source:traffic_bronze`, the seed gate selects the three explicit `asac_axes` seeds, and their constants contain exactly 40 and 7 tuples respectively. Add a subprocess-output test proving `dbt ls` is invoked before the test command for a contract task.

- [ ] **Step 2: Run the focused tests to verify failure**

```powershell
python -m pytest domains/traffic/tests/test_traffic_transform_dbt_selection.py -k "source_contract or seed_contract or dbt_ls" -q
```

Expected: FAIL because the new task IDs, constants, and contract preflight arguments do not exist.

- [ ] **Step 3: Implement the minimal contract preflight**

Add the 40 source tuples and 7 seed tuples, optional `contract_selector`/`expected_test_tuples` arguments to `dbt_task` and `run_dbt_phase`, and execute `dbt ls --resource-type test --select <selector> --output json` with the same target, vars, and isolated target path before the actual dbt test. Fail before the test command when the exact set differs.

- [ ] **Step 4: Run the focused tests to verify success**

Run the same pytest command and expect PASS.

### Task 3: Wire both gates into the Traffic DAG

**Files:**
- Modify: `dags/domains/traffic/traffic_incident_transform.py`
- Test: `dags/domains/traffic/tests/test_traffic_transform_dbt_selection.py`

**Interfaces:**
- Consumes: source/seed contract tasks from Task 2.
- Produces: DAG order `availability -> source contract -> seed -> dim run/test -> seed contract -> Silver` and watcher coverage.

- [ ] **Step 1: Write the failing dependency tests**

Extend the existing expected order and watcher task set with `dbt_test_traffic_bronze_source_contract` and `dbt_test_asac_axes_seed_contract`. Assert every adjacent task points to the next task and failure watcher, and assert no Silver or Gold task is upstream of either gate.

- [ ] **Step 2: Run the focused tests to verify failure**

```powershell
python -m pytest domains/traffic/tests/test_traffic_transform_dbt_selection.py -k "bootstraps_asac_axes or failure_propagation" -q
```

Expected: FAIL because the new tasks are not present in the DAG chain.

- [ ] **Step 3: Implement the DAG wiring**

Add both task IDs to `DBT_PHASE_TASK_IDS`, define the two `dbt_task` instances, insert them at the pre-Silver positions, and include them in the failure-propagating watcher loop. Do not alter existing Gold selectors or fresh-parse flags.

- [ ] **Step 4: Run all Traffic selector tests**

```powershell
python -m pytest domains/traffic/tests/test_traffic_transform_dbt_selection.py -q
```

Expected: PASS.

### Task 4: Document operations and validate the branch

**Files:**
- Modify: `dags/domains/traffic/docs/dbt_contracts.md`

**Interfaces:**
- Consumes: final task IDs and selectors from Tasks 2–3.
- Produces: rerun procedure for selector drift, source failure, and seed failure without full-refresh/backfill.

- [ ] **Step 1: Add the failure/retry runbook**

Document the two task IDs, exact selectors, dev-only rerun behavior, and the rule that a gate failure must be fixed or the corresponding dbt test rerun before Silver/Gold is considered valid.

- [ ] **Step 2: Run static and focused validation**

```powershell
python -m compileall domains/traffic/traffic_incident_transform.py
python -m pytest domains/traffic/tests/test_traffic_transform_dbt_selection.py -q
git diff --check
```

Expected: compile succeeds, all selector tests pass, and diff check is clean.

- [ ] **Step 3: Run the broader Traffic test suite**

```powershell
python -m pytest domains/traffic/tests -q
```

Expected: PASS, or any pre-existing/environment-only failure is recorded explicitly without weakening the new contract tests.

- [ ] **Step 4: Run dev dbt/DAG validation when the runtime is available**

Run Python DAG import and the dev dbt parse/ls/test smoke using the repository’s configured Airflow/dbt runtime. Record command results, task order, selected tuple counts, and any runtime limitation in the PR body.

- [ ] **Step 5: Commit the ASAC-DAG changes**

```powershell
git add domains/traffic/traffic_incident_transform.py domains/traffic/tests/test_traffic_transform_dbt_selection.py domains/traffic/docs
git commit -m "feat(traffic): gate transform on Bronze and axis contracts (#266)"
```
