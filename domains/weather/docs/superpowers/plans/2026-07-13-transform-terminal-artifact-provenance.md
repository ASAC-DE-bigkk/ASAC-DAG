# Transform Terminal Failure and Artifact Provenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent Weather and Traffic dbt failures from being hidden by the `ALL_DONE` metrics leaf, and make Traffic metrics consume only an isolated `run_results.json` produced by the current DAG run.

**Architecture:** Keep the existing metrics task as an independent `ALL_DONE` leaf so observability still runs after dbt failures. Add a second, failure-propagating leaf with `ONE_FAILED` that is directly downstream of every transform task and always raises when scheduled; successful runs leave it skipped/upstream-finished, while any transform failure schedules it and leaves the DAG run failed. For Traffic, resolve artifacts by scanning all dbt phases from latest to earliest, accepting only current-run XCom paths and returning no artifact when none exists; never fall back to the shared dbt `target/run_results.json`.

**Tech Stack:** Python 3, Apache Airflow `PythonOperator` and `TriggerRule`, dbt `run_results.json`, pytest.

## Global Constraints

- Modify only `domains/weather/**` and `domains/traffic/**` in the ASAC-DAG worktree.
- Preserve the metrics tasks' existing `TriggerRule.ALL_DONE` contract and existing dbt command/target-path behavior.
- The failure-propagation task is state-only: it must not replace metrics collection, rerun dbt, or create an additional domain failure notification.
- Traffic artifact resolution must use only current-run XCom values emitted by `run_dbt_phase`; a missing artifact is an explicit skipped metric, never permission to read the shared target.
- Follow strict RED-GREEN-REFACTOR: tests are edited and observed failing before either production DAG file is changed.
- Do not commit secrets, generated dbt targets, pytest caches, bytecode, or Airflow runtime files.

---

## File Structure

| File | Action | Responsibility |
| --- | --- | --- |
| `domains/weather/tests/test_weather_transform_dbt_selection.py` | Modify | Prove metrics remains `ALL_DONE`, the independent watcher is a leaf with `ONE_FAILED`, all transform tasks feed it, and its callable fails when triggered. |
| `domains/weather/weather_vilage_fcst_transform.py` | Modify | Add the Weather failure-propagation callable and leaf without changing dbt or metrics behavior. |
| `domains/traffic/tests/test_traffic_transform_dbt_selection.py` | Modify | Prove Traffic leaf semantics and current-run artifact selection across successful and failed dbt phases, including stale shared artifact rejection. |
| `domains/traffic/traffic_incident_transform.py` | Modify | Add the Traffic failure leaf and replace terminal/shared fallback resolution with all-phase current-run resolution. |
| `domains/weather/docs/superpowers/plans/2026-07-13-transform-terminal-artifact-provenance.md` | Create | Record the approved implementation and verification boundary. |

## Task 1: Lock the Weather Failure-Leaf Contract with RED Tests

**Files:**

- Modify: `domains/weather/tests/test_weather_transform_dbt_selection.py`

**Interfaces:**

- Existing task: `publish_dbt_run_metrics` with `TriggerRule.ALL_DONE`.
- New task: `fail_transform_if_upstream_failed` with `TriggerRule.ONE_FAILED`.
- New callable: `fail_transform_if_upstream_failed()`; it always raises when Airflow schedules it.

- [x] **Step 1: Extend the fake Airflow graph objects to retain upstream task IDs and expose `ONE_FAILED`.**

  Keep current downstream assertions working. Record both sides of `task_a >> task_b`, because the watcher contract depends on every transform task being a direct upstream.

- [x] **Step 2: Add a graph test for the two independent leaves.**

  Assert that:

  - `publish_dbt_run_metrics` still has `trigger_rule == "all_done"`;
  - `fail_transform_if_upstream_failed` has `trigger_rule == "one_failed"` and no downstream task;
  - the metrics task and watcher are separate DAG leaves;
  - every task from `validate_dev_runtime` through `dbt_test_place_mart` is a direct watcher upstream;
  - the normal terminal dbt task still feeds metrics.

- [x] **Step 3: Add a callable test proving the watcher cannot succeed when scheduled.**

  Call `fail_transform_if_upstream_failed()` directly and assert it raises the chosen Airflow failure exception.

- [x] **Step 4: Run the Weather test file and observe RED.**

  Run:

  ```powershell
  $env:PYTHONDONTWRITEBYTECODE='1'
  python -m pytest -p no:cacheprovider domains/weather/tests/test_weather_transform_dbt_selection.py -q
  ```

  Expected failure: the watcher callable/task and `ONE_FAILED` trigger rule do not exist yet.

## Task 2: Implement the Minimal Weather Failure Leaf

**Files:**

- Modify: `domains/weather/weather_vilage_fcst_transform.py`

**Interfaces:**

- Add: `fail_transform_if_upstream_failed()`.
- Add DAG task: `fail_transform_if_upstream_failed` (`PythonOperator`, `TriggerRule.ONE_FAILED`, `retries=0`).

- [x] **Step 1: Add an unconditional failure callable.**

  Raise an explicit Airflow failure exception with a concise message. Do not perform logging, metrics publication, or notifications in this callable.

- [x] **Step 2: Add the independent watcher leaf.**

  Instantiate it beside the metrics task. Keep `publish_dbt_run_metrics` unchanged as `ALL_DONE`.

- [x] **Step 3: Wire every transform task directly to the watcher.**

  Preserve the existing linear transform chain and terminal-to-metrics edge. Add direct watcher edges without placing metrics upstream or downstream of the watcher.

- [x] **Step 4: Run the Weather test file and observe GREEN.**

  Run the Task 1 command. Expected result: all Weather transform selection tests pass.

## Task 3: Lock Traffic Failure and Artifact-Provenance Contracts with RED Tests

**Files:**

- Modify: `domains/traffic/tests/test_traffic_transform_dbt_selection.py`

**Interfaces:**

- Existing metrics task remains `TriggerRule.ALL_DONE`.
- New watcher task/callable mirrors the Weather failure-leaf contract.
- Artifact resolver consumes `ti.xcom_pull(task_ids=<phase>)` and `ti.xcom_pull(task_ids=<phase>, key="traffic_dbt_failure")` for every dbt phase.
- Artifact resolver returns a path from the most advanced current-run phase that has one, otherwise `None`.

- [x] **Step 1: Extend Traffic fake graph objects and add watcher leaf tests.**

  Assert the same two-leaf and raising-callable contract as Weather, using all Traffic transform tasks from `validate_dev_runtime` through `dbt_test_gold`.

- [x] **Step 2: Add resolver tests for all-phase current-run provenance.**

  Cover at least:

  - a successful earlier phase when later phases have no result;
  - an earlier-phase failure record carrying `dbt_artifact_path`;
  - multiple phase artifacts where the most advanced current-run phase wins;
  - no phase XCom, which returns `None` even if the shared `target/run_results.json` exists.

- [x] **Step 3: Add a publisher regression test for a stale shared artifact.**

  Create a valid stale JSON file at the monkeypatched shared `RUN_RESULTS_PATH`, supply a task instance with no current-run phase artifacts, and assert metrics returns `{"skipped": True}` without opening or publishing the stale result.

- [x] **Step 4: Run the Traffic test file and observe RED.**

  Run:

  ```powershell
  $env:PYTHONDONTWRITEBYTECODE='1'
  python -m pytest -p no:cacheprovider domains/traffic/tests/test_traffic_transform_dbt_selection.py -q
  ```

  Expected failures: no watcher exists, only terminal Gold XCom is consulted, and the current implementation falls back to shared `target/run_results.json`.

## Task 4: Implement Minimal Traffic Failure and Artifact Resolution

**Files:**

- Modify: `domains/traffic/traffic_incident_transform.py`

**Interfaces:**

- Add an ordered constant containing every dbt phase task ID.
- Add: `fail_transform_if_upstream_failed()` and its watcher task.
- Replace the terminal-only resolver with a current-run resolver returning `str | None`.

- [x] **Step 1: Centralize the ordered Traffic dbt phase task IDs.**

  Keep the order identical to the existing DAG chain. Artifact lookup scans the order in reverse so the furthest completed/failed current-run phase wins.

- [x] **Step 2: Resolve each phase's normal and failure XCom artifacts.**

  For each phase, first inspect its normal return mapping's `artifact_path`, then its `traffic_dbt_failure` mapping's `dbt_artifact_path`. Ignore missing or malformed mappings. Return `None` when no current-run artifact path is available.

- [x] **Step 3: Remove the shared artifact fallback from implicit metrics publication.**

  Keep an explicitly supplied `run_results_path` supported for unit-level direct calls. If neither an explicit path nor a current-run XCom path exists, return the existing skipped-metrics result without reading `RUN_RESULTS_PATH`.

- [x] **Step 4: Add and wire the Traffic watcher leaf.**

  Mirror Weather: `ONE_FAILED`, `retries=0`, no duplicate failure callback, direct upstream edges from every transform task, independent of metrics.

- [x] **Step 5: Run the Traffic test file and observe GREEN.**

  Run the Task 3 command. Expected result: all Traffic transform selection tests pass.

## Task 5: Verify the Complete Change and Commit Once

**Files:**

- Verify all five files in this plan; modify no additional paths.

- [x] **Step 1: Run both focused test files together.**

  ```powershell
  $env:PYTHONDONTWRITEBYTECODE='1'
  python -m pytest -p no:cacheprovider domains/weather/tests/test_weather_transform_dbt_selection.py domains/traffic/tests/test_traffic_transform_dbt_selection.py -q
  ```

- [x] **Step 2: Run both related domain test suites.**

  ```powershell
  $env:PYTHONDONTWRITEBYTECODE='1'
  python -m pytest -p no:cacheprovider domains/weather/tests domains/traffic/tests -q
  ```

- [x] **Step 3: Compile the changed Python source and tests without generating bytecode.**

  Use Python's built-in `compile()` over the four changed Python files. The focused tests' fake-Airflow imports serve as the deterministic local DAG import check; do not require a scheduler, database, or secrets.

- [x] **Step 4: Check formatting, diff, scope, and worktree state.**

  Run:

  ```powershell
  git diff --check
  git diff --name-only
  git status --short --branch
  ```

  Expected scope: only the five paths listed in this plan. Review the complete diff and confirm no secret values, generated artifacts, or unrelated changes appear.

- [ ] **Step 5: Create one coherent issue commit after all checks pass.**

  Stage exactly the five listed files and commit with:

  ```text
  fix(transform): 실패 leaf와 Traffic 산출물 provenance 보장 (#323)
  ```

  This is one commit boundary because the two DAG failure leaves and Traffic artifact resolution jointly enforce the issue's single terminal-correctness invariant. Do not push or open a pull request.
