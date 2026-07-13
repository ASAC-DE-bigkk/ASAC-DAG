# Traffic Snapshot Recovery DAG Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a manual dev-only Airflow DAG that validates one publishable Traffic Bronze snapshot and executes only DBT recovery relations.

**Architecture:** A standalone DAG avoids changing the scheduled transform's snapshot-pinning behavior. It validates `snapshot_dag_run_id` with the Bronze manifest, passes that value to each DBT recovery phase, stores per-task artifacts, and records the final recovery evidence through the existing redacted R2 and Discord facilities.

**Tech Stack:** Apache Airflow 3, Python 3.11, dbt-trino, Trino/Iceberg, pytest.

## Global Constraints

- Target is `dev` only; prod catalog/schema and scheduled transform behavior remain unchanged.
- Snapshot validity means `source_id='seoul_traffic_incident'`, `status='SUCCESS'`, and `is_publishable=true`.
- Only recovery models and recovery tests may be selected.
- DBT data-contract failures are non-retryable; infrastructure failures retry once.
- DBT artifacts are stored under `target/traffic-snapshot-recovery/<run>/<task>/try<N>/run_results.json`.

---

### Task 1: Specify recovery DAG behavior with failing tests

**Files:**
- Create: `domains/traffic/tests/test_traffic_snapshot_recovery.py`

**Interfaces:**
- Consumes: Airflow DAG and `Param` interfaces, Traffic manifest helpers.
- Produces: executable behavioral assertions for the recovery DAG.

- [x] **Step 1: Write failing tests for manual recovery topology and selectors**

```python
assert dag.kwargs["schedule"] is None
assert dag.task_dict["validate_publishable_snapshot"].downstream_task_ids == {"dbt_deps"}
assert task_args["dbt_run_recovery_silver"] == "run --select recovery_silver_seoul_traffic_incident"
assert "recovery_gold_traffic_incident_summary" in task_args["dbt_test_recovery_gold"]
```

- [x] **Step 2: Run the new test to verify it fails**

Run: `python -m pytest domains/traffic/tests/test_traffic_snapshot_recovery.py -q`

Expected: FAIL because `traffic_snapshot_recovery.py` does not exist.

### Task 2: Add the manual recovery DAG

**Files:**
- Create: `domains/traffic/traffic_snapshot_recovery.py`
- Test: `domains/traffic/tests/test_traffic_snapshot_recovery.py`

**Interfaces:**
- Consumes: `traffic_ingest.common.runtime.trino_cursor`, `traffic_dbt_failure` recovery sink/classifier, `common.discord` notification API.
- Produces: DAG ID `traffic_snapshot_recovery`; Python callables `validate_publishable_snapshot`, `run_recovery_dbt_phase`, and `record_recovery_completion`.

- [x] **Step 1: Implement the preflight callable**

```python
def validate_publishable_snapshot(**context) -> str:
    snapshot = str((context.get("params") or {}).get("snapshot_dag_run_id") or "").strip()
    if not snapshot:
        raise AirflowFailException("snapshot_dag_run_id is required")
    # Query collection_run_manifest for this exact SUCCESS + publishable Traffic run.
    return snapshot
```

- [x] **Step 2: Implement DBT phases in this exact order**

```python
"deps"
"run --select recovery_silver_seoul_traffic_incident"
"run --select recovery_traffic_snapshot_metadata"
"test --select recovery_traffic_snapshot_metadata recovery_silver_seoul_traffic_incident assert_recovery_silver_traffic_snapshot_matches_bronze"
"run --select recovery_gold_traffic_incident_summary"
"test --select recovery_traffic_snapshot_metadata recovery_silver_seoul_traffic_incident recovery_gold_traffic_incident_summary assert_recovery_gold_traffic_counts_match_silver"
```

- [x] **Step 3: Record success evidence**

```python
record = {
    "recovery_purpose": "historical-snapshot-validation",
    "recovery_status": "success",
    "traffic_snapshot_dag_run_id": snapshot,
    "recovery_relations": RECOVERY_RELATIONS,
    "dbt_artifacts": artifacts,
}
```

- [x] **Step 4: Run focused tests**

Run: `python -m pytest domains/traffic/tests/test_traffic_snapshot_recovery.py -q`

Expected: PASS.

### Task 3: Document the operator path

**Files:**
- Modify: `domains/traffic/README.md`
- Test: `domains/traffic/tests/test_traffic_snapshot_recovery.py`

**Interfaces:**
- Consumes: DAG ID and parameter names from Task 2.
- Produces: a manual-trigger example and explicit canonical-table safety boundary.

- [x] **Step 1: Add recovery instructions**

```text
Trigger traffic_snapshot_recovery with snapshot_dag_run_id=<publishable Bronze run ID>.
It rebuilds only recovery_silver_seoul_traffic_incident,
recovery_traffic_snapshot_metadata, and recovery_gold_traffic_incident_summary.
```

- [x] **Step 2: Run the traffic suite and DAG syntax check**

Run: `python -m pytest domains/traffic/tests -q`

Run: `python -m compileall -q domains/traffic/traffic_snapshot_recovery.py`

Expected: both commands exit 0.
