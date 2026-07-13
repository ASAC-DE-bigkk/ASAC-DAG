# Traffic Snapshot Recovery DAG Design

## Goal

Allow an operator to validate and rebuild the DBT recovery relations for one historical, publishable TOPIS Bronze snapshot without changing canonical traffic Silver, Current, or Gold tables.

## Scope

Create a manual-only `traffic_snapshot_recovery` DAG in ASAC-DAG #299. It accepts `snapshot_dag_run_id` and only executes the recovery models already supplied by ASAC-DBT #161.

## Decision

The scheduled `traffic_incident_transform` remains unchanged. Its resolver must continue to pin the newest publishable snapshot at run start; changing that behavior would reintroduce the Bronze/transform race fixed by ASAC-DAG #291.

The new recovery DAG runs only in the dev target and validates the supplied snapshot before dbt starts. Its ordered path is:

```text
validate_dev_runtime
  -> validate_publishable_snapshot
  -> dbt_deps
  -> dbt_run_recovery_silver
  -> dbt_run_recovery_metadata
  -> dbt_test_recovery_silver
  -> dbt_run_recovery_gold
  -> dbt_test_recovery_gold
  -> record_recovery_completion
```

Each dbt task receives the preflight-validated snapshot ID through `traffic_snapshot_dag_run_id`. Every dbt run/test phase writes a run/task/try-specific artifact under `target/traffic-snapshot-recovery/`; `dbt deps` contributes only its task status because dbt does not create `run_results.json` for that command. Current traffic failure classification rules remain in force: infrastructure failures retry once and data-contract failures do not retry.

`record_recovery_completion` writes a redacted R2 recovery record and sends a best-effort Discord success notification containing the snapshot ID, per-phase artifact paths, task status, recovery purpose, and the three recovery relations. Failure callbacks use the existing R2/Discord error path with the same snapshot and artifact context.

## Safety constraints

- The DAG has no schedule, `catchup=False`, and `max_active_runs=1`.
- `snapshot_dag_run_id` is mandatory and must match a `SUCCESS + is_publishable` manifest row for `seoul_traffic_incident`.
- Only `recovery_silver_seoul_traffic_incident`, `recovery_traffic_snapshot_metadata`, and `recovery_gold_traffic_incident_summary` are selected.
- No Bronze API collection, canonical transform task, canonical relation, prod target, or destructive cleanup is invoked.
- Recovery evidence contains no secrets.

## Verification

- Unit tests assert the parameter contract, publishable-preflight behavior, exact dbt selectors, artifact scoping, order, recovery-only relation list, and completion evidence.
- The traffic DAG test suite remains green.
- A manual dev DAG run with an existing publishable snapshot proves the selected recovery relations and final row count; this requires the local Airflow runtime and dev catalog.
