"""Runtime implementation for Traffic transform dbt phases."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any, Callable

from airflow.exceptions import AirflowException
from airflow.sdk.exceptions import AirflowFailException

import traffic_dbt_execution as traffic_dbt
from traffic_dbt_failure import (
    build_recovery_record,
    classify_dbt_failure,
    load_dbt_results,
    silver_persisted_from_results,
)
from traffic_ingest.silver_snapshot_fence import (
    ExternalCompactionRace,
    SilverSnapshotEvidence,
    SnapshotFenceTelemetryError,
    assert_safe_post_write,
    assert_snapshot_unchanged,
    collect_silver_snapshot_evidence,
)
from traffic_ingest.transform_dag_support import dbt_snapshot_variables
from traffic_ingest.transform_metrics import DBT_FAILURE_XCOM_KEY, DBT_RUN_RESULTS_RECORD_KEY
from traffic_ingest.transform_test_tier import _selector_for_test_tier


PREFLIGHT_SNAPSHOT_DAG_RUN_ID = "__traffic_preflight__"


def _fail_closed_fence(exc: Exception) -> None:
    if isinstance(exc, ExternalCompactionRace):
        raise AirflowFailException(f"EXTERNAL_COMPACTION_RACE: {exc}") from exc
    raise AirflowFailException(f"invalid silver snapshot evidence: {exc}") from exc


def _expected_write_evidence(ti: Any) -> SilverSnapshotEvidence:
    raw_result = ti.xcom_pull(task_ids="dbt_run_silver")
    if not isinstance(raw_result, dict):
        raise SnapshotFenceTelemetryError("dbt_run_silver result is missing")
    if "silver_snapshot_evidence" not in raw_result:
        raise SnapshotFenceTelemetryError("silver_snapshot_evidence is missing")
    return SilverSnapshotEvidence.from_dict(raw_result["silver_snapshot_evidence"])


def run_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    snapshot_task_id: str,
    silver_persisted: bool,
    fresh_parse: bool = False,
    snapshot_required: bool = False,
    citydata_snapshot_required: bool = False,
    silver_fence_mode: str | None = None,
    threads: int | None = None,
    selector_by_test_tier=None,
    dbt_bin: str | None = None,
    dbt_project: str | None = None,
    flow_xcom_key: str = "traffic_flow_snapshot_dag_run_id",
    citydata_xcom_key: str = "traffic_citydata_crowding_snapshot_id",
    load_results: Callable[[str], list[dict[str, object]]] = load_dbt_results,
    classify_failure: Callable[..., Any] = classify_dbt_failure,
    recovery_record_builder: Callable[..., dict[str, object]] = build_recovery_record,
    persisted_from_results: Callable[..., bool] = silver_persisted_from_results,
    runner: Callable[..., Any] = subprocess.run,
    **context,
) -> dict[str, object]:
    """Run one dbt phase, preserving classified failures and optional Silver fencing."""
    if silver_fence_mode not in {None, "write", "verify"}:
        raise AirflowFailException(f"invalid silver_fence_mode: {silver_fence_mode}")

    ti = context["ti"]
    snapshot_run_id = ti.xcom_pull(task_ids=snapshot_task_id)
    run_id = context.get("run_id")
    task_id = getattr(ti, "task_id", None)
    try_number = getattr(ti, "try_number", None)
    params = context.get("params") or {}
    target = params.get("target", "dev")
    effective_selector, tier_skipped = _selector_for_test_tier(
        selector=selector,
        selector_by_test_tier=selector_by_test_tier,
        ti=ti,
    )
    if tier_skipped:
        return {
            "status": "success",
            "skipped": True,
            "skip_reason": "traffic_test_tier_noop",
            "run_results_path": None,
            "sources_path": None,
            "manifest_path": None,
            "selected_unique_ids": [],
        }
    if snapshot_required and not snapshot_run_id:
        raise AirflowFailException(
            f"traffic dbt phase requires resolved snapshot: {task_id or dbt_command}"
        )
    effective_snapshot_run_id = (
        str(snapshot_run_id) if snapshot_run_id else PREFLIGHT_SNAPSHOT_DAG_RUN_ID
    )
    dbt_variables = dbt_snapshot_variables(
        ti,
        snapshot_task_id,
        effective_snapshot_run_id,
        flow_xcom_key,
        citydata_xcom_key,
    )
    citydata_crowding_snapshot_id = dbt_variables.get(citydata_xcom_key)
    if citydata_snapshot_required and citydata_xcom_key not in dbt_variables:
        raise AirflowFailException(
            "traffic dbt phase requires Citydata crowding snapshot: "
            f"{task_id or dbt_command}"
        )

    baseline = None
    expected = None
    try:
        if silver_fence_mode == "write":
            baseline = collect_silver_snapshot_evidence()
        elif silver_fence_mode == "verify":
            expected = _expected_write_evidence(ti)
            assert_snapshot_unchanged(expected, collect_silver_snapshot_evidence())
    except Exception as exc:  # fence telemetry and external rewrites both fail closed
        _fail_closed_fence(exc)

    execution = traffic_dbt.execute_dbt_phase(
        dbt_command=dbt_command,
        selector=effective_selector,
        invocation_id=task_id or dbt_command.replace(" ", "-"),
        pipeline="traffic-transform",
        run_id=run_id,
        task_id=task_id,
        try_number=try_number,
        target=target,
        variables=json.dumps(dbt_variables),
        threads=threads,
        fresh_parse=fresh_parse,
        project_dir=dbt_project,
        executable=dbt_bin,
        runner=runner,
    )
    for completed in execution.attempts:
        if completed.stdout:
            print(completed.stdout, end="")
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr)
    completed = execution.completed
    missing_artifact_error = (
        "missing expected dbt artifacts: " + ", ".join(execution.missing_expected_artifacts)
        if completed.returncode == 0 and execution.missing_expected_artifacts
        else ""
    )
    if completed.returncode == 0 and not missing_artifact_error:
        result: dict[str, object] = {
            "status": "success",
            "traffic_citydata_crowding_snapshot_id": citydata_crowding_snapshot_id,
            "run_results_path": execution.existing_run_results_path,
            "sources_path": execution.existing_sources_path,
            "manifest_path": execution.existing_manifest_path,
            "selected_unique_ids": list(execution.selected_unique_ids),
        }
        try:
            if silver_fence_mode == "write":
                current = collect_silver_snapshot_evidence()
                assert baseline is not None
                assert_safe_post_write(baseline, current)
                result["silver_snapshot_evidence"] = current.as_dict()
            elif silver_fence_mode == "verify":
                assert expected is not None
                assert_snapshot_unchanged(expected, collect_silver_snapshot_evidence())
        except Exception as exc:  # never classify a fence failure as a dbt failure
            _fail_closed_fence(exc)
        return result

    results = load_results(execution.existing_run_results_path) if execution.existing_run_results_path else []
    failure = classify_failure(
        returncode=completed.returncode or 2,
        results=results,
        artifact_path=execution.primary_artifact_path,
        command_output=f"{completed.stdout}\n{completed.stderr}\n{missing_artifact_error}",
    )
    record = recovery_record_builder(
        failure,
        traffic_snapshot_dag_run_id=str(snapshot_run_id) if snapshot_run_id else None,
        traffic_citydata_crowding_snapshot_id=citydata_crowding_snapshot_id,
        dag_id=getattr(ti, "dag_id", "traffic_incident_transform"),
        task_id=task_id,
        run_id=run_id,
        try_number=try_number if isinstance(try_number, int) else None,
        silver_persisted=persisted_from_results(
            results,
            selected_unique_ids=execution.selected_unique_ids,
            default=silver_persisted,
        ),
        occurred_at=datetime.now(timezone.utc),
    )
    record.update(
        {
            DBT_RUN_RESULTS_RECORD_KEY: execution.existing_run_results_path,
            "dbt_sources_path": execution.existing_sources_path,
            "dbt_manifest_path": execution.existing_manifest_path,
        }
    )
    ti.xcom_push(key=DBT_FAILURE_XCOM_KEY, value=record)
    message = f"traffic dbt {failure.classification}: artifact={execution.primary_artifact_path or 'unknown'}"
    if failure.retryable:
        raise AirflowException(message)
    raise AirflowFailException(message)


__all__ = ["run_dbt_phase"]
