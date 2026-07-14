"""Manual recovery DAG for one historical Traffic Bronze snapshot."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowFailException

DOMAIN_DIR = os.path.dirname(os.path.abspath(__file__))
if DOMAIN_DIR not in sys.path:
    sys.path.insert(0, DOMAIN_DIR)
DOMAINS_DIR = os.path.dirname(DOMAIN_DIR)
if DOMAINS_DIR not in sys.path:
    sys.path.insert(0, DOMAINS_DIR)
DAGS_ROOT_DIR = os.path.dirname(DOMAINS_DIR)
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.discord import COLOR_FAIL, COLOR_OK, first_notice_for_run, send_embed  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from traffic_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402
from traffic_dbt_failure import (  # noqa: E402
    R2RecoveryRecordSink,
    build_failure_notification,
    build_recovery_record,
    classify_dbt_failure,
    load_dbt_results,
    silver_persisted_from_results,
)
import traffic_dbt_execution as traffic_dbt  # noqa: E402
from traffic_ingest.run_manifest import RunNotPublishableError  # noqa: E402
from traffic_ingest.runtime import build_traffic_manifest  # noqa: E402
from traffic_lineage import enable_lineage_if_configured  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
LOGGER = logging.getLogger(__name__)
SNAPSHOT_TASK_ID = "validate_publishable_snapshot"
DBT_BIN = traffic_dbt.dbt_bin()
DBT_PROJECT = traffic_dbt.dbt_project_dir()
DBT_FAILURE_XCOM_KEY = "traffic_dbt_failure"
DBT_RETRY_DELAY = timedelta(minutes=2)


@dataclass(frozen=True)
class DbtPhaseSpec:
    task_id: str
    dbt_command: str
    selection: str | None = None
    recovery_silver_persisted: bool = False


RECOVERY_DBT_PHASE_SPECS = (
    DbtPhaseSpec("dbt_deps", "deps"),
    DbtPhaseSpec(
        "dbt_run_recovery_silver",
        "run",
        "tag:ask_seoul_traffic_recovery_silver",
    ),
    DbtPhaseSpec(
        "dbt_run_recovery_metadata",
        "run",
        "tag:ask_seoul_traffic_recovery_metadata",
        recovery_silver_persisted=True,
    ),
    DbtPhaseSpec(
        "dbt_test_recovery_silver",
        "test",
        "tag:ask_seoul_traffic_recovery_silver",
        recovery_silver_persisted=True,
    ),
    DbtPhaseSpec(
        "dbt_run_recovery_gold",
        "run",
        "tag:ask_seoul_traffic_recovery_gold",
        recovery_silver_persisted=True,
    ),
    DbtPhaseSpec(
        "dbt_test_recovery_gold",
        "test",
        "tag:ask_seoul_traffic_recovery_gold",
        recovery_silver_persisted=True,
    ),
)
RECOVERY_DBT_TASK_IDS = tuple(spec.task_id for spec in RECOVERY_DBT_PHASE_SPECS)
RECOVERY_ARTIFACT_TASK_IDS = tuple(
    spec.task_id for spec in RECOVERY_DBT_PHASE_SPECS if spec.dbt_command != "deps"
)
RECOVERY_PURPOSE = "historical-snapshot-validation"
record_traffic_problem = problem_failure_callback(domain="traffic")
DEFAULT_PARAMS = {
    "target": Param(
        default="dev",
        type="string",
        enum=["dev"],
        description="dbt target profile name (dev only).",
    ),
    "snapshot_dag_run_id": Param(
        default="",
        type="string",
        description="Required publishable Traffic Bronze dag_run_id to recover.",
    ),
}


def validate_publishable_snapshot(**context) -> str:
    """Reject missing or non-publishable historical snapshots before dbt starts."""
    snapshot_run_id = str(
        (context.get("params") or {}).get("snapshot_dag_run_id") or ""
    ).strip()
    if not snapshot_run_id:
        raise AirflowFailException("snapshot_dag_run_id is required")

    try:
        return build_traffic_manifest().require_publishable(snapshot_run_id)
    except RunNotPublishableError as exc:
        raise AirflowFailException(
            f"snapshot_dag_run_id is not publishable: {snapshot_run_id}"
        ) from exc


def recovery_silver_persisted_from_results(
    results: Iterable[dict[str, Any]],
    *,
    selected_unique_ids: Iterable[str],
    default: bool,
) -> bool:
    """Apply the selected-model persistence contract to recovery."""
    return silver_persisted_from_results(
        results,
        selected_unique_ids=selected_unique_ids,
        default=default,
    )


def run_recovery_dbt_phase(
    *,
    dbt_command: str,
    selection: str | None,
    snapshot_task_id: str,
    recovery_silver_persisted: bool,
    **context,
) -> dict[str, object]:
    """Run one recovery-only dbt phase against the preflight-validated snapshot."""
    ti = context["ti"]
    snapshot_run_id = ti.xcom_pull(task_ids=snapshot_task_id)
    run_id = context.get("run_id")
    task_id = getattr(ti, "task_id", None)
    try_number = getattr(ti, "try_number", None)
    target = (context.get("params") or {}).get("target", "dev")
    execution = traffic_dbt.execute_dbt_phase(
        dbt_command=dbt_command,
        selection=selection,
        pipeline="traffic-snapshot-recovery",
        run_id=run_id,
        task_id=task_id,
        try_number=try_number,
        target=target,
        variables=json.dumps({"traffic_snapshot_dag_run_id": snapshot_run_id}),
        project_dir=DBT_PROJECT,
        executable=DBT_BIN,
        runner=subprocess.run,
    )
    for completed in execution.attempts:
        if completed.stdout:
            print(completed.stdout, end="")
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr)
    completed = execution.completed
    missing_artifact_error = (
        "missing expected dbt artifacts: "
        + ", ".join(execution.missing_expected_artifacts)
        if completed.returncode == 0 and execution.missing_expected_artifacts
        else ""
    )
    if completed.returncode == 0 and not missing_artifact_error:
        return {
            "status": "success",
            "run_results_path": execution.existing_run_results_path,
            "sources_path": execution.existing_sources_path,
            "manifest_path": execution.existing_manifest_path,
            "selected_unique_ids": list(execution.selected_unique_ids),
        }

    results = (
        load_dbt_results(execution.existing_run_results_path)
        if execution.existing_run_results_path
        else []
    )
    failure = classify_dbt_failure(
        returncode=completed.returncode or 2,
        results=results,
        artifact_path=execution.primary_artifact_path,
        command_output=(
            f"{completed.stdout}\n{completed.stderr}\n{missing_artifact_error}"
        ),
    )
    record = build_recovery_record(
        failure,
        traffic_snapshot_dag_run_id=str(snapshot_run_id) if snapshot_run_id else None,
        dag_id=getattr(ti, "dag_id", "traffic_snapshot_recovery"),
        task_id=task_id,
        run_id=run_id,
        try_number=try_number if isinstance(try_number, int) else None,
        silver_persisted=recovery_silver_persisted_from_results(
            results,
            selected_unique_ids=execution.selected_unique_ids,
            default=recovery_silver_persisted,
        ),
        occurred_at=datetime.now(timezone.utc),
    )
    record.update(
        {
            "dbt_run_results_path": execution.existing_run_results_path,
            "dbt_sources_path": execution.existing_sources_path,
            "dbt_manifest_path": execution.existing_manifest_path,
        }
    )
    ti.xcom_push(key=DBT_FAILURE_XCOM_KEY, value=record)
    message = (
        f"traffic recovery dbt {failure.classification}: "
        f"artifact={execution.primary_artifact_path or 'unknown'}"
    )
    if failure.retryable:
        raise AirflowException(message)
    raise AirflowFailException(message)


def record_recovery_dbt_problem(context: dict[str, Any]) -> None:
    """Persist and notify classified recovery failures without masking the task error."""
    ti = context.get("task_instance") or context.get("ti")
    try:
        record = ti.xcom_pull(
            task_ids=getattr(ti, "task_id", None), key=DBT_FAILURE_XCOM_KEY
        )
    except Exception as exc:  # noqa: BLE001 - fall back to shared Problem record
        LOGGER.warning(
            "traffic recovery failure XCom lookup failed: %s",
            type(exc).__name__,
        )
        record = None
    if not isinstance(record, dict):
        record_traffic_problem(context)
        return

    try:
        R2RecoveryRecordSink().write(record)
    except Exception as exc:  # noqa: BLE001 - preserve the original task failure
        LOGGER.warning(
            "traffic recovery record write failed: %s",
            type(exc).__name__,
        )
    try:
        if first_notice_for_run(record.get("dag_id"), record.get("run_id")):
            title, description, footer = build_failure_notification(record)
            send_embed(
                title,
                description,
                color=COLOR_FAIL,
                footer=footer,
                domain="traffic",
            )
    except Exception as exc:  # noqa: BLE001 - preserve the original task failure
        LOGGER.warning(
            "traffic recovery failure notification failed: %s",
            type(exc).__name__,
        )


def _recovery_phase_evidence(
    ti: Any,
) -> tuple[dict[str, str], dict[str, str], list[str]]:
    artifacts: dict[str, str] = {}
    statuses: dict[str, str] = {}
    selected_relations: set[str] = set()
    for task_id in RECOVERY_DBT_TASK_IDS:
        result = ti.xcom_pull(task_ids=task_id)
        if not isinstance(result, dict) or not result.get("status"):
            raise AirflowFailException(
                f"recovery dbt phase evidence is unavailable: {task_id}"
            )
        statuses[task_id] = str(result.get("status") or "unknown")
        selected_unique_ids = result.get("selected_unique_ids") or []
        selected_relations.update(
            str(unique_id).rsplit(".", 1)[-1]
            for unique_id in selected_unique_ids
            if str(unique_id).startswith("model.")
        )
        if task_id in RECOVERY_ARTIFACT_TASK_IDS:
            run_results_path = result.get("run_results_path")
            if not run_results_path:
                raise AirflowFailException(
                    f"recovery dbt artifact is unavailable: {task_id}"
                )
            artifacts[task_id] = str(run_results_path)
    return artifacts, statuses, sorted(selected_relations)


def record_recovery_completion(**context: Any) -> dict[str, Any]:
    """Record the successful validation of one explicitly selected Bronze snapshot."""
    ti = context["ti"]
    snapshot_dag_run_id = ti.xcom_pull(task_ids=SNAPSHOT_TASK_ID)
    if not snapshot_dag_run_id:
        raise AirflowFailException("validated recovery snapshot is unavailable")

    artifacts, statuses, recovery_relations = _recovery_phase_evidence(ti)
    record = {
        "schema_version": "v1",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "domain": "traffic",
        "dag_id": getattr(ti, "dag_id", "traffic_snapshot_recovery"),
        "task_id": getattr(ti, "task_id", "record_recovery_completion"),
        "run_id": context.get("run_id"),
        "try_number": getattr(ti, "try_number", 1),
        "traffic_snapshot_dag_run_id": str(snapshot_dag_run_id),
        "recovery_purpose": RECOVERY_PURPOSE,
        "recovery_status": "success",
        "recovery_relations": recovery_relations,
        "dbt_task_statuses": statuses,
        "dbt_artifacts": artifacts,
    }
    R2RecoveryRecordSink().write(record)

    if first_notice_for_run(record["dag_id"], record["run_id"]):
        description = "\n".join(
            (
                f"snapshot_dag_run_id={record['traffic_snapshot_dag_run_id']}",
                f"purpose={RECOVERY_PURPOSE}",
                f"relations={', '.join(recovery_relations)}",
                *(f"{task_id}={path}" for task_id, path in artifacts.items()),
            )
        )
        send_embed(
            "✅ traffic snapshot recovery success",
            description,
            color=COLOR_OK,
            footer=f"run_id={record['run_id']} task={record['task_id']}",
            domain="traffic",
        )
    return record


def dbt_task(spec: DbtPhaseSpec) -> PythonOperator:
    return PythonOperator(
        task_id=spec.task_id,
        python_callable=run_recovery_dbt_phase,
        op_kwargs={
            "dbt_command": spec.dbt_command,
            "selection": spec.selection,
            "snapshot_task_id": SNAPSHOT_TASK_ID,
            "recovery_silver_persisted": spec.recovery_silver_persisted,
        },
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        pool=TRINO_HEAVY_POOL,
        on_failure_callback=record_recovery_dbt_problem,
    )


with DAG(
    dag_id="traffic_snapshot_recovery",
    description="Recover and validate one explicit Traffic Bronze snapshot via dbt recovery relations.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "traffic", "recovery", "dbt", "manual"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "traffic", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_traffic_problem,
    )
    validate_snapshot = PythonOperator(
        task_id="validate_publishable_snapshot",
        python_callable=validate_publishable_snapshot,
        on_failure_callback=record_traffic_problem,
    )
    recovery_dbt_tasks = {
        spec.task_id: dbt_task(spec) for spec in RECOVERY_DBT_PHASE_SPECS
    }
    recovery_dbt_tasks_in_order = list(recovery_dbt_tasks.values())
    record_completion = PythonOperator(
        task_id="record_recovery_completion",
        python_callable=record_recovery_completion,
        on_failure_callback=record_traffic_problem,
    )

    recovery_tasks = [
        validate_runtime,
        validate_snapshot,
        *recovery_dbt_tasks_in_order,
        record_completion,
    ]
    for upstream, downstream in zip(recovery_tasks, recovery_tasks[1:]):
        upstream >> downstream


enable_lineage_if_configured(dag)
