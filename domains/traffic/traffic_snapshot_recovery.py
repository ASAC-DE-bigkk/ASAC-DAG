"""Manual recovery DAG for one historical Traffic Bronze snapshot."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
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

from weather.bronze_run_manifest import MANIFEST_TABLE, STATUS_SUCCESS  # noqa: E402
from common.discord import COLOR_FAIL, COLOR_OK, first_notice_for_run, send_embed  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from traffic_ingest.common.runtime import sql_string, trino_cursor  # noqa: E402
from traffic_dbt_failure import (  # noqa: E402
    R2RecoveryRecordSink,
    build_failure_notification,
    build_recovery_record,
    classify_dbt_failure,
    load_dbt_results,
)


KST = ZoneInfo("Asia/Seoul")
TRAFFIC_SOURCE_ID = "seoul_traffic_incident"
SNAPSHOT_TASK_ID = "validate_publishable_snapshot"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/traffic"
DBT_FAILURE_XCOM_KEY = "traffic_dbt_failure"
DBT_RETRY_DELAY = timedelta(minutes=2)
RECOVERY_SILVER_MODEL_ID = "model.traffic.recovery_silver_seoul_traffic_incident"
RECOVERY_DBT_TASK_IDS = (
    "dbt_deps",
    "dbt_run_recovery_silver",
    "dbt_run_recovery_metadata",
    "dbt_test_recovery_silver",
    "dbt_run_recovery_gold",
    "dbt_test_recovery_gold",
)
RECOVERY_ARTIFACT_TASK_IDS = tuple(
    task_id for task_id in RECOVERY_DBT_TASK_IDS if task_id != "dbt_deps"
)
RECOVERY_RELATIONS = (
    "recovery_silver_seoul_traffic_incident",
    "recovery_traffic_snapshot_metadata",
    "recovery_gold_traffic_incident_summary",
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

    cursor, catalog, schema = trino_cursor()
    cursor.execute(
        f"""
        SELECT CAST(dag_run_id AS varchar)
        FROM {catalog}.{schema}.{MANIFEST_TABLE}
        WHERE source_id = {sql_string(TRAFFIC_SOURCE_ID)}
          AND status = {sql_string(STATUS_SUCCESS)}
          AND is_publishable
          AND CAST(dag_run_id AS varchar) = {sql_string(snapshot_run_id)}
        LIMIT 1
        """
    )
    if not cursor.fetchone():
        raise AirflowFailException(
            f"snapshot_dag_run_id is not publishable: {snapshot_run_id}"
        )
    return snapshot_run_id


def _artifact_path(*, run_id: str | None, task_id: str | None, try_number: int | None) -> str:
    def safe(value: str | None) -> str:
        return "".join(char if char.isalnum() or char in "._=-" else "-" for char in value or "unknown")

    return str(
        PurePosixPath(DBT_PROJECT)
        / "target"
        / "traffic-snapshot-recovery"
        / safe(run_id)
        / safe(task_id)
        / f"try{try_number if try_number is not None else 'unknown'}"
        / "run_results.json"
    )


def recovery_silver_persisted_from_results(
    results: Iterable[dict[str, Any]], *, default: bool
) -> bool:
    """Report recovery Silver persistence from one dbt artifact."""
    for result in results:
        if (
            result.get("unique_id") == RECOVERY_SILVER_MODEL_ID
            and str(result.get("status") or "").lower() in {"success", "pass"}
        ):
            return True
    return default


def run_recovery_dbt_phase(
    *,
    dbt_args: str,
    snapshot_task_id: str,
    recovery_silver_persisted: bool,
    **context,
) -> dict[str, str | None]:
    """Run one recovery-only dbt phase against the preflight-validated snapshot."""
    ti = context["ti"]
    snapshot_run_id = ti.xcom_pull(task_ids=snapshot_task_id)
    run_id = context.get("run_id")
    task_id = getattr(ti, "task_id", None)
    try_number = getattr(ti, "try_number", None)
    artifact_path = _artifact_path(run_id=run_id, task_id=task_id, try_number=try_number)
    target = (context.get("params") or {}).get("target", "dev")
    command = [
        DBT_BIN,
        *shlex.split(dbt_args),
        "--target",
        target,
        "--no-use-colors",
        "--vars",
        f'{{"traffic_snapshot_dag_run_id": "{snapshot_run_id}"}}',
    ]
    if shlex.split(dbt_args)[0] != "deps":
        command.extend(["--target-path", str(PurePosixPath(artifact_path).parent)])
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = DBT_PROJECT
    env["DBT_PROJECT_DIR"] = DBT_PROJECT
    completed = subprocess.run(
        command,
        cwd=DBT_PROJECT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode == 0:
        return {
            "status": "success",
            "artifact_path": (
                None if shlex.split(dbt_args)[0] == "deps" else artifact_path
            ),
        }

    results = load_dbt_results(artifact_path)
    failure = classify_dbt_failure(
        returncode=completed.returncode,
        results=results,
        artifact_path=artifact_path,
        command_output=f"{completed.stdout}\n{completed.stderr}",
    )
    record = build_recovery_record(
        failure,
        traffic_snapshot_dag_run_id=str(snapshot_run_id) if snapshot_run_id else None,
        dag_id=getattr(ti, "dag_id", "traffic_snapshot_recovery"),
        task_id=task_id,
        run_id=run_id,
        try_number=try_number if isinstance(try_number, int) else None,
        silver_persisted=recovery_silver_persisted_from_results(
            results, default=recovery_silver_persisted
        ),
        occurred_at=datetime.now(timezone.utc),
    )
    ti.xcom_push(key=DBT_FAILURE_XCOM_KEY, value=record)
    message = f"traffic recovery dbt {failure.classification}: artifact={artifact_path}"
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
    except Exception:  # noqa: BLE001 - fall back to the shared Problem record
        record = None
    if not isinstance(record, dict):
        record_traffic_problem(context)
        return

    try:
        R2RecoveryRecordSink().write(record)
    except Exception:  # noqa: BLE001 - a record failure must not hide the task failure
        pass
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
    except Exception:  # noqa: BLE001 - a notification failure must not hide the task failure
        pass


def _recovery_phase_evidence(ti: Any) -> tuple[dict[str, str], dict[str, str]]:
    artifacts: dict[str, str] = {}
    statuses: dict[str, str] = {}
    for task_id in RECOVERY_DBT_TASK_IDS:
        result = ti.xcom_pull(task_ids=task_id)
        if not isinstance(result, dict) or not result.get("status"):
            raise AirflowFailException(
                f"recovery dbt phase evidence is unavailable: {task_id}"
            )
        statuses[task_id] = str(result.get("status") or "unknown")
        if task_id in RECOVERY_ARTIFACT_TASK_IDS:
            artifact_path = result.get("artifact_path")
            if not artifact_path:
                raise AirflowFailException(
                    f"recovery dbt artifact is unavailable: {task_id}"
                )
            artifacts[task_id] = str(artifact_path)
    return artifacts, statuses


def record_recovery_completion(**context: Any) -> dict[str, Any]:
    """Record the successful validation of one explicitly selected Bronze snapshot."""
    ti = context["ti"]
    snapshot_dag_run_id = ti.xcom_pull(task_ids=SNAPSHOT_TASK_ID)
    if not snapshot_dag_run_id:
        raise AirflowFailException("validated recovery snapshot is unavailable")

    artifacts, statuses = _recovery_phase_evidence(ti)
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
        "recovery_relations": list(RECOVERY_RELATIONS),
        "dbt_task_statuses": statuses,
        "dbt_artifacts": artifacts,
    }
    R2RecoveryRecordSink().write(record)

    if first_notice_for_run(record["dag_id"], record["run_id"]):
        description = "\n".join(
            (
                f"snapshot_dag_run_id={record['traffic_snapshot_dag_run_id']}",
                f"purpose={RECOVERY_PURPOSE}",
                f"relations={', '.join(RECOVERY_RELATIONS)}",
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


def dbt_task(
    task_id: str, dbt_args: str, *, recovery_silver_persisted: bool = False
) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=run_recovery_dbt_phase,
        op_kwargs={
            "dbt_args": dbt_args,
            "snapshot_task_id": SNAPSHOT_TASK_ID,
            "recovery_silver_persisted": recovery_silver_persisted,
        },
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
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
    dbt_deps = dbt_task("dbt_deps", "deps")
    dbt_run_recovery_silver = dbt_task(
        "dbt_run_recovery_silver", "run --select recovery_silver_seoul_traffic_incident"
    )
    dbt_run_recovery_metadata = dbt_task(
        "dbt_run_recovery_metadata",
        "run --select recovery_traffic_snapshot_metadata",
        recovery_silver_persisted=True,
    )
    dbt_test_recovery_silver = dbt_task(
        "dbt_test_recovery_silver",
        (
            "test --select recovery_traffic_snapshot_metadata "
            "recovery_silver_seoul_traffic_incident "
            "assert_recovery_silver_traffic_snapshot_matches_bronze"
        ),
        recovery_silver_persisted=True,
    )
    dbt_run_recovery_gold = dbt_task(
        "dbt_run_recovery_gold",
        "run --select recovery_gold_traffic_incident_summary",
        recovery_silver_persisted=True,
    )
    dbt_test_recovery_gold = dbt_task(
        "dbt_test_recovery_gold",
        (
            "test --select recovery_traffic_snapshot_metadata "
            "recovery_silver_seoul_traffic_incident "
            "recovery_gold_traffic_incident_summary "
            "assert_recovery_gold_traffic_counts_match_silver"
        ),
        recovery_silver_persisted=True,
    )
    record_completion = PythonOperator(
        task_id="record_recovery_completion",
        python_callable=record_recovery_completion,
        on_failure_callback=record_traffic_problem,
    )

    (
        validate_runtime
        >> validate_snapshot
        >> dbt_deps
        >> dbt_run_recovery_silver
        >> dbt_run_recovery_metadata
        >> dbt_test_recovery_silver
        >> dbt_run_recovery_gold
        >> dbt_test_recovery_gold
        >> record_completion
    )
