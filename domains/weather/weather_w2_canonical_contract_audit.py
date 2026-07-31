"""Daily read-only audit for the complete Weather W2 canonical contract."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)

DAGS_ROOT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runmetrics import dump_dbt_run_results  # noqa: E402
from common.runtime_guard import (  # noqa: E402
    TARGET_CHOICES,
    default_target,
    validate_dev_runtime,
)
from weather_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402
from weather_ingest.w2_canonical_runtime import (  # noqa: E402
    AdminDongCrosswalkSnapshotUnavailableError,
    resolve_admin_dong_crosswalk_snapshot_id,
)
import weather_dbt_execution as weather_dbt  # noqa: E402
from weather_dbt_failure import classify_weather_dbt_failure  # noqa: E402
from weather_lineage import enable_lineage_if_configured  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
DAG_ID = "weather_w2_canonical_contract_audit"
DBT_PIPELINE = "weather-w2-canonical-contract-audit"
DBT_BIN = weather_dbt.dbt_bin()
DBT_PROJECT = weather_dbt.dbt_project_dir()
DBT_RETRY_DELAY = timedelta(minutes=2)
DBT_RUN_RESULTS_XCOM_KEY = "weather_dbt_run_results_path"
CROSSWALK_SNAPSHOT_TASK_ID = "resolve_admin_dong_crosswalk_snapshot"
AUDIT_TASK_ID = "dbt_test_w2_canonical_full_contracts"
FULL_CONTRACT_SELECTOR = "ask_seoul_weather_w2_canonical_full_contracts"
CANONICAL_REVISION_DATE = "2025-04-01"
DOMAIN = "weather"
DEFAULT_PARAMS = {
    "target": Param(
        default=default_target(),
        type="string",
        enum=list(TARGET_CHOICES),
        description="dbt target profile name; defaults to the runtime env (#561).",
    )
}
record_weather_problem = problem_failure_callback(
    domain=DOMAIN,
    dbt_project_dir=DBT_PROJECT,
    dbt_run_results_xcom_key=DBT_RUN_RESULTS_XCOM_KEY,
)


def resolve_crosswalk_snapshot() -> int:
    """Resolve one positive crosswalk snapshot for the entire audit run."""
    try:
        return resolve_admin_dong_crosswalk_snapshot_id()
    except AdminDongCrosswalkSnapshotUnavailableError as exc:
        raise AirflowFailException(str(exc)) from exc


def run_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    include_project_vars: bool,
    crosswalk_snapshot_task_id: str | None,
    threads: int | None,
    **context,
) -> dict[str, object]:
    """Run one audit dbt phase and expose its current-attempt artifacts."""
    ti = context["ti"]
    task_id = getattr(ti, "task_id", None)
    is_deps = dbt_command == "deps"
    target = (context.get("params") or {}).get("target", "dev")
    crosswalk_snapshot_id = (
        ti.xcom_pull(task_ids=crosswalk_snapshot_task_id)
        if crosswalk_snapshot_task_id
        else None
    )
    if include_project_vars and not is_deps and (
        isinstance(crosswalk_snapshot_id, bool)
        or not isinstance(crosswalk_snapshot_id, int)
        or crosswalk_snapshot_id <= 0
    ):
        raise AirflowFailException(
            "canonical contract audit crosswalk snapshot must be a positive integer"
        )

    run_results_path = None
    execution = None
    try:
        execution = weather_dbt.execute_dbt_phase(
            dbt_command=dbt_command,
            selector=selector,
            invocation_id=task_id or dbt_command.replace(" ", "-"),
            pipeline=DBT_PIPELINE,
            run_id=context.get("run_id"),
            task_id=task_id,
            try_number=getattr(ti, "try_number", None),
            target=target,
            variables=(
                json.dumps(
                    {
                        "weather_w2_canonical_revision_date": (
                            CANONICAL_REVISION_DATE
                        ),
                        "admin_dong_crosswalk_pin_snapshot_id": (
                            crosswalk_snapshot_id
                        ),
                    },
                    separators=(",", ":"),
                )
                if include_project_vars and not is_deps
                else None
            ),
            threads=threads,
            project_dir=DBT_PROJECT,
            executable=DBT_BIN,
            runner=subprocess.run,
        )
        run_results_path = execution.existing_run_results_path
    finally:
        ti.xcom_push(
            key=DBT_RUN_RESULTS_XCOM_KEY,
            value=run_results_path,
        )

    for completed_attempt in execution.attempts:
        if completed_attempt.stdout:
            print(completed_attempt.stdout, end="")
        if completed_attempt.stderr:
            print(completed_attempt.stderr, end="", file=sys.stderr)

    completed = execution.completed
    if completed.returncode != 0 or execution.missing_expected_artifacts:
        command_output = "\n".join(
            str(value)
            for attempt in execution.attempts
            for value in (attempt.stdout, attempt.stderr)
            if value
        )
        failure = classify_weather_dbt_failure(
            dbt_command=dbt_command,
            returncode=int(completed.returncode),
            artifact_path=execution.existing_run_results_path,
            missing_expected_artifacts=execution.missing_expected_artifacts,
            command_output=command_output,
        )
        exception_type = AirflowException if failure.retryable else AirflowFailException
        raise exception_type(
            "weather dbt audit failed: "
            f"classification={failure.classification}; "
            f"exit_code={completed.returncode}"
        )

    return {
        "status": "success",
        "run_results_path": execution.existing_run_results_path,
        "sources_path": getattr(execution, "existing_sources_path", None),
        "manifest_path": getattr(execution, "existing_manifest_path", None),
        "selected_unique_ids": list(
            getattr(execution, "selected_unique_ids", ())
        ),
    }


def publish_dbt_run_metrics(**context) -> dict[str, object]:
    """Persist audit test metrics without changing the contract gate."""
    ti = context.get("ti") or context.get("task_instance")
    result = ti.xcom_pull(task_ids=AUDIT_TASK_ID) if ti is not None else None
    run_results_path = (
        result.get("run_results_path") if isinstance(result, dict) else None
    )
    if not run_results_path or not os.path.exists(run_results_path):
        print(f"run_results.json missing; metrics skipped: {run_results_path}")
        return {"rows": 0, "skipped": True}
    target = (context.get("params") or {}).get("target")
    records = dump_dbt_run_results(run_results_path, domain=DOMAIN, target=target)
    print(
        "dbt audit metrics persisted: "
        f"{len(records)} records (domain={DOMAIN}, target={target})"
    )
    return {"rows": len(records), "skipped": False}


with DAG(
    dag_id=DAG_ID,
    description="Audit all Weather W2 canonical contracts without data writes.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule="15 9 * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 1, "retry_delay": DBT_RETRY_DELAY},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "w2", "canonical", "audit", "dbt"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": DOMAIN, "requested_target": "{{ params.target }}"},
        on_failure_callback=record_weather_problem,
    )

    resolve_snapshot = PythonOperator(
        task_id=CROSSWALK_SNAPSHOT_TASK_ID,
        python_callable=resolve_crosswalk_snapshot,
        on_failure_callback=record_weather_problem,
    )

    dbt_deps = PythonOperator(
        task_id="dbt_deps",
        python_callable=run_dbt_phase,
        op_kwargs={
            "dbt_command": "deps",
            "selector": None,
            "include_project_vars": False,
            "crosswalk_snapshot_task_id": None,
            "threads": None,
        },
        weight_rule="absolute",
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        on_failure_callback=record_weather_problem,
    )

    audit_contracts = PythonOperator(
        task_id=AUDIT_TASK_ID,
        python_callable=run_dbt_phase,
        op_kwargs={
            "dbt_command": "test",
            "selector": FULL_CONTRACT_SELECTOR,
            "include_project_vars": True,
            "crosswalk_snapshot_task_id": CROSSWALK_SNAPSHOT_TASK_ID,
            "threads": 1,
        },
        pool=TRINO_HEAVY_POOL,
        weight_rule="absolute",
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        on_failure_callback=record_weather_problem,
    )

    publish_metrics = PythonOperator(
        task_id="publish_dbt_run_metrics",
        python_callable=publish_dbt_run_metrics,
        on_failure_callback=record_weather_problem,
    ).as_teardown(on_failure_fail_dagrun=False)

    tasks = [
        validate_runtime,
        resolve_snapshot,
        dbt_deps,
        audit_contracts,
        publish_metrics,
    ]
    for upstream, downstream in zip(tasks, tasks[1:]):
        upstream >> downstream


enable_lineage_if_configured(dag)
