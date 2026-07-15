"""Airflow DAG: traffic silver/gold transform via dbt.

The bronze DAG stores TOPIS AccInfo raw XML in R2 and publishes verified
Iceberg bronze runs. This transform DAG consumes only publishable bronze runs
through the dbt models and keeps transform retries independent from API calls.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from airflow.sdk.exceptions import AirflowFailException

# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
DOMAINS_DIR = os.path.dirname(DAG_DIR)
if DOMAINS_DIR not in sys.path:
    sys.path.insert(0, DOMAINS_DIR)
DAGS_ROOT_DIR = os.path.dirname(os.path.dirname(DAG_DIR))
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.discord import COLOR_FAIL, first_notice_for_run, send_embed  # noqa: E402
from common.assets import TRAFFIC_BRONZE_ASSET as TRAFFIC_BRONZE_ASSET  # noqa: E402
from common.errors.airflow import problem_failure_callback, problem_from_airflow_context  # noqa: E402
from common.errors.sink import R2ErrorSink  # noqa: E402
from common.runmetrics import dump_dbt_run_results  # noqa: E402
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
from traffic_ingest.runtime import build_traffic_manifest  # noqa: E402
from traffic_ingest.flow_ingest import build_traffic_flow_manifest  # noqa: E402
from traffic_ingest.assets import (  # noqa: E402
    TRAFFIC_FLOW_BRONZE_ASSET as TRAFFIC_FLOW_BRONZE_ASSET,
)
from traffic_ingest.transform_specs import (  # noqa: E402
    DBT_PHASE_SPECS,
    DBT_PHASE_TASK_IDS,
    DbtPhaseSpec,
)
from traffic_ingest.transform_dag_support import (  # noqa: E402
    TRAFFIC_TRANSFORM_CRON_KST as TRAFFIC_TRANSFORM_CRON_KST,
    TransformFailurePorts,
    dbt_snapshot_variables,
    record_classified_dbt_problem,
    resolve_transform_snapshot_pair,
    transform_schedule,
)
from traffic_lineage import enable_lineage_if_configured  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
LOGGER = logging.getLogger(__name__)
DBT_BIN = traffic_dbt.dbt_bin()
DBT_PROJECT = traffic_dbt.dbt_project_dir()
DOMAIN = "traffic"
SNAPSHOT_TASK_ID = "resolve_traffic_snapshot_run"
FLOW_SNAPSHOT_XCOM_KEY = "traffic_flow_snapshot_dag_run_id"
DBT_FAILURE_XCOM_KEY = "traffic_dbt_failure"
DBT_RUN_RESULTS_RECORD_KEY = "dbt_run_results_path"


DBT_RETRY_DELAY = timedelta(minutes=2)
DEFAULT_PARAMS = {
    "target": Param(
        default="dev",
        type="string",
        enum=["dev"],
        description="dbt target profile name (dev only until production rollout).",
    )
}
# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_traffic_problem = problem_failure_callback(domain="traffic")


def resolve_traffic_snapshot_run(**context) -> str:
    """Pin a non-regressing Incident/Flow snapshot pair."""
    pair = resolve_transform_snapshot_pair(
        context=context,
        incident_manifest_factory=build_traffic_manifest,
        flow_manifest_factory=build_traffic_flow_manifest,
    )
    task_instance = context.get("ti") or context.get("task_instance")
    if task_instance is not None:
        task_instance.xcom_push(
            key=FLOW_SNAPSHOT_XCOM_KEY,
            value=pair.flow_run_id,
        )
    return pair.incident_run_id


def run_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    snapshot_task_id: str,
    silver_persisted: bool,
    fresh_parse: bool = False,
    **context,
) -> dict[str, object]:
    """Run one pinned dbt phase and let Airflow retry infrastructure failures only."""
    ti = context["ti"]
    snapshot_run_id = ti.xcom_pull(task_ids=snapshot_task_id)
    run_id = context.get("run_id")
    task_id = getattr(ti, "task_id", None)
    try_number = getattr(ti, "try_number", None)
    params = context.get("params") or {}
    target = params.get("target", "dev")
    dbt_variables = dbt_snapshot_variables(
        ti,
        snapshot_task_id,
        snapshot_run_id,
        FLOW_SNAPSHOT_XCOM_KEY,
    )
    execution = traffic_dbt.execute_dbt_phase(
        dbt_command=dbt_command,
        selector=selector,
        invocation_id=task_id or dbt_command.replace(" ", "-"),
        pipeline="traffic-transform",
        run_id=run_id,
        task_id=task_id,
        try_number=try_number,
        target=target,
        variables=json.dumps(dbt_variables),
        fresh_parse=fresh_parse,
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
        dag_id=getattr(ti, "dag_id", "traffic_incident_transform"),
        task_id=task_id,
        run_id=run_id,
        try_number=try_number if isinstance(try_number, int) else None,
        silver_persisted=silver_persisted_from_results(
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
    message = (
        f"traffic dbt {failure.classification}: "
        f"artifact={execution.primary_artifact_path or 'unknown'}"
    )
    if failure.retryable:
        raise AirflowException(message)
    raise AirflowFailException(message)


def record_traffic_dbt_problem(context) -> None:
    """Persist and notify final classified dbt failures without changing task state."""
    record_classified_dbt_problem(
        context,
        failure_xcom_key=DBT_FAILURE_XCOM_KEY,
        run_results_record_key=DBT_RUN_RESULTS_RECORD_KEY,
        ports=TransformFailurePorts(
            fallback_recorder=record_traffic_problem,
            problem_from_context=problem_from_airflow_context,
            error_sink_factory=R2ErrorSink,
            recovery_sink_factory=R2RecoveryRecordSink,
            first_notice=first_notice_for_run,
            notification_builder=build_failure_notification,
            send_notification=send_embed,
            failure_color=COLOR_FAIL,
            logger=LOGGER,
        ),
    )


def dbt_task(spec: DbtPhaseSpec) -> PythonOperator:
    return PythonOperator(
        task_id=spec.task_id,
        python_callable=run_dbt_phase,
        op_kwargs={
            "dbt_command": spec.dbt_command,
            "selector": spec.selector,
            "snapshot_task_id": SNAPSHOT_TASK_ID,
            "silver_persisted": spec.silver_persisted,
            "fresh_parse": spec.fresh_parse,
        },
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        pool=TRINO_HEAVY_POOL,
        on_failure_callback=record_traffic_dbt_problem,
    )


def _current_run_results_path(**context) -> str | None:
    """Return the latest isolated dbt artifact recorded by this DAG run."""
    ti = context.get("ti") or context.get("task_instance")
    if ti is None:
        return None
    for task_id in reversed(DBT_PHASE_TASK_IDS):
        try:
            result = ti.xcom_pull(task_ids=task_id)
        except Exception as exc:  # noqa: BLE001 - inspect earlier current-run phases
            LOGGER.debug(
                "traffic dbt result XCom lookup failed for %s: %s",
                task_id,
                type(exc).__name__,
            )
            result = None
        if isinstance(result, dict) and result.get("run_results_path"):
            return str(result["run_results_path"])
        try:
            failure = ti.xcom_pull(task_ids=task_id, key=DBT_FAILURE_XCOM_KEY)
        except Exception as exc:  # noqa: BLE001 - inspect earlier current-run phases
            LOGGER.debug(
                "traffic dbt failure XCom lookup failed for %s: %s",
                task_id,
                type(exc).__name__,
            )
            failure = None
        if isinstance(failure, dict) and failure.get(DBT_RUN_RESULTS_RECORD_KEY):
            return str(failure[DBT_RUN_RESULTS_RECORD_KEY])
    return None


def publish_dbt_run_metrics(run_results_path: str | None = None, **context) -> dict:
    """Persist Traffic dbt metrics while preserving terminal dbt failure semantics."""
    resolved_path = (
        run_results_path
        if run_results_path is not None
        else _current_run_results_path(**context)
    )
    if not resolved_path or not os.path.exists(resolved_path):
        print(f"run_results.json 없음 — 메트릭 적재 skip: {resolved_path}")
        return {"rows": 0, "skipped": True}
    target = (context.get("params") or {}).get("target")
    records = dump_dbt_run_results(resolved_path, domain=DOMAIN, target=target)
    print(
        f"dbt 실행 메트릭 적재: {len(records)} records (domain={DOMAIN}, target={target})"
    )
    return {"rows": len(records), "skipped": False}


with DAG(
    dag_id="traffic_incident_transform",
    description="Transform traffic bronze -> silver/gold via dbt.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=transform_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": DBT_RETRY_DELAY},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "traffic", "transform", "silver", "gold", "dbt"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "traffic", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_traffic_problem,
    )

    resolve_snapshot = PythonOperator(
        task_id=SNAPSHOT_TASK_ID,
        python_callable=resolve_traffic_snapshot_run,
        on_failure_callback=record_traffic_problem,
    )

    dbt_phase_tasks = {spec.task_id: dbt_task(spec) for spec in DBT_PHASE_SPECS}
    dbt_tasks_in_order = list(dbt_phase_tasks.values())

    publish_dbt_metrics = PythonOperator(
        task_id="publish_dbt_run_metrics",
        python_callable=publish_dbt_run_metrics,
        on_failure_callback=record_traffic_problem,
    ).as_teardown(on_failure_fail_dagrun=False)

    transform_tasks = [validate_runtime, resolve_snapshot, *dbt_tasks_in_order]
    pipeline_tasks = [*transform_tasks, publish_dbt_metrics]
    for upstream, downstream in zip(pipeline_tasks, pipeline_tasks[1:]):
        upstream >> downstream


enable_lineage_if_configured(dag)
