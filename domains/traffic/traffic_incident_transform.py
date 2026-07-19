"""Airflow DAG: publish Traffic Incident Bronze into the Silver Asset."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param, Variable
from airflow.sdk.exceptions import AirflowSkipException

DIR = os.path.dirname(os.path.abspath(__file__))
for path in (DIR, os.path.dirname(DIR), os.path.dirname(os.path.dirname(DIR))):
    if path not in sys.path:
        sys.path.insert(0, path)

from common.assets import TRAFFIC_BRONZE_ASSET as TRAFFIC_INCIDENT_BRONZE_ASSET  # noqa: E402
from common.discord import COLOR_FAIL, first_notice_for_run, send_embed  # noqa: E402
from common.errors.airflow import problem_failure_callback, problem_from_airflow_context  # noqa: E402
from common.errors.sink import R2ErrorSink  # noqa: E402
from common.runmetrics import dump_dbt_run_results  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from traffic_dbt_failure import (  # noqa: E402
    R2RecoveryRecordSink,
    build_failure_notification,
    build_recovery_record,
    classify_dbt_failure,
    load_dbt_results,
    silver_persisted_from_results,
)
import traffic_dbt_execution as traffic_dbt  # noqa: E402
from traffic_ingest import transform_runtime  # noqa: E402
from traffic_ingest.transform_runtime import PREFLIGHT_SNAPSHOT_DAG_RUN_ID  # noqa: E402, F401
from traffic_ingest.assets import (  # noqa: E402
    TRAFFIC_INCIDENT_SILVER_ASSET_REF,
    TRAFFIC_INCIDENT_SILVER_MATERIALIZED_ALIAS,
    publish_through_alias,
    schedule_asset,
)
from traffic_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402
from traffic_ingest.runtime import build_traffic_manifest  # noqa: E402
from traffic_ingest.transform_dag_support import (  # noqa: E402
    SILVER_ASSET_CONTRACT,
    SILVER_SUCCESS_MARKER_KEY,
    STALE_INCIDENT_RUN_IDS_XCOM_KEY,  # noqa: F401
    SilverOutputEvidence,  # noqa: F401
    TransformFailurePorts,
    TransformIdentity,
    admit_transform,
    build_dbt_phase_task,
    coalesce_deferred_incident_runs,
    current_silver_output_evidence,
    record_classified_dbt_problem,
    resolve_traffic_silver_snapshot_run as _resolve_traffic_silver_snapshot_run,
    silver_output_evidence_from_dbt_run,
    write_success_marker,
)
from traffic_ingest.transform_metrics import (  # noqa: E402
    DBT_FAILURE_XCOM_KEY,
    DBT_RUN_RESULTS_RECORD_KEY,
    _current_run_results_path as _current_run_results_path,
    publish_dbt_run_metrics as _publish_dbt_run_metrics,
)
from traffic_ingest.transform_specs import SILVER_DBT_PHASE_SPECS  # noqa: E402
from traffic_lineage import enable_lineage_if_configured  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
LOGGER = logging.getLogger(__name__)
DBT_BIN = traffic_dbt.dbt_bin()
DBT_PROJECT = traffic_dbt.dbt_project_dir()
SNAPSHOT_TASK_ID = "resolve_traffic_snapshot_run"
TRAFFIC_BRONZE_ASSET = TRAFFIC_INCIDENT_BRONZE_ASSET
PIN_CRITICAL_PRIORITY = 10
DBT_RETRY_DELAY = timedelta(minutes=2)
DEFAULT_PARAMS = {
    "target": Param(
        default="dev",
        type="string",
        enum=["dev"],
        description="dbt target profile name (dev only until production rollout).",
    )
}
record_traffic_problem = problem_failure_callback(domain="traffic")


def resolve_traffic_snapshot_run(**context) -> str:
    return _resolve_traffic_silver_snapshot_run(
        context=context,
        incident_manifest_factory=build_traffic_manifest,
    )


def admit_traffic_silver_snapshot(**context) -> dict[str, object]:
    ti = context["ti"]
    incident_run_id = ti.xcom_pull(task_ids=SNAPSHOT_TASK_ID)
    try:
        return admit_transform(
            variable=Variable,
            marker_key=SILVER_SUCCESS_MARKER_KEY,
            identity=TransformIdentity.silver(incident_run_id),
            current_evidence_loader=current_silver_output_evidence,
        )
    except AirflowSkipException:
        coalesce_deferred_incident_runs(
            ti,
            snapshot_task_id=SNAPSHOT_TASK_ID,
            incident_manifest_factory=build_traffic_manifest,
        )
        raise


def publish_traffic_incident_silver_asset(**context) -> dict[str, object]:
    ti = context["ti"]
    incident_run_id = ti.xcom_pull(task_ids=SNAPSHOT_TASK_ID)
    evidence = silver_output_evidence_from_dbt_run(ti)
    metadata = {
        "source_id": "seoul_traffic_incident",
        "incident_run_id": TransformIdentity.silver(incident_run_id).incident_run_id,
        "silver_snapshot_id": evidence.snapshot_id,
        "compacted_files_fingerprint": evidence.compacted_files_fingerprint,
        "event_at": datetime.now(timezone.utc).isoformat(),
        "is_publishable": True,
        "contract": SILVER_ASSET_CONTRACT,
    }
    coalesce_deferred_incident_runs(
        ti,
        snapshot_task_id=SNAPSHOT_TASK_ID,
        incident_manifest_factory=build_traffic_manifest,
    )
    publish_through_alias(
        context,
        alias=TRAFFIC_INCIDENT_SILVER_MATERIALIZED_ALIAS,
        asset=TRAFFIC_INCIDENT_SILVER_ASSET_REF,
        metadata=metadata,
    )
    return metadata


def mark_traffic_silver_success(**context) -> dict[str, object]:
    ti = context["ti"]
    incident_run_id = ti.xcom_pull(task_ids=SNAPSHOT_TASK_ID)
    serialized = write_success_marker(
        variable=Variable,
        marker_key=SILVER_SUCCESS_MARKER_KEY,
        identity=TransformIdentity.silver(incident_run_id),
        evidence=silver_output_evidence_from_dbt_run(ti),
    )
    return {"marker": serialized}


def run_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    snapshot_task_id: str,
    silver_persisted: bool,
    fresh_parse: bool = False,
    snapshot_required: bool = False,
    silver_fence_mode: str | None = None,
    threads: int | None = None,
    **context,
) -> dict[str, object]:
    return transform_runtime.run_dbt_phase(
        dbt_command=dbt_command,
        selector=selector,
        snapshot_task_id=snapshot_task_id,
        silver_persisted=silver_persisted,
        fresh_parse=fresh_parse,
        snapshot_required=snapshot_required,
        silver_fence_mode=silver_fence_mode,
        threads=threads,
        dbt_bin=DBT_BIN,
        dbt_project=DBT_PROJECT,
        load_results=load_dbt_results,
        classify_failure=classify_dbt_failure,
        recovery_record_builder=build_recovery_record,
        persisted_from_results=silver_persisted_from_results,
        runner=subprocess.run,
        **context,
    )


def record_traffic_dbt_problem(context) -> None:
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


def publish_dbt_run_metrics(run_results_path: str | None = None, **context) -> dict:
    return _publish_dbt_run_metrics(
        run_results_path, dump_results=dump_dbt_run_results, **context
    )


with DAG(
    dag_id="traffic_incident_transform",
    description="Transform Traffic Incident Bronze into the Silver Asset.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=schedule_asset(TRAFFIC_INCIDENT_BRONZE_ASSET),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": DBT_RETRY_DELAY},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "traffic", "transform", "silver", "dbt"],
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
        pool=TRINO_HEAVY_POOL,
        priority_weight=PIN_CRITICAL_PRIORITY,
        weight_rule="absolute",
        on_failure_callback=record_traffic_problem,
    )
    admit_snapshot = PythonOperator(
        task_id="admit_traffic_silver_snapshot",
        python_callable=admit_traffic_silver_snapshot,
        pool=TRINO_HEAVY_POOL,
        priority_weight=PIN_CRITICAL_PRIORITY,
        weight_rule="absolute",
        on_failure_callback=record_traffic_problem,
    )
    dbt_phase_tasks = {
        spec.task_id: build_dbt_phase_task(
            spec,
            python_callable=run_dbt_phase,
            snapshot_task_id=SNAPSHOT_TASK_ID,
            retry_delay=DBT_RETRY_DELAY,
            pin_critical_priority=PIN_CRITICAL_PRIORITY,
            failure_callback=record_traffic_dbt_problem,
        )
        for spec in SILVER_DBT_PHASE_SPECS
    }
    publish_silver = PythonOperator(
        task_id="publish_traffic_incident_silver_asset",
        python_callable=publish_traffic_incident_silver_asset,
        outlets=[TRAFFIC_INCIDENT_SILVER_MATERIALIZED_ALIAS],
        pool=TRINO_HEAVY_POOL,
        priority_weight=PIN_CRITICAL_PRIORITY,
        weight_rule="absolute",
        on_failure_callback=record_traffic_problem,
    )
    mark_success = PythonOperator(
        task_id="mark_traffic_silver_success",
        python_callable=mark_traffic_silver_success,
        on_failure_callback=record_traffic_problem,
    )
    publish_metrics = PythonOperator(
        task_id="publish_dbt_run_metrics",
        python_callable=publish_dbt_run_metrics,
        on_failure_callback=record_traffic_problem,
    ).as_teardown(on_failure_fail_dagrun=False)

    chain = [
        validate_runtime,
        resolve_snapshot,
        admit_snapshot,
        *dbt_phase_tasks.values(),
        publish_silver,
        mark_success,
        publish_metrics,
    ]
    for upstream, downstream in zip(chain, chain[1:]):
        upstream >> downstream


enable_lineage_if_configured(dag)
