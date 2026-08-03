"""Airflow DAG: combine Traffic Silver with compatible Flow and Citydata for Gold."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param, Variable

DIR = os.path.dirname(os.path.abspath(__file__))
for path in (DIR, os.path.dirname(DIR), os.path.dirname(os.path.dirname(DIR))):
    if path not in sys.path:
        sys.path.insert(0, path)

from common.discord import COLOR_FAIL, first_notice_for_run, send_embed  # noqa: E402
from common.errors.airflow import problem_failure_callback, problem_from_airflow_context  # noqa: E402
from common.errors.sink import R2ErrorSink  # noqa: E402
from common.ops.product_observability import record_domain_stage_event  # noqa: E402
from common.runmetrics import dump_dbt_run_results  # noqa: E402
from common.runtime_guard import (  # noqa: E402
    TARGET_CHOICES,
    default_target,
    validate_dev_runtime,
)
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
    TRAFFIC_FLOW_SILVER_ASSET,
    TRAFFIC_GOLD_PUBLICATION_PRODUCT_IDS,
    TRAFFIC_GOLD_PUBLICATION_READY_ASSET_REF,
    TRAFFIC_GOLD_PUBLICATION_SCOPE_KEY,
    TRAFFIC_INCIDENT_PUBLICATION_PRODUCT_IDS,
    TRAFFIC_INCIDENT_SILVER_ASSET,
    schedule_asset,
)
from traffic_ingest.common.resources import TRINO_TRANSFORM_POOL  # noqa: E402
from traffic_ingest.external_snapshot import (  # noqa: E402
    resolve_admin_dong_crosswalk_snapshot_id,
    resolve_citydata_crowding_snapshot_id,
    traffic_gold_anchor_exists,
)
from traffic_ingest.flow_ingest import build_traffic_flow_manifest  # noqa: E402
from traffic_ingest.runtime import build_traffic_manifest  # noqa: E402
from traffic_ingest.transform_admission import TransformIdentity, TransformSuccessMarker  # noqa: E402, F401
from traffic_ingest.transform_dag_support import (  # noqa: E402
    GOLD_SUCCESS_MARKER_KEY,
    SILVER_OUTPUT_EVIDENCE_XCOM_KEY,  # noqa: F401
    SILVER_SUCCESS_MARKER_KEY,  # noqa: F401
    SilverOutputEvidence,  # noqa: F401
    TransformFailurePorts,
    admit_transform,
    build_dbt_phase_task,
    current_silver_output_evidence,
    record_classified_dbt_problem,
    require_current_silver_output_evidence,
    require_publishable_incident_snapshot,
    resolve_traffic_gold_snapshot_run as _resolve_traffic_gold_snapshot_run,
    silver_output_evidence_from_resolver,
    write_success_marker,
)
from traffic_ingest.transform_metrics import (  # noqa: E402
    DBT_FAILURE_XCOM_KEY,
    DBT_RUN_RESULTS_RECORD_KEY,
    _current_run_results_path as _current_run_results_path,
    publish_dbt_run_metrics as _publish_dbt_run_metrics,
)
from traffic_ingest.transform_specs import GOLD_DBT_PHASE_SPECS  # noqa: E402
from traffic_ingest.transform_test_tier import (  # noqa: E402
    SELECT_TEST_TIER_TASK_ID,  # noqa: F401
    TrafficTestTier,  # noqa: F401
    mark_traffic_test_tier,  # noqa: F401
    select_traffic_test_tier,  # noqa: F401
)
from traffic_lineage import enable_lineage_if_configured  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
LOGGER = logging.getLogger(__name__)
DBT_BIN = traffic_dbt.dbt_bin()
DBT_PROJECT = traffic_dbt.dbt_project_dir()
SNAPSHOT_TASK_ID = "resolve_traffic_gold_snapshot_run"
FLOW_SNAPSHOT_XCOM_KEY = "traffic_flow_snapshot_dag_run_id"
CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY = "traffic_citydata_crowding_snapshot_id"
ADMIN_DONG_CROSSWALK_PIN_XCOM_KEY = "admin_dong_crosswalk_pin_snapshot_id"
GOLD_BOOTSTRAP_REQUIRED_XCOM_KEY = "traffic_gold_bootstrap_required"
GOLD_HOT_SELECTOR = "ask_seoul_traffic_transform_gold_hot_build"
GOLD_INCIDENT_HOT_SELECTOR = (
    "ask_seoul_traffic_transform_gold_incident_hot_build"
)
GOLD_BOOTSTRAP_HOT_SELECTOR = (
    "ask_seoul_traffic_transform_gold_bootstrap_hot_build"
)
GOLD_INCIDENT_BOOTSTRAP_HOT_SELECTOR = (
    "ask_seoul_traffic_transform_gold_incident_bootstrap_hot_build"
)
# Gold validation must win the next slot after its priority-1 build. Silver
# writers remain priority 10, preserving their precedence before Gold starts.
PIN_CRITICAL_PRIORITY = 20
DBT_RETRY_DELAY = timedelta(minutes=2)
DEFAULT_PARAMS = {
    "target": Param(
        default=default_target(), type="string", enum=list(TARGET_CHOICES)
    )
}
record_traffic_problem = problem_failure_callback(domain="traffic")
record_traffic_gold_product_event = record_domain_stage_event("traffic", "gold")
record_traffic_gold_product_failure = record_domain_stage_event(
    "traffic", "gold", status="failed"
)


def resolve_traffic_gold_snapshot_run(**context) -> str:
    incident_run_id = _resolve_traffic_gold_snapshot_run(
        context=context,
        variable=Variable,
        incident_manifest_factory=build_traffic_manifest,
        flow_manifest_factory=build_traffic_flow_manifest,
        citydata_snapshot_resolver=resolve_citydata_crowding_snapshot_id,
        admin_dong_crosswalk_snapshot_resolver=resolve_admin_dong_crosswalk_snapshot_id,
        current_silver_evidence_loader=current_silver_output_evidence,
        flow_xcom_key=FLOW_SNAPSHOT_XCOM_KEY,
        citydata_xcom_key=CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY,
        admin_dong_crosswalk_xcom_key=ADMIN_DONG_CROSSWALK_PIN_XCOM_KEY,
    )
    task_instance = context.get("ti") or context.get("task_instance")
    if task_instance is not None:
        task_instance.xcom_push(
            key=GOLD_BOOTSTRAP_REQUIRED_XCOM_KEY,
            value=not traffic_gold_anchor_exists(),
        )
    return incident_run_id


def resolve_gold_citydata_snapshot_id(*, ti):
    return ti.xcom_pull(
        task_ids=SNAPSHOT_TASK_ID,
        key=CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY,
    )


def _gold_identity(*, ti) -> TransformIdentity:
    incident_run_id = ti.xcom_pull(task_ids=SNAPSHOT_TASK_ID)
    flow_run_id = ti.xcom_pull(task_ids=SNAPSHOT_TASK_ID, key=FLOW_SNAPSHOT_XCOM_KEY)
    return TransformIdentity.gold(
        incident_run_id,
        flow_run_id=flow_run_id,
        citydata_snapshot_id=resolve_gold_citydata_snapshot_id(ti=ti),
    )


def _publication_product_ids(*, ti) -> tuple[str, ...]:
    flow_run_id = ti.xcom_pull(
        task_ids=SNAPSHOT_TASK_ID,
        key=FLOW_SNAPSHOT_XCOM_KEY,
    )
    if isinstance(flow_run_id, str) and flow_run_id.strip():
        return TRAFFIC_GOLD_PUBLICATION_PRODUCT_IDS
    return TRAFFIC_INCIDENT_PUBLICATION_PRODUCT_IDS


def admit_traffic_gold_snapshot(**context) -> dict[str, object]:
    ti = context["ti"]
    return admit_transform(
        variable=Variable,
        marker_key=GOLD_SUCCESS_MARKER_KEY,
        identity=_gold_identity(ti=ti),
        current_evidence_loader=current_silver_output_evidence,
    )


def mark_traffic_gold_success(**context) -> dict[str, object]:
    """Persist the admission marker only after the hot build receipt passes."""
    ti = context["ti"]
    serialized = write_success_marker(
        variable=Variable,
        marker_key=GOLD_SUCCESS_MARKER_KEY,
        identity=_gold_identity(ti=ti),
        evidence=current_silver_output_evidence(),
    )
    outlet_events = context.get("outlet_events")
    if outlet_events is not None:
        outlet_events[TRAFFIC_GOLD_PUBLICATION_READY_ASSET_REF].extra = {
            "gold_dag_run_id": str(context.get("run_id") or ""),
            "gold_success_marker": serialized,
            TRAFFIC_GOLD_PUBLICATION_SCOPE_KEY: list(
                _publication_product_ids(ti=ti)
            ),
        }
    return {"marker": serialized}


def run_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    snapshot_task_id: str,
    silver_persisted: bool,
    fresh_parse: bool = False,
    snapshot_required: bool = False,
    citydata_snapshot_required: bool = False,
    admin_dong_crosswalk_pin_required: bool = False,
    threads: int | None = None,
    selector_by_test_tier=None,
    selector_when_flow_missing: str | None = None,
    selector_by_test_tier_when_flow_missing=None,
    **context,
) -> dict[str, object]:
    if dbt_command == "build" and selector == GOLD_HOT_SELECTOR:
        bootstrap_required = context["ti"].xcom_pull(
            task_ids=SNAPSHOT_TASK_ID,
            key=GOLD_BOOTSTRAP_REQUIRED_XCOM_KEY,
        )
        if not isinstance(bootstrap_required, bool):
            raise AirflowFailException(
                "Traffic Gold bootstrap requirement is unavailable"
            )
        if bootstrap_required:
            selector = GOLD_BOOTSTRAP_HOT_SELECTOR
            selector_when_flow_missing = GOLD_INCIDENT_BOOTSTRAP_HOT_SELECTOR

    def guard_current_silver_output() -> None:
        mismatch_action = "skip" if dbt_command in {"run", "build"} else "fail"
        expected_evidence = silver_output_evidence_from_resolver(
            context["ti"],
            snapshot_task_id=snapshot_task_id,
        )
        require_current_silver_output_evidence(
            expected_evidence,
            current_evidence_loader=current_silver_output_evidence,
            mismatch_action=mismatch_action,
        )
        incident_run_id = context["ti"].xcom_pull(task_ids=snapshot_task_id)
        require_publishable_incident_snapshot(
            build_traffic_manifest(),
            str(incident_run_id),
            mismatch_action=mismatch_action,
        )

    return transform_runtime.run_dbt_phase(
        dbt_command=dbt_command,
        selector=selector,
        snapshot_task_id=snapshot_task_id,
        silver_persisted=silver_persisted,
        fresh_parse=fresh_parse,
        snapshot_required=snapshot_required,
        citydata_snapshot_required=citydata_snapshot_required,
        admin_dong_crosswalk_pin_required=admin_dong_crosswalk_pin_required,
        threads=threads,
        selector_by_test_tier=selector_by_test_tier,
        selector_when_flow_missing=selector_when_flow_missing,
        selector_by_test_tier_when_flow_missing=(
            selector_by_test_tier_when_flow_missing
        ),
        dbt_bin=DBT_BIN,
        dbt_project=DBT_PROJECT,
        flow_xcom_key=FLOW_SNAPSHOT_XCOM_KEY,
        citydata_xcom_key=CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY,
        admin_dong_crosswalk_xcom_key=ADMIN_DONG_CROSSWALK_PIN_XCOM_KEY,
        load_results=load_dbt_results,
        classify_failure=classify_dbt_failure,
        recovery_record_builder=build_recovery_record,
        persisted_from_results=silver_persisted_from_results,
        pre_execution_guard=(
            guard_current_silver_output if snapshot_required else None
        ),
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
    dag_id="traffic_gold_transform",
    description="Build Traffic Gold from Silver, compatible Flow, and Citydata.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=schedule_asset(TRAFFIC_INCIDENT_SILVER_ASSET)
    | schedule_asset(TRAFFIC_FLOW_SILVER_ASSET),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": DBT_RETRY_DELAY},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "traffic", "transform", "gold", "dbt"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "traffic", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_traffic_problem,
    )
    resolve_snapshot = PythonOperator(
        task_id=SNAPSHOT_TASK_ID,
        python_callable=resolve_traffic_gold_snapshot_run,
        pool=TRINO_TRANSFORM_POOL,
        priority_weight=PIN_CRITICAL_PRIORITY,
        weight_rule="absolute",
        on_failure_callback=[
            record_traffic_problem,
            record_traffic_gold_product_failure,
        ],
        on_success_callback=record_traffic_gold_product_event,
    )
    admit_snapshot = PythonOperator(
        task_id="admit_traffic_gold_snapshot",
        python_callable=admit_traffic_gold_snapshot,
        pool=TRINO_TRANSFORM_POOL,
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
        for spec in GOLD_DBT_PHASE_SPECS
    }
    mark_success = PythonOperator(
        task_id="mark_traffic_gold_success",
        python_callable=mark_traffic_gold_success,
        outlets=[TRAFFIC_GOLD_PUBLICATION_READY_ASSET_REF],
        pool=TRINO_TRANSFORM_POOL,
        priority_weight=PIN_CRITICAL_PRIORITY,
        weight_rule="absolute",
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
        mark_success,
        publish_metrics,
    ]
    for upstream, downstream in zip(chain, chain[1:]):
        upstream >> downstream


enable_lineage_if_configured(dag)
