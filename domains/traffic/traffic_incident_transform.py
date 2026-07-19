"""Airflow DAG: traffic silver/gold transform via dbt.

The bronze DAG stores TOPIS AccInfo raw XML in R2 and publishes verified
Iceberg bronze runs. This transform DAG consumes only publishable bronze runs
through the dbt models and keeps transform retries independent from API calls.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

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
from traffic_ingest.external_snapshot import (  # noqa: E402
    resolve_citydata_crowding_snapshot_id,
)
from traffic_ingest.assets import (  # noqa: E402
    TRAFFIC_FLOW_BRONZE_ASSET as TRAFFIC_FLOW_BRONZE_ASSET,
)
from traffic_ingest.transform_specs import (  # noqa: E402
    DBT_PHASE_SPECS,
    DBT_PHASE_TASK_IDS as DBT_PHASE_TASK_IDS,
)
from traffic_ingest.transform_test_tier import (  # noqa: E402
    MARK_TEST_TIER_TASK_ID,
    SELECT_TEST_TIER_TASK_ID,
    TrafficTestDecision as TrafficTestDecision,
    TrafficTestTier,
    _parse_traffic_test_decision as _parse_traffic_test_decision,
    choose_test_decision as choose_test_decision,
    mark_successful_decision as mark_successful_decision,
    mark_traffic_test_tier,
    select_traffic_test_tier,
)
from traffic_ingest.transform_metrics import (  # noqa: E402
    DBT_FAILURE_XCOM_KEY,
    DBT_RUN_RESULTS_RECORD_KEY,
    DOMAIN as DOMAIN,
    _current_run_results_path as _current_run_results_path,
    publish_dbt_run_metrics as _publish_dbt_run_metrics,
)
from traffic_ingest import transform_runtime  # noqa: E402
from traffic_ingest.transform_dag_support import (  # noqa: E402
    TRAFFIC_TRANSFORM_CRON_KST as TRAFFIC_TRANSFORM_CRON_KST,
    TransformFailurePorts,
    build_dbt_phase_task,
    record_classified_dbt_problem,
    resolve_traffic_snapshot_run as _resolve_traffic_snapshot_run,
    resolve_transform_snapshot_pair as resolve_transform_snapshot_pair,
    transform_schedule,
)
from traffic_lineage import enable_lineage_if_configured  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
LOGGER = logging.getLogger(__name__)
DBT_BIN = traffic_dbt.dbt_bin()
DBT_PROJECT = traffic_dbt.dbt_project_dir()
SNAPSHOT_TASK_ID = "resolve_traffic_snapshot_run"
FLOW_SNAPSHOT_XCOM_KEY = "traffic_flow_snapshot_dag_run_id"
CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY = "traffic_citydata_crowding_snapshot_id"
PREFLIGHT_SNAPSHOT_DAG_RUN_ID = "__traffic_preflight__"
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
# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_traffic_problem = problem_failure_callback(domain="traffic")


def resolve_traffic_snapshot_run(**context) -> str:
    return _resolve_traffic_snapshot_run(
        context=context,
        snapshot_pair_resolver=resolve_transform_snapshot_pair,
        incident_manifest_factory=build_traffic_manifest,
        flow_manifest_factory=build_traffic_flow_manifest,
        citydata_snapshot_resolver=resolve_citydata_crowding_snapshot_id,
        flow_xcom_key=FLOW_SNAPSHOT_XCOM_KEY,
        citydata_xcom_key=CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY,
    )


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
    selector_by_test_tier: dict[TrafficTestTier, str | None] | None = None,
    **context,
) -> dict[str, object]:
    """Compatibility adapter for the isolated Traffic transform runtime."""
    return transform_runtime.run_dbt_phase(
        dbt_command=dbt_command,
        selector=selector,
        snapshot_task_id=snapshot_task_id,
        silver_persisted=silver_persisted,
        fresh_parse=fresh_parse,
        snapshot_required=snapshot_required,
        citydata_snapshot_required=citydata_snapshot_required,
        silver_fence_mode=silver_fence_mode,
        threads=threads,
        selector_by_test_tier=selector_by_test_tier,
        dbt_bin=DBT_BIN,
        dbt_project=DBT_PROJECT,
        flow_xcom_key=FLOW_SNAPSHOT_XCOM_KEY,
        citydata_xcom_key=CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY,
        load_results=load_dbt_results,
        classify_failure=classify_dbt_failure,
        recovery_record_builder=build_recovery_record,
        persisted_from_results=silver_persisted_from_results,
        runner=subprocess.run,
        **context,
    )


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


def publish_dbt_run_metrics(run_results_path: str | None = None, **context) -> dict:
    return _publish_dbt_run_metrics(
        run_results_path,
        dump_results=dump_dbt_run_results,
        **context,
    )


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

    select_test_tier_task = PythonOperator(
        task_id=SELECT_TEST_TIER_TASK_ID,
        python_callable=select_traffic_test_tier,
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

    dbt_phase_tasks = {
        spec.task_id: build_dbt_phase_task(
            spec,
            python_callable=run_dbt_phase,
            snapshot_task_id=SNAPSHOT_TASK_ID,
            retry_delay=DBT_RETRY_DELAY,
            pin_critical_priority=PIN_CRITICAL_PRIORITY,
            failure_callback=record_traffic_dbt_problem,
        )
        for spec in DBT_PHASE_SPECS
    }
    pre_snapshot_tasks = [
        dbt_phase_tasks[spec.task_id]
        for spec in DBT_PHASE_SPECS
        if not spec.snapshot_required
    ]
    pinned_snapshot_tasks = [
        dbt_phase_tasks[spec.task_id]
        for spec in DBT_PHASE_SPECS
        if spec.snapshot_required
    ]

    mark_test_tier_task = PythonOperator(
        task_id=MARK_TEST_TIER_TASK_ID,
        python_callable=mark_traffic_test_tier,
        on_failure_callback=record_traffic_problem,
    )

    publish_dbt_metrics = PythonOperator(
        task_id="publish_dbt_run_metrics",
        python_callable=publish_dbt_run_metrics,
        on_failure_callback=record_traffic_problem,
    ).as_teardown(on_failure_fail_dagrun=False)

    transform_tasks = [
        validate_runtime,
        select_test_tier_task,
        *pre_snapshot_tasks,
        resolve_snapshot,
        *pinned_snapshot_tasks,
        mark_test_tier_task,
    ]
    pipeline_tasks = [*transform_tasks, publish_dbt_metrics]
    for upstream, downstream in zip(pipeline_tasks, pipeline_tasks[1:]):
        upstream >> downstream


enable_lineage_if_configured(dag)
