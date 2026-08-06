"""Airflow DAG: build and publish Traffic×Weather Gold independently of Core."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

DIR = os.path.dirname(os.path.abspath(__file__))
for path in (DIR, os.path.dirname(DIR), os.path.dirname(os.path.dirname(DIR))):
    if path not in sys.path:
        sys.path.insert(0, path)

import traffic_gold_transform as core  # noqa: E402
from common.assets import WEATHER_GOLD_PUBLICATION_READY_ASSET  # noqa: E402
from common.runtime_guard import default_target, validate_dev_runtime  # noqa: E402
from traffic_ingest.assets import (  # noqa: E402
    TRAFFIC_CROSS_DOMAIN_GOLD_PUBLICATION_READY_ASSET_REF,
    TRAFFIC_CROSS_DOMAIN_PUBLICATION_PRODUCT_IDS,
    TRAFFIC_GOLD_PUBLICATION_SCOPE_KEY,
    TRAFFIC_INCIDENT_SILVER_ASSET,
    schedule_asset,
)
from traffic_ingest.common.resources import TRINO_TRANSFORM_POOL  # noqa: E402
from traffic_ingest.transform_admission import (  # noqa: E402
    CROSS_DOMAIN_GOLD_SUCCESS_MARKER_KEY,
)
from traffic_ingest.transform_dag_support import (  # noqa: E402
    admit_transform,
    build_dbt_phase_task,
    current_silver_output_evidence,
    write_success_marker,
)
from traffic_ingest.transform_specs import DbtPhaseSpec  # noqa: E402
from traffic_lineage import enable_lineage_if_configured  # noqa: E402
from airflow.sdk import Param, Variable  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
SNAPSHOT_TASK_ID = core.SNAPSHOT_TASK_ID
CROSS_DOMAIN_SELECTOR = "ask_seoul_traffic_transform_cross_domain_gold_hot_build"
CROSS_DOMAIN_PHASE_SPECS = (
    DbtPhaseSpec(
        "dbt_deps_cross_domain_gold",
        "deps",
        workload=core.GOLD_DBT_PHASE_SPECS[0].workload,
        threads=None,
    ),
    DbtPhaseSpec(
        "dbt_run_cross_domain_gold",
        "build",
        CROSS_DOMAIN_SELECTOR,
        silver_persisted=True,
        fresh_parse=True,
        snapshot_required=True,
        pin_critical=True,
        admin_dong_crosswalk_pin_required=True,
    ),
)
DEFAULT_PARAMS = {
    "target": Param(
        default=default_target(),
        type="string",
        enum=list(core.TARGET_CHOICES),
    )
}


def admit_cross_domain_gold_snapshot(**context) -> dict[str, object]:
    return admit_transform(
        variable=Variable,
        marker_key=CROSS_DOMAIN_GOLD_SUCCESS_MARKER_KEY,
        identity=core._gold_identity(ti=context["ti"]),
        current_evidence_loader=current_silver_output_evidence,
    )


def mark_cross_domain_gold_success(**context) -> dict[str, object]:
    ti = context["ti"]
    serialized = write_success_marker(
        variable=Variable,
        marker_key=CROSS_DOMAIN_GOLD_SUCCESS_MARKER_KEY,
        identity=core._gold_identity(ti=ti),
        evidence=current_silver_output_evidence(),
    )
    outlet_events = context.get("outlet_events")
    if outlet_events is not None:
        outlet_events[TRAFFIC_CROSS_DOMAIN_GOLD_PUBLICATION_READY_ASSET_REF].extra = {
            "gold_dag_run_id": str(context.get("run_id") or ""),
            "gold_success_marker": serialized,
            TRAFFIC_GOLD_PUBLICATION_SCOPE_KEY: list(
                TRAFFIC_CROSS_DOMAIN_PUBLICATION_PRODUCT_IDS
            ),
        }
    return {"marker": serialized}


with DAG(
    dag_id="traffic_cross_domain_gold_transform",
    description="Build Traffic×Weather Gold after Traffic or Weather updates.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=(
        schedule_asset(TRAFFIC_INCIDENT_SILVER_ASSET)
        | schedule_asset(WEATHER_GOLD_PUBLICATION_READY_ASSET)
    ),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "traffic", "cross-domain", "gold", "dbt"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "traffic", "requested_target": "{{ params.target }}"},
        on_failure_callback=core.record_traffic_problem,
    )
    resolve_snapshot = PythonOperator(
        task_id=SNAPSHOT_TASK_ID,
        python_callable=core.resolve_traffic_gold_snapshot_run,
        pool=TRINO_TRANSFORM_POOL,
        priority_weight=core.PIN_CRITICAL_PRIORITY,
        weight_rule="absolute",
        on_failure_callback=[
            core.record_traffic_problem,
            core.record_traffic_gold_product_failure,
        ],
        on_success_callback=core.record_traffic_gold_product_event,
    )
    admit_snapshot = PythonOperator(
        task_id="admit_cross_domain_gold_snapshot",
        python_callable=admit_cross_domain_gold_snapshot,
        pool=TRINO_TRANSFORM_POOL,
        priority_weight=core.PIN_CRITICAL_PRIORITY,
        weight_rule="absolute",
        on_failure_callback=core.record_traffic_problem,
    )
    dbt_phase_tasks = {
        spec.task_id: build_dbt_phase_task(
            spec,
            python_callable=core.run_dbt_phase,
            snapshot_task_id=SNAPSHOT_TASK_ID,
            retry_delay=timedelta(minutes=2),
            pin_critical_priority=core.PIN_CRITICAL_PRIORITY,
            failure_callback=core.record_traffic_dbt_problem,
        )
        for spec in CROSS_DOMAIN_PHASE_SPECS
    }
    mark_success = PythonOperator(
        task_id="mark_cross_domain_gold_success",
        python_callable=mark_cross_domain_gold_success,
        outlets=[TRAFFIC_CROSS_DOMAIN_GOLD_PUBLICATION_READY_ASSET_REF],
        pool=TRINO_TRANSFORM_POOL,
        priority_weight=core.PIN_CRITICAL_PRIORITY,
        weight_rule="absolute",
        on_failure_callback=core.record_traffic_problem,
    )
    publish_metrics = PythonOperator(
        task_id="publish_dbt_run_metrics",
        python_callable=core.publish_dbt_run_metrics,
        on_failure_callback=core.record_traffic_problem,
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
