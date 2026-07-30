"""Airflow entrypoint for five-minute TOPIS raw snapshot landing."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
DOMAINS_DIR = os.path.dirname(DAG_DIR)
if DOMAINS_DIR not in sys.path:
    sys.path.insert(0, DOMAINS_DIR)
DAGS_ROOT_DIR = os.path.dirname(DOMAINS_DIR)
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.ops.product_observability import record_domain_stage_event  # noqa: E402
from traffic_ingest.acc_info import KST, resolve_acc_info_page_window  # noqa: E402
from traffic_ingest.assets import TRAFFIC_INCIDENT_RAW_ASSET_REF  # noqa: E402
from traffic_ingest.bronze_dag_support import (  # noqa: E402
    dag_run_conf,
    fail_fast_traffic_bronze,
    traffic_dag_schedule,
)
from traffic_ingest.incident_pipeline import LANDING_TASK_ID  # noqa: E402
from traffic_ingest.landing import (  # noqa: E402
    RunIdentity,
    TrafficCollectionMode,
    TrafficLandingRequest,
)
from traffic_ingest.runtime import build_incident_landing_lifecycle  # noqa: E402
from traffic_lineage import enable_lineage_if_configured  # noqa: E402


DAG_ID = "traffic_incident_landing"
record_traffic_problem = problem_failure_callback(
    domain="traffic", source_system="seoul_topis"
)
record_traffic_raw_product_event = record_domain_stage_event("traffic", "raw")
record_traffic_raw_product_failure = record_domain_stage_event(
    "traffic", "raw", status="failed"
)


@fail_fast_traffic_bronze
def land_traffic_incident_snapshot(**context) -> dict[str, object]:
    start_index, end_index, page_size = resolve_acc_info_page_window(
        dag_run_conf(context)
    )
    outcome = build_incident_landing_lifecycle().run(
        run=RunIdentity(DAG_ID, str(context["run_id"])),
        logical_date=context.get("logical_date") or datetime.now(KST),
        request=TrafficLandingRequest(
            start_index,
            end_index,
            page_size,
            mode=TrafficCollectionMode.FULL_SNAPSHOT,
        ),
    )
    outlet_events = context.get("outlet_events")
    if outlet_events is None:
        raise RuntimeError("Traffic raw Asset outlet event is unavailable")
    outlet_events[TRAFFIC_INCIDENT_RAW_ASSET_REF].extra = outcome.asset_metadata
    return {
        "raw_result": outcome.raw_result,
        "receipt_key": outcome.receipt_key,
        "snapshot_run_id": str(context["run_id"]),
    }


with DAG(
    dag_id=DAG_ID,
    description="Lands five-minute Seoul TOPIS AccInfo snapshots in R2.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=traffic_dag_schedule(),
    catchup=False,
    max_active_runs=2,
    tags=["ask_seoul", "traffic", "landing", "r2", "asset"],
) as dag:
    land_snapshot = PythonOperator(
        task_id=LANDING_TASK_ID,
        python_callable=land_traffic_incident_snapshot,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        outlets=[TRAFFIC_INCIDENT_RAW_ASSET_REF],
        on_success_callback=record_traffic_raw_product_event,
        on_failure_callback=[
            record_traffic_problem,
            record_traffic_raw_product_failure,
        ],
    )


enable_lineage_if_configured(dag)
