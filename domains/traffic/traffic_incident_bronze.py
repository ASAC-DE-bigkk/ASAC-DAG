"""Airflow entrypoint for receipt-driven Traffic Incident Bronze materialization."""

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
from traffic_ingest.acc_info import KST  # noqa: E402
from traffic_ingest.assets import (  # noqa: E402
    TRAFFIC_INCIDENT_BRONZE_ASSET_REF,
    TRAFFIC_INCIDENT_MATERIALIZED_ALIAS,
    materializer_schedule,
    publish_through_alias,
)
from traffic_ingest.bronze_dag_support import DAG_ID, current_dag_id  # noqa: E402
from traffic_ingest.common.resources import TRINO_INGEST_POOL  # noqa: E402
from traffic_ingest.errors import TrafficBronzeConfigurationError  # noqa: E402
from traffic_ingest.incident_pipeline import MATERIALIZER_TASK_ID  # noqa: E402
from traffic_ingest.runtime import (  # noqa: E402
    build_incident_materializer,
    build_traffic_snapshot_receipts,
)
from traffic_lineage import enable_lineage_if_configured  # noqa: E402


TRAFFIC_BRONZE_ASSET_REF = TRAFFIC_INCIDENT_BRONZE_ASSET_REF
record_traffic_problem = problem_failure_callback(
    domain="traffic", source_system="seoul_topis"
)
record_traffic_bronze_product_event = record_domain_stage_event("traffic", "bronze")
record_traffic_bronze_product_failure = record_domain_stage_event(
    "traffic", "bronze", status="failed"
)


def _materializer_batch_limit() -> int:
    raw_value = os.environ.get("ASK_SEOUL_TRAFFIC_MATERIALIZER_BATCH_SIZE", "24")
    try:
        configured = int(raw_value)
    except ValueError as exc:
        raise TrafficBronzeConfigurationError(
            "ASK_SEOUL_TRAFFIC_MATERIALIZER_BATCH_SIZE must be a positive integer"
        ) from exc
    if configured < 1:
        raise TrafficBronzeConfigurationError(
            "ASK_SEOUL_TRAFFIC_MATERIALIZER_BATCH_SIZE must be a positive integer"
        )
    return configured


def materialize_pending_traffic_incident_snapshots(**context) -> dict[str, object]:
    result = build_incident_materializer().run(
        materializer_dag_id=current_dag_id(context),
        materializer_run_id=str(context["run_id"]),
        limit=_materializer_batch_limit(),
    )
    if result.latest_asset_metadata is not None:
        publish_through_alias(
            context,
            alias=TRAFFIC_INCIDENT_MATERIALIZED_ALIAS,
            asset=TRAFFIC_BRONZE_ASSET_REF,
            metadata=result.latest_asset_metadata,
        )
    return {
        "processed": result.processed_count,
        "snapshot_run_ids": list(result.snapshot_run_ids),
    }


def acknowledge_materialized_traffic_incident_snapshots(
    context: dict,
) -> list[str]:
    result = context["ti"].xcom_pull(task_ids=MATERIALIZER_TASK_ID) or {}
    snapshot_run_ids = result.get("snapshot_run_ids") or []
    if not isinstance(snapshot_run_ids, list):
        raise RuntimeError("Traffic materializer result has malformed snapshot_run_ids")
    if not snapshot_run_ids:
        return []
    receipts = build_traffic_snapshot_receipts()
    acknowledged: list[str] = []
    for value in snapshot_run_ids:
        snapshot_run_id = str(value or "")
        if not snapshot_run_id:
            raise RuntimeError("Traffic materializer result contains an empty snapshot ID")
        receipts.acknowledge_materialized(snapshot_run_id)
        acknowledged.append(snapshot_run_id)
    return acknowledged


with DAG(
    dag_id=DAG_ID,
    description="Materializes pending TOPIS raw receipts into verified Iceberg Bronze.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=materializer_schedule(),
    catchup=False,
    max_active_runs=1,
    tags=["ask_seoul", "traffic", "bronze", "receipt", "asset", "iceberg"],
) as dag:
    materialize = PythonOperator(
        task_id=MATERIALIZER_TASK_ID,
        python_callable=materialize_pending_traffic_incident_snapshots,
        pool=TRINO_INGEST_POOL,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        outlets=[TRAFFIC_INCIDENT_MATERIALIZED_ALIAS],
        on_success_callback=[
            acknowledge_materialized_traffic_incident_snapshots,
            record_traffic_bronze_product_event,
        ],
        on_failure_callback=[
            record_traffic_problem,
            record_traffic_bronze_product_failure,
        ],
    )


enable_lineage_if_configured(dag)
