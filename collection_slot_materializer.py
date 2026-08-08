"""Batch R2 collection-slot receipts into Iceberg expected/event relations."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
TRAFFIC_DOMAIN_DIR = os.path.join(DAG_DIR, "domains", "traffic")
if os.path.isdir(TRAFFIC_DOMAIN_DIR) and TRAFFIC_DOMAIN_DIR not in sys.path:
    sys.path.insert(0, TRAFFIC_DOMAIN_DIR)

from common.collection_slots.iceberg_sink import TrinoCollectionSlotSink  # noqa: E402
from common.collection_slots.materializer import CollectionSlotMaterializer  # noqa: E402
from common.collection_slots.reconciler import (  # noqa: E402
    CollectionSlotReceiptReconciler,
    ReconciliationError,
)
from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from common.storage import resolve_storage  # noqa: E402
from traffic_ingest.collection_slots import traffic_incident_slot  # noqa: E402


DAG_ID = "collection_slot_materializer"
TASK_ID = "materialize_collection_slot_receipts"
record_materializer_problem = problem_failure_callback(
    domain="collection_slots",
    source_system="r2_collection_slot_receipts",
)


def materialize_collection_slot_receipts(**_context) -> dict[str, int]:
    """Materialize all validated receipts; errors remain Airflow task failures."""
    validate_dev_runtime("traffic")
    storage = resolve_storage()
    reconciliation = CollectionSlotReceiptReconciler(
        storage,
        _resolve_legacy_expected_slot,
    ).run()
    sink = TrinoCollectionSlotSink()
    sink.ensure_tables()
    result = CollectionSlotMaterializer(storage, sink).run()
    return {**reconciliation.as_dict(), **result}


def _resolve_legacy_expected_slot(event: dict[str, object]):
    """Rebuild pre-rollout Traffic evidence only when identity is exact."""
    if event.get("dag_id") != "traffic_incident_bronze":
        return None
    if not str(event.get("raw_manifest_key", "")).startswith(
        "raw/traffic/seoul_traffic_incident/"
    ):
        return None
    dag_run_id = str(event.get("dag_run_id", ""))
    if "__" not in dag_run_id:
        raise ReconciliationError(f"legacy Traffic run id is malformed: {dag_run_id}")
    slot = traffic_incident_slot(dag_run_id.split("__", 1)[1])
    if slot.expected_slot_id != event.get("expected_slot_id"):
        raise ReconciliationError(
            "legacy Traffic run does not reproduce expected_slot_id: "
            + str(event.get("expected_slot_id"))
        )
    return slot


with DAG(
    dag_id=DAG_ID,
    description="Materializes immutable collection-slot receipts into Iceberg state tables.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ask_seoul", "collection_slots", "iceberg", "control"],
) as dag:
    PythonOperator(
        task_id=TASK_ID,
        python_callable=materialize_collection_slot_receipts,
        pool="trino_heavy",
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        on_failure_callback=[record_materializer_problem],
    )
