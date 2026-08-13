"""Paused, API-free reconciliation for Traffic Incident collection slots."""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from datetime import datetime, timezone

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

from common.collection_slots import (  # noqa: E402
    CollectionOutcome,
    DueSlotReconciler,
    ExpectedSlot,
    parse_activation_at,
    require_policy_boundary,
)
from common.collection_slots.receipts import CollectionSlotReceipts  # noqa: E402
from common.raw_manifest import validate_raw_manifest  # noqa: E402
from traffic_ingest.acc_info import KST  # noqa: E402
from traffic_ingest.bronze_dag_support import traffic_dag_schedule  # noqa: E402
from traffic_ingest.collection_slots import (  # noqa: E402
    INCIDENT_SOURCE_ID,
    floor_to_five_minutes,
    traffic_incident_slot,
)
from traffic_ingest.runtime import build_traffic_collection_slot_storage  # noqa: E402
from traffic_ingest.snapshot_receipt import TrafficSnapshotReceipts  # noqa: E402


DAG_ID = "traffic_incident_collection_slot_reconciliation"
TASK_ID = "reconcile_due_traffic_collection_slots"
_ACTIVATION_ENV = "ASK_SEOUL_COLLECTION_SLOT_ACTIVATION_AT"
_RAW_RETENTION_BOUNDARY_ENV = "ASK_SEOUL_TRAFFIC_RAW_RETENTION_BOUNDARY_AT"


def traffic_missing_outcome(
    slot: ExpectedSlot,
    *,
    raw_manifest_key: object | None,
    raw_object_count: object | None,
    source_result_code: object | None,
    raw_manifest_verified: bool,
    event_at: datetime | str,
    dag_id: str,
    dag_run_id: str,
) -> CollectionOutcome:
    """Select the missed-slot recovery state without accepting diagnostic raw."""
    if raw_manifest_verified:
        if not isinstance(raw_manifest_key, str) or not raw_manifest_key:
            raise ValueError("verified Traffic raw replay requires manifest_key")
        if (
            isinstance(raw_object_count, bool)
            or not isinstance(raw_object_count, int)
            or raw_object_count < 1
        ):
            raise ValueError("verified Traffic raw replay requires object count")
        return CollectionOutcome.create(
            expected_slot_id=slot.expected_slot_id,
            collection_state="missing_unknown",
            recovery_state="pending",
            recovery_class="raw_replay",
            gap_reason_code="missed_collection",
            recovery_evidence_code="raw_manifest_verified",
            event_at=event_at,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=TASK_ID,
            raw_manifest_key=raw_manifest_key,
            raw_object_count=raw_object_count,
            source_result_code=(
                source_result_code
                if isinstance(source_result_code, str) and source_result_code
                else None
            ),
        )
    return CollectionOutcome.create(
        expected_slot_id=slot.expected_slot_id,
        collection_state="missing_unknown",
        recovery_state="unrecoverable",
        recovery_class="none",
        gap_reason_code="missed_collection",
        event_at=event_at,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=TASK_ID,
    )


def _verified_raw_evidence(
    storage,
    raw_result: object,
    *,
    dag_run_id: str,
) -> dict[str, object] | None:
    if not isinstance(raw_result, Mapping):
        return None
    manifest_key = raw_result.get("manifest_key")
    raw_objects = raw_result.get("raw_objects")
    if not isinstance(manifest_key, str) or not manifest_key:
        return None
    if not isinstance(raw_objects, list) or not raw_objects:
        return None
    object_keys: list[str] = []
    for raw_object in raw_objects:
        if not isinstance(raw_object, Mapping):
            return None
        raw_object_key = raw_object.get("raw_object_key")
        if not isinstance(raw_object_key, str) or not raw_object_key:
            return None
        object_keys.append(raw_object_key)
    if len(object_keys) != len(set(object_keys)):
        return None
    try:
        manifest = storage.read_json(manifest_key)
        validate_raw_manifest(
            manifest,
            run_id=dag_run_id,
            dataset=INCIDENT_SOURCE_ID,
            object_keys=object_keys,
        )
    except (FileNotFoundError, TypeError, ValueError):
        return None
    return {
        "raw_manifest_key": manifest_key,
        "raw_object_count": len(object_keys),
        "source_result_code": raw_result.get("result_code"),
        "raw_manifest_verified": True,
    }


def _pending_evidence_by_slot(storage) -> dict[str, dict[str, object]]:
    evidence_by_slot: dict[str, dict[str, object]] = {}
    for receipt in TrafficSnapshotReceipts(storage).pending():
        slot_at = floor_to_five_minutes(receipt.logical_date).isoformat()
        evidence = _verified_raw_evidence(
            storage,
            receipt.raw_result,
            dag_run_id=receipt.snapshot_run_id,
        )
        if evidence is None:
            continue
        previous = evidence_by_slot.get(slot_at)
        if previous is not None and previous != evidence:
            raise ValueError(
                f"conflicting Traffic raw replay evidence for slot={slot_at}"
            )
        evidence_by_slot[slot_at] = evidence
    return evidence_by_slot


def _unique_slots(slots: list[ExpectedSlot]) -> tuple[ExpectedSlot, ...]:
    by_id = {slot.expected_slot_id: slot for slot in slots}
    return tuple(
        sorted(
            by_id.values(),
            key=lambda slot: (slot.collection_slot_at, slot.expected_slot_id),
        )
    )


def _candidate_at(context: dict) -> datetime:
    logical_date = context.get("logical_date")
    if isinstance(logical_date, datetime) and logical_date.tzinfo is not None:
        return logical_date.astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def reconcile_due_traffic_collection_slots(**context) -> dict[str, int]:
    activation_at = parse_activation_at(os.environ.get(_ACTIVATION_ENV))
    if activation_at is None:
        return {"declared": 0, "not_due": 0, "already_terminal": 0, "finalized": 0}
    recovery_boundary = require_policy_boundary(
        os.environ.get(_RAW_RETENTION_BOUNDARY_ENV),
        _RAW_RETENTION_BOUNDARY_ENV,
    )
    candidate_at = _candidate_at(context)
    storage = build_traffic_collection_slot_storage()
    receipts = CollectionSlotReceipts(storage)
    now = datetime.now(timezone.utc)
    evidence_by_slot = _pending_evidence_by_slot(storage)
    reconciler = DueSlotReconciler(
        storage=storage,
        receipts=receipts,
        clock=lambda: now,
        outcome_factory=lambda slot: traffic_missing_outcome(
            slot,
            **evidence_by_slot.get(
                slot.collection_slot_at,
                {
                    "raw_manifest_key": None,
                    "raw_object_count": None,
                    "source_result_code": None,
                    "raw_manifest_verified": False,
                },
            ),
            event_at=now,
            dag_id=DAG_ID,
            dag_run_id=str(context["run_id"]),
        ),
    )
    candidates = _unique_slots(
        [
            *reconciler.existing_slots(
                domain="traffic",
                source_id=INCIDENT_SOURCE_ID,
            ),
            traffic_incident_slot(
                candidate_at,
                recovery_boundary=recovery_boundary,
            ),
        ]
    )
    return reconciler.run(candidates, activation_at=activation_at).as_dict()


with DAG(
    dag_id=DAG_ID,
    description="Declare and settle Traffic Incident collection slots without source API calls.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=traffic_dag_schedule(),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ask_seoul", "traffic", "collection-slot", "reconciliation", "control"],
) as dag:
    reconcile = PythonOperator(
        task_id=TASK_ID,
        python_callable=reconcile_due_traffic_collection_slots,
    )
