import sys
from dataclasses import replace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.collection_slots.contract import CollectionOutcome, ExpectedSlot
from common.collection_slots.receipts import (
    CollectionSlotReceipts,
    ReceiptConflictError,
)
from common.storage import LocalStorage, R2Storage


class MemoryStorage:
    def __init__(self) -> None:
        self.documents: dict[str, object] = {}
        self.write_count = 0
        self.next_conditional_write_creates = True

    def write_json_if_absent(self, key: str, value: object) -> bool:
        if not self.next_conditional_write_creates:
            return False
        if key in self.documents:
            return False
        self.documents[key] = value
        self.write_count += 1
        return True

    def read_json(self, key: str) -> object:
        return self.documents[key]


def _expected() -> ExpectedSlot:
    return ExpectedSlot.create(
        contract_version="v1",
        domain="traffic",
        collection_contract_id="traffic.incident.v1",
        source_id="seoul_traffic_incident",
        collection_slot_at="2026-08-08T00:05:00+00:00",
        scheduled_at="2026-08-08T00:05:00+00:00",
        deadline_at="2026-08-08T00:20:00+00:00",
        grain={"source_id": "seoul_traffic_incident"},
        schedule_version="traffic-incident-5m-v1",
        is_scheduled=True,
        recovery_boundary_type="raw_retention",
        recovery_boundary="unknown",
        declared_at="2026-08-08T00:05:00+00:00",
        declared_by="traffic_incident_landing",
    )


def _outcome() -> CollectionOutcome:
    return CollectionOutcome.create(
        expected_slot_id=_expected().expected_slot_id,
        collection_state="observed",
        recovery_state="not_required",
        recovery_class="none",
        event_at="2026-08-08T00:08:00+00:00",
        dag_id="traffic_incident_bronze",
        dag_run_id="scheduled__2026-08-08T00:05:00+00:00",
        task_id="materialize_pending_traffic_incident_snapshots",
        raw_manifest_key="raw/traffic/manifest.json",
        raw_object_count=1,
        row_count=0,
        source_result_code="INFO-000",
    )


def test_identical_expected_receipt_is_written_once():
    storage = MemoryStorage()
    store = CollectionSlotReceipts(storage)

    assert store.record_expected(_expected()) == store.record_expected(_expected())
    assert storage.write_count == 1


def test_conflicting_expected_receipt_fails_closed():
    storage = MemoryStorage()
    store = CollectionSlotReceipts(storage)
    store.record_expected(_expected())

    with pytest.raises(ReceiptConflictError, match="expected_slot_id"):
        store.record_expected(
            replace(_expected(), deadline_at="2026-08-08T00:25:00+00:00")
        )


def test_existing_receipt_is_compared_after_conditional_write_loses_race():
    storage = MemoryStorage()
    store = CollectionSlotReceipts(storage)
    expected = _expected()
    key = store.expected_key(expected)
    storage.documents[key] = expected.to_document()
    storage.next_conditional_write_creates = False

    assert store.record_expected(expected) == key
    assert storage.write_count == 0


def test_outcome_receipt_uses_event_id_and_conflicting_retry_fails_closed():
    storage = MemoryStorage()
    store = CollectionSlotReceipts(storage)
    outcome = _outcome()

    assert store.record_outcome(outcome) == store.outcome_key(outcome)
    with pytest.raises(ReceiptConflictError, match="event_id"):
        store.record_outcome(
            replace(outcome, event_at="2026-08-08T00:09:00+00:00")
        )


def test_local_storage_conditional_write_preserves_first_document(tmp_path):
    storage = LocalStorage(str(tmp_path))

    assert storage.write_json_if_absent("receipts/a.json", {"attempt": 1}) is True
    assert storage.write_json_if_absent("receipts/a.json", {"attempt": 2}) is False
    assert storage.read_json("receipts/a.json") == {"attempt": 1}


def test_r2_storage_uses_if_none_match_for_receipt_create():
    calls: list[dict[str, object]] = []

    class S3:
        def put_object(self, **kwargs):
            calls.append(kwargs)

    storage = object.__new__(R2Storage)
    storage.bucket = "seoul-dev"
    storage._s3 = S3()

    assert storage.write_bytes_if_absent("receipts/a.json", b"payload") is True
    assert calls == [
        {
            "Bucket": "seoul-dev",
            "Key": "receipts/a.json",
            "Body": b"payload",
            "IfNoneMatch": "*",
        }
    ]
