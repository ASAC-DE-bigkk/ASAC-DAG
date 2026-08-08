from __future__ import annotations

from dataclasses import dataclass
import json

import pytest

from common.collection_slots.contract import ExpectedSlot
from common.collection_slots.materializer import (
    EVENT_RECEIPTS_PREFIX,
    EXPECTED_RECEIPTS_PREFIX,
)
from common.collection_slots.reconciler import (
    CollectionSlotReceiptReconciler,
    ReconciliationError,
)
from common.tests.test_collection_slot_materializer import (
    _event_document,
    _expected_document,
)


@dataclass
class FakeStorage:
    documents: dict[str, object]

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self.documents if key.startswith(prefix))

    def read_json(self, key: str) -> object:
        return self.documents[key]

    def write_json_if_absent(self, key: str, value: object) -> bool:
        if key in self.documents:
            return False
        self.documents[key] = value
        return True


def _slot_from_document(document: dict[str, object]) -> ExpectedSlot:
    return ExpectedSlot.create(
        contract_version=document["contract_version"],
        domain=document["domain"],
        collection_contract_id=document["collection_contract_id"],
        source_id=document["source_id"],
        collection_slot_at=document["collection_slot_at"],
        scheduled_at=document["scheduled_at"],
        deadline_at=document["deadline_at"],
        grain=json.loads(document["grain_json"]),
        schedule_version=document["schedule_version"],
        is_scheduled=document["is_scheduled"],
        recovery_boundary_type=document["recovery_boundary_type"],
        recovery_boundary=document["recovery_boundary"],
        declared_at=document["declared_at"],
        declared_by=document["declared_by"],
    )


def test_reconciler_repairs_only_an_exact_legacy_slot_and_is_idempotent():
    expected = _expected_document()
    slot_id = str(expected["expected_slot_id"])
    storage = FakeStorage(
        {
            f"{EVENT_RECEIPTS_PREFIX}expected_slot_id={slot_id}/event-1.json": _event_document(slot_id),
        }
    )
    reconciler = CollectionSlotReceiptReconciler(
        storage,
        lambda _event: _slot_from_document(expected),
    )

    first = reconciler.run()
    second = reconciler.run()

    assert first.repaired_expected == 1
    assert first.scanned_events == 1
    assert second.repaired_expected == 0
    assert second.existing_expected == 1
    assert any(key.startswith(EXPECTED_RECEIPTS_PREFIX) for key in storage.documents)


def test_reconciler_fails_closed_when_no_adapter_can_resolve_event():
    slot_id = str(_expected_document()["expected_slot_id"])
    storage = FakeStorage(
        {
            f"{EVENT_RECEIPTS_PREFIX}expected_slot_id={slot_id}/event-1.json": _event_document(slot_id),
        }
    )

    with pytest.raises(ReconciliationError, match="unresolvable"):
        CollectionSlotReceiptReconciler(storage, lambda _event: None).run()


def test_reconciler_rejects_adapter_identity_mismatch():
    expected = _expected_document()
    slot_id = str(expected["expected_slot_id"])
    storage = FakeStorage(
        {
            f"{EVENT_RECEIPTS_PREFIX}expected_slot_id={slot_id}/event-1.json": _event_document(slot_id),
        }
    )
    mismatched = dict(expected, collection_slot_at="2026-08-08T00:05:00+00:00")

    with pytest.raises(ReconciliationError, match="identity mismatch"):
        CollectionSlotReceiptReconciler(
            storage,
            lambda _event: _slot_from_document(mismatched),
        ).run()
