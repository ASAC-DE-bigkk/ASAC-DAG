from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

from common.collection_slots.contract import CollectionOutcome, ExpectedSlot
from common.collection_slots.materializer import (
    CollectionSlotMaterializer,
    EVENT_RECEIPTS_PREFIX,
    EXPECTED_RECEIPTS_PREFIX,
    MaterializationError,
)


def _expected_document(slot_id: str = "slot-1") -> dict[str, object]:
    slot = ExpectedSlot.create(
        contract_version="v1",
        domain="traffic",
        collection_contract_id="traffic.incident.v1",
        source_id="seoul_traffic_incident",
        collection_slot_at="2026-08-08T00:00:00+00:00",
        scheduled_at="2026-08-08T00:00:00+00:00",
        deadline_at="2026-08-08T00:15:00+00:00",
        grain={"source_id": "seoul_traffic_incident"},
        schedule_version="traffic-v1",
        is_scheduled=True,
        recovery_boundary_type="raw_retention",
        recovery_boundary="r2-control",
        declared_at="2026-08-08T00:00:01+00:00",
        declared_by="traffic_incident_landing",
    )
    document = slot.to_document()
    return document if slot_id == "slot-1" else dict(document, expected_slot_id=slot_id)


def _event_document(slot_id: str = "slot-1", event_id: str = "event-1") -> dict[str, object]:
    outcome = CollectionOutcome.create(
        expected_slot_id=slot_id,
        event_type="terminal",
        collection_state="observed",
        recovery_state="not_required",
        recovery_class="none",
        dag_id="traffic_incident_bronze",
        dag_run_id="run-1",
        task_id="materialize_pending_traffic_incident_snapshots",
        raw_manifest_key="raw/traffic/manifest.json",
        raw_object_count=1,
        row_count=6,
        source_result_code="INFO-000",
        event_at="2026-08-08T00:06:00+00:00",
    )
    document = outcome.to_document()
    return document if event_id == "event-1" else dict(document, event_id=event_id)


def _recovered_event_document(slot_id: str = "slot-1") -> dict[str, object]:
    outcome = CollectionOutcome.create(
        expected_slot_id=slot_id,
        event_type="terminal",
        collection_state="collection_failed",
        recovery_state="recovered",
        recovery_class="raw_replay",
        gap_reason_code="bronze_load_failed",
        dag_id="traffic_incident_bronze",
        dag_run_id="run-1",
        recovery_run_id="replay-run",
        recovered_at="2026-08-08T00:20:00+00:00",
        recovery_evidence_code="raw_manifest_verified",
        event_at="2026-08-08T00:06:00+00:00",
    )
    return outcome.to_document()


@dataclass
class FakeStorage:
    documents: dict[str, object]

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self.documents if key.startswith(prefix))

    def read_json(self, key: str) -> object:
        return self.documents[key]


@dataclass
class FakeSink:
    expected: list[dict[str, object]]
    events: list[dict[str, object]]

    def write_expected(self, rows: list[dict[str, object]]) -> int:
        self.expected.extend(rows)
        return len(rows)

    def write_events(self, rows: list[dict[str, object]]) -> int:
        self.events.extend(rows)
        return len(rows)


def _materializer(documents: dict[str, object], sink: FakeSink | None = None):
    sink = sink or FakeSink([], [])
    return (
        CollectionSlotMaterializer(FakeStorage(documents), sink),
        sink,
    )


def test_materializer_reads_and_validates_expected_and_event_receipts():
    expected = _expected_document()
    slot_id = str(expected["expected_slot_id"])
    materializer, sink = _materializer(
        {
            f"{EXPECTED_RECEIPTS_PREFIX}{slot_id}.json": expected,
            f"{EVENT_RECEIPTS_PREFIX}{slot_id}/event-1.json": _event_document(slot_id),
        }
    )

    result = materializer.run()

    assert result == {"expected": 1, "events": 1}
    assert sink.expected[0]["expected_slot_id"] == slot_id
    assert sink.events[0]["event_id"] == _event_document(slot_id)["event_id"]


def test_materializer_allows_legacy_missing_recovery_evidence_code_and_new_code():
    expected = _expected_document()
    slot_id = str(expected["expected_slot_id"])
    legacy_event = _event_document(slot_id)
    legacy_event.pop("recovery_evidence_code", None)
    new_event = _recovered_event_document(slot_id)
    materializer, sink = _materializer(
        {
            f"{EXPECTED_RECEIPTS_PREFIX}{slot_id}.json": expected,
            f"{EVENT_RECEIPTS_PREFIX}{slot_id}/legacy.json": legacy_event,
            f"{EVENT_RECEIPTS_PREFIX}{slot_id}/new.json": new_event,
        }
    )

    result = materializer.run()

    assert result == {"expected": 1, "events": 2}
    by_event_id = {str(row["event_id"]): row for row in sink.events}
    assert by_event_id[str(legacy_event["event_id"])]["recovery_evidence_code"] is None
    assert (
        by_event_id[str(new_event["event_id"])]["recovery_evidence_code"]
        == "raw_manifest_verified"
    )


def test_materializer_rejects_event_without_expected_slot():
    materializer, sink = _materializer(
        {f"{EVENT_RECEIPTS_PREFIX}missing/event-1.json": _event_document("missing")}
    )

    with pytest.raises(MaterializationError, match="expected_slot_id"):
        materializer.run()
    assert sink.expected == []
    assert sink.events == []


def test_materializer_rejects_conflicting_duplicate_event_documents():
    expected = _expected_document()
    slot_id = str(expected["expected_slot_id"])
    first = _event_document(slot_id)
    conflicting = dict(first, row_count=7)
    materializer, sink = _materializer(
        {
            f"{EXPECTED_RECEIPTS_PREFIX}{slot_id}.json": expected,
            f"{EVENT_RECEIPTS_PREFIX}{slot_id}/event-1.json": first,
            f"{EVENT_RECEIPTS_PREFIX}{slot_id}/event-2.json": conflicting,
        }
    )

    with pytest.raises(MaterializationError, match="event_id"):
        materializer.run()
    assert sink.expected == []
    assert sink.events == []


def test_materializer_rejects_document_with_tampered_expected_identity():
    document = _expected_document()
    slot_id = str(document["expected_slot_id"])
    document["grain_json"] = json.dumps({"different": "grain"}, separators=(",", ":"))
    materializer, sink = _materializer(
        {f"{EXPECTED_RECEIPTS_PREFIX}{slot_id}.json": document}
    )

    with pytest.raises(MaterializationError, match="expected_slot_id"):
        materializer.run()
    assert sink.expected == []
