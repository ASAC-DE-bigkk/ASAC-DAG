from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class LandingBatch:
    def __init__(self, raw_updates=None):
        self._raw_updates = raw_updates or {}

    def to_xcom(self):
        return {
            "source_id": "seoul_traffic_incident",
            "raw_objects": [
                {
                    "raw_object_key": "raw/traffic/page.xml",
                    "raw_hash": "a" * 64,
                    "collected_at": "2026-07-16T00:05:02+00:00",
                }
            ],
            "raw_object_keys": ["raw/traffic/page.xml"],
            "page_count": 1,
            "parsed_rows": 4,
            "expected_rows": 4,
            "list_total_count": 4,
            "is_publishable": True,
            **self._raw_updates,
        }


def _lifecycle(
    events,
    *,
    collect_error=None,
    failed_ledger_error=None,
    expected_receipt_error=None,
    landed_receipt_error=None,
    raw_updates=None,
    raw_manifest_is_verified=None,
    slot_for_logical_date=None,
):
    from traffic_ingest.incident_pipeline import IncidentLandingLifecycle
    from traffic_ingest.collection_slots import traffic_incident_slot

    class Ledger:
        def record(self, **kwargs):
            events.append(("ledger", kwargs["status"]))
            if kwargs["status"] == "FAILED" and failed_ledger_error:
                raise failed_ledger_error

    class Landing:
        def collect(self, run, request):
            events.append(("collect", run, request))
            if collect_error:
                raise collect_error
            return LandingBatch(raw_updates=raw_updates)

    class Receipts:
        def record_landed(self, receipt):
            events.append(("receipt", receipt))
            if landed_receipt_error:
                raise landed_receipt_error
            return "receipt/LANDED.json"

    class SlotReceipts:
        def record_expected(self, slot):
            events.append(("slot_expected", slot))
            if expected_receipt_error:
                raise expected_receipt_error
            return "receipt/expected.json"

        def record_outcome(self, outcome):
            events.append(("slot_outcome", outcome))
            return "receipt/outcome.json"

    return IncidentLandingLifecycle(
        runtime_guard=lambda: events.append(("runtime_guard", None)),
        ledger=Ledger(),
        landing=Landing(),
        receipts=Receipts(),
        slot_receipts=SlotReceipts(),
        slot_for_logical_date=slot_for_logical_date
        or (
            lambda logical_date: traffic_incident_slot(
                logical_date,
                recovery_boundary="2026-07-01T00:00:00+00:00",
            )
        ),
        raw_manifest_is_verified=raw_manifest_is_verified or (lambda *_args, **_kwargs: False),
        clock=lambda: datetime(2026, 7, 16, 0, 5, 3, tzinfo=timezone.utc),
    )


def test_landing_lifecycle_records_durable_state_in_order():
    from traffic_ingest.landing import RunIdentity, TrafficLandingRequest

    events = []
    lifecycle = _lifecycle(events)
    request = TrafficLandingRequest(1, 1000, 1000)

    outcome = lifecycle.run(
        run=RunIdentity("traffic_incident_landing", "scheduled__snapshot-1"),
        logical_date=datetime(2026, 7, 16, 0, 5, tzinfo=timezone.utc),
        request=request,
    )

    assert [event[0:2] for event in events] == [
        ("slot_expected", events[0][1]),
        ("ledger", "STARTED"),
        ("runtime_guard", None),
        ("collect", RunIdentity("traffic_incident_landing", "scheduled__snapshot-1")),
        ("receipt", events[4][1]),
        ("ledger", "SUCCESS"),
    ]
    slot = events[0][1]
    assert slot.collection_slot_at == "2026-07-16T00:05:00+00:00"
    receipt = events[4][1]
    assert receipt.snapshot_run_id == "scheduled__snapshot-1"
    assert receipt.snapshot_at == "2026-07-16T00:05:02+00:00"
    assert receipt.raw_result == outcome.raw_result
    assert outcome.receipt_key == "receipt/LANDED.json"
    assert outcome.asset_metadata == {
        "source_id": "seoul_traffic_incident",
        "snapshot_run_id": "scheduled__snapshot-1",
        "event_at": "2026-07-16T00:05:02+00:00",
        "raw_object_count": 1,
        "payload_hash": "a" * 64,
        "is_publishable": True,
    }


def test_landing_lifecycle_records_failed_without_masking_collection_error():
    from traffic_ingest.landing import RunIdentity, TrafficLandingRequest

    events = []
    source_error = RuntimeError("source unavailable")
    lifecycle = _lifecycle(
        events,
        collect_error=source_error,
        failed_ledger_error=RuntimeError("ledger unavailable"),
    )

    with pytest.raises(RuntimeError, match="source unavailable") as raised:
        lifecycle.run(
            run=RunIdentity("traffic_incident_landing", "scheduled__snapshot-1"),
            logical_date="2026-07-16T00:05:00+00:00",
            request=TrafficLandingRequest(1, 1000, 1000),
        )

    assert raised.value is source_error
    assert [(event[0], event[1]) for event in events if event[0] == "ledger"] == [
        ("ledger", "STARTED"),
        ("ledger", "FAILED"),
    ]
    outcome = next(event[1] for event in events if event[0] == "slot_outcome")
    assert outcome.collection_state == "collection_failed"
    assert outcome.recovery_state == "unrecoverable"
    assert outcome.recovery_class == "none"
    assert outcome.gap_reason_code == "landing_failed"
    assert not any(event[0] == "receipt" for event in events)


def test_landing_lifecycle_record_landed_failure_after_raw_manifest_is_raw_replay_pending():
    from traffic_ingest.landing import RunIdentity, TrafficLandingRequest

    events = []
    landing_failure = RuntimeError("snapshot receipt unavailable")
    lifecycle = _lifecycle(
        events,
        landed_receipt_error=landing_failure,
        raw_manifest_is_verified=lambda *_args, **_kwargs: True,
        raw_updates={
            "manifest_key": "raw/traffic/scheduled__snapshot-1/_manifest.json",
            "result_code": "INFO-000",
        },
    )

    with pytest.raises(RuntimeError, match="snapshot receipt unavailable") as raised:
        lifecycle.run(
            run=RunIdentity("traffic_incident_landing", "scheduled__snapshot-1"),
            logical_date="2026-07-16T00:05:00+00:00",
            request=TrafficLandingRequest(1, 1000, 1000),
        )

    assert raised.value is landing_failure
    outcome = next(event[1] for event in events if event[0] == "slot_outcome")
    assert outcome.collection_state == "collection_failed"
    assert outcome.recovery_state == "pending"
    assert outcome.recovery_class == "raw_replay"
    assert outcome.gap_reason_code == "landing_failed"
    assert outcome.raw_manifest_key == "raw/traffic/scheduled__snapshot-1/_manifest.json"
    assert outcome.raw_object_count == 1
    assert outcome.source_result_code == "INFO-000"
    assert outcome.recovery_evidence_code == "raw_manifest_verified"
    assert outcome.dag_id == "traffic_incident_landing"
    assert outcome.dag_run_id == "scheduled__snapshot-1"
    assert outcome.task_id == "land_traffic_incident_snapshot"


def test_landing_lifecycle_does_not_promote_unverified_manifest_to_raw_replay():
    from traffic_ingest.landing import RunIdentity, TrafficLandingRequest

    events = []
    landing_failure = RuntimeError("snapshot receipt unavailable")
    lifecycle = _lifecycle(
        events,
        landed_receipt_error=landing_failure,
        raw_updates={
            "manifest_key": "raw/traffic/scheduled__snapshot-1/_manifest.json",
            "result_code": "INFO-000",
        },
    )

    with pytest.raises(RuntimeError, match="snapshot receipt unavailable"):
        lifecycle.run(
            run=RunIdentity("traffic_incident_landing", "scheduled__snapshot-1"),
            logical_date="2026-07-16T00:05:00+00:00",
            request=TrafficLandingRequest(1, 1000, 1000),
        )

    outcome = next(event[1] for event in events if event[0] == "slot_outcome")
    assert outcome.recovery_state == "unrecoverable"
    assert outcome.recovery_class == "none"
    assert outcome.raw_manifest_key is None
    assert outcome.recovery_evidence_code is None


def test_landing_lifecycle_pre_activation_slot_does_not_mutate_slot_receipts():
    from traffic_ingest.landing import RunIdentity, TrafficLandingRequest

    events = []
    lifecycle = _lifecycle(events, slot_for_logical_date=lambda _logical_date: None)

    lifecycle.run(
        run=RunIdentity("traffic_incident_landing", "scheduled__snapshot-1"),
        logical_date="2026-07-16T00:05:00+00:00",
        request=TrafficLandingRequest(1, 1000, 1000),
    )

    assert not any(event[0].startswith("slot_") for event in events)
    assert [event[0] for event in events] == [
        "ledger",
        "runtime_guard",
        "collect",
        "receipt",
        "ledger",
    ]


def test_landing_lifecycle_stops_before_topis_when_expected_receipt_fails():
    from traffic_ingest.landing import RunIdentity, TrafficLandingRequest

    events = []
    lifecycle = _lifecycle(
        events,
        expected_receipt_error=RuntimeError("collection receipt unavailable"),
    )

    with pytest.raises(RuntimeError, match="collection receipt unavailable"):
        lifecycle.run(
            run=RunIdentity("traffic_incident_landing", "scheduled__snapshot-1"),
            logical_date="2026-07-16T00:05:00+00:00",
            request=TrafficLandingRequest(1, 1000, 1000),
        )

    assert [event[0] for event in events] == ["slot_expected", "ledger"]
    assert events[-1] == ("ledger", "FAILED")


def test_landing_outcome_aggregates_multiple_payload_hashes_deterministically():
    from traffic_ingest.incident_pipeline import landing_asset_metadata

    raw_result = LandingBatch().to_xcom()
    raw_result["raw_objects"].append(
        {
            "raw_object_key": "raw/traffic/page-2.xml",
            "raw_hash": "b" * 64,
            "collected_at": "2026-07-16T00:05:01+00:00",
        }
    )

    first = landing_asset_metadata("snapshot-1", raw_result)
    raw_result["raw_objects"].reverse()
    second = landing_asset_metadata("snapshot-1", raw_result)

    assert first == second
    assert first["event_at"] == "2026-07-16T00:05:02+00:00"
    assert len(first["payload_hash"]) == 64
