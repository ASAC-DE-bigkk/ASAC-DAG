from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class LandingBatch:
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
        }


def _lifecycle(events, *, collect_error=None, failed_ledger_error=None):
    from traffic_ingest.incident_pipeline import IncidentLandingLifecycle

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
            return LandingBatch()

    class Receipts:
        def record_landed(self, receipt):
            events.append(("receipt", receipt))
            return "receipt/LANDED.json"

    return IncidentLandingLifecycle(
        runtime_guard=lambda: events.append(("runtime_guard", None)),
        ledger=Ledger(),
        landing=Landing(),
        receipts=Receipts(),
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
        ("ledger", "STARTED"),
        ("runtime_guard", None),
        ("collect", RunIdentity("traffic_incident_landing", "scheduled__snapshot-1")),
        ("receipt", events[3][1]),
        ("ledger", "SUCCESS"),
    ]
    receipt = events[3][1]
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
    assert not any(event[0] == "receipt" for event in events)


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
