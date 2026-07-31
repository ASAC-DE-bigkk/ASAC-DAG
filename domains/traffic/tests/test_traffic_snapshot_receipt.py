from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class MemoryStorage:
    def __init__(self) -> None:
        self.objects: dict[str, object] = {}
        self.operations: list[tuple[str, str]] = []

    def write_json(self, key: str, value: object) -> None:
        self.operations.append(("write", key))
        self.objects[key] = value

    def read_json(self, key: str) -> object:
        try:
            return self.objects[key]
        except KeyError as exc:
            raise FileNotFoundError(key) from exc

    def exists(self, key: str) -> bool:
        return key in self.objects

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self.objects if key.startswith(prefix))

    def delete(self, key: str) -> None:
        self.operations.append(("delete", key))
        self.objects.pop(key, None)


def _landed(run_id: str, snapshot_at: str):
    from traffic_ingest.snapshot_receipt import LandedSnapshot

    return LandedSnapshot(
        source_id="seoul_traffic_incident",
        producer_dag_id="traffic_incident_landing",
        snapshot_run_id=run_id,
        logical_date=snapshot_at,
        snapshot_at=snapshot_at,
        raw_result={
            "source_id": "seoul_traffic_incident",
            "raw_objects": [
                {
                    "raw_object_key": f"raw/{run_id}.xml",
                    "raw_hash": "a" * 64,
                    "collected_at": snapshot_at,
                }
            ],
            "page_count": 1,
            "parsed_rows": 4,
            "expected_rows": 4,
            "list_total_count": 4,
            "is_publishable": True,
        },
        event_at=snapshot_at,
    )


def test_landed_receipt_is_written_before_pending_marker_and_round_trips():
    from traffic_ingest.snapshot_receipt import TrafficSnapshotReceipts

    storage = MemoryStorage()
    receipts = TrafficSnapshotReceipts(storage)
    receipt = _landed("scheduled__2026-07-16T00:05:00+00:00", "2026-07-16T00:05:00+00:00")

    key = receipts.record_landed(receipt)

    assert key.endswith("/LANDED.json")
    assert storage.operations == [
        ("write", key),
        ("write", receipts.pending_key(receipt.snapshot_run_id)),
    ]
    assert receipts.is_pending(receipt.snapshot_run_id) is True
    assert receipts.pending() == [receipt]
    assert "secret" not in repr(storage.objects)


def test_pending_receipts_are_oldest_first_and_limit_is_applied_after_sorting():
    from traffic_ingest.snapshot_receipt import TrafficSnapshotReceipts

    storage = MemoryStorage()
    receipts = TrafficSnapshotReceipts(storage)
    newest = _landed("snapshot-new", "2026-07-16T00:10:00+00:00")
    oldest = _landed("snapshot-old", "2026-07-16T00:00:00+00:00")
    middle = _landed("snapshot-mid", "2026-07-16T00:05:00+00:00")
    for receipt in (newest, oldest, middle):
        receipts.record_landed(receipt)

    assert receipts.pending(limit=2) == [oldest, middle]


def test_pending_check_raises_for_storage_failure_instead_of_treating_it_as_acknowledged():
    from traffic_ingest.snapshot_receipt import TrafficSnapshotReceipts

    class DeniedStorage(MemoryStorage):
        def exists(self, _key: str) -> bool:
            return False

        def read_json(self, _key: str) -> object:
            error = RuntimeError("R2 access denied")
            error.response = {"Error": {"Code": "AccessDenied"}}
            raise error

    receipts = TrafficSnapshotReceipts(DeniedStorage())

    with pytest.raises(RuntimeError, match="access denied"):
        receipts.is_pending("snapshot-1")


def test_landed_retry_is_idempotent_but_divergent_payload_is_rejected():
    from dataclasses import replace

    from traffic_ingest.snapshot_receipt import (
        SnapshotReceiptConflict,
        TrafficSnapshotReceipts,
    )

    storage = MemoryStorage()
    receipts = TrafficSnapshotReceipts(storage)
    original = _landed("snapshot-1", "2026-07-16T00:00:00+00:00")
    first_key = receipts.record_landed(original)
    storage.operations.clear()

    assert receipts.record_landed(
        replace(original, event_at="2026-07-16T00:01:00+00:00")
    ) == first_key
    assert storage.operations == [
        ("write", receipts.pending_key(original.snapshot_run_id))
    ]

    with pytest.raises(SnapshotReceiptConflict, match="divergent LANDED"):
        receipts.record_landed(
            replace(original, raw_result={**original.raw_result, "parsed_rows": 5})
        )


def test_materialized_receipt_is_written_before_pending_ack_and_retry_is_safe():
    from traffic_ingest.snapshot_receipt import (
        MaterializedSnapshot,
        TrafficSnapshotReceipts,
    )

    storage = MemoryStorage()
    receipts = TrafficSnapshotReceipts(storage)
    landed = _landed("snapshot-1", "2026-07-16T00:00:00+00:00")
    receipts.record_landed(landed)
    storage.operations.clear()
    materialized = MaterializedSnapshot(
        source_id=landed.source_id,
        snapshot_run_id=landed.snapshot_run_id,
        snapshot_at=landed.snapshot_at,
        materializer_dag_id="traffic_incident_bronze",
        materializer_run_id="asset__materializer-1",
        row_count=4,
        raw_object_count=1,
        event_at="2026-07-16T00:02:00+00:00",
    )

    key = receipts.record_materialized(materialized)

    assert storage.operations == [("write", key)]
    assert receipts.is_pending(landed.snapshot_run_id) is True
    assert receipts.pending() == [landed]

    receipts.acknowledge_materialized(landed.snapshot_run_id)

    assert storage.operations[-1] == (
        "delete",
        receipts.pending_key(landed.snapshot_run_id),
    )
    assert receipts.is_pending(landed.snapshot_run_id) is False
    assert receipts.pending() == []
    storage.operations.clear()
    receipts.record_materialized(materialized)
    receipts.acknowledge_materialized(landed.snapshot_run_id)
    assert storage.operations == []


def test_pending_keeps_materialized_receipt_until_asset_success_ack():
    from traffic_ingest.snapshot_receipt import (
        MaterializedSnapshot,
        TrafficSnapshotReceipts,
    )

    storage = MemoryStorage()
    receipts = TrafficSnapshotReceipts(storage)
    landed = _landed("snapshot-1", "2026-07-16T00:00:00+00:00")
    receipts.record_landed(landed)
    materialized = MaterializedSnapshot(
        source_id=landed.source_id,
        snapshot_run_id=landed.snapshot_run_id,
        snapshot_at=landed.snapshot_at,
        materializer_dag_id="traffic_incident_bronze",
        materializer_run_id="asset__materializer-1",
        row_count=4,
        raw_object_count=1,
        event_at="2026-07-16T00:02:00+00:00",
    )
    storage.write_json(
        receipts.materialized_key(materialized), materialized.to_document()
    )
    storage.operations.clear()

    assert receipts.pending() == [landed]
    assert storage.operations == []


def test_receipt_keys_preserve_distinct_run_identities():
    from traffic_ingest.snapshot_receipt import TrafficSnapshotReceipts

    storage = MemoryStorage()
    receipts = TrafficSnapshotReceipts(storage)
    with_slash = _landed("scheduled__a/b", "2026-07-16T00:00:00+00:00")
    with_dash = _landed("scheduled__a-b", "2026-07-16T00:05:00+00:00")

    receipts.record_landed(with_slash)
    receipts.record_landed(with_dash)

    assert receipts.pending_key(with_slash.snapshot_run_id) != receipts.pending_key(
        with_dash.snapshot_run_id
    )
    assert receipts.pending() == [with_slash, with_dash]


def test_pending_summary_reports_count_and_oldest_age_without_scanning_terminal_rows_twice():
    from traffic_ingest.snapshot_receipt import TrafficSnapshotReceipts

    storage = MemoryStorage()
    receipts = TrafficSnapshotReceipts(storage)
    receipts.record_landed(_landed("snapshot-1", "2026-07-16T00:00:00+00:00"))
    receipts.record_landed(_landed("snapshot-2", "2026-07-16T00:05:00+00:00"))

    assert receipts.pending_summary(
        datetime(2026, 7, 16, 0, 12, tzinfo=timezone.utc)
    ) == {
        "count": 2,
        "oldest_snapshot_at": "2026-07-16T00:00:00+00:00",
        "oldest_age_minutes": 12,
    }
