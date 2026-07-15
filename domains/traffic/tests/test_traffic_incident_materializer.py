from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _receipt(run_id: str, snapshot_at: str):
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
                    "raw_hash": ("a" if run_id.endswith("1") else "b") * 64,
                    "collected_at": snapshot_at,
                }
            ],
            "raw_object_keys": [f"raw/{run_id}.xml"],
            "page_count": 1,
            "parsed_rows": 4,
            "expected_rows": 4,
            "list_total_count": 4,
            "is_publishable": True,
        },
        event_at=snapshot_at,
    )


def test_materializer_preserves_landing_run_ids_and_publishes_latest_batch_event():
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    events = []
    old = _receipt("snapshot-1", "2026-07-16T00:00:00+00:00")
    new = _receipt("snapshot-2", "2026-07-16T00:05:00+00:00")

    class Receipts:
        def pending(self, *, limit):
            events.append(("pending", limit))
            return [old, new]

        def record_materialized(self, receipt):
            events.append(("materialized", receipt.snapshot_run_id))

    class Manifest:
        def start(self, run, **metrics):
            events.append(("manifest_start", run.run_id, metrics))

        def publish(self, run, **metrics):
            events.append(("manifest_publish", run.run_id, metrics))

        def fail(self, *_args, **_kwargs):
            pytest.fail("successful materialization must not fail manifest")

    def load(raw_result, snapshot_run_id):
        events.append(("load", snapshot_run_id))
        assert raw_result["raw_object_keys"] == [f"raw/{snapshot_run_id}.xml"]
        return {
            "raw_object_keys": raw_result["raw_object_keys"],
            "inserted": 4,
            "expected_rows": 4,
            "list_total_count": 4,
            "page_count": 1,
            "is_publishable": True,
        }

    def verify(load_result, snapshot_run_id):
        events.append(("verify", snapshot_run_id))
        assert load_result["inserted"] == 4
        return 4

    result = IncidentMaterializer(
        receipts=Receipts(),
        manifest=Manifest(),
        load=load,
        verify=verify,
        clock=lambda: datetime(2026, 7, 16, 0, 6, tzinfo=timezone.utc),
    ).run(
        materializer_dag_id="traffic_incident_bronze",
        materializer_run_id="asset__materializer-1",
        limit=24,
    )

    assert result.processed_count == 2
    assert result.snapshot_run_ids == ("snapshot-1", "snapshot-2")
    assert result.latest_asset_metadata == {
        "source_id": "seoul_traffic_incident",
        "bronze_run_id": "snapshot-2",
        "bronze_dag_run_id": "snapshot-2",
        "event_at": "2026-07-16T00:05:00+00:00",
        "load_date": "2026-07-16",
        "row_count": 4,
        "payload_hash": "b" * 64,
        "is_publishable": True,
    }
    assert [event for event in events if event[0] == "load"] == [
        ("load", "snapshot-1"),
        ("load", "snapshot-2"),
    ]
    assert [event for event in events if event[0] == "materialized"] == [
        ("materialized", "snapshot-1"),
        ("materialized", "snapshot-2"),
    ]


def test_materializer_stops_at_first_failure_and_records_snapshot_manifest_failure():
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    events = []
    old = _receipt("snapshot-1", "2026-07-16T00:00:00+00:00")
    new = _receipt("snapshot-2", "2026-07-16T00:05:00+00:00")

    class Receipts:
        def pending(self, *, limit):
            return [old, new]

        def record_materialized(self, receipt):
            events.append(("materialized", receipt.snapshot_run_id))

    class Manifest:
        def start(self, run, **_metrics):
            events.append(("start", run.run_id))

        def publish(self, *_args, **_kwargs):
            pytest.fail("failed snapshot must not publish")

        def fail(self, run, *, task_id, error, **_metrics):
            events.append(("fail", run.run_id, task_id, error))

    failure = RuntimeError("Trino unavailable")

    def load(_raw_result, snapshot_run_id):
        events.append(("load", snapshot_run_id))
        raise failure

    materializer = IncidentMaterializer(
        receipts=Receipts(),
        manifest=Manifest(),
        load=load,
        verify=lambda *_args: 0,
        clock=lambda: datetime.now(timezone.utc),
    )

    with pytest.raises(RuntimeError, match="Trino unavailable") as raised:
        materializer.run(
            materializer_dag_id="traffic_incident_bronze",
            materializer_run_id="asset__materializer-1",
            limit=24,
        )

    assert raised.value is failure
    assert events[0:2] == [("start", "snapshot-1"), ("load", "snapshot-1")]
    assert events[2][0:3] == (
        "fail",
        "snapshot-1",
        "materialize_pending_traffic_incident_snapshots",
    )
    assert not any(event == ("load", "snapshot-2") for event in events)
    assert not any(event[0] == "materialized" for event in events)


def test_materializer_empty_queue_is_success_without_asset_metadata():
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    class Receipts:
        def pending(self, *, limit):
            return []

    result = IncidentMaterializer(
        receipts=Receipts(),
        manifest=object(),
        load=lambda *_args: pytest.fail("empty queue must not load"),
        verify=lambda *_args: pytest.fail("empty queue must not verify"),
        clock=lambda: datetime.now(timezone.utc),
    ).run(
        materializer_dag_id="traffic_incident_bronze",
        materializer_run_id="scheduled__fallback",
        limit=24,
    )

    assert result.processed_count == 0
    assert result.snapshot_run_ids == ()
    assert result.latest_asset_metadata is None
