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
            "result_code": "INFO-000",
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
    assert result.row_count == 8
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


def test_materializer_records_valid_zero_slot_outcome_after_verification_before_ack():
    from traffic_ingest.collection_slots import traffic_incident_slot
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    events = []
    receipt = _receipt("snapshot-zero", "2026-07-16T00:05:00+00:00")
    receipt.raw_result.update(
        {
            "expected_rows": 0,
            "parsed_rows": 0,
            "list_total_count": 0,
            "manifest_key": "raw/snapshot-zero/_manifest.json",
        }
    )

    class Receipts:
        def pending(self, *, limit):
            assert limit == 1
            return [receipt]

        def record_materialized(self, value):
            events.append(("materialized", value.snapshot_run_id))

    class SlotReceipts:
        def record_outcome(self, outcome):
            events.append(("slot_outcome", outcome))
            return "ops/control/state/collection_slots/event.json"

    class Manifest:
        def start(self, *_args, **_kwargs):
            return None

        def publish(self, *_args, **_kwargs):
            return None

        def fail(self, *_args, **_kwargs):
            pytest.fail("verified zero-row snapshot must not fail")

    def verify(_result, _snapshot_run_id):
        events.append(("verify", receipt.snapshot_run_id))
        return 0

    IncidentMaterializer(
        receipts=Receipts(),
        manifest=Manifest(),
        load=lambda raw_result, _snapshot_run_id: {
            "raw_object_keys": raw_result["raw_object_keys"],
            "inserted": 0,
            "expected_rows": 0,
            "page_count": 1,
            "is_publishable": True,
        },
        verify=verify,
        clock=lambda: datetime(2026, 7, 16, 0, 6, tzinfo=timezone.utc),
        slot_receipts=SlotReceipts(),
        slot_for_logical_date=lambda logical_date: traffic_incident_slot(
            logical_date,
            recovery_boundary="2026-07-01T00:00:00+00:00",
        ),
        raw_manifest_is_verified=lambda *_args, **_kwargs: True,
    ).run(
        materializer_dag_id="traffic_incident_bronze",
        materializer_run_id="asset__materializer-1",
        limit=1,
    )

    outcome = next(event[1] for event in events if event[0] == "slot_outcome")
    assert outcome.collection_state == "source_empty_valid"
    assert outcome.source_result_code == "INFO-000"
    assert outcome.dag_id == "traffic_incident_landing"
    assert outcome.dag_run_id == "snapshot-zero"
    assert outcome.task_id == "land_traffic_incident_snapshot"
    assert [event[0] for event in events] == [
        "verify",
        "slot_outcome",
        "materialized",
    ]


@pytest.mark.parametrize(
    ("raw_updates", "verified_rows"),
    [
        ({"result_code": "INFO-001", "expected_rows": 0, "list_total_count": 0}, 0),
        ({"result_code": "INFO-000", "expected_rows": 1, "list_total_count": 0}, 0),
        ({"result_code": "INFO-000", "expected_rows": 0, "list_total_count": 1}, 0),
        ({"result_code": "INFO-000", "expected_rows": 0, "list_total_count": 0}, 1),
    ],
)
def test_materializer_zero_slot_outcome_requires_topis_empty_and_verified_zero(
    raw_updates,
    verified_rows,
):
    from traffic_ingest.collection_slots import traffic_incident_slot
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    receipt = _receipt("snapshot-zero", "2026-07-16T00:05:00+00:00")
    receipt.raw_result.update(
        {
            "manifest_key": "raw/snapshot-zero/_manifest.json",
            "parsed_rows": int(raw_updates.get("expected_rows", 0)),
            **raw_updates,
        }
    )

    class Receipts:
        def pending(self, *, limit):
            return [receipt]

        def record_materialized(self, _value):
            pytest.fail("invalid zero evidence must not ack the receipt")

    class SlotReceipts:
        def record_outcome(self, outcome):
            assert outcome.collection_state != "source_empty_valid"
            return "ops/control/state/collection_slots/event.json"

    class Manifest:
        def start(self, *_args, **_kwargs):
            return None

        def publish(self, *_args, **_kwargs):
            return None

        def fail(self, *_args, **_kwargs):
            return None

    with pytest.raises(ValueError, match="source_empty_valid"):
        IncidentMaterializer(
            receipts=Receipts(),
            manifest=Manifest(),
            load=lambda raw_result, _snapshot_run_id: {
                "raw_object_keys": raw_result["raw_object_keys"],
                "inserted": int(raw_updates.get("expected_rows", 0)),
                "expected_rows": int(raw_updates.get("expected_rows", 0)),
                "page_count": 1,
                "is_publishable": True,
            },
            verify=lambda _result, _snapshot_run_id: verified_rows,
            clock=lambda: datetime(2026, 7, 16, 0, 6, tzinfo=timezone.utc),
            slot_receipts=SlotReceipts(),
            slot_for_logical_date=lambda logical_date: traffic_incident_slot(
                logical_date,
                recovery_boundary="2026-07-01T00:00:00+00:00",
            ),
            raw_manifest_is_verified=lambda *_args, **_kwargs: True,
        ).run(
            materializer_dag_id="traffic_incident_bronze",
            materializer_run_id="asset__materializer-1",
            limit=1,
        )


def test_materializer_pre_activation_slot_does_not_mutate_slot_receipts():
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    events = []
    receipt = _receipt("snapshot-1", "2026-07-16T00:05:00+00:00")

    class Receipts:
        def pending(self, *, limit):
            return [receipt]

        def record_materialized(self, value):
            events.append(("materialized", value.snapshot_run_id))

    class SlotReceipts:
        def record_outcome(self, _outcome):
            events.append(("slot_outcome", None))
            pytest.fail("pre-activation materializer must not write slot outcome")

    class Manifest:
        def start(self, *_args, **_kwargs):
            return None

        def publish(self, *_args, **_kwargs):
            return None

        def fail(self, *_args, **_kwargs):
            pytest.fail("successful snapshot must not fail")

    IncidentMaterializer(
        receipts=Receipts(),
        manifest=Manifest(),
        load=lambda raw_result, _snapshot_run_id: {
            "raw_object_keys": raw_result["raw_object_keys"],
            "inserted": 4,
            "expected_rows": 4,
            "page_count": 1,
            "is_publishable": True,
        },
        verify=lambda _result, _snapshot_run_id: 4,
        clock=lambda: datetime(2026, 7, 16, 0, 6, tzinfo=timezone.utc),
        slot_receipts=SlotReceipts(),
        slot_for_logical_date=lambda _logical_date: None,
    ).run(
        materializer_dag_id="traffic_incident_bronze",
        materializer_run_id="asset__materializer-1",
        limit=1,
    )

    assert events == [("materialized", "snapshot-1")]


def test_materializer_failure_after_landed_receipt_records_raw_replay_pending_without_ack():
    from traffic_ingest.collection_slots import traffic_incident_slot
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    events = []
    receipt = _receipt("snapshot-1", "2026-07-16T00:05:00+00:00")
    receipt.raw_result["manifest_key"] = "raw/snapshot-1/_manifest.json"
    failure = RuntimeError("Trino load failed")

    class Receipts:
        def pending(self, *, limit):
            return [receipt]

        def record_materialized(self, _value):
            pytest.fail("failed materialization must leave receipt pending")

    class SlotReceipts:
        def record_outcome(self, outcome):
            events.append(("slot_outcome", outcome))
            return "ops/control/state/collection_slots/event.json"

    class Manifest:
        def start(self, *_args, **_kwargs):
            return None

        def fail(self, *_args, **_kwargs):
            return None

    with pytest.raises(RuntimeError, match="Trino load failed"):
        IncidentMaterializer(
            receipts=Receipts(),
            manifest=Manifest(),
            load=lambda *_args: (_ for _ in ()).throw(failure),
            verify=lambda *_args: pytest.fail("failed load must not verify"),
            clock=lambda: datetime(2026, 7, 16, 0, 6, tzinfo=timezone.utc),
            slot_receipts=SlotReceipts(),
            slot_for_logical_date=lambda logical_date: traffic_incident_slot(
                logical_date,
                recovery_boundary="2026-07-01T00:00:00+00:00",
            ),
            raw_manifest_is_verified=lambda *_args, **_kwargs: True,
        ).run(
            materializer_dag_id="traffic_incident_bronze",
            materializer_run_id="asset__materializer-1",
            limit=1,
        )

    outcome = next(event[1] for event in events if event[0] == "slot_outcome")
    assert outcome.collection_state == "collection_failed"
    assert outcome.recovery_state == "pending"
    assert outcome.recovery_class == "raw_replay"
    assert outcome.gap_reason_code == "materialization_failed"
    assert outcome.raw_manifest_key == "raw/snapshot-1/_manifest.json"
    assert outcome.raw_object_count == 1
    assert outcome.source_result_code == "INFO-000"
    assert outcome.recovery_evidence_code == "raw_manifest_verified"


def test_materializer_failure_does_not_promote_unverified_manifest_to_raw_replay():
    from traffic_ingest.collection_slots import traffic_incident_slot
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    events = []
    receipt = _receipt("snapshot-1", "2026-07-16T00:05:00+00:00")
    receipt.raw_result["manifest_key"] = "raw/snapshot-1/_manifest.json"
    failure = RuntimeError("Trino load failed")

    class Receipts:
        def pending(self, *, limit):
            assert limit == 1
            return [receipt]

        def record_materialized(self, _value):
            pytest.fail("failed materialization must leave receipt pending")

    class SlotReceipts:
        def record_outcome(self, outcome):
            events.append(outcome)
            return "ops/control/state/collection_slots/event.json"

    class Manifest:
        def start(self, *_args, **_kwargs):
            return None

        def fail(self, *_args, **_kwargs):
            return None

    with pytest.raises(RuntimeError, match="Trino load failed"):
        IncidentMaterializer(
            receipts=Receipts(),
            manifest=Manifest(),
            load=lambda *_args: (_ for _ in ()).throw(failure),
            verify=lambda *_args: pytest.fail("failed load must not verify"),
            clock=lambda: datetime(2026, 7, 16, 0, 6, tzinfo=timezone.utc),
            slot_receipts=SlotReceipts(),
            slot_for_logical_date=lambda logical_date: traffic_incident_slot(
                logical_date,
                recovery_boundary="2026-07-01T00:00:00+00:00",
            ),
            raw_manifest_is_verified=lambda *_args, **_kwargs: False,
        ).run(
            materializer_dag_id="traffic_incident_bronze",
            materializer_run_id="asset__materializer-1",
            limit=1,
        )

    assert events == []


def test_materializer_preflight_failure_records_raw_replay_for_pending_raw_manifest_receipts():
    from traffic_ingest.collection_slots import traffic_incident_slot
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    events = []
    first = _receipt("snapshot-1", "2026-07-16T00:05:00+00:00")
    second = _receipt("snapshot-2", "2026-07-16T00:10:00+00:00")
    first.raw_result["manifest_key"] = "raw/snapshot-1/_manifest.json"
    second.raw_result["manifest_key"] = "raw/snapshot-2/_manifest.json"
    failure = ConnectionError("preflight Trino unavailable")

    class Receipts:
        def pending(self, *, limit):
            assert limit == 24
            return [first, second]

        def record_materialized(self, _value):
            pytest.fail("preflight failure must not ack snapshot receipts")

    class SlotReceipts:
        def record_outcome(self, outcome):
            events.append(("slot_outcome", outcome))
            return "ops/control/state/collection_slots/event.json"

    class Manifest:
        def start(self, run, **_kwargs):
            events.append(("start", run.run_id))

        def fail(self, run, *, task_id, error, **_kwargs):
            events.append(("fail", run.run_id, task_id, error))

    with pytest.raises(ConnectionError, match="preflight Trino unavailable") as raised:
        IncidentMaterializer(
            receipts=Receipts(),
            manifest=Manifest(),
            load=lambda *_args: pytest.fail("preflight failure must not load"),
            verify=lambda *_args: pytest.fail("preflight failure must not verify"),
            verified_receipts=lambda _receipts: (_ for _ in ()).throw(failure),
            clock=lambda: datetime(2026, 7, 16, 0, 11, tzinfo=timezone.utc),
            slot_receipts=SlotReceipts(),
            slot_for_logical_date=lambda logical_date: traffic_incident_slot(
                logical_date,
                recovery_boundary="2026-07-01T00:00:00+00:00",
            ),
            raw_manifest_is_verified=lambda *_args, **_kwargs: True,
        ).run(
            materializer_dag_id="traffic_incident_bronze",
            materializer_run_id="asset__materializer-1",
            limit=24,
        )

    assert raised.value is failure
    outcomes = [event[1] for event in events if event[0] == "slot_outcome"]
    assert [outcome.dag_run_id for outcome in outcomes] == [
        "snapshot-1",
        "snapshot-2",
    ]
    assert {outcome.recovery_state for outcome in outcomes} == {"pending"}
    assert {outcome.recovery_class for outcome in outcomes} == {"raw_replay"}
    assert {outcome.recovery_evidence_code for outcome in outcomes} == {
        "raw_manifest_verified"
    }
    assert [outcome.raw_manifest_key for outcome in outcomes] == [
        "raw/snapshot-1/_manifest.json",
        "raw/snapshot-2/_manifest.json",
    ]
    assert [outcome.raw_object_count for outcome in outcomes] == [1, 1]
    assert {outcome.source_result_code for outcome in outcomes} == {"INFO-000"}
    assert [event[0] for event in events] == [
        "start",
        "fail",
        "slot_outcome",
        "slot_outcome",
    ]


def test_materializer_batch_failure_appends_raw_replay_only_for_unterminalized_receipts():
    from traffic_ingest.collection_slots import traffic_incident_slot
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    events = []
    terminalized = _receipt("snapshot-1", "2026-07-16T00:05:00+00:00")
    unterminalized = _receipt("snapshot-2", "2026-07-16T00:10:00+00:00")
    terminalized.raw_result["manifest_key"] = "raw/snapshot-1/_manifest.json"
    unterminalized.raw_result["manifest_key"] = "raw/snapshot-2/_manifest.json"
    failure = RuntimeError("terminal outcome write failed")

    class Receipts:
        def pending(self, *, limit):
            assert limit == 24
            return [terminalized, unterminalized]

        def record_materialized(self, value):
            events.append(("materialized", value.snapshot_run_id))

    class SlotReceipts:
        def record_outcome(self, outcome):
            events.append(("slot_outcome", outcome))
            if (
                outcome.dag_run_id == "snapshot-2"
                and outcome.recovery_state == "not_required"
            ):
                raise failure
            return "ops/control/state/collection_slots/event.json"

    class Manifest:
        def start_many(self, entries):
            events.append(("start_many", [run.run_id for run, _count in entries]))

        def publish_many(self, entries):
            events.append(("publish_many", [run.run_id for run, _metrics in entries]))

        def fail_many(self, entries, *, task_id, error):
            events.append(("fail_many", [run.run_id for run, _count in entries], task_id, error))

    with pytest.raises(RuntimeError, match="terminal outcome write failed") as raised:
        IncidentMaterializer(
            receipts=Receipts(),
            manifest=Manifest(),
            load_many=lambda raw_results: {
                run_id: {
                    "raw_object_keys": raw_result["raw_object_keys"],
                    "inserted": 4,
                    "expected_rows": 4,
                    "page_count": 1,
                    "is_publishable": True,
                }
                for run_id, raw_result in raw_results.items()
            },
            verify_many=lambda load_results, _receipts: {
                run_id: 4 for run_id in load_results
            },
            load=lambda *_args: pytest.fail("batch load port must be used"),
            verify=lambda *_args: pytest.fail("batch verify port must be used"),
            clock=lambda: datetime(2026, 7, 16, 0, 11, tzinfo=timezone.utc),
            slot_receipts=SlotReceipts(),
            slot_for_logical_date=lambda logical_date: traffic_incident_slot(
                logical_date,
                recovery_boundary="2026-07-01T00:00:00+00:00",
            ),
            raw_manifest_is_verified=lambda *_args, **_kwargs: True,
        ).run(
            materializer_dag_id="traffic_incident_bronze",
            materializer_run_id="asset__materializer-1",
            limit=24,
        )

    assert raised.value is failure
    terminal = [
        event[1]
        for event in events
        if event[0] == "slot_outcome"
        and event[1].recovery_state == "not_required"
    ]
    replay = [
        event[1]
        for event in events
        if event[0] == "slot_outcome"
        and event[1].recovery_class == "raw_replay"
    ]
    assert [outcome.dag_run_id for outcome in terminal] == [
        "snapshot-1",
        "snapshot-2",
    ]
    assert [event for event in events if event[0] == "materialized"] == [
        ("materialized", "snapshot-1"),
    ]
    assert [outcome.dag_run_id for outcome in replay] == [
        "snapshot-2",
    ]
    assert [outcome.raw_manifest_key for outcome in replay] == [
        "raw/snapshot-2/_manifest.json",
    ]
    assert {outcome.recovery_evidence_code for outcome in replay} == {
        "raw_manifest_verified"
    }


def test_materializer_row_count_includes_nonpublishable_verified_snapshot():
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    receipt = _receipt("snapshot-1", "2026-07-16T00:00:00+00:00")

    class Receipts:
        def pending(self, *, limit):
            assert limit == 24
            return [receipt]

        def record_materialized(self, _receipt):
            return None

    class Manifest:
        def start(self, *_args, **_kwargs):
            return None

        def publish(self, *_args, **_kwargs):
            return None

        def fail(self, *_args, **_kwargs):
            pytest.fail("verified nonpublishable snapshot must not fail")

    result = IncidentMaterializer(
        receipts=Receipts(),
        manifest=Manifest(),
        load=lambda raw_result, _run_id: {
            "raw_object_keys": raw_result["raw_object_keys"],
            "inserted": 4,
            "expected_rows": 4,
            "page_count": 1,
            "is_publishable": False,
        },
        verify=lambda _result, _run_id: 4,
        clock=lambda: datetime(2026, 7, 16, 0, 6, tzinfo=timezone.utc),
    ).run(
        materializer_dag_id="traffic_incident_bronze",
        materializer_run_id="scheduled__materializer",
        limit=24,
    )

    assert result.row_count == 4
    assert result.asset_metadata == ()


def test_materializer_uses_batch_manifest_load_and_verification_ports_once():
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
        def start_many(self, entries):
            events.append(("start_many", [run.run_id for run, _count in entries]))

        def publish_many(self, entries):
            events.append(
                ("publish_many", [run.run_id for run, _metrics in entries])
            )

        def fail_many(self, *_args, **_kwargs):
            pytest.fail("successful batch must not fail")

    def load_many(raw_results):
        events.append(("load_many", list(raw_results)))
        return {
            run_id: {
                "raw_object_keys": raw_result["raw_object_keys"],
                "inserted": 4,
                "expected_rows": 4,
                "page_count": 1,
                "is_publishable": True,
            }
            for run_id, raw_result in raw_results.items()
        }

    def verify_many(load_results, receipts):
        events.append(("verify_many", list(load_results)))
        assert [receipt.snapshot_run_id for receipt in receipts] == [
            "snapshot-1",
            "snapshot-2",
        ]
        return {run_id: 4 for run_id in load_results}

    result = IncidentMaterializer(
        receipts=Receipts(),
        manifest=Manifest(),
        load=lambda *_args: pytest.fail("batch load port must be used"),
        verify=lambda *_args: pytest.fail("batch verify port must be used"),
        load_many=load_many,
        verify_many=verify_many,
        clock=lambda: datetime(2026, 7, 16, 0, 6, tzinfo=timezone.utc),
    ).run(
        materializer_dag_id="traffic_incident_bronze",
        materializer_run_id="scheduled__materializer",
        limit=24,
    )

    assert result.snapshot_run_ids == ("snapshot-1", "snapshot-2")
    assert [event[0] for event in events].count("start_many") == 1
    assert [event[0] for event in events].count("load_many") == 1
    assert [event[0] for event in events].count("verify_many") == 1
    assert [event[0] for event in events].count("publish_many") == 1


def test_materializer_skips_load_for_exact_preverified_receipt():
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    events = []
    old = _receipt("snapshot-1", "2026-07-16T00:00:00+00:00")
    new = _receipt("snapshot-2", "2026-07-16T00:05:00+00:00")

    class Receipts:
        def pending(self, *, limit):
            assert limit == 24
            return [old, new]

        def is_pending(self, snapshot_run_id):
            return snapshot_run_id == "snapshot-1"

        def record_materialized(self, receipt):
            events.append(("materialized", receipt.snapshot_run_id, receipt.row_count))

    class Manifest:
        def start(self, run, **metrics):
            events.append(("start", run.run_id, metrics))

        def publish(self, run, **metrics):
            events.append(("publish", run.run_id, metrics))

        def fail(self, *_args, **_kwargs):
            pytest.fail("successful materialization must not fail manifest")

    def load(_raw_result, snapshot_run_id):
        events.append(("load", snapshot_run_id))
        return {
            "raw_object_keys": [f"raw/{snapshot_run_id}.xml"],
            "inserted": 4,
            "expected_rows": 4,
            "page_count": 1,
            "is_publishable": True,
        }

    result = IncidentMaterializer(
        receipts=Receipts(),
        manifest=Manifest(),
        load=load,
        verify=lambda load_result, snapshot_run_id: (
            events.append(("verify", snapshot_run_id)) or int(load_result["inserted"])
        ),
        verified_receipts=lambda receipts: (
            events.append(("preflight", [receipt.snapshot_run_id for receipt in receipts]))
            or {"snapshot-1": 4}
        ),
        clock=lambda: datetime(2026, 7, 16, 0, 6, tzinfo=timezone.utc),
    ).run(
        materializer_dag_id="traffic_incident_bronze",
        materializer_run_id="asset__materializer-1",
        limit=24,
    )

    assert result.snapshot_run_ids == ("snapshot-1", "snapshot-2")
    assert ("load", "snapshot-1") not in events
    assert [event for event in events if event[0] == "load"] == [("load", "snapshot-2")]
    assert [event for event in events if event[0] == "verify"] == [("verify", "snapshot-2")]
    assert [event for event in events if event[0] == "materialized"] == [
        ("materialized", "snapshot-1", 4),
        ("materialized", "snapshot-2", 4),
    ]


def test_materializer_coalesces_exact_preverified_receipt_acknowledged_after_batch_read():
    """The fence catches a stale pending list, not a failed publish recovery."""

    from traffic_ingest.incident_pipeline import IncidentMaterializer

    events = []
    stale = _receipt("snapshot-1", "2026-07-16T00:00:00+00:00")
    active = _receipt("snapshot-2", "2026-07-16T00:05:00+00:00")

    class Receipts:
        def pending(self, *, limit):
            assert limit == 24
            return [stale, active]

        def is_pending(self, snapshot_run_id):
            return snapshot_run_id == "snapshot-2"

        def record_materialized(self, receipt):
            events.append(("materialized", receipt.snapshot_run_id))

    class Manifest:
        def start(self, run, **_metrics):
            events.append(("start", run.run_id))

        def publish(self, run, **_metrics):
            events.append(("publish", run.run_id))

        def fail(self, *_args, **_kwargs):
            pytest.fail("coalesced or successful receipt must not fail manifest")

    result = IncidentMaterializer(
        receipts=Receipts(),
        manifest=Manifest(),
        load=lambda *_args: pytest.fail("preverified receipts must not load"),
        verify=lambda *_args: pytest.fail("preverified receipts must not verify"),
        verified_receipts=lambda _receipts: {"snapshot-1": 4, "snapshot-2": 4},
        clock=lambda: datetime(2026, 7, 16, 0, 6, tzinfo=timezone.utc),
    ).run(
        materializer_dag_id="traffic_incident_bronze",
        materializer_run_id="asset__materializer-1",
        limit=24,
    )

    assert result.snapshot_run_ids == ("snapshot-2",)
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
    assert [event for event in events if event[0] in {"start", "publish"}] == [
        ("start", "snapshot-2"),
        ("publish", "snapshot-2"),
    ]
    assert [event for event in events if event[0] == "materialized"] == [
        ("materialized", "snapshot-2")
    ]


def test_materializer_rejects_inconsistent_preverified_row_count():
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    events = []
    receipt = _receipt("snapshot-1", "2026-07-16T00:00:00+00:00")

    class Receipts:
        def pending(self, *, limit):
            return [receipt]

        def is_pending(self, snapshot_run_id):
            assert snapshot_run_id == "snapshot-1"
            return True

        def record_materialized(self, _receipt):
            pytest.fail("invalid preflight must not materialize receipt")

    class Manifest:
        def start(self, run, **_metrics):
            events.append(("start", run.run_id))

        def publish(self, *_args, **_kwargs):
            pytest.fail("invalid preflight must not publish")

        def fail(self, run, **_kwargs):
            events.append(("fail", run.run_id))

    with pytest.raises(ValueError, match="preflight row count"):
        IncidentMaterializer(
            receipts=Receipts(),
            manifest=Manifest(),
            load=lambda *_args: pytest.fail("invalid preflight must not load"),
            verify=lambda *_args: pytest.fail("invalid preflight must not verify"),
            verified_receipts=lambda _receipts: {"snapshot-1": 3},
            clock=lambda: datetime.now(timezone.utc),
        ).run(
            materializer_dag_id="traffic_incident_bronze",
            materializer_run_id="asset__materializer-1",
            limit=24,
        )

    assert events == [("start", "snapshot-1"), ("fail", "snapshot-1")]


def test_materializer_records_first_receipt_failure_when_preflight_errors():
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    events = []
    receipt = _receipt("snapshot-1", "2026-07-16T00:00:00+00:00")
    failure = ConnectionError("Trino unavailable")

    class Receipts:
        def pending(self, *, limit):
            return [receipt]

        def record_materialized(self, _receipt):
            pytest.fail("failed preflight must not materialize receipt")

    class Manifest:
        def start(self, run, **_metrics):
            events.append(("start", run.run_id))

        def fail(self, run, *, task_id, error, **_metrics):
            events.append(("fail", run.run_id, task_id, error))

    def raise_preflight(_receipts):
        raise failure

    with pytest.raises(ConnectionError, match="Trino unavailable") as raised:
        IncidentMaterializer(
            receipts=Receipts(),
            manifest=Manifest(),
            load=lambda *_args: pytest.fail("failed preflight must not load"),
            verify=lambda *_args: pytest.fail("failed preflight must not verify"),
            verified_receipts=raise_preflight,
            clock=lambda: datetime.now(timezone.utc),
        ).run(
            materializer_dag_id="traffic_incident_bronze",
            materializer_run_id="asset__materializer-1",
            limit=24,
        )

    assert raised.value is failure
    assert events[0] == ("start", "snapshot-1")
    assert events[1][0:3] == (
        "fail",
        "snapshot-1",
        "materialize_pending_traffic_incident_snapshots",
    )
    assert events[1][3] is failure


def test_materializer_fails_all_started_receipts_when_batch_load_fails():
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
    assert events[0:3] == [
        ("start", "snapshot-1"),
        ("start", "snapshot-2"),
        ("load", "snapshot-1"),
    ]
    assert events[3][0:3] == (
        "fail",
        "snapshot-1",
        "materialize_pending_traffic_incident_snapshots",
    )
    assert events[4][0:3] == (
        "fail",
        "snapshot-2",
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
    assert result.row_count == 0
    assert result.latest_asset_metadata is None


def test_materializer_recovers_legacy_receipt_manifest_before_bronze_load():
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    receipt = _receipt("legacy-snapshot", "2026-07-16T00:00:00+00:00")
    events = []

    class Receipts:
        def pending(self, *, limit):
            assert limit == 24
            return [receipt]

        def record_materialized(self, value):
            events.append(("materialized", value.snapshot_run_id))

    class Manifest:
        def start(self, *_args, **_kwargs):
            return None

        def publish(self, *_args, **_kwargs):
            return None

        def fail(self, *_args, **_kwargs):
            pytest.fail("recovered legacy receipt must not fail")

    def recover(raw_result, snapshot_run_id):
        events.append(("recover", snapshot_run_id))
        return {
            **raw_result,
            "manifest_key": "raw/traffic_incident/legacy/_manifest.json",
        }

    def load(raw_result, snapshot_run_id):
        events.append(("load", snapshot_run_id))
        assert raw_result["manifest_key"].endswith("/_manifest.json")
        return {
            "raw_object_keys": raw_result["raw_object_keys"],
            "inserted": 4,
            "expected_rows": 4,
            "page_count": 1,
            "is_publishable": True,
        }

    result = IncidentMaterializer(
        receipts=Receipts(),
        manifest=Manifest(),
        load=load,
        verify=lambda result, _run_id: int(result["inserted"]),
        recover_legacy_raw_result=recover,
        clock=lambda: datetime(2026, 7, 16, 0, 6, tzinfo=timezone.utc),
    ).run(
        materializer_dag_id="traffic_incident_bronze",
        materializer_run_id="scheduled__materializer",
        limit=24,
    )

    assert result.snapshot_run_ids == ("legacy-snapshot",)
    assert events == [
        ("recover", "legacy-snapshot"),
        ("load", "legacy-snapshot"),
        ("materialized", "legacy-snapshot"),
    ]


def test_materializer_keeps_legacy_receipt_pending_when_manifest_recovery_fails():
    from traffic_ingest.incident_pipeline import IncidentMaterializer

    receipt = _receipt("legacy-snapshot", "2026-07-16T00:00:00+00:00")
    events = []

    class Receipts:
        def pending(self, *, limit):
            return [receipt]

        def record_materialized(self, _value):
            pytest.fail("unrecoverable receipt must remain pending")

    class Manifest:
        def start(self, run, **_kwargs):
            events.append(("start", run.run_id))

        def fail(self, run, **_kwargs):
            events.append(("fail", run.run_id))

    with pytest.raises(ValueError, match="legacy raw is incomplete"):
        IncidentMaterializer(
            receipts=Receipts(),
            manifest=Manifest(),
            load=lambda *_args: pytest.fail("must not load without manifest"),
            verify=lambda *_args: pytest.fail("must not verify without manifest"),
            recover_legacy_raw_result=lambda *_args: (
                (_ for _ in ()).throw(ValueError("legacy raw is incomplete"))
            ),
            clock=lambda: datetime.now(timezone.utc),
        ).run(
            materializer_dag_id="traffic_incident_bronze",
            materializer_run_id="scheduled__materializer",
            limit=24,
        )

    assert events == [("start", "legacy-snapshot"), ("fail", "legacy-snapshot")]
