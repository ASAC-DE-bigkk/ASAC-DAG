from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_incident_collection_slot_reconciliation as module  # noqa: E402
from traffic_ingest.collection_slots import traffic_incident_slot  # noqa: E402


def test_traffic_reconciliation_dag_is_paused_api_free_control_plane():
    source = (
        Path(__file__).resolve().parents[1]
        / "traffic_incident_collection_slot_reconciliation.py"
    ).read_text(encoding="utf-8")
    task = module.dag.get_task("reconcile_due_traffic_collection_slots")

    assert module.dag.is_paused_upon_creation is True
    assert module.dag.catchup is False
    assert module.dag.max_active_runs == 1
    assert module.dag.schedule_interval == "*/5 * * * *"
    assert task.python_callable is module.reconcile_due_traffic_collection_slots
    assert "build_traffic_landing" not in source
    assert "TopisHttpAdapter" not in source
    assert "SeoulOpenApiClient" not in source


def test_traffic_due_policy_uses_raw_replay_only_with_verified_manifest():
    slot = traffic_incident_slot(
        "2026-08-08T00:05:00+00:00",
        recovery_boundary="2026-08-01T00:00:00+00:00",
    )
    common = {
        "event_at": datetime(2026, 8, 8, 0, 25, tzinfo=timezone.utc),
        "dag_id": module.DAG_ID,
        "dag_run_id": "scheduled__2026-08-08T00:25:00+00:00",
    }

    raw_replay = module.traffic_missing_outcome(
        slot,
        raw_manifest_key="raw/traffic/_manifest.json",
        raw_object_count=1,
        source_result_code="INFO-000",
        raw_manifest_verified=True,
        **common,
    )
    assert raw_replay.collection_state == "missing_unknown"
    assert raw_replay.recovery_state == "pending"
    assert raw_replay.recovery_class == "raw_replay"
    assert raw_replay.recovery_evidence_code == "raw_manifest_verified"

    no_raw = module.traffic_missing_outcome(
        slot,
        raw_manifest_key="raw/traffic/diagnostic.xml",
        raw_object_count=1,
        source_result_code="INFO-000",
        raw_manifest_verified=False,
        **common,
    )
    assert no_raw.collection_state == "missing_unknown"
    assert no_raw.recovery_state == "unrecoverable"
    assert no_raw.recovery_class == "none"
    assert no_raw.raw_manifest_key is None
