from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.errors import TrafficCompletenessError


def _pipeline(events):
    from traffic_ingest.flow_pipeline import TrafficFlowPipeline

    class IncidentManifest:
        def require_publishable(self, run_id):
            events.append(("require_incident", run_id))
            return run_id

    class FlowManifest:
        def start(self, run, **metrics):
            events.append(("flow_start", run.run_id, metrics))

        def publish(self, run, **metrics):
            events.append(("flow_publish", run.run_id, metrics))

        def fail(self, run, **metrics):
            events.append(("flow_fail", run.run_id, metrics))

    class Landing:
        def collect(self, *, link_ids, dag_run_id, landing_load_date=None):
            events.append(("land", tuple(link_ids), dag_run_id, landing_load_date))
            return {
                "source_id": "seoul_traffic_flow",
                "raw_objects": [
                    {
                        "raw_object_key": "raw/flow.xml",
                        "raw_hash": "c" * 64,
                        "collected_at": "2026-07-16T00:06:00+00:00",
                    }
                ],
                "raw_object_keys": ["raw/flow.xml"],
                "expected_rows": 1,
                "expected_raw_objects": 1,
                "is_publishable": True,
            }

    return TrafficFlowPipeline(
        runtime_guard=lambda: events.append(("guard",)),
        incident_manifest=IncidentManifest(),
        flow_manifest=FlowManifest(),
        landing=Landing(),
        load=lambda raw_result, run_id: events.append(("load", run_id))
        or {
            "raw_object_keys": raw_result["raw_object_keys"],
            "inserted": 1,
            "expected_rows": 1,
            "page_count": 1,
            "is_publishable": True,
        },
        verify=lambda result, run_id: events.append(("verify", run_id)) or 1,
    )


def test_flow_pipeline_preserves_exact_incident_parent_from_landing_to_asset():
    events = []
    pipeline = _pipeline(events)

    raw_result = pipeline.land(
        parent_incident_run_id="incident-1",
        flow_run_id="asset__flow-1",
        link_ids=["1220003800"],
        conf={},
    )
    outcome = pipeline.materialize(
        raw_result=raw_result,
        flow_run_id="asset__flow-1",
    )

    assert raw_result["parent_incident_run_id"] == "incident-1"
    assert ("land", ("1220003800",), "asset__flow-1", None) in events
    assert outcome.asset_metadata == {
        "source_id": "seoul_traffic_flow",
        "flow_run_id": "asset__flow-1",
        "flow_dag_run_id": "asset__flow-1",
        "parent_incident_run_id": "incident-1",
        "event_at": "2026-07-16T00:06:00+00:00",
        "load_date": "2026-07-16",
        "row_count": 1,
        "payload_hash": "c" * 64,
        "is_publishable": True,
    }
    assert any(event[0:2] == ("flow_publish", "asset__flow-1") for event in events)


def test_flow_pipeline_uses_explicit_load_date_for_backfill_partition():
    events = []
    pipeline = _pipeline(events)

    pipeline.land(
        parent_incident_run_id="incident-1",
        flow_run_id="manual__flow-backfill",
        link_ids=["1220003800"],
        conf={"load_date": "2026-07-10"},
    )

    assert ("land", ("1220003800",), "manual__flow-backfill", "2026-07-10") in events


def test_flow_pipeline_revalidates_and_publishes_the_exact_pinned_parent():
    events = []
    pipeline = _pipeline(events)
    raw_result = pipeline.land(
        parent_incident_run_id="incident-1",
        flow_run_id="asset__flow-1",
        link_ids=["1220003800"],
        conf={},
    )

    outcome = pipeline.materialize(
        raw_result=raw_result,
        flow_run_id="asset__flow-1",
    )

    assert outcome.row_count == 1
    assert outcome.asset_metadata["parent_incident_run_id"] == "incident-1"
    assert events.count(("require_incident", "incident-1")) == 2
    assert any(event[0:2] == ("flow_publish", "asset__flow-1") for event in events)
    assert not any(event[0] == "flow_fail" for event in events)


def test_flow_pipeline_rejects_an_empty_reference_handoff_before_landing():
    events = []
    pipeline = _pipeline(events)

    with pytest.raises(TrafficCompletenessError, match="referenced link"):
        pipeline.land(
            parent_incident_run_id="incident-1",
            flow_run_id="asset__flow-1",
            link_ids=[],
            conf={},
        )

    assert not any(event[0] == "land" for event in events)
