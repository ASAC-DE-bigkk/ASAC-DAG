from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _pipeline(events, *, latest_incident="incident-1"):
    from traffic_ingest.flow_pipeline import TrafficFlowPipeline

    class IncidentManifest:
        def require_publishable(self, run_id):
            events.append(("require_incident", run_id))
            return run_id

        def latest_publishable_run_id(self):
            return latest_incident

    class FlowManifest:
        def start(self, run, **metrics):
            events.append(("flow_start", run.run_id, metrics))

        def publish(self, run, **metrics):
            events.append(("flow_publish", run.run_id, metrics))

        def fail(self, run, **metrics):
            events.append(("flow_fail", run.run_id, metrics))

    class Landing:
        def collect(self, *, link_ids, dag_run_id):
            events.append(("land", tuple(link_ids), dag_run_id))
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
        resolve_links=lambda conf, parent: events.append(
            ("resolve", conf, parent)
        )
        or ["1220003800"],
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
        conf={},
    )
    outcome = pipeline.materialize(
        raw_result=raw_result,
        flow_run_id="asset__flow-1",
    )

    assert raw_result["parent_incident_run_id"] == "incident-1"
    assert ("resolve", {}, "incident-1") in events
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


def test_flow_pipeline_keeps_success_bronze_but_suppresses_stale_parent_asset():
    events = []
    pipeline = _pipeline(events, latest_incident="incident-2")
    raw_result = pipeline.land(
        parent_incident_run_id="incident-1",
        flow_run_id="asset__flow-1",
        conf={},
    )

    outcome = pipeline.materialize(
        raw_result=raw_result,
        flow_run_id="asset__flow-1",
    )

    assert outcome.row_count == 1
    assert outcome.asset_metadata is None
    assert any(event[0:2] == ("flow_publish", "asset__flow-1") for event in events)
    assert not any(event[0] == "flow_fail" for event in events)
