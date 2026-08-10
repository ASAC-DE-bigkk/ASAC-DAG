import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.errors import (  # noqa: E402
    TrafficBronzeConfigurationError,
    TrafficCompletenessError,
)
from traffic_ingest.link_reference_pipeline import (  # noqa: E402
    TrafficLinkReferencePipeline,
)


class IncidentManifest:
    def __init__(self, events):
        self.events = events

    def require_publishable(self, run_id):
        self.events.append(("require_parent", run_id))
        return run_id


class Landing:
    def __init__(self, events):
        self.events = events

    def collect(self, *, link_ids, dag_run_id, landing_load_date=None):
        self.events.append(
            ("land_reference", list(link_ids), dag_run_id, landing_load_date)
        )
        return {
            "source_id": "seoul_traffic_link_reference",
            "requested_link_ids": list(link_ids),
            "raw_objects": (
                [
                    {"service_name": "LinkInfo"},
                    {"service_name": "LinkVerInfo"},
                ]
                if link_ids
                else []
            ),
            "raw_object_keys": ["raw/info.xml", "raw/vertex.xml"] if link_ids else [],
            "manifest_key": "raw/_manifest.json" if link_ids else None,
            "expected_raw_objects": 2 * len(link_ids),
            "is_publishable": True,
        }


def _pipeline(events, unresolved_results):
    unresolved_results = iter(unresolved_results)

    def unresolved(link_ids):
        result = next(unresolved_results)
        events.append(("cache", list(link_ids), list(result)))
        return list(result)

    def load(*, raw_result, dag_run_id):
        events.append(("load_reference", dag_run_id, raw_result["raw_object_keys"]))
        return {
            "inserted_info": 1,
            "inserted_vertices": 2,
            "audit_rows": 2,
            "raw_object_keys": raw_result["raw_object_keys"],
        }

    def verify(*, dag_run_id, load_result):
        events.append(("verify_reference", dag_run_id, load_result["audit_rows"]))
        return {
            "info_rows": 1,
            "vertex_rows": 2,
            "raw_objects": 2,
            "audit_rows": 2,
        }

    return TrafficLinkReferencePipeline(
        runtime_guard=lambda: events.append(("guard",)),
        incident_manifest=IncidentManifest(events),
        resolve_links=lambda conf, parent: events.append(
            ("resolve", conf, parent)
        )
        or ["cached", "missing"],
        unresolved_link_ids=unresolved,
        landing=Landing(events),
        load=load,
        verify=verify,
    )


def test_pipeline_calls_static_apis_only_for_unresolved_links_then_rechecks_all():
    events = []
    pipeline = _pipeline(events, [["missing"], []])

    raw = pipeline.land(
        parent_incident_run_id="incident-42",
        link_reference_run_id="flow-42",
        conf={"load_date": "2026-08-10"},
    )
    materialized = pipeline.materialize(
        raw_result=raw,
        link_reference_run_id="flow-42",
    )

    assert raw["requested_link_ids"] == ["cached", "missing"]
    assert raw["unresolved_link_ids"] == ["missing"]
    assert ("land_reference", ["missing"], "flow-42", "2026-08-10") in events
    assert materialized["requested_link_ids"] == ["cached", "missing"]
    assert materialized["parent_incident_run_id"] == "incident-42"
    assert events.count(("require_parent", "incident-42")) == 1
    assert [event[0] for event in events].index("resolve") < [
        event[0] for event in events
    ].index("cache")


def test_pipeline_cache_hit_is_a_noop_without_loader_or_verifier():
    events = []
    pipeline = _pipeline(events, [[], []])

    raw = pipeline.land(
        parent_incident_run_id="incident-42",
        link_reference_run_id="flow-42",
        conf={},
    )
    result = pipeline.materialize(
        raw_result=raw,
        link_reference_run_id="flow-42",
    )

    assert ("land_reference", [], "flow-42", None) in events
    assert not any(event[0] == "load_reference" for event in events)
    assert not any(event[0] == "verify_reference" for event in events)
    assert result["inserted_info"] == 0
    assert result["inserted_vertices"] == 0


def test_force_refresh_must_be_a_subset_of_the_exact_parent_links():
    events = []
    pipeline = _pipeline(events, [[]])

    with pytest.raises(TrafficBronzeConfigurationError, match="subset"):
        pipeline.land(
            parent_incident_run_id="incident-42",
            link_reference_run_id="flow-42",
            conf={"force_refresh_link_ids": ["outside"]},
        )

    assert not any(event[0] == "land_reference" for event in events)


def test_materialize_fails_if_any_requested_link_remains_unresolved():
    events = []
    pipeline = _pipeline(events, [["missing"], ["missing"]])
    raw = pipeline.land(
        parent_incident_run_id="incident-42",
        link_reference_run_id="flow-42",
        conf={},
    )

    with pytest.raises(TrafficCompletenessError, match="remains unresolved"):
        pipeline.materialize(
            raw_result=raw,
            link_reference_run_id="flow-42",
        )
