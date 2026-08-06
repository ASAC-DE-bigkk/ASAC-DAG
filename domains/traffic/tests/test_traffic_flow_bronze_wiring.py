from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_flow_bronze as dag_module  # noqa: E402


def _incident_event(run_id="incident-1"):
    return types.SimpleNamespace(
        extra={
            "source_id": "seoul_traffic_incident",
            "bronze_run_id": run_id,
            "bronze_dag_run_id": run_id,
            "event_at": "2026-07-16T00:05:00+00:00",
            "load_date": "2026-07-16",
            "row_count": 4,
            "payload_hash": "a" * 64,
            "is_publishable": True,
        }
    )


def test_flow_dag_has_two_meaningful_tasks_and_no_independent_cron():
    assert dag_module.dag.task_ids == [
        "land_traffic_flow_snapshot",
        "materialize_verify_publish_traffic_flow",
    ]
    land = dag_module.dag.get_task("land_traffic_flow_snapshot")
    materialize = dag_module.dag.get_task("materialize_verify_publish_traffic_flow")

    assert land.downstream_task_ids == {materialize.task_id}
    assert materialize.pool == dag_module.TRINO_INGEST_POOL
    assert materialize.outlets == [dag_module.TRAFFIC_FLOW_MATERIALIZED_ALIAS]
    assert dag_module.dag.max_active_runs == 1
    assert type(dag_module.dag.timetable).__name__ in {
        "DatasetTriggeredTimetable",
        "AssetTriggeredTimetable",
    }
    schedule_repr = repr(
        getattr(dag_module.dag, "schedule", dag_module.dag.timetable)
    )
    if dag_module.TRAFFIC_INCIDENT_BRONZE_ASSET not in schedule_repr:
        schedule_repr = repr(getattr(dag_module.dag.timetable, "dataset_condition", ""))
    assert dag_module.TRAFFIC_INCIDENT_BRONZE_ASSET in schedule_repr


def test_flow_landing_wrapper_uses_exact_triggering_incident_parent(monkeypatch):
    captured = {}

    class Pipeline:
        def land(self, **kwargs):
            captured.update(kwargs)
            return {"parent_incident_run_id": kwargs["parent_incident_run_id"]}

    monkeypatch.setattr(dag_module, "build_traffic_flow_pipeline", lambda: Pipeline())

    result = dag_module.land_traffic_flow_snapshot(
        run_id="asset__flow-1",
        dag_run=types.SimpleNamespace(conf={}),
        triggering_asset_events={
            dag_module.TRAFFIC_INCIDENT_BRONZE_ASSET: [
                types.SimpleNamespace(
                    timestamp=datetime(2026, 7, 7, tzinfo=timezone.utc),
                    extra={},
                ),
                _incident_event(),
            ]
        },
    )

    assert result == {"parent_incident_run_id": "incident-1"}
    assert captured == {
        "parent_incident_run_id": "incident-1",
        "flow_run_id": "asset__flow-1",
        "conf": {},
    }


def test_flow_materializer_does_not_emit_asset_for_stale_parent(monkeypatch):
    class Outcome:
        row_count = 1
        asset_metadata = None

    class Pipeline:
        def materialize(self, **_kwargs):
            return Outcome()

    class Accessor:
        def add(self, *_args, **_kwargs):
            pytest.fail("stale Flow must not emit an Asset")

    monkeypatch.setattr(dag_module, "build_traffic_flow_pipeline", lambda: Pipeline())
    ti = types.SimpleNamespace(
        xcom_pull=lambda **_kwargs: {"parent_incident_run_id": "incident-old"}
    )

    assert dag_module.materialize_verify_publish_traffic_flow(
        run_id="asset__flow-1",
        ti=ti,
        outlet_events={dag_module.TRAFFIC_FLOW_MATERIALIZED_ALIAS: Accessor()},
    ) == {"row_count": 1, "asset_published": False}


def test_flow_product_events_use_raw_and_bronze_manifest_rows(monkeypatch):
    captured = []
    monkeypatch.setattr(
        dag_module,
        "record_product_event",
        lambda _context, **kwargs: captured.append(kwargs) or kwargs,
    )

    class TI:
        def xcom_pull(self, *, task_ids):
            if task_ids == dag_module.LAND_TASK_ID:
                return {"expected_rows": 9}
            if task_ids == dag_module.FLOW_MATERIALIZE_TASK_ID:
                return {"row_count": 8}
            raise AssertionError(f"unexpected task_id: {task_ids}")

    context = {"ti": TI(), "run_id": "run-1"}
    dag_module.record_traffic_raw_product_event(context)
    dag_module.record_traffic_bronze_product_event(context)

    assert captured == [
        {
            "domain": "traffic",
            "layer": "raw",
            "row_count": 9,
            "rows_source": "raw_manifest",
        },
        {
            "domain": "traffic",
            "layer": "bronze",
            "row_count": 8,
            "rows_source": "bronze_run_manifest",
        },
    ]


def test_flow_product_event_keeps_malformed_rows_unknown(monkeypatch):
    captured = []
    monkeypatch.setattr(
        dag_module,
        "record_product_event",
        lambda _context, **kwargs: captured.append(kwargs) or kwargs,
    )

    class TI:
        def xcom_pull(self, *, task_ids):
            assert task_ids == dag_module.LAND_TASK_ID
            return {"expected_rows": "9"}

    dag_module.record_traffic_raw_product_event({"ti": TI(), "run_id": "run-1"})

    assert captured == [
        {
            "domain": "traffic",
            "layer": "raw",
            "row_count": None,
            "rows_source": "not_observed",
        }
    ]
