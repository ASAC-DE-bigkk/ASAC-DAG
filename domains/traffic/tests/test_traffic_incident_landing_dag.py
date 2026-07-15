from __future__ import annotations

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_incident_landing_dag_has_one_non_trino_task_and_five_minute_cadence(
    monkeypatch,
):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    import traffic_incident_landing as module

    assert module.dag.task_ids == ["land_traffic_incident_snapshot"]
    task = module.dag.get_task("land_traffic_incident_snapshot")
    assert task.pool is None or task.pool == "default_pool"
    assert task.outlets == [module.TRAFFIC_INCIDENT_RAW_ASSET_REF]
    assert module.dag.max_active_runs == 2
    assert module.traffic_dag_schedule() == "*/5 * * * *"
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "trino_cursor" not in source
    assert "TRINO_HEAVY_POOL" not in source


def test_incident_landing_wrapper_maps_context_and_sets_raw_asset_metadata(monkeypatch):
    import traffic_incident_landing as module

    captured = {}

    class Outcome:
        raw_result = {"raw_object_keys": ["raw/traffic/page.xml"]}
        receipt_key = "receipt/LANDED.json"
        asset_metadata = {
            "source_id": "seoul_traffic_incident",
            "snapshot_run_id": "scheduled__snapshot-1",
        }

    class Lifecycle:
        def run(self, **kwargs):
            captured.update(kwargs)
            return Outcome()

    class Event:
        extra = None

    event = Event()
    monkeypatch.setattr(module, "build_incident_landing_lifecycle", lambda: Lifecycle())
    monkeypatch.setattr(
        module, "resolve_acc_info_page_window", lambda _conf: (1, 1000, 1000)
    )

    result = module.land_traffic_incident_snapshot(
        dag=type("Dag", (), {"dag_id": "traffic_incident_landing"})(),
        dag_run=type("DagRun", (), {"conf": {}})(),
        run_id="scheduled__snapshot-1",
        logical_date="2026-07-16T00:05:00+00:00",
        outlet_events={module.TRAFFIC_INCIDENT_RAW_ASSET_REF: event},
    )

    assert result == {
        "raw_result": Outcome.raw_result,
        "receipt_key": Outcome.receipt_key,
        "snapshot_run_id": "scheduled__snapshot-1",
    }
    assert captured["run"].dag_id == "traffic_incident_landing"
    assert captured["request"].start_index == 1
    assert event.extra == Outcome.asset_metadata
