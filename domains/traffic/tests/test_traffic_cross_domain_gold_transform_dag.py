from __future__ import annotations

from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_cross_domain_gold_transform as module  # noqa: E402


def test_cross_domain_dag_reconverges_after_core_gold_and_weather_updates():
    schedule_text = repr(module.dag.timetable.dataset_condition)

    assert module.TRAFFIC_INCIDENT_SILVER_ASSET in schedule_text
    assert module.TRAFFIC_CORE_GOLD_PUBLICATION_READY_ASSET in schedule_text
    assert module.WEATHER_GOLD_PUBLICATION_READY_ASSET in schedule_text


def test_cross_domain_success_asset_scopes_both_cross_domain_products(monkeypatch):
    captured = {}

    class Accessor:
        extra = None

    accessor = Accessor()
    monkeypatch.setattr(module, "write_success_marker", lambda **_kwargs: "marker")
    monkeypatch.setattr(
        module,
        "current_silver_output_evidence",
        lambda: {"flow_run_id": "flow-42"},
    )
    monkeypatch.setattr(
        module.core,
        "_gold_identity",
        lambda **_kwargs: {"flow_run_id": "flow-42"},
    )

    result = module.mark_cross_domain_gold_success(
        ti=object(),
        run_id="asset__cross-domain-42",
        outlet_events={
            module.TRAFFIC_CROSS_DOMAIN_GOLD_PUBLICATION_READY_ASSET_REF: accessor
        },
    )

    captured.update(accessor.extra)
    assert result == {"marker": "marker"}
    assert captured[module.TRAFFIC_GOLD_PUBLICATION_SCOPE_KEY] == [
        "traffic_incident_x_weather_current_hourly",
        "traffic_road_congestion_context_current",
    ]
