from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_transform_test_support import (  # noqa: E402
    FakeAssetExpression,
    load_gold_transform_module,
)
from traffic_transform_test_support import (  # noqa: E402, F401
    restore_airflow_modules_after_dag_import,
)


@pytest.fixture
def module():
    module_path = (
        Path(__file__).resolve().parents[1]
        / "traffic_cross_domain_gold_transform.py"
    )
    original_core = sys.modules.get("traffic_gold_transform")
    core = load_gold_transform_module()
    sys.modules["traffic_gold_transform"] = core
    spec = importlib.util.spec_from_file_location(
        "traffic_cross_domain_gold_transform_under_test",
        module_path,
    )
    loaded = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    try:
        spec.loader.exec_module(loaded)
        yield loaded
    finally:
        if original_core is None:
            sys.modules.pop("traffic_gold_transform", None)
        else:
            sys.modules["traffic_gold_transform"] = original_core


def _scheduled_asset_uris(value) -> set[str]:
    if isinstance(value, FakeAssetExpression):
        return {
            uri
            for child in value.assets
            for uri in _scheduled_asset_uris(child)
        }
    uri = getattr(value, "uri", None)
    return {str(uri)} if uri else set()


def test_cross_domain_dag_reconverges_after_core_gold_and_weather_updates(module):
    schedule_uris = _scheduled_asset_uris(module.dag.kwargs["schedule"])

    assert schedule_uris == {
        module.TRAFFIC_INCIDENT_SILVER_ASSET,
        module.TRAFFIC_CORE_GOLD_PUBLICATION_READY_ASSET,
        module.WEATHER_GOLD_PUBLICATION_READY_ASSET,
    }


def test_cross_domain_build_is_single_threaded(module):
    specs = {spec.task_id: spec for spec in module.CROSS_DOMAIN_PHASE_SPECS}

    assert specs["dbt_run_cross_domain_gold"].threads == 1
    assert module.dbt_phase_tasks["dbt_run_cross_domain_gold"].kwargs[
        "op_kwargs"
    ]["threads"] == 1


def test_cross_domain_success_asset_scopes_both_cross_domain_products(
    module, monkeypatch
):
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
