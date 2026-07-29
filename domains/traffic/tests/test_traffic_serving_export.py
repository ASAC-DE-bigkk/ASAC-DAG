import importlib.util
import sys
import types
from pathlib import Path

import pytest


DAG_PATH = Path(__file__).resolve().parents[1] / "traffic_serving_export.py"


def test_traffic_serving_export_delegates_all_six_products_to_common_publisher(monkeypatch):
    """Catch a Traffic wrapper that duplicates Publisher policy or omits a selected product."""
    if not DAG_PATH.is_file():
        pytest.fail("traffic_serving_export wrapper is missing")

    captured = {}
    sentinel_dag = object()
    factory_module = types.ModuleType("common.serving.dag_factory")

    def build_serving_export_dag(**kwargs):
        captured.update(kwargs)
        return sentinel_dag

    factory_module.build_serving_export_dag = build_serving_export_dag
    monkeypatch.setitem(sys.modules, "common.serving.dag_factory", factory_module)

    spec = importlib.util.spec_from_file_location("traffic_serving_export_under_test", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    assert module.dag is sentinel_dag
    assert captured == {
        "domain": "traffic",
        "product_ids": [
            "traffic_incident_x_weather_current_hourly",
            "traffic_flow_congestion_hotspots_hourly",
            "traffic_flow_link_latest",
            "traffic_flow_change_latest",
            "traffic_flow_link_time_profile",
            "traffic_flow_anomaly_current",
        ],
        "exact_domain_contracts": True,
        "schedule": None,
        "dag_id": "traffic_serving_export",
        "target": "dev",
        "schema": "traffic",
    }


def test_traffic_export_is_visible_to_airflow_safe_mode():
    if not DAG_PATH.is_file():
        pytest.fail("traffic_serving_export wrapper is missing")

    source = DAG_PATH.read_text(encoding="utf-8").lower()
    assert "airflow" in source
    assert "dag" in source


def test_traffic_export_is_the_only_traffic_serving_dag():
    assert not (DAG_PATH.parent / "traffic_insight_serving_export.py").exists()
