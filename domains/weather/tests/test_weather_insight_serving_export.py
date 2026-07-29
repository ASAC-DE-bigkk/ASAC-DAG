import importlib.util
import sys
import types
from pathlib import Path


DAG_PATH = Path(__file__).resolve().parents[1] / "weather_insight_serving_export.py"


def test_weather_insight_export_delegates_only_the_three_new_products(monkeypatch):
    captured = {}
    sentinel_dag = object()
    factory_module = types.ModuleType("common.serving.dag_factory")

    def build_serving_export_dag(**kwargs):
        captured.update(kwargs)
        return sentinel_dag

    factory_module.build_serving_export_dag = build_serving_export_dag
    monkeypatch.setitem(sys.modules, "common.serving.dag_factory", factory_module)

    spec = importlib.util.spec_from_file_location(
        "weather_insight_serving_export_under_test",
        DAG_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.dag is sentinel_dag
    assert captured == {
        "domain": "weather",
        "product_ids": [
            "weather_place_precipitation_window",
            "weather_place_risk_window",
            "weather_place_forecast_change_daily",
        ],
        "schedule": None,
        "dag_id": "weather_insight_serving_export",
        "target": "dev",
        "schema": "weather",
    }


def test_weather_insight_export_is_visible_to_airflow_safe_mode():
    source = DAG_PATH.read_text(encoding="utf-8").lower()
    assert "airflow" in source
    assert "dag" in source
