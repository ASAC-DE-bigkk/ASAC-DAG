import importlib.util
import sys
import types
from pathlib import Path

from common.serving.d1_client import CATALOG_COLUMNS


DAG_PATH = Path(__file__).resolve().parents[1] / "weather_serving_export.py"


def test_weather_serving_export_is_a_thin_common_publisher_wrapper(monkeypatch):
    captured = {}
    sentinel_dag = object()
    factory_module = types.ModuleType("common.serving.dag_factory")

    def build_serving_export_dag(**kwargs):
        captured.update(kwargs)
        return sentinel_dag

    factory_module.build_serving_export_dag = build_serving_export_dag
    monkeypatch.setitem(sys.modules, "common.serving.dag_factory", factory_module)

    spec = importlib.util.spec_from_file_location(
        "weather_serving_export_under_test",
        DAG_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    assert module.dag is sentinel_dag
    assert captured == {
        "domain": "weather",
        "product_ids": ["weather_place_current_outlook"],
        "schedule": None,
        "dag_id": "weather_serving_export",
        "target": "dev",
        "schema": "weather",
    }


def test_weather_export_stays_manual_while_legacy_worker_catalog_field_is_missing():
    assert "serving_tier" not in CATALOG_COLUMNS
    assert "schedule=None" in DAG_PATH.read_text(encoding="utf-8")


def test_weather_export_is_visible_to_airflow_safe_mode():
    source = DAG_PATH.read_text(encoding="utf-8").lower()

    assert "airflow" in source
    assert "dag" in source
