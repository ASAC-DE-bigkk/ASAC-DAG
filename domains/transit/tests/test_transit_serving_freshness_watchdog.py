"""Thin wrapper contract for the Transit independent serving watchdog (#720)."""

import importlib.util
import sys
import types
from pathlib import Path


DAG_PATH = Path(__file__).resolve().parents[1] / "transit_serving_freshness_watchdog.py"


def _load_module(monkeypatch):
    calls = []
    factory_module = types.ModuleType("common.serving.dag_factory")
    factory_module.build_serving_freshness_watchdog_dag = (
        lambda **kwargs: calls.append(kwargs) or object()
    )
    errors_module = types.ModuleType("common.errors.airflow")
    errors_module.problem_failure_callback = lambda **kwargs: ("callback", kwargs)
    monkeypatch.setitem(sys.modules, "common.serving.dag_factory", factory_module)
    monkeypatch.setitem(sys.modules, "common.errors.airflow", errors_module)

    spec = importlib.util.spec_from_file_location(
        "transit_serving_freshness_watchdog_under_test", DAG_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module, calls


def test_fast_tier_watchdog_uses_kst_source_time_contract(monkeypatch):
    monkeypatch.delenv("DBT_TARGET", raising=False)

    _, calls = _load_module(monkeypatch)

    assert calls == [
        {
            "domain": "transit",
            "product_ids": ["transit_dong_now", "transit_parking_full_risk"],
            "schedule": "7,22,37,52 * * * *",
            "dag_id": "transit_serving_freshness_watchdog",
            "target": "prod",
            "naive_freshness_timezones": {
                "transit_dong_now": "Asia/Seoul",
                "transit_parking_full_risk": "Asia/Seoul",
            },
            "publication_grace_minutes": 10,
            "failure_callback": (
                "callback",
                {"domain": "transit", "source_system": "serving_freshness_watchdog"},
            ),
        }
    ]


def test_watchdog_target_follows_dbt_target_env(monkeypatch):
    monkeypatch.setenv("DBT_TARGET", "dev")

    _, calls = _load_module(monkeypatch)

    assert calls[0]["target"] == "dev"
