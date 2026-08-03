"""transit_serving_export 얇은 wrapper 계약 테스트 (#668).

3-tier 분할(fast/hourly/daily)이라 공통 exact_domain_contracts 검사를 DAG 단위로 못 쓴다
(citydata 관례 동일) — 대신 티어 합집합이 transit enabled 6제품과 정확히 일치함을 여기서
고정해, 티어 누락·중복으로 인한 조용한 부분 게시를 막는다.
"""

import importlib.util
import sys
import types
from pathlib import Path

DAG_PATH = Path(__file__).resolve().parents[1] / "transit_serving_export.py"

EXPECTED_PRODUCTS = {
    "transit_dong_now",
    "transit_parking_full_risk",
    "transit_dong_hourly",
    "transit_forecast_card",
    "transit_event_access",
    "transit_parking_profile",
}


def _load_module_with_fake_factory(monkeypatch):
    calls = []
    factory_module = types.ModuleType("common.serving.dag_factory")

    def build_serving_export_dag(**kwargs):
        calls.append(kwargs)
        return object()

    factory_module.build_serving_export_dag = build_serving_export_dag
    monkeypatch.setitem(sys.modules, "common.serving.dag_factory", factory_module)

    spec = importlib.util.spec_from_file_location(
        "transit_serving_export_under_test", DAG_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module, calls


def test_transit_serving_export_declares_three_thin_tier_wrappers(monkeypatch):
    monkeypatch.delenv("DBT_TARGET", raising=False)

    _, calls = _load_module_with_fake_factory(monkeypatch)

    assert calls == [
        {
            "domain": "transit",
            "product_ids": ["transit_dong_now", "transit_parking_full_risk"],
            "schedule": "10,25,40,55 * * * *",
            "dag_id": "transit_serving_export_fast",
            "target": "prod",
        },
        {
            "domain": "transit",
            "product_ids": ["transit_dong_hourly"],
            "schedule": "40 * * * *",
            "dag_id": "transit_serving_export_hourly",
            "target": "prod",
        },
        {
            "domain": "transit",
            "product_ids": [
                "transit_forecast_card",
                "transit_event_access",
                "transit_parking_profile",
            ],
            "schedule": "40 8 * * *",
            "dag_id": "transit_serving_export_daily",
            "target": "prod",
        },
    ]


def test_target_knob_follows_dbt_target_env(monkeypatch):
    monkeypatch.setenv("DBT_TARGET", "dev")

    _, calls = _load_module_with_fake_factory(monkeypatch)

    assert [c["target"] for c in calls] == ["dev", "dev", "dev"]


def test_tiers_cover_the_six_enabled_products_disjointly(monkeypatch):
    monkeypatch.delenv("DBT_TARGET", raising=False)

    _, calls = _load_module_with_fake_factory(monkeypatch)

    tiers = [c["product_ids"] for c in calls]
    flat = [pid for tier in tiers for pid in tier]
    assert len(flat) == len(set(flat)), "티어 간 product_id 중복"
    assert set(flat) == EXPECTED_PRODUCTS
