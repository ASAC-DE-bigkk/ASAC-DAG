from __future__ import annotations

import json

import pytest

from common.serving import contract as contract_module
from common.serving import dag_factory


WEATHER_PRODUCTS = [
    "weather_place_current_outlook",
    "weather_place_precipitation_window",
    "weather_place_risk_window",
    "weather_place_forecast_change_daily",
]


def _manifest(tmp_path):
    nodes = {}
    for product_id in WEATHER_PRODUCTS:
        nodes[f"model.project.gold_{product_id}"] = {
            "resource_type": "model",
            "name": f"gold_{product_id}",
            "config": {
                "meta": {
                    "serving": {
                        "enabled": True,
                        "external": True,
                        "product_id": product_id,
                        "publication_mode": "snapshot",
                        "zero_policy": "fail",
                        "primary_key": ["product_row_id"],
                    }
                }
            },
        }
    nodes["model.project.gold_traffic_flow_link_latest"] = {
        "resource_type": "model",
        "name": "gold_traffic_flow_link_latest",
        "config": {
            "meta": {
                "serving": {
                    "enabled": True,
                    "external": True,
                    "product_id": "traffic_flow_link_latest",
                    "publication_mode": "upsert",
                    "zero_policy": "retain_last_good",
                    "primary_key": ["link_id"],
                }
            }
        },
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": nodes}), encoding="utf-8")
    return path


def _load_domain_contracts():
    loader = getattr(contract_module, "load_domain_contracts", None)
    assert loader is not None, "domain contract exact-set gate is missing"
    return loader


def test_load_domain_contracts_requires_every_enabled_weather_product(tmp_path):
    loader = _load_domain_contracts()

    with pytest.raises(ValueError, match="missing=.*weather_place_risk_window"):
        loader(_manifest(tmp_path), "weather", WEATHER_PRODUCTS[:-2])


def test_load_domain_contracts_rejects_duplicate_wrapper_product_ids(tmp_path):
    loader = _load_domain_contracts()

    with pytest.raises(ValueError, match="duplicate.*weather_place_current_outlook"):
        loader(_manifest(tmp_path), "weather", [*WEATHER_PRODUCTS, WEATHER_PRODUCTS[0]])


def test_load_domain_contracts_rejects_another_domain_product_id(tmp_path):
    loader = _load_domain_contracts()

    with pytest.raises(ValueError, match="unexpected=traffic_flow_link_latest"):
        loader(_manifest(tmp_path), "weather", [*WEATHER_PRODUCTS, "traffic_flow_link_latest"])


def test_load_domain_contracts_returns_the_exact_enabled_domain_set(tmp_path):
    loader = _load_domain_contracts()

    contracts = loader(_manifest(tmp_path), "weather", WEATHER_PRODUCTS)

    assert [contract.product_id for contract in contracts] == sorted(WEATHER_PRODUCTS)


def test_non_exact_domain_exporter_can_load_its_intended_citydata_subset(tmp_path):
    path = _manifest(tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    for product_id in ("citydata_place_latest", "citydata_ppltn_hourly"):
        payload["nodes"][f"model.project.gold_{product_id}"] = {
            "resource_type": "model",
            "name": f"gold_{product_id}",
            "config": {"meta": {"serving": {
                "enabled": True,
                "external": True,
                "product_id": product_id,
                "publication_mode": "snapshot",
                "zero_policy": "retain_last_good",
                "primary_key": ["product_row_id"],
            }}},
        }
    path.write_text(json.dumps(payload), encoding="utf-8")

    loader = getattr(dag_factory, "_load_export_contracts", None)
    assert loader is not None, "factory must keep exact-domain validation opt-in"
    contracts = loader(path, "citydata", ["citydata_place_latest"], exact_domain_contracts=False)

    assert [contract.product_id for contract in contracts] == ["citydata_place_latest"]
    with pytest.raises(ValueError, match="missing=citydata_ppltn_hourly"):
        loader(path, "citydata", ["citydata_place_latest"], exact_domain_contracts=True)
