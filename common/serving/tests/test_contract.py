from __future__ import annotations

import json

import pytest

from common.serving import contract as contract_module
from common.serving import dag_factory
from common.serving.contract import load_contracts


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
                    "upsert_strategy": "exact_set",
                    "zero_policy": "retain_last_good",
                    "primary_key": ["link_id"],
                }
            }
        },
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": nodes}), encoding="utf-8")
    return path


def _projection_manifest(tmp_path, *, projection=None, column_overrides=None):
    columns = {
        "product_row_id": {
            "description": "row id",
            "data_type": "VARCHAR",
            "config": {
                "meta": {
                    "nullable": False,
                    "semantic_role": "primary_key",
                    "unit": "not_applicable",
                }
            },
        },
        "value": {
            "description": "measurement",
            "data_type": " DOUBLE ",
            "config": {
                "meta": {
                    "nullable": True,
                    "semantic_role": "metric",
                    "unit": "km/h",
                    "null_meaning": "not_measured",
                }
            },
        },
    }
    if column_overrides:
        for name, override in column_overrides.items():
            columns[name] = override
    serving = {
        "enabled": True,
        "external": True,
        "product_id": "weather_place_current_outlook",
        "product_question": "current outlook?",
        "grain": "place row",
        "publication_mode": "snapshot",
        "zero_policy": "fail",
        "primary_key": ["product_row_id"],
    }
    if projection is not None:
        serving["public_projection"] = projection
    path = tmp_path / "projection_manifest.json"
    path.write_text(
        json.dumps(
            {
                "nodes": {
                    "model.project.gold_weather_place_current_outlook": {
                        "resource_type": "model",
                        "name": "gold_weather_place_current_outlook",
                        "columns": columns,
                        "config": {"meta": {"serving": serving}},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
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


def test_load_domain_contracts_allows_an_explicit_partitioned_scope(tmp_path):
    loader = _load_domain_contracts()

    contracts = loader(
        _manifest(tmp_path),
        "weather",
        WEATHER_PRODUCTS[:2],
        allow_partitioned_scope=True,
    )

    assert [contract.product_id for contract in contracts] == sorted(WEATHER_PRODUCTS[:2])


def test_load_contracts_reads_opt_in_upsert_strategy(tmp_path):
    contract = contract_module.load_contracts(_manifest(tmp_path), ["traffic_flow_link_latest"])[0]

    assert contract.publication_mode == "upsert"
    assert contract.upsert_strategy == "exact_set"


def test_load_contracts_reads_source_evidence_and_freshness_slo(tmp_path):
    path = _projection_manifest(tmp_path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    serving = manifest["nodes"]["model.project.gold_weather_place_current_outlook"]["config"]["meta"]["serving"]
    serving["event_time"] = "observed_at"
    serving["freshness_slo_minutes"] = 90
    serving["source_evidence"] = [{
        "source_id": "kma_vilage_fcst",
        "source_url": "https://example.test/kma",
        "license": "KOGL-1",
        "license_url": "https://example.test/kogl",
        "redistribution": "allowed_with_attribution",
        "attribution": "기상청",
        "rights_checked_at": "2026-08-04",
    }]
    path.write_text(json.dumps(manifest), encoding="utf-8")

    contract = load_contracts(path)[0]

    assert contract.freshness_slo_minutes == 90
    assert contract.source_evidence == (
        {
            "source_id": "kma_vilage_fcst",
            "source_url": "https://example.test/kma",
            "license": "KOGL-1",
            "license_url": "https://example.test/kogl",
            "redistribution": "allowed_with_attribution",
            "attribution": "기상청",
            "rights_checked_at": "2026-08-04",
        },
    )


def test_load_contracts_reads_freshness_field_separately_from_event_time(tmp_path):
    timestamp_meta = {
        "description": "quality timestamp",
        "data_type": "TIMESTAMP",
        "config": {
            "meta": {
                "nullable": False,
                "semantic_role": "timestamp",
                "unit": "datetime",
            }
        },
    }
    path = _projection_manifest(
        tmp_path,
        projection={
            "schema_version": "1.0.0",
            "columns": ["product_row_id", "value", "observed_at", "collected_at"],
        },
        column_overrides={
            "observed_at": timestamp_meta,
            "collected_at": timestamp_meta,
        },
    )
    manifest = json.loads(path.read_text(encoding="utf-8"))
    serving = manifest["nodes"]["model.project.gold_weather_place_current_outlook"]["config"]["meta"]["serving"]
    serving["event_time"] = "observed_at"
    serving["freshness_field"] = "collected_at"
    serving["freshness_timezone"] = "UTC"
    serving["freshness_slo_minutes"] = 90
    path.write_text(json.dumps(manifest), encoding="utf-8")

    contract = load_contracts(path)[0]

    assert contract.event_time == "observed_at"
    assert contract.freshness_field == "collected_at"
    assert contract.freshness_timezone == "UTC"


def test_load_contracts_retains_publication_trigger_for_runtime_watchdog(tmp_path):
    path = _projection_manifest(tmp_path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    serving = manifest["nodes"]["model.project.gold_weather_place_current_outlook"]["config"]["meta"]["serving"]
    serving["publication_trigger"] = {"schedule_cron": "10 * * * *"}
    path.write_text(json.dumps(manifest), encoding="utf-8")

    contract = load_contracts(path)[0]

    assert contract.publication_trigger == {"schedule_cron": "10 * * * *"}


def test_load_contracts_rejects_unknown_freshness_field(tmp_path):
    path = _projection_manifest(tmp_path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    serving = manifest["nodes"]["model.project.gold_weather_place_current_outlook"]["config"]["meta"]["serving"]
    serving["freshness_field"] = "missing_collected_at"
    path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="freshness_field unknown column missing_collected_at"):
        load_contracts(path)


def test_load_contracts_reads_quality_coverage_gate(tmp_path):
    path = _projection_manifest(tmp_path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    serving = manifest["nodes"]["model.project.gold_weather_place_current_outlook"]["config"]["meta"]["serving"]
    serving["quality_coverage"] = {
        "field": "value",
        "expected_distinct_count": 427,
        "minimum_ratio": 1.0,
    }
    path.write_text(json.dumps(manifest), encoding="utf-8")

    contract = load_contracts(path)[0]

    assert contract.quality_coverage == {
        "field": "value",
        "expected_distinct_count": 427,
        "minimum_ratio": 1.0,
        "measurement_scope": "published_rows",
    }


def test_load_contracts_reads_source_relation_coverage_outside_public_projection(tmp_path):
    path = _projection_manifest(
        tmp_path,
        projection={"schema_version": "1.0.0", "columns": ["product_row_id", "value"]},
        column_overrides={
            "dataset": {
                "description": "source dataset",
                "data_type": "VARCHAR",
                "config": {
                    "meta": {
                        "nullable": False,
                        "semantic_role": "source_identifier",
                        "unit": "not_applicable",
                    }
                },
            }
        },
    )
    manifest = json.loads(path.read_text(encoding="utf-8"))
    serving = manifest["nodes"]["model.project.gold_weather_place_current_outlook"]["config"]["meta"]["serving"]
    serving["quality_coverage"] = {
        "field": "dataset",
        "expected_distinct_count": 152,
        "minimum_ratio": 0.95,
        "measurement_scope": "source_relation",
    }
    path.write_text(json.dumps(manifest), encoding="utf-8")

    contract = load_contracts(path)[0]

    assert contract.quality_coverage == {
        "field": "dataset",
        "expected_distinct_count": 152,
        "minimum_ratio": 0.95,
        "measurement_scope": "source_relation",
    }


def test_load_contracts_reads_explicit_not_applicable_coverage_reason(tmp_path):
    path = _projection_manifest(tmp_path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    serving = manifest["nodes"]["model.project.gold_weather_place_current_outlook"]["config"]["meta"]["serving"]
    serving["quality_coverage"] = {
        "not_applicable_reason": "eligible source population is dynamic",
    }
    path.write_text(json.dumps(manifest), encoding="utf-8")

    contract = load_contracts(path)[0]

    assert contract.quality_coverage == {
        "not_applicable_reason": "eligible source population is dynamic",
    }


def test_load_contracts_uses_public_primary_key_for_rollup_projection(tmp_path):
    path = _projection_manifest(
        tmp_path,
        projection={"schema_version": "1.0.0", "columns": ["product_row_id", "value"]},
    )
    manifest = json.loads(path.read_text(encoding="utf-8"))
    serving = manifest["nodes"]["model.project.gold_weather_place_current_outlook"]["config"]["meta"]["serving"]
    serving["primary_key"] = ["product_row_id", "value"]
    serving["public_primary_key"] = ["product_row_id"]
    path.write_text(json.dumps(manifest), encoding="utf-8")

    contract = load_contracts(path)[0]

    assert contract.primary_key == ("product_row_id",)


def test_load_contracts_rejects_source_evidence_missing_attribution(tmp_path):
    path = _projection_manifest(tmp_path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    serving = manifest["nodes"]["model.project.gold_weather_place_current_outlook"]["config"]["meta"]["serving"]
    serving["source_evidence"] = [{
        "source_id": "kma_vilage_fcst",
        "source_url": "https://example.test/kma",
        "license": "KOGL-1",
        "license_url": "https://example.test/kogl",
        "redistribution": "allowed_with_attribution",
        "rights_checked_at": "2026-08-04",
    }]
    path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="source_evidence.*attribution"):
        load_contracts(path)


def test_load_contracts_rejects_source_evidence_url_with_credentials(tmp_path):
    path = _projection_manifest(tmp_path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    serving = manifest["nodes"]["model.project.gold_weather_place_current_outlook"]["config"]["meta"]["serving"]
    serving["source_evidence"] = [{
        "source_id": "kma_vilage_fcst",
        "source_url": "https://user:password@example.test/kma",
        "license": "KOGL-1",
        "license_url": "https://example.test/kogl",
        "redistribution": "allowed_with_attribution",
        "attribution": "기상청",
        "rights_checked_at": "2026-08-04",
    }]
    path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="source_url and license_url"):
        load_contracts(path)


def test_load_contracts_reads_public_projection_in_order_with_canonical_hash(tmp_path):
    contract = load_contracts(
        _projection_manifest(
            tmp_path,
            projection={"schema_version": "1.0.0", "columns": ["product_row_id", "value"]},
        )
    )[0]

    assert contract.public_projection == ("product_row_id", "value")
    assert contract.projection_schema_version == "1.0.0"
    assert contract.projection_schema_hash == "111fb92a950e1d4a7beb1a661c64e1c3f6ab47e602b154287696b7a89db01614"


def test_public_projection_hash_ignores_description_but_changes_on_identity_metadata(tmp_path):
    projection = {"schema_version": "1.0.0", "columns": ["product_row_id", "value"]}
    baseline = load_contracts(_projection_manifest(tmp_path, projection=projection))[0].projection_schema_hash
    description_only = load_contracts(
        _projection_manifest(
            tmp_path,
            projection=projection,
            column_overrides={
                "value": {
                    "description": "changed copy",
                    "data_type": " DOUBLE ",
                    "config": {
                        "meta": {
                            "nullable": True,
                            "semantic_role": "metric",
                            "unit": "km/h",
                        }
                    },
                }
            },
        )
    )[0].projection_schema_hash
    reordered = load_contracts(
        _projection_manifest(
            tmp_path,
            projection={"schema_version": "1.0.0", "columns": ["value", "product_row_id"]},
        )
    )[0].projection_schema_hash
    type_changed = load_contracts(
        _projection_manifest(
            tmp_path,
            projection=projection,
            column_overrides={
                "value": {
                    "description": "measurement",
                    "data_type": "DECIMAL(10,2)",
                    "config": {
                        "meta": {
                            "nullable": True,
                            "semantic_role": "metric",
                            "unit": "km/h",
                        }
                    },
                }
            },
        )
    )[0].projection_schema_hash
    nullable_changed = load_contracts(
        _projection_manifest(
            tmp_path,
            projection=projection,
            column_overrides={
                "value": {
                    "description": "measurement",
                    "data_type": "DOUBLE",
                    "config": {
                        "meta": {
                            "nullable": False,
                            "semantic_role": "metric",
                            "unit": "km/h",
                        }
                    },
                }
            },
        )
    )[0].projection_schema_hash
    unit_changed = load_contracts(
        _projection_manifest(
            tmp_path,
            projection=projection,
            column_overrides={
                "value": {
                    "description": "measurement",
                    "data_type": "DOUBLE",
                    "config": {
                        "meta": {
                            "nullable": True,
                            "semantic_role": "metric",
                            "unit": "m/s",
                        }
                    },
                }
            },
        )
    )[0].projection_schema_hash
    role_changed = load_contracts(
        _projection_manifest(
            tmp_path,
            projection=projection,
            column_overrides={
                "value": {
                    "description": "measurement",
                    "data_type": "DOUBLE",
                    "config": {
                        "meta": {
                            "nullable": True,
                            "semantic_role": "analytical_value",
                            "unit": "km/h",
                        }
                    },
                }
            },
        )
    )[0].projection_schema_hash

    assert description_only == baseline
    assert reordered != baseline
    assert type_changed != baseline
    assert nullable_changed != baseline
    assert unit_changed != baseline
    assert role_changed != baseline


@pytest.mark.parametrize(
    "projection,error",
    [
        (["product_row_id"], "object"),
        ({"schema_version": "1.0", "columns": ["product_row_id"]}, "semver"),
        ({"schema_version": "1.0.0", "columns": []}, "columns"),
        ({"schema_version": "1.0.0", "columns": ["product_row_id", "product_row_id"]}, "duplicate"),
        ({"schema_version": "1.0.0", "columns": ["product_row_id as id"]}, "identifier"),
        ({"schema_version": "1.0.0", "columns": ["*"]}, "identifier"),
        ({"schema_version": "1.0.0", "columns": ["unknown_column"]}, "unknown"),
        ({"schema_version": "1.0.0", "columns": ["product_row_id"], "rename_map": {}}, "exactly"),
    ],
)
def test_load_contracts_rejects_malformed_public_projection(tmp_path, projection, error):
    with pytest.raises(ValueError, match=error):
        load_contracts(_projection_manifest(tmp_path, projection=projection))


def test_load_contracts_rejects_public_projection_with_incomplete_identity_metadata(tmp_path):
    projection = {"schema_version": "1.0.0", "columns": ["product_row_id", "value"]}
    incomplete_value = {
        "description": "measurement",
        "data_type": "DOUBLE",
        "config": {"meta": {"nullable": True, "semantic_role": "metric"}},
    }

    with pytest.raises(ValueError, match="identity metadata"):
        load_contracts(
            _projection_manifest(
                tmp_path,
                projection=projection,
                column_overrides={"value": incomplete_value},
            )
        )


def test_load_contracts_rejects_top_level_meta_only_projection_metadata(tmp_path):
    projection = {"schema_version": "1.0.0", "columns": ["product_row_id", "value"]}
    compiled_only_value = {
        "description": "measurement",
        "data_type": "DOUBLE",
        "meta": {
            "nullable": True,
            "semantic_role": "metric",
            "unit": "km/h",
        },
    }

    with pytest.raises(ValueError, match="identity metadata"):
        load_contracts(
            _projection_manifest(
                tmp_path,
                projection=projection,
                column_overrides={"value": compiled_only_value},
            )
        )


def test_require_public_projection_rejects_missing_projection_before_runtime(tmp_path):
    with pytest.raises(ValueError, match="public_projection required"):
        load_contracts(
            _projection_manifest(tmp_path, projection=None),
            require_public_projection=True,
        )


def test_default_contract_loading_accepts_missing_public_projection(tmp_path):
    contract = load_contracts(_projection_manifest(tmp_path, projection=None))[0]

    assert contract.public_projection is None
    assert contract.projection_schema_hash is None


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

    partitioned = loader(
        path,
        "citydata",
        ["citydata_place_latest"],
        exact_domain_contracts=True,
        partitioned_domain_scope=True,
    )
    assert [contract.product_id for contract in partitioned] == ["citydata_place_latest"]


def test_export_contract_loader_keeps_public_projection_requirement_explicit(tmp_path):
    loader = getattr(dag_factory, "_load_export_contracts", None)
    assert loader is not None

    legacy = loader(
        _projection_manifest(tmp_path, projection=None),
        "weather",
        ["weather_place_current_outlook"],
        exact_domain_contracts=False,
        require_public_projection=False,
    )

    assert legacy[0].public_projection is None
    with pytest.raises(ValueError, match="public_projection required"):
        loader(
            _projection_manifest(tmp_path, projection=None),
            "weather",
            ["weather_place_current_outlook"],
            exact_domain_contracts=False,
            require_public_projection=True,
        )
