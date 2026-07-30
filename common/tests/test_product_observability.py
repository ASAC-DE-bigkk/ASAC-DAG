from __future__ import annotations

from datetime import datetime, timezone

from common.ops.product_observability import (
    build_product_event,
    build_traffic_product_health,
    build_weather_product_health,
    record_domain_stage_event,
)


class _TaskInstance:
    dag_id = "weather_vilage_fcst_bronze"
    task_id = "verify_kma_bronze_runtime"
    run_id = "scheduled__2026-07-30T00:00:00+00:00"
    try_number = 2


def _context(**extra):
    context = {
        "ti": _TaskInstance(),
        "logical_date": datetime(2026, 7, 30, 0, 0, tzinfo=timezone.utc),
        "params": {"target": "prod"},
    }
    context.update(extra)
    return context


def test_product_event_keeps_product_ids_runtime_publication_and_quality_together():
    key, event = build_product_event(
        _context(),
        domain="weather",
        layer="bronze",
        product_ids=("weather_place_current_outlook",),
        row_count=427,
        quality={"coverage": {"value": 1.0, "quality_state": "observed"}},
    )

    assert key == (
        "ops/product-events/observed_date=2026-07-30/domain=weather/"
        "layer=bronze/weather_vilage_fcst_bronze__"
        "scheduled__2026-07-30T00_00_00_00_00__"
        "verify_kma_bronze_runtime__try2.json"
    )
    assert event["schema_version"] == "product-observability/v1"
    assert event["product_ids"] == ["weather_place_current_outlook"]
    assert event["publication_id"] is None
    assert event["row_count"] == 427
    assert event["quality"]["coverage"]["quality_state"] == "observed"


def test_weather_health_distinguishes_observed_zero_from_unknown_metric():
    health = build_weather_product_health(
        weather={
            "expected_base_time_count": 8,
            "base_time_count": 8,
            "expected_grid_count": 80,
            "base_date": "20260730",
            "base_time": "0500",
            "freshness_minutes": 20,
        },
        profile={
            "core_category_count": 4,
            "mapped_place_count": 80,
            "forecast_horizon_hours": 72,
        },
        detected_at=datetime(2026, 7, 30, 6, 0, tzinfo=timezone.utc),
    )

    assert health["metrics"]["expected_issue_count"] == {
        "value": 8,
        "unit": "issue",
        "quality_state": "observed",
        "null_meaning": None,
    }
    assert health["metrics"]["core_category_coverage"]["value"] == 1.0
    assert health["metrics"]["mapped_place_coverage"]["value"] == 1.0
    assert health["metrics"]["forecast_horizon"]["value"] == 72
    assert health["metrics"]["publication_delay"]["value"] is None
    assert health["metrics"]["publication_delay"]["quality_state"] == "unknown"
    assert health["metrics"]["publication_delay"]["null_meaning"] == "d1_event_not_joined"


def test_traffic_health_keeps_failed_gold_lookup_unknown_without_synthetic_zeroes():
    health = build_traffic_product_health(profile=None)

    assert set(health["metrics"]) == {
        "observed_link_count",
        "available_value_ratio",
        "link_age_p50",
        "link_age_p95",
        "source_observation_delay",
        "collection_delay",
        "stale_link_ratio",
        "publication_delay",
    }
    assert all(metric["value"] is None for metric in health["metrics"].values())
    assert all(
        metric["quality_state"] == "unknown" for metric in health["metrics"].values()
    )


def test_domain_stage_callback_records_only_after_task_success(monkeypatch):
    captured = []
    monkeypatch.setattr(
        "common.ops.product_observability.record_product_event",
        lambda context, **kwargs: captured.append((context, kwargs)) or kwargs,
    )
    callback = record_domain_stage_event("weather", "gold")
    context = _context()

    callback(context)

    assert captured == [
        (
            context,
            {
                "domain": "weather",
                "layer": "gold",
                "product_ids": (),
                "status": "success",
            },
        )
    ]


def test_domain_stage_failure_callback_records_failed_event(monkeypatch):
    captured = []
    monkeypatch.setattr(
        "common.ops.product_observability.record_product_event",
        lambda context, **kwargs: captured.append((context, kwargs)) or kwargs,
    )
    callback = record_domain_stage_event("traffic", "bronze", status="failed")
    context = _context(exception=RuntimeError("materialize failed"))

    callback(context)

    assert captured == [
        (
            context,
            {
                "domain": "traffic",
                "layer": "bronze",
                "product_ids": (),
                "status": "failed",
            },
        )
    ]
