from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.delivery_reliability.contract import (  # noqa: E402
    DeliveryEvidence,
    DeliveryState,
    ensure_unique_grain,
)


def evidence(**overrides):
    value = {
        "domain": "weather",
        "scheduled_run_id": "scheduled__2026-07-19T00:00:00+00:00",
        "scheduled_at": "2026-07-19T00:00:00Z",
        "bronze_status": "SUCCESS",
        "is_publishable": True,
        "completeness_status": "COMPLETE",
        "source_status": "SUCCESS",
        "transform_status": "SUCCESS",
        "gold_query_status": "SUCCESS",
        "gold_available_at": "2026-07-19T00:12:00Z",
        "gold_row_count": 12,
        "sla_minutes": 30,
    }
    value.update(overrides)
    return DeliveryEvidence.from_mapping(value)


def test_delivered_evidence_requires_bronze_transform_and_gold_success():
    row = evidence()

    assert row.state is DeliveryState.DELIVERED
    assert row.bronze_supplied is True
    assert row.gold_delivered is True
    assert row.sla_met is True
    assert row.delivery_latency_minutes == 12
    assert row.scheduled_at == datetime(2026, 7, 19, tzinfo=timezone.utc)


def test_normal_zero_row_is_delivered_without_fabricating_fact_rows():
    row = evidence(
        domain="traffic",
        source_status="ZERO_ROW",
        gold_row_count=0,
    )

    assert row.state is DeliveryState.DELIVERED_ZERO_ROW
    assert row.gold_delivered is True
    assert row.gold_row_count == 0


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        (
            {"completeness_status": "PARTIAL", "is_publishable": False},
            DeliveryState.PARTIAL,
        ),
        (
            {"source_status": "API_FAILURE", "is_publishable": False},
            DeliveryState.API_FAILURE,
        ),
        (
            {"source_status": "SOURCE_FAILURE", "is_publishable": False},
            DeliveryState.SOURCE_FAILURE,
        ),
        ({"transform_status": "CONTRACT_FAILURE"}, DeliveryState.CONTRACT_FAILURE),
        ({"transform_status": "FAILED"}, DeliveryState.TRANSFORM_FAILURE),
        ({"gold_query_status": "FAILED"}, DeliveryState.GOLD_QUERY_FAILURE),
        (
            {"bronze_status": "FAILED", "is_publishable": False},
            DeliveryState.BRONZE_FAILURE,
        ),
        ({"is_publishable": False}, DeliveryState.NOT_PUBLISHABLE),
        ({"gold_available_at": None}, DeliveryState.NOT_AVAILABLE),
    ],
)
def test_failure_states_remain_distinct(overrides, expected):
    assert evidence(**overrides).state is expected


def test_not_available_values_are_not_coerced_to_zero():
    row = evidence(
        transform_status=None,
        gold_query_status=None,
        gold_available_at=None,
        gold_row_count=None,
    )

    assert row.state is DeliveryState.NOT_AVAILABLE
    assert row.gold_row_count is None
    assert row.gold_delivered is None
    assert row.sla_met is None
    assert row.delivery_latency_minutes is None


def test_sla_miss_is_distinct_from_delivery_failure():
    row = evidence(gold_available_at="2026-07-19T00:45:00Z")

    assert row.state is DeliveryState.SLA_MISSED
    assert row.gold_delivered is True
    assert row.sla_met is False


def test_invalid_zero_row_contract_is_rejected():
    with pytest.raises(ValueError, match="ZERO_ROW requires gold_row_count=0"):
        evidence(source_status="ZERO_ROW", gold_row_count=2)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("bronze_status", "SUCCEEDED"),
        ("completeness_status", "GOOD"),
        ("source_status", "EMPTY"),
        ("transform_status", "ERROR"),
        ("gold_query_status", "OK"),
    ],
)
def test_unknown_status_values_are_rejected(field, value):
    with pytest.raises(ValueError, match=field):
        evidence(**{field: value})


def test_partial_or_source_failure_cannot_be_publishable():
    with pytest.raises(ValueError, match="cannot be publishable"):
        evidence(completeness_status="PARTIAL")

    with pytest.raises(ValueError, match="cannot be publishable"):
        evidence(source_status="API_FAILURE")

    with pytest.raises(ValueError, match="cannot be publishable"):
        evidence(bronze_status="FAILED")


def test_detection_time_cannot_precede_the_scheduled_run():
    with pytest.raises(ValueError, match="detected_at cannot precede scheduled_at"):
        evidence(detected_at="2026-07-18T23:59:00Z")


def test_duplicate_domain_and_scheduled_run_id_is_rejected():
    first = evidence()
    duplicate = evidence(gold_row_count=99)

    with pytest.raises(ValueError, match="duplicate delivery evidence grain"):
        ensure_unique_grain([first, duplicate])
