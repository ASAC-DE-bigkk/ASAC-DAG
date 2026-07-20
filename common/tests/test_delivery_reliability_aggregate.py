from datetime import datetime, timezone

import pytest

from common.delivery_reliability.aggregate import build_pilot_report
from common.delivery_reliability.contract import DeliveryEvidence


OBSERVED_AT = datetime(2026, 7, 19, 12, tzinfo=timezone.utc)


def row(run_hour, **overrides):
    scheduled = f"2026-07-19T{run_hour:02d}:00:00Z"
    value = {
        "domain": "weather",
        "scheduled_run_id": f"scheduled__{scheduled}",
        "scheduled_at": scheduled,
        "bronze_status": "SUCCESS",
        "is_publishable": True,
        "completeness_status": "COMPLETE",
        "source_status": "SUCCESS",
        "transform_status": "SUCCESS",
        "gold_query_status": "SUCCESS",
        "gold_available_at": f"2026-07-19T{run_hour:02d}:10:00Z",
        "gold_row_count": 10,
        "sla_minutes": 30,
    }
    value.update(overrides)
    return DeliveryEvidence.from_mapping(value)


def test_report_exposes_rate_numerators_denominators_and_unavailable_counts():
    rows = [
        row(
            0,
            bronze_status="FAILED",
            is_publishable=False,
            completeness_status="PARTIAL",
            source_status="API_FAILURE",
            gold_query_status="FAILED",
            gold_available_at=None,
            gold_row_count=None,
            detected_at="2026-07-19T00:05:00Z",
            api_failure_type="429",
            retry_count=3,
        ),
        row(3),
        row(6, gold_available_at="2026-07-19T06:45:00Z"),
        row(
            9,
            transform_status=None,
            gold_query_status=None,
            gold_available_at=None,
            gold_row_count=None,
        ),
    ]

    report = build_pilot_report(rows, observed_at=OBSERVED_AT)
    summary = report["domains"]["weather"]

    assert summary["scheduled_runs"] == 4
    assert summary["supply"] == {
        "successful_runs": 3,
        "evaluable_runs": 4,
        "rate": 0.75,
    }
    assert summary["gold_delivery"] == {
        "delivered_runs": 2,
        "evaluable_runs": 3,
        "rate": pytest.approx(2 / 3),
    }
    assert summary["sla_delivery"] == {
        "within_sla_runs": 1,
        "evaluable_runs": 2,
        "rate": 0.5,
    }
    assert summary["completeness"] == {
        "complete_runs": 3,
        "evaluable_runs": 4,
        "rate": 0.75,
    }
    assert summary["not_available_runs"] == 1
    assert summary["state_counts"] == {
        "API_FAILURE": 1,
        "DELIVERED": 1,
        "NOT_AVAILABLE": 1,
        "SLA_MISSED": 1,
    }
    assert summary["api_failure_profile"] == {"429": 1}
    assert summary["retry_count"] == 3


def test_freshness_distribution_and_mttr_use_next_successful_gold_delivery():
    rows = [
        row(
            0,
            bronze_status="FAILED",
            is_publishable=False,
            source_status="SOURCE_FAILURE",
            gold_query_status="FAILED",
            gold_available_at=None,
            gold_row_count=None,
            detected_at="2026-07-19T00:05:00Z",
        ),
        row(3),
        row(6, gold_available_at="2026-07-19T06:20:00Z"),
    ]

    summary = build_pilot_report(rows, observed_at=OBSERVED_AT)["domains"]["weather"]

    assert summary["freshness_minutes"] == {
        "sample_count": 2,
        "p50": 15.0,
        "max": 20.0,
    }
    assert summary["recovery"] == {
        "recovered_failures": 1,
        "unrecovered_failures": 0,
        "mttr_minutes": 185.0,
        "events": [
            {
                "domain": "weather",
                "failed_run_id": "scheduled__2026-07-19T00:00:00Z",
                "recovered_by_run_id": "scheduled__2026-07-19T03:00:00Z",
                "minutes": 185.0,
            }
        ],
    }


def test_report_filters_to_seven_day_window_and_sorts_runs_deterministically():
    current = row(3)
    old = DeliveryEvidence.from_mapping(
        {
            "domain": "traffic",
            "scheduled_run_id": "scheduled__2026-07-01T00:00:00Z",
            "scheduled_at": "2026-07-01T00:00:00Z",
            "bronze_status": "SUCCESS",
            "is_publishable": True,
            "completeness_status": "COMPLETE",
            "source_status": "ZERO_ROW",
            "transform_status": "SUCCESS",
            "gold_query_status": "SUCCESS",
            "gold_available_at": "2026-07-01T00:05:00Z",
            "gold_row_count": 0,
            "sla_minutes": 30,
        }
    )

    report = build_pilot_report([current, old], observed_at=OBSERVED_AT)

    assert report["lookback_days"] == 7
    assert report["excluded_outside_window"] == 1
    assert list(report["domains"]) == ["weather"]
    assert report["runs"][0]["scheduled_run_id"] == current.scheduled_run_id


def test_unavailable_metric_is_none_instead_of_zero():
    unavailable = row(
        9,
        transform_status=None,
        gold_query_status=None,
        gold_available_at=None,
        gold_row_count=None,
    )

    summary = build_pilot_report([unavailable], observed_at=OBSERVED_AT)["domains"][
        "weather"
    ]

    assert summary["gold_delivery"]["rate"] is None
    assert summary["sla_delivery"]["rate"] is None
    assert summary["freshness_minutes"]["p50"] is None
    assert summary["recovery"]["mttr_minutes"] is None


def test_overall_mttr_never_recovers_a_failure_with_another_domain():
    traffic_failure = row(
        0,
        domain="traffic",
        bronze_status="FAILED",
        is_publishable=False,
        source_status="SOURCE_FAILURE",
        gold_query_status="FAILED",
        gold_available_at=None,
        gold_row_count=None,
        detected_at="2026-07-19T00:05:00Z",
    )
    weather_success = DeliveryEvidence.from_mapping(
        {
            "domain": "weather",
            "scheduled_run_id": "scheduled__2026-07-19T00:10:00Z",
            "scheduled_at": "2026-07-19T00:10:00Z",
            "bronze_status": "SUCCESS",
            "is_publishable": True,
            "completeness_status": "COMPLETE",
            "source_status": "SUCCESS",
            "transform_status": "SUCCESS",
            "gold_query_status": "SUCCESS",
            "gold_available_at": "2026-07-19T00:15:00Z",
            "gold_row_count": 10,
            "sla_minutes": 30,
        }
    )

    recovery = build_pilot_report(
        [traffic_failure, weather_success], observed_at=OBSERVED_AT
    )["overall"]["recovery"]

    assert recovery["recovered_failures"] == 0
    assert recovery["unrecovered_failures"] == 1
    assert recovery["mttr_minutes"] is None


def test_mttr_uses_first_success_after_failure_detection():
    failure = row(
        0,
        bronze_status="FAILED",
        is_publishable=False,
        source_status="SOURCE_FAILURE",
        gold_query_status="FAILED",
        gold_available_at=None,
        gold_row_count=None,
        detected_at="2026-07-19T03:30:00Z",
    )

    recovery = build_pilot_report(
        [failure, row(3), row(6)], observed_at=OBSERVED_AT
    )["domains"]["weather"]["recovery"]

    assert recovery["recovered_failures"] == 1
    assert recovery["unrecovered_failures"] == 0
    assert recovery["mttr_minutes"] == 160.0
    assert recovery["events"][0]["recovered_by_run_id"] == (
        "scheduled__2026-07-19T06:00:00Z"
    )
