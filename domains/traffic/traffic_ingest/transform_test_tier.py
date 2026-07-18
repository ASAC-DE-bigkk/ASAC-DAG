"""Airflow adapters for the frozen Traffic Gold test-tier decision."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import Variable
from airflow.sdk.exceptions import AirflowFailException

from traffic_ingest.test_cadence import (
    TrafficTestDecision,
    TrafficTestTier,
    choose_test_decision,
    mark_successful_decision,
)


SELECT_TEST_TIER_TASK_ID = "select_traffic_test_tier"
MARK_TEST_TIER_TASK_ID = "mark_traffic_test_tier"


def select_traffic_test_tier(**context) -> dict[str, str]:
    decision = choose_test_decision(variable=Variable)
    return decision.as_dict()


def _parse_traffic_test_decision(raw_decision) -> TrafficTestDecision:
    if not isinstance(raw_decision, dict):
        raise AirflowFailException(f"invalid traffic test decision: {raw_decision}")
    try:
        tier_value = raw_decision["tier"]
        hour_bucket = raw_decision["hour_bucket"]
        day_bucket = raw_decision["day_bucket"]
        if not all(
            isinstance(value, str)
            for value in (tier_value, hour_bucket, day_bucket)
        ):
            raise TypeError("traffic test decision fields must be strings")
        tier = TrafficTestTier(tier_value)
        parsed_hour = datetime.strptime(hour_bucket, "%Y-%m-%dT%H")
        parsed_day = datetime.strptime(day_bucket, "%Y-%m-%d")
        if parsed_hour.strftime("%Y-%m-%dT%H") != hour_bucket:
            raise ValueError("non-canonical traffic test hour bucket")
        if parsed_day.strftime("%Y-%m-%d") != day_bucket:
            raise ValueError("non-canonical traffic test day bucket")
        if parsed_hour.date() != parsed_day.date():
            raise ValueError("inconsistent traffic test buckets")
    except (KeyError, TypeError, ValueError) as exc:
        raise AirflowFailException(
            f"invalid traffic test decision: {raw_decision}"
        ) from exc
    return TrafficTestDecision(
        tier=tier,
        hour_bucket=hour_bucket,
        day_bucket=day_bucket,
    )


def mark_traffic_test_tier(**context) -> dict[str, str]:
    ti = context["ti"]
    raw_decision = ti.xcom_pull(task_ids=SELECT_TEST_TIER_TASK_ID)
    decision = _parse_traffic_test_decision(raw_decision)
    return mark_successful_decision(variable=Variable, decision=decision)


def _selector_for_test_tier(
    *,
    selector: str | None,
    selector_by_test_tier: dict[TrafficTestTier, str | None] | None,
    ti,
) -> tuple[str | None, bool]:
    if selector_by_test_tier is None:
        return selector, False
    raw_decision = ti.xcom_pull(task_ids=SELECT_TEST_TIER_TASK_ID)
    tier = _parse_traffic_test_decision(raw_decision).tier
    if tier not in selector_by_test_tier:
        raise AirflowFailException(
            f"missing dbt selector for traffic test tier: {tier.value}"
        )
    selected = selector_by_test_tier[tier]
    if selected is None:
        return None, True
    if not selected.strip():
        raise AirflowFailException(
            f"empty dbt selector for traffic test tier: {tier.value}"
        )
    return selected, False


__all__ = [
    "MARK_TEST_TIER_TASK_ID",
    "SELECT_TEST_TIER_TASK_ID",
    "TrafficTestDecision",
    "TrafficTestTier",
    "_parse_traffic_test_decision",
    "_selector_for_test_tier",
    "choose_test_decision",
    "mark_successful_decision",
    "mark_traffic_test_tier",
    "select_traffic_test_tier",
]
