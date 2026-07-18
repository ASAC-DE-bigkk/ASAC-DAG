from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_transform_test_support import FakeVariable  # noqa: E402
from traffic_ingest.test_cadence import (  # noqa: E402
    TRAFFIC_GOLD_TEST_DAY_KEY,
    TRAFFIC_GOLD_TEST_HOUR_KEY,
    TrafficTestDecision,
    TrafficTestTier,
    choose_test_decision,
    mark_successful_decision,
)


KST = ZoneInfo("Asia/Seoul")


@pytest.fixture(autouse=True)
def reset_variable():
    FakeVariable.reset()


def test_first_run_fails_closed_to_full():
    decision = choose_test_decision(
        variable=FakeVariable,
        now=datetime(2026, 7, 18, 10, 12, tzinfo=KST),
    )
    assert decision == TrafficTestDecision(
        tier=TrafficTestTier.FULL,
        hour_bucket="2026-07-18T10",
        day_bucket="2026-07-18",
    )
    assert decision.as_dict() == {
        "tier": "full",
        "hour_bucket": "2026-07-18T10",
        "day_bucket": "2026-07-18",
    }


def test_same_hour_after_full_success_uses_gate():
    FakeVariable.values = {
        TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
        TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T10",
    }

    assert (
        choose_test_decision(
            variable=FakeVariable,
            now=datetime(2026, 7, 18, 10, 55, tzinfo=KST),
        ).tier
        is TrafficTestTier.GATE
    )


def test_new_hour_after_daily_success_uses_hourly():
    FakeVariable.values = {
        TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
        TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T09",
    }

    assert (
        choose_test_decision(
            variable=FakeVariable,
            now=datetime(2026, 7, 18, 10, 12, tzinfo=KST),
        ).tier
        is TrafficTestTier.HOURLY
    )


def test_new_day_uses_full():
    FakeVariable.values = {
        TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-17",
        TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-17T23",
    }

    assert (
        choose_test_decision(
            variable=FakeVariable,
            now=datetime(2026, 7, 18, 0, 12, tzinfo=KST),
        ).tier
        is TrafficTestTier.FULL
    )


@pytest.mark.parametrize(
    "values",
    [
        {TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18"},
        {TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T10"},
        {
            TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
            TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-17T23",
        },
    ],
)
def test_missing_or_inconsistent_ledger_fails_closed_to_full(values):
    FakeVariable.values = values

    assert (
        choose_test_decision(
            variable=FakeVariable,
            now=datetime(2026, 7, 18, 10, 12, tzinfo=KST),
        ).tier
        is TrafficTestTier.FULL
    )


@pytest.mark.parametrize(
    "values",
    [
        pytest.param(
            {
                TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
                TRAFFIC_GOLD_TEST_HOUR_KEY: 2026071810,
            },
            id="non-string-hour",
        ),
        pytest.param(
            {
                TRAFFIC_GOLD_TEST_DAY_KEY: 20260718,
                TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T10",
            },
            id="non-string-day",
        ),
        pytest.param(
            {
                TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
                TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18Tbad",
            },
            id="malformed-hour",
        ),
        pytest.param(
            {
                TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
                TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T1",
            },
            id="noncanonical-hour",
        ),
        pytest.param(
            {
                TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
                TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T10:00",
            },
            id="overlong-hour",
        ),
        pytest.param(
            {
                TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
                TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T24",
            },
            id="impossible-hour",
        ),
        pytest.param(
            {
                TRAFFIC_GOLD_TEST_DAY_KEY: "2026-7-18",
                TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-7-18T10",
            },
            id="noncanonical-day",
        ),
        pytest.param(
            {
                TRAFFIC_GOLD_TEST_DAY_KEY: "2026-02-29",
                TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-02-29T10",
            },
            id="impossible-date",
        ),
        pytest.param(
            {
                TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
                TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-17T23",
            },
            id="mismatched-real-buckets",
        ),
    ],
)
def test_invalid_persisted_ledger_buckets_fail_closed_to_full(values):
    FakeVariable.values = values

    assert (
        choose_test_decision(
            variable=FakeVariable,
            now=datetime(2026, 7, 18, 10, 12, tzinfo=KST),
        ).tier
        is TrafficTestTier.FULL
    )


def test_variable_read_failure_fails_closed_to_full():
    class BrokenVariable:
        @staticmethod
        def get(*_args, **_kwargs):
            raise RuntimeError("metadata unavailable")

    assert (
        choose_test_decision(
            variable=BrokenVariable,
            now=datetime(2026, 7, 18, 10, 12, tzinfo=KST),
        ).tier
        is TrafficTestTier.FULL
    )


def test_full_success_marks_day_and_hour():
    decision = TrafficTestDecision(
        tier=TrafficTestTier.FULL,
        hour_bucket="2026-07-18T10",
        day_bucket="2026-07-18",
    )

    mark_successful_decision(variable=FakeVariable, decision=decision)

    assert FakeVariable.values == {
        TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
        TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T10",
    }


def test_full_success_writes_hour_before_day():
    writes = []

    class OrderedVariable(FakeVariable):
        @classmethod
        def set(cls, key, value):
            writes.append((key, value))
            super().set(key, value)

    decision = TrafficTestDecision(
        tier=TrafficTestTier.FULL,
        hour_bucket="2026-07-18T10",
        day_bucket="2026-07-18",
    )

    mark_successful_decision(variable=OrderedVariable, decision=decision)

    assert writes == [
        (TRAFFIC_GOLD_TEST_HOUR_KEY, "2026-07-18T10"),
        (TRAFFIC_GOLD_TEST_DAY_KEY, "2026-07-18"),
    ]


def test_full_success_day_write_failure_leaves_conservative_hour_only_marker():
    class PartiallyBrokenVariable(FakeVariable):
        @classmethod
        def set(cls, key, value):
            if key == TRAFFIC_GOLD_TEST_DAY_KEY:
                raise RuntimeError("day write failed")
            super().set(key, value)

    decision = TrafficTestDecision(
        tier=TrafficTestTier.FULL,
        hour_bucket="2026-07-18T10",
        day_bucket="2026-07-18",
    )

    with pytest.raises(RuntimeError, match="day write failed"):
        mark_successful_decision(variable=PartiallyBrokenVariable, decision=decision)

    assert FakeVariable.values == {TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T10"}
    assert (
        choose_test_decision(
            variable=FakeVariable,
            now=datetime(2026, 7, 18, 10, 55, tzinfo=KST),
        ).tier
        is TrafficTestTier.FULL
    )


def test_hourly_success_marks_only_frozen_hour():
    decision = TrafficTestDecision(
        tier=TrafficTestTier.HOURLY,
        hour_bucket="2026-07-18T10",
        day_bucket="2026-07-18",
    )

    mark_successful_decision(variable=FakeVariable, decision=decision)

    assert FakeVariable.values == {TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T10"}


def test_gate_success_does_not_write_ledger():
    decision = TrafficTestDecision(
        tier=TrafficTestTier.GATE,
        hour_bucket="2026-07-18T10",
        day_bucket="2026-07-18",
    )

    assert mark_successful_decision(variable=FakeVariable, decision=decision) == {}
    assert FakeVariable.values == {}
