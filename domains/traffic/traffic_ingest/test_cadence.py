"""Traffic Gold test cadence ledger helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol
from zoneinfo import ZoneInfo


KST = ZoneInfo("Asia/Seoul")
TRAFFIC_GOLD_TEST_HOUR_KEY = "ask_seoul_traffic_gold_test_last_success_hour_kst"
TRAFFIC_GOLD_TEST_DAY_KEY = "ask_seoul_traffic_gold_test_last_success_day_kst"


class TrafficTestTier(str, Enum):
    GATE = "gate"
    HOURLY = "hourly"
    FULL = "full"


@dataclass(frozen=True)
class TrafficTestDecision:
    tier: TrafficTestTier
    hour_bucket: str
    day_bucket: str

    def as_dict(self) -> dict[str, str]:
        return {
            "tier": self.tier.value,
            "hour_bucket": self.hour_bucket,
            "day_bucket": self.day_bucket,
        }


class VariableLike(Protocol):
    @staticmethod
    def get(key: str, default=None): ...

    @staticmethod
    def set(key: str, value: str) -> None: ...


def _kst_buckets(now: datetime | None = None) -> tuple[str, str]:
    resolved = now or datetime.now(tz=KST)
    kst_now = resolved.astimezone(KST)
    return kst_now.strftime("%Y-%m-%dT%H"), kst_now.strftime("%Y-%m-%d")


def _is_valid_bucket_pair(hour_bucket: object, day_bucket: object) -> bool:
    if not isinstance(hour_bucket, str) or not isinstance(day_bucket, str):
        return False
    try:
        parsed_hour = datetime.strptime(hour_bucket, "%Y-%m-%dT%H")
        parsed_day = datetime.strptime(day_bucket, "%Y-%m-%d")
    except (TypeError, ValueError):
        return False
    return (
        parsed_hour.strftime("%Y-%m-%dT%H") == hour_bucket
        and parsed_day.strftime("%Y-%m-%d") == day_bucket
        and parsed_hour.date() == parsed_day.date()
    )


def choose_test_decision(
    *,
    variable: VariableLike,
    now: datetime | None = None,
) -> TrafficTestDecision:
    hour_bucket, day_bucket = _kst_buckets(now)
    try:
        last_day = variable.get(TRAFFIC_GOLD_TEST_DAY_KEY, default=None)
        last_hour = variable.get(TRAFFIC_GOLD_TEST_HOUR_KEY, default=None)
    except Exception:
        return TrafficTestDecision(TrafficTestTier.FULL, hour_bucket, day_bucket)
    if not _is_valid_bucket_pair(last_hour, last_day):
        return TrafficTestDecision(TrafficTestTier.FULL, hour_bucket, day_bucket)
    if last_day != day_bucket:
        return TrafficTestDecision(TrafficTestTier.FULL, hour_bucket, day_bucket)
    if last_hour != hour_bucket:
        return TrafficTestDecision(TrafficTestTier.HOURLY, hour_bucket, day_bucket)
    return TrafficTestDecision(TrafficTestTier.GATE, hour_bucket, day_bucket)


def mark_successful_decision(
    *,
    variable: VariableLike,
    decision: TrafficTestDecision,
) -> dict[str, str]:
    written: dict[str, str] = {}
    if decision.tier is TrafficTestTier.FULL:
        variable.set(TRAFFIC_GOLD_TEST_HOUR_KEY, decision.hour_bucket)
        written[TRAFFIC_GOLD_TEST_HOUR_KEY] = decision.hour_bucket
        variable.set(TRAFFIC_GOLD_TEST_DAY_KEY, decision.day_bucket)
        written[TRAFFIC_GOLD_TEST_DAY_KEY] = decision.day_bucket
    elif decision.tier is TrafficTestTier.HOURLY:
        variable.set(TRAFFIC_GOLD_TEST_HOUR_KEY, decision.hour_bucket)
        written[TRAFFIC_GOLD_TEST_HOUR_KEY] = decision.hour_bucket
    return written
