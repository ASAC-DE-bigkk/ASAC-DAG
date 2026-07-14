"""Pure contracts for serial Weather W2 historical recovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
MAX_WINDOW = timedelta(days=1) - timedelta(microseconds=1)
BRIDGE_VERSION = "weather_admin_dong_grid_bridge_v1"
CANONICAL_REVISION_DATE = "2025-04-01"


@dataclass(frozen=True)
class RepairWindow:
    start_at: datetime
    cutoff_at: datetime

    @property
    def label(self) -> str:
        return f"{format_timestamp(self.start_at)}__{format_timestamp(self.cutoff_at)}"

    @property
    def duration_microseconds(self) -> int:
        return int((self.cutoff_at - self.start_at).total_seconds() * 1_000_000) + 1


def parse_timestamp(value: str) -> datetime:
    try:
        return datetime.strptime(value, TIMESTAMP_FORMAT)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"timestamp must match {TIMESTAMP_FORMAT}: {value!r}") from exc


def format_timestamp(value: datetime) -> str:
    return value.strftime(TIMESTAMP_FORMAT)


def split_repair_windows(start_at: str, cutoff_at: str) -> list[RepairWindow]:
    start = parse_timestamp(start_at)
    cutoff = parse_timestamp(cutoff_at)
    if start > cutoff:
        raise ValueError("start_at must be before or equal to cutoff_at")

    windows: list[RepairWindow] = []
    current_start = start
    while current_start <= cutoff:
        current_cutoff = min(current_start + MAX_WINDOW, cutoff)
        windows.append(RepairWindow(start_at=current_start, cutoff_at=current_cutoff))
        current_start = current_cutoff + timedelta(microseconds=1)
    return windows


def window_dbt_vars(window: RepairWindow) -> dict[str, str]:
    return {
        "weather_w2_repair_mode": "bounded_reconcile",
        "weather_w2_repair_start_at": format_timestamp(window.start_at),
        "weather_w2_publishable_cutoff_at": format_timestamp(window.cutoff_at),
        "weather_w2_bridge_version": BRIDGE_VERSION,
        "weather_w2_canonical_revision_date": CANONICAL_REVISION_DATE,
    }


def _range_payload(windows: list[RepairWindow]) -> dict[str, str]:
    if not windows:
        raise ValueError("at least one repair window is required")
    return {
        "start_at": format_timestamp(windows[0].start_at),
        "cutoff_at": format_timestamp(windows[-1].cutoff_at),
    }


def checkpoint_payload(
    windows: list[RepairWindow], *, completed_labels: list[str]
) -> dict[str, Any]:
    allowed = {window.label for window in windows}
    unknown = sorted(set(completed_labels) - allowed)
    if unknown:
        raise ValueError(f"checkpoint contains unknown repair windows: {unknown}")
    return {
        "range": _range_payload(windows),
        "completed_windows": list(dict.fromkeys(completed_labels)),
    }


def completed_window_labels(payload: dict[str, Any] | None, windows: list[RepairWindow]) -> set[str]:
    if not payload:
        return set()
    expected_range = _range_payload(windows)
    if payload.get("range") != expected_range:
        raise ValueError("checkpoint range does not match requested repair range")
    completed = payload.get("completed_windows") or []
    if not isinstance(completed, list) or not all(isinstance(label, str) for label in completed):
        raise ValueError("checkpoint completed_windows must be a list of labels")
    allowed = {window.label for window in windows}
    unknown = sorted(set(completed) - allowed)
    if unknown:
        raise ValueError(f"checkpoint contains unknown repair windows: {unknown}")
    return set(completed)
