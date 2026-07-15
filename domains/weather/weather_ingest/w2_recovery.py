"""Pure contracts for serial Weather W2 historical recovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
MAX_WINDOW = timedelta(hours=6) - timedelta(microseconds=1)
LEGACY_MAX_WINDOW = timedelta(days=1) - timedelta(microseconds=1)
BRIDGE_VERSION = "weather_admin_dong_grid_bridge_v1"
CANONICAL_REVISION_DATE = "2025-04-01"
LINEAGE_RUN_BUCKET_COUNT = 4


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


@dataclass(frozen=True, slots=True)
class DbtPhase:
    command: str
    selector: str | None
    invocation_id: str


_PREPARATION_PHASES = (
    DbtPhase("deps", None, "prepare-dependencies"),
    DbtPhase("seed", "ask_seoul_weather_w1_inputs", "prepare-w1-inputs"),
    DbtPhase(
        "run",
        "ask_seoul_weather_transform_common_admin",
        "prepare-common-admin",
    ),
    DbtPhase("run", "ask_seoul_weather_w1_bridge", "prepare-w1-bridge"),
    DbtPhase("test", "ask_seoul_weather_w1_bridge", "prepare-w1-contract"),
)


def preparation_phase_plan() -> tuple[DbtPhase, ...]:
    return _PREPARATION_PHASES


def window_phase_plan(window_index: int) -> tuple[DbtPhase, ...]:
    invocation_prefix = f"window-{window_index:04d}"
    return (
        DbtPhase(
            "run",
            "ask_seoul_weather_w2_recovery_window_models",
            f"{invocation_prefix}-models",
        ),
        DbtPhase(
            "test",
            "ask_seoul_weather_w2_recovery_window_contracts",
            f"{invocation_prefix}-contracts",
        ),
    )


def lineage_phase(window_index: int, bucket_index: int) -> DbtPhase:
    return DbtPhase(
        "test",
        "ask_seoul_weather_w2_recovery_lineage_contract",
        f"window-{window_index:04d}-lineage-{bucket_index}",
    )


def final_phase() -> DbtPhase:
    return DbtPhase(
        "test",
        "ask_seoul_weather_w2_recovery_final_contract",
        "final-contract",
    )


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


def select_windows_with_publishable_anchors(
    windows: list[RepairWindow], anchor_window_indexes: set[int]
) -> list[RepairWindow]:
    invalid_indexes = sorted(
        index for index in anchor_window_indexes if index < 0 or index >= len(windows)
    )
    if invalid_indexes:
        raise ValueError(
            f"anchor query returned unknown repair window indexes: {invalid_indexes}"
        )
    return [
        window for index, window in enumerate(windows) if index in anchor_window_indexes
    ]


def window_dbt_vars(window: RepairWindow) -> dict[str, str]:
    return {
        "weather_w2_repair_mode": "bounded_reconcile",
        "weather_w2_repair_start_at": format_timestamp(window.start_at),
        "weather_w2_publishable_cutoff_at": format_timestamp(window.cutoff_at),
        "weather_w2_bridge_version": BRIDGE_VERSION,
        "weather_w2_canonical_revision_date": CANONICAL_REVISION_DATE,
    }


def preparation_dbt_vars(
    first_window: RepairWindow, final_window: RepairWindow
) -> dict[str, str]:
    """Build the bounded evidence scope required by immutable bridge preparation."""
    if final_window.cutoff_at < first_window.start_at:
        raise ValueError("final repair window must not end before the first window")
    preparation_window = RepairWindow(
        start_at=first_window.start_at,
        cutoff_at=min(
            first_window.start_at + LEGACY_MAX_WINDOW,
            final_window.cutoff_at,
        ),
    )
    return window_dbt_vars(preparation_window)


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


def _parse_window_label(label: str) -> RepairWindow:
    start_at, separator, cutoff_at = label.partition("__")
    if not separator:
        raise ValueError(f"invalid checkpoint window label: {label!r}")
    return RepairWindow(
        start_at=parse_timestamp(start_at),
        cutoff_at=parse_timestamp(cutoff_at),
    )


def completed_window_labels(
    payload: dict[str, Any] | None, windows: list[RepairWindow]
) -> set[str]:
    if not payload:
        return set()
    expected_range = _range_payload(windows)
    if payload.get("range") != expected_range:
        raise ValueError("checkpoint range does not match requested repair range")
    completed = payload.get("completed_windows") or []
    if not isinstance(completed, list) or not all(
        isinstance(label, str) for label in completed
    ):
        raise ValueError("checkpoint completed_windows must be a list of labels")
    range_start = windows[0].start_at
    range_cutoff = windows[-1].cutoff_at
    completed_ranges: list[RepairWindow] = []
    for label in dict.fromkeys(completed):
        completed_window = _parse_window_label(label)
        if (
            completed_window.start_at < range_start
            or completed_window.cutoff_at > range_cutoff
            or completed_window.start_at > completed_window.cutoff_at
            or completed_window.duration_microseconds
            > int(LEGACY_MAX_WINDOW.total_seconds() * 1_000_000) + 1
        ):
            raise ValueError(f"checkpoint contains unknown repair window: {label}")
        completed_ranges.append(completed_window)

    return {
        window.label
        for window in windows
        if any(
            completed_window.start_at <= window.start_at
            and window.cutoff_at <= completed_window.cutoff_at
            for completed_window in completed_ranges
        )
    }
