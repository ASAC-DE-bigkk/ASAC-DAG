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
WINNER_RUN_BUCKET_COUNT = 8
LINEAGE_RUN_BUCKET_COUNT = 4
CHECKPOINT_CONTRACT_VERSION = 2
STAGED_CHECKPOINT_CONTRACT_VERSION = 3
STAGED_RECOVERY_MODE = "staged_gold_only"
STAGED_RECOVERY_STATE = "staging"
STAGED_RECOVERY_TRANSITIONS = {
    "staging": "prepublish_validated",
    "prepublish_validated": "published",
    "published": "verified",
}
STAGED_RECOVERY_STATES = frozenset(
    {STAGED_RECOVERY_STATE, *STAGED_RECOVERY_TRANSITIONS.values()}
)
YONGSIN_ADMIN_DONG_CODE = "1123053600"
YONGSIN_NX = 61
YONGSIN_NY = 127
WEATHER_BRIDGE_TABLE = "bridge_weather_admin_dong_grid"
WEATHER_SILVER_GRID_TABLE = "silver_kma_vilage_fcst_grid"
WEATHER_CANONICAL_GOLD_TABLE = "gold_weather_forecast_by_admin_dong"
WEATHER_SERVING_CURRENT_TABLE = "gold_weather_current_wide_by_admin_dong"


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


@dataclass(frozen=True, slots=True)
class RecoveryPins:
    admin_dong_crosswalk_snapshot_id: int
    weather_bridge_snapshot_id: int
    silver_grid_snapshot_id: int
    gold_baseline_snapshot_id: int

    def __post_init__(self) -> None:
        snapshot_ids = (
            self.admin_dong_crosswalk_snapshot_id,
            self.weather_bridge_snapshot_id,
            self.silver_grid_snapshot_id,
            self.gold_baseline_snapshot_id,
        )
        if any(
            isinstance(snapshot_id, bool)
            or not isinstance(snapshot_id, int)
            or snapshot_id <= 0
            for snapshot_id in snapshot_ids
        ):
            raise ValueError("recovery snapshot IDs must be positive integers")


@dataclass(frozen=True, slots=True)
class RecoveryBaseline:
    non_target_row_count: int
    non_target_checksum: str
    source_watermark: str


@dataclass(frozen=True, slots=True)
class StagedCheckpoint:
    checkpoint_id: str
    pins: RecoveryPins
    baseline: RecoveryBaseline
    selected_labels: tuple[str, ...]
    completed_labels: frozenset[str]
    known_gap_labels: frozenset[str]
    state: str


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


def winner_phase(window_index: int, bucket_index: int) -> DbtPhase:
    return DbtPhase(
        "test",
        "ask_seoul_weather_w2_recovery_winner_contract",
        f"window-{window_index:04d}-winner-{bucket_index}",
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


def staged_preparation_phase() -> DbtPhase:
    return DbtPhase("deps", None, "prepare-dependencies")


def staged_window_phase_plan(window_index: int) -> tuple[DbtPhase, ...]:
    return (
        DbtPhase(
            "run",
            "ask_seoul_weather_w2_recovery_stage_model",
            f"stage-window-{window_index:04d}",
        ),
        DbtPhase(
            "test",
            "ask_seoul_weather_w2_recovery_stage_window_contract",
            f"test-window-{window_index:04d}",
        ),
    )


def staged_final_phase() -> DbtPhase:
    return DbtPhase(
        "test",
        "ask_seoul_weather_w2_recovery_stage_final",
        "stage-final",
    )


def staged_winner_phase(bucket_index: int) -> DbtPhase:
    return DbtPhase(
        "test",
        "ask_seoul_weather_w2_recovery_stage_winner",
        f"stage-winner-{bucket_index}",
    )


def staged_lineage_phase(bucket_index: int) -> DbtPhase:
    return DbtPhase(
        "test",
        "ask_seoul_weather_w2_recovery_stage_lineage",
        f"stage-lineage-{bucket_index}",
    )


def staged_publish_phase() -> DbtPhase:
    return DbtPhase(
        "run-operation weather_w2_publish_recovery_stage",
        None,
        "publish-gold",
    )


def staged_post_publish_bridge_phase() -> DbtPhase:
    return DbtPhase(
        "test",
        "ask_seoul_weather_w2_recovery_post_publish_bridge_contract",
        "canonical-bridge-reconcile",
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
        "contract_version": CHECKPOINT_CONTRACT_VERSION,
        "range": _range_payload(windows),
        "completed_windows": list(dict.fromkeys(completed_labels)),
    }


def staged_checkpoint_payload(
    windows: list[RepairWindow],
    *,
    selected_labels: list[str],
    completed_labels: list[str],
    checkpoint_id: str,
    pins: RecoveryPins,
    baseline: RecoveryBaseline,
) -> dict[str, Any]:
    selected = list(dict.fromkeys(selected_labels))
    completed = list(dict.fromkeys(completed_labels))
    allowed = {window.label for window in windows}
    if not set(selected) <= allowed:
        raise ValueError("selected staged windows must be requested repair windows")
    if not set(completed) <= set(selected):
        raise ValueError("completed staged windows must be selected repair windows")
    return {
        "contract_version": STAGED_CHECKPOINT_CONTRACT_VERSION,
        "mode": STAGED_RECOVERY_MODE,
        "checkpoint_id": checkpoint_id,
        "range": _range_payload(windows),
        "target": {
            "admin_dong_code": YONGSIN_ADMIN_DONG_CODE,
            "nx": YONGSIN_NX,
            "ny": YONGSIN_NY,
        },
        "pins": {
            "admin_dong_crosswalk_snapshot_id": (
                pins.admin_dong_crosswalk_snapshot_id
            ),
            "weather_bridge_snapshot_id": pins.weather_bridge_snapshot_id,
            "silver_grid_snapshot_id": pins.silver_grid_snapshot_id,
            "gold_baseline_snapshot_id": pins.gold_baseline_snapshot_id,
        },
        "baseline": {
            "non_target_row_count": baseline.non_target_row_count,
            "non_target_checksum": baseline.non_target_checksum,
            "source_watermark": baseline.source_watermark,
        },
        "selected_windows": selected,
        "completed_windows": completed,
        "known_gaps": [],
        "state": STAGED_RECOVERY_STATE,
    }


def load_staged_checkpoint(
    payload: dict[str, Any],
    windows: list[RepairWindow],
    *,
    checkpoint_id: str,
) -> StagedCheckpoint:
    if payload.get("contract_version") != STAGED_CHECKPOINT_CONTRACT_VERSION:
        raise ValueError("staged checkpoint contract version must be 3")
    if payload.get("mode") != STAGED_RECOVERY_MODE:
        raise ValueError("staged checkpoint mode must be staged_gold_only")
    if payload.get("checkpoint_id") != checkpoint_id:
        raise ValueError("staged checkpoint identity does not match requested checkpoint")
    if payload.get("range") != _range_payload(windows):
        raise ValueError("staged checkpoint range does not match requested repair range")
    if payload.get("target") != {
        "admin_dong_code": YONGSIN_ADMIN_DONG_CODE,
        "nx": YONGSIN_NX,
        "ny": YONGSIN_NY,
    }:
        raise ValueError("staged checkpoint target must be Yongsin-dong")

    state = payload.get("state")
    if state not in STAGED_RECOVERY_STATES:
        raise ValueError("staged checkpoint state is invalid")
    selected = payload.get("selected_windows")
    completed = payload.get("completed_windows")
    known_gaps = payload.get("known_gaps", [])
    if not isinstance(selected, list) or not all(
        isinstance(label, str) for label in selected
    ):
        raise ValueError("selected staged windows must be a list of labels")
    if not isinstance(completed, list) or not all(
        isinstance(label, str) for label in completed
    ):
        raise ValueError("completed staged windows must be a list of labels")
    if not isinstance(known_gaps, list) or not all(
        isinstance(gap, dict)
        and isinstance(gap.get("window"), str)
        and isinstance(gap.get("reason"), str)
        and bool(gap["reason"])
        for gap in known_gaps
    ):
        raise ValueError("known staged gaps must be window/reason objects")
    allowed = {window.label for window in windows}
    known_gap_labels = [gap["window"] for gap in known_gaps]
    if not set(selected) <= allowed:
        raise ValueError("selected staged windows must be requested repair windows")
    if not set(completed) <= set(selected):
        raise ValueError("completed staged windows must be selected repair windows")
    if not set(known_gap_labels) <= set(selected):
        raise ValueError("known staged gaps must be selected repair windows")
    if len(known_gap_labels) != len(set(known_gap_labels)):
        raise ValueError("known staged gaps must not contain duplicate windows")
    if set(completed).intersection(known_gap_labels):
        raise ValueError("a staged window cannot be both completed and a known gap")

    return StagedCheckpoint(
        checkpoint_id=checkpoint_id,
        pins=RecoveryPins(**payload["pins"]),
        baseline=RecoveryBaseline(**payload["baseline"]),
        selected_labels=tuple(selected),
        completed_labels=frozenset(completed),
        known_gap_labels=frozenset(known_gap_labels),
        state=str(state),
    )


def complete_staged_window(
    payload: dict[str, Any],
    window_label: str,
) -> dict[str, Any]:
    if payload.get("state") != STAGED_RECOVERY_STATE:
        raise ValueError("staged windows can complete only while checkpoint is staging")
    selected = list(payload.get("selected_windows") or [])
    if window_label not in selected:
        raise ValueError("completed staged windows must be selected repair windows")
    completed = set(payload.get("completed_windows") or [])
    known_gap_labels = {
        gap["window"] for gap in payload.get("known_gaps", []) if isinstance(gap, dict)
    }
    if window_label in known_gap_labels:
        raise ValueError("a known staged gap cannot be marked complete")
    completed.add(window_label)
    return {
        **payload,
        "completed_windows": [label for label in selected if label in completed],
    }


def record_staged_known_gap(
    payload: dict[str, Any],
    window_label: str,
    *,
    reason: str,
) -> dict[str, Any]:
    if payload.get("state") != STAGED_RECOVERY_STATE:
        raise ValueError("staged gaps can be recorded only while checkpoint is staging")
    selected = list(payload.get("selected_windows") or [])
    if window_label not in selected:
        raise ValueError("known staged gaps must be selected repair windows")
    if not isinstance(reason, str) or not reason:
        raise ValueError("known staged gap reason must be non-empty")
    if window_label in set(payload.get("completed_windows") or []):
        raise ValueError("a completed staged window cannot become a known gap")
    gaps_by_window = {
        gap["window"]: gap
        for gap in payload.get("known_gaps", [])
        if isinstance(gap, dict)
    }
    gaps_by_window[window_label] = {
        "window": window_label,
        "reason": reason,
    }
    return {
        **payload,
        "known_gaps": [
            gaps_by_window[label] for label in selected if label in gaps_by_window
        ],
    }


def advance_staged_checkpoint(
    payload: dict[str, Any],
    *,
    next_state: str,
) -> dict[str, Any]:
    current_state = str(payload.get("state"))
    expected_next_state = STAGED_RECOVERY_TRANSITIONS.get(current_state)
    if expected_next_state != next_state:
        raise ValueError(
            f"invalid staged checkpoint transition: {current_state} -> {next_state}"
        )
    if next_state == "prepublish_validated":
        completed = set(payload.get("completed_windows") or [])
        known_gaps = {
            gap["window"]
            for gap in payload.get("known_gaps", [])
            if isinstance(gap, dict)
        }
        if completed.intersection(known_gaps):
            raise ValueError(
                "a staged window cannot be both completed and a known gap"
            )
        if completed.union(known_gaps) != set(
            payload.get("selected_windows") or []
        ):
            raise ValueError(
                "all selected staged windows must resolve before prepublish validation"
            )
    return {**payload, "state": next_state}


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
    if payload.get("contract_version") != CHECKPOINT_CONTRACT_VERSION:
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
