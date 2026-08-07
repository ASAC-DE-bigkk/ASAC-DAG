"""loader 장기 실행 감시 순수 판정 테스트 (#719)."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

_TRANSIT = Path(__file__).resolve().parents[1]
_DAGS = Path(__file__).resolve().parents[3]
for path in (str(_DAGS), str(_TRANSIT)):
    if path not in sys.path:
        sys.path.insert(0, path)

from seoul_transit.loader_watchdog import (  # noqa: E402
    claim_first_unalerted_run,
    overdue_running_runs,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 6, 14, 20, tzinfo=UTC)


def _run(*, run_id="scheduled__1", started_at=None, state="running"):
    return SimpleNamespace(
        run_id=run_id,
        start_date=started_at or NOW - timedelta(minutes=16),
        state=state,
    )


def test_overdue_running_run_is_loader_delay_candidate():
    overdue = overdue_running_runs(
        [_run()], now=NOW, max_runtime=timedelta(minutes=15)
    )

    assert len(overdue) == 1
    assert overdue[0].run_id == "scheduled__1"
    assert overdue[0].elapsed == timedelta(minutes=16)


def test_exact_threshold_and_terminal_runs_do_not_alert():
    overdue = overdue_running_runs(
        [
            _run(run_id="exact", started_at=NOW - timedelta(minutes=15)),
            _run(run_id="success", state="success"),
            _run(run_id="failed", state="failed"),
        ],
        now=NOW,
        max_runtime=timedelta(minutes=15),
    )

    assert overdue == ()


def test_airflow_state_enum_value_is_supported():
    class State:
        value = "running"

    overdue = overdue_running_runs(
        [_run(state=State())], now=NOW, max_runtime=timedelta(minutes=15)
    )

    assert len(overdue) == 1


def test_naive_metadata_timestamp_fails_closed_instead_of_guessing_timezone():
    with pytest.raises(ValueError, match="timezone-aware"):
        overdue_running_runs(
            [_run(started_at=datetime(2026, 8, 6, 14, 0))],
            now=NOW,
            max_runtime=timedelta(minutes=15),
        )


def test_missing_metadata_start_time_fails_closed():
    with pytest.raises(ValueError, match="must be a datetime"):
        overdue_running_runs(
            [SimpleNamespace(run_id="missing-start", start_date=None, state="running")],
            now=NOW,
            max_runtime=timedelta(minutes=15),
        )


def test_non_positive_runtime_threshold_is_rejected():
    with pytest.raises(ValueError, match="positive"):
        overdue_running_runs([_run()], now=NOW, max_runtime=timedelta(0))


def test_claim_first_unalerted_run_does_not_starve_later_overdue_run():
    overdue = overdue_running_runs(
        [
            _run(run_id="oldest", started_at=NOW - timedelta(minutes=20)),
            _run(run_id="next", started_at=NOW - timedelta(minutes=16)),
        ],
        now=NOW,
        max_runtime=timedelta(minutes=15),
    )
    claims: list[str] = []

    target = claim_first_unalerted_run(
        overdue,
        claim=lambda run_id: claims.append(run_id) or run_id == "next",
    )

    assert target is not None and target.run_id == "next"
    assert claims == ["oldest", "next"]
