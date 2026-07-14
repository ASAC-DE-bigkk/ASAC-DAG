import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.w2_recovery import (  # noqa: E402
    checkpoint_payload,
    completed_window_labels,
    dbt_cli_options,
    split_repair_windows,
    window_dbt_vars,
)


def test_splits_inclusive_range_into_at_most_24_hour_windows():
    windows = split_repair_windows(
        "2026-07-02 00:00:00.000000",
        "2026-07-03 00:00:00.000000",
    )

    assert [window.label for window in windows] == [
        "2026-07-02 00:00:00.000000__2026-07-02 23:59:59.999999",
        "2026-07-03 00:00:00.000000__2026-07-03 00:00:00.000000",
    ]
    assert all(window.duration_microseconds <= 24 * 60 * 60 * 1_000_000 for window in windows)


def test_rejects_reversed_repair_range():
    with pytest.raises(ValueError, match="start_at must be before or equal to cutoff_at"):
        split_repair_windows(
            "2026-07-03 00:00:00.000000",
            "2026-07-02 23:59:59.999999",
        )


def test_builds_all_required_w2_vars_for_each_window():
    window = split_repair_windows(
        "2026-07-08 00:00:00.000000",
        "2026-07-08 23:59:59.999999",
    )[0]

    assert window_dbt_vars(window) == {
        "weather_w2_repair_mode": "bounded_reconcile",
        "weather_w2_repair_start_at": "2026-07-08 00:00:00.000000",
        "weather_w2_publishable_cutoff_at": "2026-07-08 23:59:59.999999",
        "weather_w2_bridge_version": "weather_admin_dong_grid_bridge_v1",
        "weather_w2_canonical_revision_date": "2025-04-01",
    }


def test_only_dbt_execution_commands_receive_single_thread_option():
    variables = {"weather_w2_canonical_revision_date": "2025-04-01"}

    deps_options = dbt_cli_options("deps", target="dev", variables=variables)
    run_options = dbt_cli_options("run", target="dev", variables=variables)

    assert "--threads" not in deps_options
    assert run_options[2:4] == ("--threads", "1")


def test_checkpoint_rejects_a_different_requested_range():
    windows = split_repair_windows(
        "2026-07-02 00:00:00.000000",
        "2026-07-02 23:59:59.999999",
    )

    payload = checkpoint_payload(windows, completed_labels=[windows[0].label])

    assert payload["range"] == {
        "start_at": "2026-07-02 00:00:00.000000",
        "cutoff_at": "2026-07-02 23:59:59.999999",
    }
    assert payload["completed_windows"] == [windows[0].label]

    other_windows = split_repair_windows(
        "2026-07-03 00:00:00.000000",
        "2026-07-03 23:59:59.999999",
    )
    with pytest.raises(ValueError, match="checkpoint range does not match requested repair range"):
        completed_window_labels(payload, other_windows)

    assert completed_window_labels(payload, windows) == {windows[0].label}


def test_manual_recovery_dag_serializes_w2_writers_and_runs_final_reconciliation():
    source = (
        Path(__file__).resolve().parents[1] / "weather_w2_observation_recovery.py"
    ).read_text(encoding="utf-8")

    assert 'DAG_ID = "weather_w2_observation_recovery"' in source
    assert "schedule=None" in source
    assert "max_active_runs=1" in source
    assert 'task_id="recover_observation_windows"' in source
    assert "pool=TRINO_HEAVY_POOL" in source
    assert "pool_slots=1" in source
    assert "dbt_cli_options(args[0]" in source
    assert "window_dbt_vars" in source
    assert "preparation_variables = window_dbt_vars(windows[0])" in source
    assert "assert_weather_observation_publishable_and_counts_reconcile" in source
