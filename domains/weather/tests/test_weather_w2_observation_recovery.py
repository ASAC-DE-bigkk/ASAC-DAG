import json
import subprocess
import sys
import types
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest import w2_recovery as recovery_contracts  # noqa: E402
from weather_ingest.w2_recovery import (  # noqa: E402
    CHECKPOINT_CONTRACT_VERSION,
    checkpoint_payload,
    completed_window_labels,
    preparation_dbt_vars,
    select_windows_with_publishable_anchors,
    split_repair_windows,
    window_dbt_vars,
)
from weather_w2_recovery_test_support import (  # noqa: E402
    FakeAirflowFailException,
    FakeDAG,
    FakePythonOperator,
    MODULE_NAMES,
    load_recovery_module,
)


@pytest.fixture(autouse=True)
def restore_modules_after_recovery_import():
    originals = {name: sys.modules.get(name) for name in MODULE_NAMES}
    yield
    for name, module in originals.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


def test_splits_inclusive_range_into_six_hour_windows():
    windows = split_repair_windows(
        "2026-07-02 00:00:00.000000",
        "2026-07-02 23:59:59.999999",
    )

    assert [window.label for window in windows] == [
        "2026-07-02 00:00:00.000000__2026-07-02 05:59:59.999999",
        "2026-07-02 06:00:00.000000__2026-07-02 11:59:59.999999",
        "2026-07-02 12:00:00.000000__2026-07-02 17:59:59.999999",
        "2026-07-02 18:00:00.000000__2026-07-02 23:59:59.999999",
    ]
    assert all(
        window.duration_microseconds <= 6 * 60 * 60 * 1_000_000 for window in windows
    )


def test_rejects_reversed_repair_range():
    with pytest.raises(
        ValueError, match="start_at must be before or equal to cutoff_at"
    ):
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
        "weather_w2_publishable_cutoff_at": "2026-07-08 05:59:59.999999",
        "weather_w2_bridge_version": "weather_admin_dong_grid_bridge_v1",
        "weather_w2_canonical_revision_date": "2025-04-01",
    }


def test_preparation_uses_one_bounded_24_hour_evidence_window():
    windows = split_repair_windows(
        "2026-07-02 00:00:00.000000",
        "2026-07-02 23:59:59.999999",
    )

    assert preparation_dbt_vars(windows[0], windows[-1]) == {
        "weather_w2_repair_mode": "bounded_reconcile",
        "weather_w2_repair_start_at": "2026-07-02 00:00:00.000000",
        "weather_w2_publishable_cutoff_at": "2026-07-02 23:59:59.999999",
        "weather_w2_bridge_version": "weather_admin_dong_grid_bridge_v1",
        "weather_w2_canonical_revision_date": "2025-04-01",
    }


def test_selects_only_windows_that_have_publishable_manifest_anchors():
    windows = split_repair_windows(
        "2026-07-06 00:00:00.000000",
        "2026-07-06 23:59:59.999999",
    )

    assert select_windows_with_publishable_anchors(windows, {0, 2}) == [
        windows[0],
        windows[2],
    ]


def test_phase_plan_owns_only_named_selectors_and_stable_invocation_identity():
    preparation = recovery_contracts.preparation_phase_plan()
    window = recovery_contracts.window_phase_plan(3)
    winner = tuple(
        recovery_contracts.winner_phase(3, bucket_index)
        for bucket_index in range(recovery_contracts.WINNER_RUN_BUCKET_COUNT)
    )
    lineage = tuple(
        recovery_contracts.lineage_phase(3, bucket_index)
        for bucket_index in range(recovery_contracts.LINEAGE_RUN_BUCKET_COUNT)
    )

    assert preparation == (
        recovery_contracts.DbtPhase("deps", None, "prepare-dependencies"),
        recovery_contracts.DbtPhase(
            "seed", "ask_seoul_weather_w1_inputs", "prepare-w1-inputs"
        ),
        recovery_contracts.DbtPhase(
            "run",
            "ask_seoul_weather_transform_common_admin",
            "prepare-common-admin",
        ),
        recovery_contracts.DbtPhase(
            "run", "ask_seoul_weather_w1_bridge", "prepare-w1-bridge"
        ),
        recovery_contracts.DbtPhase(
            "test", "ask_seoul_weather_w1_bridge", "prepare-w1-contract"
        ),
    )
    assert window == (
        recovery_contracts.DbtPhase(
            "run",
            "ask_seoul_weather_w2_recovery_window_models",
            "window-0003-models",
        ),
        recovery_contracts.DbtPhase(
            "test",
            "ask_seoul_weather_w2_recovery_window_contracts",
            "window-0003-contracts",
        ),
    )
    assert recovery_contracts.WINNER_RUN_BUCKET_COUNT == 8
    assert winner == tuple(
        recovery_contracts.DbtPhase(
            "test",
            "ask_seoul_weather_w2_recovery_winner_contract",
            f"window-0003-winner-{bucket_index}",
        )
        for bucket_index in range(8)
    )
    assert lineage == tuple(
        recovery_contracts.DbtPhase(
            "test",
            "ask_seoul_weather_w2_recovery_lineage_contract",
            f"window-0003-lineage-{bucket_index}",
        )
        for bucket_index in range(4)
    )
    assert recovery_contracts.final_phase() == recovery_contracts.DbtPhase(
        "test",
        "ask_seoul_weather_w2_recovery_final_contract",
        "final-contract",
    )
    with pytest.raises(AttributeError):
        preparation[0].selector = "mutated"


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
    assert payload["contract_version"] == CHECKPOINT_CONTRACT_VERSION
    assert payload["completed_windows"] == [windows[0].label]

    other_windows = split_repair_windows(
        "2026-07-03 00:00:00.000000",
        "2026-07-03 23:59:59.999999",
    )
    with pytest.raises(
        ValueError, match="checkpoint range does not match requested repair range"
    ):
        completed_window_labels(payload, other_windows)

    assert completed_window_labels(payload, windows) == {windows[0].label}


def test_checkpoint_migrates_completed_legacy_daily_window_to_subwindows():
    windows = split_repair_windows(
        "2026-07-02 00:00:00.000000",
        "2026-07-02 23:59:59.999999",
    )
    payload = {
        "contract_version": CHECKPOINT_CONTRACT_VERSION,
        "range": {
            "start_at": "2026-07-02 00:00:00.000000",
            "cutoff_at": "2026-07-02 23:59:59.999999",
        },
        "completed_windows": ["2026-07-02 00:00:00.000000__2026-07-02 23:59:59.999999"],
    }

    assert completed_window_labels(payload, windows) == {
        window.label for window in windows
    }


def test_checkpoint_does_not_trust_unversioned_pre_winner_completion():
    windows = split_repair_windows(
        "2026-07-02 00:00:00.000000",
        "2026-07-02 05:59:59.999999",
    )
    payload = {
        "range": {
            "start_at": "2026-07-02 00:00:00.000000",
            "cutoff_at": "2026-07-02 05:59:59.999999",
        },
        "completed_windows": [windows[0].label],
    }

    assert completed_window_labels(payload, windows) == set()


def test_staged_checkpoint_keeps_pins_baseline_and_selected_windows():
    windows = split_repair_windows(
        "2026-07-11 00:00:00.000000",
        "2026-07-11 11:59:59.999999",
    )
    pins = recovery_contracts.RecoveryPins(
        admin_dong_crosswalk_snapshot_id=11,
        weather_bridge_snapshot_id=12,
        silver_grid_snapshot_id=13,
        gold_baseline_snapshot_id=14,
    )
    baseline = recovery_contracts.RecoveryBaseline(
        non_target_row_count=3_328_600,
        non_target_checksum="a1b2c3",
        source_watermark="2026-07-27 02:33:08.181476",
    )

    payload = recovery_contracts.staged_checkpoint_payload(
        windows,
        selected_labels=[windows[0].label],
        completed_labels=[],
        checkpoint_id="yongsin-0711-0724-light-v3",
        pins=pins,
        baseline=baseline,
    )

    assert payload == {
        "contract_version": 3,
        "mode": "staged_gold_only",
        "checkpoint_id": "yongsin-0711-0724-light-v3",
        "range": {
            "start_at": "2026-07-11 00:00:00.000000",
            "cutoff_at": "2026-07-11 11:59:59.999999",
        },
        "target": {
            "admin_dong_code": "1123053600",
            "nx": 61,
            "ny": 127,
        },
        "pins": {
            "admin_dong_crosswalk_snapshot_id": 11,
            "weather_bridge_snapshot_id": 12,
            "silver_grid_snapshot_id": 13,
            "gold_baseline_snapshot_id": 14,
        },
        "baseline": {
            "non_target_row_count": 3_328_600,
            "non_target_checksum": "a1b2c3",
            "source_watermark": "2026-07-27 02:33:08.181476",
        },
        "selected_windows": [windows[0].label],
        "completed_windows": [],
        "known_gaps": [],
        "state": "staging",
    }


def test_staged_checkpoint_loads_saved_pins_without_resolving_latest_again():
    windows = split_repair_windows(
        "2026-07-11 00:00:00.000000",
        "2026-07-11 11:59:59.999999",
    )
    pins = recovery_contracts.RecoveryPins(11, 12, 13, 14)
    baseline = recovery_contracts.RecoveryBaseline(
        3_328_600,
        "a1b2c3",
        "2026-07-27 02:33:08.181476",
    )
    payload = recovery_contracts.staged_checkpoint_payload(
        windows,
        selected_labels=[window.label for window in windows],
        completed_labels=[windows[0].label],
        checkpoint_id="yongsin-0711-0724-light-v3",
        pins=pins,
        baseline=baseline,
    )

    checkpoint = recovery_contracts.load_staged_checkpoint(
        payload,
        windows,
        checkpoint_id="yongsin-0711-0724-light-v3",
    )

    assert checkpoint.pins == pins
    assert checkpoint.baseline == baseline
    assert checkpoint.selected_labels == tuple(window.label for window in windows)
    assert checkpoint.completed_labels == frozenset({windows[0].label})
    assert checkpoint.state == "staging"


def test_staged_checkpoint_loads_pre_gap_v3_payload_as_no_known_gaps():
    windows = split_repair_windows(
        "2026-07-11 00:00:00.000000",
        "2026-07-11 05:59:59.999999",
    )
    payload = recovery_contracts.staged_checkpoint_payload(
        windows,
        selected_labels=[windows[0].label],
        completed_labels=[],
        checkpoint_id="yongsin-0711-0724-light-v3",
        pins=recovery_contracts.RecoveryPins(11, 12, 13, 14),
        baseline=recovery_contracts.RecoveryBaseline(
            3_328_600,
            "a1b2c3",
            "2026-07-27 02:33:08.181476",
        ),
    )
    payload.pop("known_gaps")

    checkpoint = recovery_contracts.load_staged_checkpoint(
        payload,
        windows,
        checkpoint_id="yongsin-0711-0724-light-v3",
    )

    assert checkpoint.known_gap_labels == frozenset()


@pytest.mark.parametrize("invalid_snapshot_id", [0, -1, True, "13"])
def test_staged_checkpoint_rejects_invalid_snapshot_ids(invalid_snapshot_id):
    with pytest.raises(ValueError, match="snapshot IDs must be positive integers"):
        recovery_contracts.RecoveryPins(
            admin_dong_crosswalk_snapshot_id=11,
            weather_bridge_snapshot_id=12,
            silver_grid_snapshot_id=invalid_snapshot_id,
            gold_baseline_snapshot_id=14,
        )


def test_staged_checkpoint_rejects_completed_window_outside_selected_set():
    windows = split_repair_windows(
        "2026-07-11 00:00:00.000000",
        "2026-07-11 11:59:59.999999",
    )

    with pytest.raises(
        ValueError,
        match="completed staged windows must be selected repair windows",
    ):
        recovery_contracts.staged_checkpoint_payload(
            windows,
            selected_labels=[windows[0].label],
            completed_labels=[windows[1].label],
            checkpoint_id="yongsin-0711-0724-light-v3",
            pins=recovery_contracts.RecoveryPins(11, 12, 13, 14),
            baseline=recovery_contracts.RecoveryBaseline(
                3_328_600,
                "a1b2c3",
                "2026-07-27 02:33:08.181476",
            ),
        )


def test_staged_checkpoint_requires_all_windows_and_ordered_state_transitions():
    windows = split_repair_windows(
        "2026-07-11 00:00:00.000000",
        "2026-07-11 11:59:59.999999",
    )
    payload = recovery_contracts.staged_checkpoint_payload(
        windows,
        selected_labels=[window.label for window in windows],
        completed_labels=[windows[0].label],
        checkpoint_id="yongsin-0711-0724-light-v3",
        pins=recovery_contracts.RecoveryPins(11, 12, 13, 14),
        baseline=recovery_contracts.RecoveryBaseline(
            3_328_600,
            "a1b2c3",
            "2026-07-27 02:33:08.181476",
        ),
    )

    with pytest.raises(
        ValueError,
        match="all selected staged windows must resolve before prepublish validation",
    ):
        recovery_contracts.advance_staged_checkpoint(
            payload,
            next_state="prepublish_validated",
        )

    payload = recovery_contracts.complete_staged_window(
        payload,
        windows[1].label,
    )
    payload = recovery_contracts.advance_staged_checkpoint(
        payload,
        next_state="prepublish_validated",
    )
    payload = recovery_contracts.advance_staged_checkpoint(
        payload,
        next_state="published",
    )
    payload = recovery_contracts.advance_staged_checkpoint(
        payload,
        next_state="verified",
    )

    assert payload["completed_windows"] == [window.label for window in windows]
    assert payload["state"] == "verified"

    with pytest.raises(ValueError, match="invalid staged checkpoint transition"):
        recovery_contracts.advance_staged_checkpoint(
            {**payload, "state": "staging"},
            next_state="verified",
        )


def test_staged_checkpoint_records_a_pinned_source_gap_without_marking_it_complete():
    windows = split_repair_windows(
        "2026-07-11 00:00:00.000000",
        "2026-07-11 11:59:59.999999",
    )
    payload = recovery_contracts.staged_checkpoint_payload(
        windows,
        selected_labels=[window.label for window in windows],
        completed_labels=[windows[0].label],
        checkpoint_id="yongsin-0711-0724-light-v3",
        pins=recovery_contracts.RecoveryPins(11, 12, 13, 14),
        baseline=recovery_contracts.RecoveryBaseline(
            3_328_600,
            "a1b2c3",
            "2026-07-27 02:33:08.181476",
        ),
    )

    payload = recovery_contracts.record_staged_known_gap(
        payload,
        windows[1].label,
        reason="no_target_source_rows_at_pinned_snapshot",
    )
    checkpoint = recovery_contracts.load_staged_checkpoint(
        payload,
        windows,
        checkpoint_id="yongsin-0711-0724-light-v3",
    )
    payload = recovery_contracts.advance_staged_checkpoint(
        payload,
        next_state="prepublish_validated",
    )

    assert checkpoint.completed_labels == frozenset({windows[0].label})
    assert checkpoint.known_gap_labels == frozenset({windows[1].label})
    assert payload["known_gaps"] == [
        {
            "window": windows[1].label,
            "reason": "no_target_source_rows_at_pinned_snapshot",
        }
    ]
    assert payload["state"] == "prepublish_validated"


@pytest.mark.parametrize(
    ("payload_change", "message"),
    [
        (
            {"target": {"admin_dong_code": "1123053700", "nx": 61, "ny": 127}},
            "staged checkpoint target must be Yongsin-dong",
        ),
        (
            {"state": "unknown"},
            "staged checkpoint state is invalid",
        ),
        (
            {
                "selected_windows": [
                    "2026-07-12 00:00:00.000000__2026-07-12 05:59:59.999999"
                ]
            },
            "selected staged windows must be requested repair windows",
        ),
    ],
)
def test_staged_checkpoint_loader_rejects_tampered_scope(
    payload_change,
    message,
):
    windows = split_repair_windows(
        "2026-07-11 00:00:00.000000",
        "2026-07-11 05:59:59.999999",
    )
    payload = recovery_contracts.staged_checkpoint_payload(
        windows,
        selected_labels=[windows[0].label],
        completed_labels=[],
        checkpoint_id="yongsin-0711-0724-light-v3",
        pins=recovery_contracts.RecoveryPins(11, 12, 13, 14),
        baseline=recovery_contracts.RecoveryBaseline(
            3_328_600,
            "a1b2c3",
            "2026-07-27 02:33:08.181476",
        ),
    )

    with pytest.raises(ValueError, match=message):
        recovery_contracts.load_staged_checkpoint(
            {**payload, **payload_change},
            windows,
            checkpoint_id="yongsin-0711-0724-light-v3",
        )


def _successful_execution():
    completed = subprocess.CompletedProcess(
        args=["dbt"], returncode=0, stdout="ok\n", stderr=""
    )
    return types.SimpleNamespace(
        attempts=(completed,),
        completed=completed,
        missing_expected_artifacts=(),
        existing_run_results_path="/tmp/run_results.json",
        existing_sources_path=None,
        existing_manifest_path="/tmp/manifest.json",
        selected_unique_ids=("model.asac_seoul.selected",),
    )


def _recovery_context():
    return {
        "params": {
            "target": "dev",
            "repair_start_at": "2026-07-02 00:00:00.000000",
            "repair_cutoff_at": "2026-07-02 05:59:59.999999",
            "checkpoint_id": "issue-196",
        },
        "run_id": "manual__w2",
        "ti": types.SimpleNamespace(
            task_id="recover_observation_windows", try_number=2
        ),
    }


def _staged_recovery_context():
    context = _recovery_context()
    context["params"] = {
        **context["params"],
        "recovery_mode": "staged_gold_only",
        "checkpoint_id": "yongsin-0711-0724-light-v3",
    }
    return context


def _pin_crosswalk_snapshot(module, monkeypatch):
    monkeypatch.setattr(
        module,
        "resolve_admin_dong_crosswalk_snapshot_id",
        lambda: 8738321387624398062,
    )


class SnapshotCursor:
    def __init__(self, row):
        self.row = row
        self.sql = None

    def execute(self, sql):
        self.sql = sql

    def fetchone(self):
        return self.row


class SequenceCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.sql = []

    def execute(self, sql):
        self.sql.append(sql)

    def fetchone(self):
        return self.rows.pop(0)


def test_recovery_resolves_the_latest_crosswalk_snapshot(monkeypatch):
    module = load_recovery_module()
    cursor = SnapshotCursor((8738321387624398062,))
    monkeypatch.setattr(
        module,
        "trino_cursor",
        lambda: (cursor, "iceberg_dev", "weather"),
    )
    monkeypatch.setenv("COMMON_SCHEMA", "common")

    snapshot_id = module.resolve_admin_dong_crosswalk_snapshot_id()

    assert snapshot_id == 8738321387624398062
    assert cursor.sql is not None
    assert 'iceberg_dev.common."seoul_admin_dong_crosswalk$snapshots"' in cursor.sql
    assert cursor.sql.rstrip().endswith("LIMIT 1")


def test_recovery_resolves_latest_weather_table_snapshot(monkeypatch):
    module = load_recovery_module()
    cursor = SnapshotCursor((2233013568298274386,))
    monkeypatch.setattr(
        module,
        "trino_cursor",
        lambda: (cursor, "iceberg_dev", "weather_traffic_bronze"),
    )
    monkeypatch.setenv("WEATHER_SCHEMA", "weather")

    snapshot_id = module.resolve_weather_table_snapshot_id(
        "silver_kma_vilage_fcst_grid"
    )

    assert snapshot_id == 2233013568298274386
    assert cursor.sql is not None
    assert (
        'iceberg_dev.weather."silver_kma_vilage_fcst_grid$snapshots"'
        in cursor.sql
    )


def test_recovery_baseline_uses_pinned_gold_and_silver_snapshots(monkeypatch):
    module = load_recovery_module()
    cursor = SequenceCursor(
        [
            (3_328_600, "a1b2c3"),
            ("2026-07-27 02:33:08.181476",),
        ]
    )
    monkeypatch.setattr(
        module,
        "trino_cursor",
        lambda: (cursor, "iceberg_dev", "weather_traffic_bronze"),
    )
    monkeypatch.setenv("WEATHER_SCHEMA", "weather")

    baseline = module.resolve_staged_recovery_baseline(
        recovery_contracts.RecoveryPins(11, 12, 13, 14)
    )

    assert baseline == recovery_contracts.RecoveryBaseline(
        3_328_600,
        "a1b2c3",
        "2026-07-27 02:33:08.181476",
    )
    assert "FOR VERSION AS OF 14" in cursor.sql[0]
    assert 'FROM iceberg_dev.weather."gold_weather_forecast_by_admin_dong"' in cursor.sql[0]
    assert "admin_dong_code <> '1123053600'" in cursor.sql[0]
    assert "FOR VERSION AS OF 13" in cursor.sql[1]
    assert 'FROM iceberg_dev.weather."silver_kma_vilage_fcst_grid"' in cursor.sql[1]
    assert "nx = 61" in cursor.sql[1]
    assert "ny = 127" in cursor.sql[1]


def test_staged_window_source_count_uses_pinned_silver_and_publishable_manifest(
    monkeypatch,
):
    module = load_recovery_module()
    cursor = SnapshotCursor((0,))
    monkeypatch.setattr(
        module,
        "trino_cursor",
        lambda: (cursor, "iceberg_dev", "weather_traffic_bronze"),
    )
    monkeypatch.setenv("WEATHER_SCHEMA", "weather")
    window = split_repair_windows(
        "2026-07-15 00:00:00.000000",
        "2026-07-15 05:59:59.999999",
    )[0]
    payload = {
        "pins": {"silver_grid_snapshot_id": 2233013568298274386},
    }

    assert module.staged_window_source_row_count(window, payload) == 0
    assert "FOR VERSION AS OF 2233013568298274386" in cursor.sql
    assert 'from iceberg_dev.weather."silver_kma_vilage_fcst_grid"' in cursor.sql
    assert "inner join eligible_manifest_anchors" in cursor.sql
    assert "grid.nx = 61" in cursor.sql
    assert "grid.ny = 127" in cursor.sql


def test_recovery_rejects_non_target_gold_fingerprint_drift(monkeypatch):
    module = load_recovery_module()
    cursor = SnapshotCursor((3_328_599, "changed"))
    monkeypatch.setattr(
        module,
        "trino_cursor",
        lambda: (cursor, "iceberg_dev", "weather_traffic_bronze"),
    )
    monkeypatch.setenv("WEATHER_SCHEMA", "weather")
    payload = {
        "baseline": {
            "non_target_row_count": 3_328_600,
            "non_target_checksum": "a1b2c3",
            "source_watermark": "2026-07-27 02:33:08.181476",
        }
    }

    with pytest.raises(
        FakeAirflowFailException,
        match="non-target Gold fingerprint changed",
    ):
        module.verify_non_target_baseline(payload)


def test_recovery_result_requires_yongsin_in_serving_projection(monkeypatch):
    module = load_recovery_module()
    cursor = SequenceCursor(
        [
            (3_328_600, "a1b2c3"),
            (6_818, 0),
        ]
    )
    monkeypatch.setattr(
        module,
        "trino_cursor",
        lambda: (cursor, "iceberg_dev", "weather_traffic_bronze"),
    )
    monkeypatch.setenv("WEATHER_SCHEMA", "weather")
    payload = {
        "baseline": {
            "non_target_row_count": 3_328_600,
            "non_target_checksum": "a1b2c3",
            "source_watermark": "2026-07-27 02:33:08.181476",
        }
    }

    with pytest.raises(
        FakeAirflowFailException,
        match="Serving projection does not contain Yongsin-dong",
    ):
        module.verify_staged_recovery_result(payload)
    assert 'FROM iceberg_dev.weather."gold_weather_forecast_by_admin_dong"' in cursor.sql[1]
    assert 'FROM iceberg_dev.weather."gold_weather_current_wide_by_admin_dong"' in cursor.sql[1]


@pytest.mark.parametrize("row", [None, (None,), (0,), (-1,), (True,), ("invalid",)])
def test_recovery_rejects_unusable_crosswalk_snapshot(monkeypatch, row):
    module = load_recovery_module()
    cursor = SnapshotCursor(row)
    monkeypatch.setattr(
        module,
        "trino_cursor",
        lambda: (cursor, "iceberg_dev", "weather"),
    )

    with pytest.raises(module.AdminDongCrosswalkSnapshotUnavailableError):
        module.resolve_admin_dong_crosswalk_snapshot_id()


def test_manual_recovery_dag_shape_is_serial_dev_only_and_domain_pooled():
    module = load_recovery_module()
    dag = module.dag

    assert isinstance(dag, FakeDAG)
    assert dag.dag_id == "weather_w2_observation_recovery"
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["params"]["target"].schema["enum"] == ["dev", "prod"]
    assert dag.task_dict["validate_dev_runtime"].downstream_task_ids == {
        "recover_observation_windows"
    }
    recover_task = dag.task_dict["recover_observation_windows"]
    assert isinstance(recover_task, FakePythonOperator)
    assert module.TRINO_WEATHER_RECOVERY_HEAVY_POOL == "trino_weather_recovery_heavy"
    assert recover_task.kwargs["pool"] == module.TRINO_WEATHER_RECOVERY_HEAVY_POOL
    assert recover_task.kwargs["pool_slots"] == 1
    assert recover_task.kwargs["retries"] == 1
    assert recover_task.kwargs["retry_delay"] == module.DBT_RETRY_DELAY


def test_staged_recovery_validates_before_one_publish_and_then_verifies(
    monkeypatch,
):
    module = load_recovery_module()
    windows = split_repair_windows(
        "2026-07-02 00:00:00.000000",
        "2026-07-02 05:59:59.999999",
    )
    checkpoint_payload_v3 = recovery_contracts.staged_checkpoint_payload(
        windows,
        selected_labels=[windows[0].label],
        completed_labels=[],
        checkpoint_id="yongsin-0711-0724-light-v3",
        pins=recovery_contracts.RecoveryPins(11, 12, 13, 14),
        baseline=recovery_contracts.RecoveryBaseline(
            3_328_600,
            "a1b2c3",
            "2026-07-27 02:33:08.181476",
        ),
    )
    events = []
    saved_payloads = []

    monkeypatch.setattr(
        module,
        "_load_or_initialize_staged_checkpoint",
        lambda **_kwargs: checkpoint_payload_v3,
    )
    monkeypatch.setattr(
        module,
        "verify_non_target_baseline",
        lambda _payload: events.append("verify-non-target"),
    )
    monkeypatch.setattr(
        module,
        "verify_staged_recovery_result",
        lambda _payload: events.append("verify-result") or {},
    )
    monkeypatch.setattr(
        module,
        "staged_window_source_row_count",
        lambda _window, _payload: 1,
    )
    monkeypatch.setattr(
        module.Variable,
        "set",
        staticmethod(
            lambda _name, payload, **_kwargs: (
                saved_payloads.append(payload),
                events.append(f"checkpoint-{payload['state']}"),
            )
        ),
    )

    def execute_dbt_phase(**kwargs):
        events.append(kwargs["invocation_id"])
        return _successful_execution()

    monkeypatch.setattr(module.weather_dbt, "execute_dbt_phase", execute_dbt_phase)

    result = module.recover_observation_windows(**_staged_recovery_context())

    assert events == [
        "prepare-dependencies",
        "stage-window-0000",
        "test-window-0000",
        "checkpoint-staging",
        "stage-final",
        *[f"stage-winner-{bucket_index}" for bucket_index in range(8)],
        *[f"stage-lineage-{bucket_index}" for bucket_index in range(4)],
        "verify-non-target",
        "checkpoint-prepublish_validated",
        "publish-gold",
        "checkpoint-published",
        "canonical-bridge-reconcile",
        "verify-result",
        "checkpoint-verified",
    ]
    assert result["state"] == "verified"
    assert saved_payloads[-1]["state"] == "verified"


def test_staged_recovery_checkpoints_known_source_gap_and_skips_dbt_window(
    monkeypatch,
):
    module = load_recovery_module()
    windows = split_repair_windows(
        "2026-07-15 00:00:00.000000",
        "2026-07-15 05:59:59.999999",
    )
    checkpoint_payload_v3 = recovery_contracts.staged_checkpoint_payload(
        windows,
        selected_labels=[windows[0].label],
        completed_labels=[],
        checkpoint_id="yongsin-0711-0724-light-v3",
        pins=recovery_contracts.RecoveryPins(11, 12, 13, 14),
        baseline=recovery_contracts.RecoveryBaseline(
            3_328_600,
            "a1b2c3",
            "2026-07-27 02:33:08.181476",
        ),
    )
    events = []
    saved_payloads = []

    monkeypatch.setattr(
        module,
        "_load_or_initialize_staged_checkpoint",
        lambda **_kwargs: checkpoint_payload_v3,
    )
    monkeypatch.setattr(
        module,
        "staged_window_source_row_count",
        lambda _window, _payload: 0,
    )
    monkeypatch.setattr(
        module,
        "verify_non_target_baseline",
        lambda _payload: events.append("verify-non-target"),
    )
    monkeypatch.setattr(
        module,
        "verify_staged_recovery_result",
        lambda _payload: events.append("verify-result") or {},
    )
    monkeypatch.setattr(
        module.Variable,
        "set",
        staticmethod(
            lambda _name, payload, **_kwargs: saved_payloads.append(payload)
        ),
    )

    def execute_dbt_phase(**kwargs):
        events.append(kwargs["invocation_id"])
        return _successful_execution()

    monkeypatch.setattr(module.weather_dbt, "execute_dbt_phase", execute_dbt_phase)

    context = _staged_recovery_context()
    context["params"] = {
        **context["params"],
        "repair_start_at": "2026-07-15 00:00:00.000000",
        "repair_cutoff_at": "2026-07-15 05:59:59.999999",
    }
    result = module.recover_observation_windows(**context)

    assert "stage-window-0000" not in events
    assert "test-window-0000" not in events
    assert saved_payloads[0]["known_gaps"] == [
        {
            "window": windows[0].label,
            "reason": "no_target_source_rows_at_pinned_snapshot",
        }
    ]
    assert result["known_gap_windows"] == 1


def test_staged_checkpoint_initialization_pins_inputs_and_saves_before_windows(
    monkeypatch,
):
    module = load_recovery_module()
    windows = split_repair_windows(
        "2026-07-02 00:00:00.000000",
        "2026-07-02 11:59:59.999999",
    )
    saved = []
    snapshots = {
        "bridge_weather_admin_dong_grid": 12,
        "silver_kma_vilage_fcst_grid": 13,
        "gold_weather_forecast_by_admin_dong": 14,
    }
    baseline = recovery_contracts.RecoveryBaseline(
        3_328_600,
        "a1b2c3",
        "2026-07-27 02:33:08.181476",
    )

    monkeypatch.setattr(
        module.Variable,
        "get",
        staticmethod(lambda *_args, **_kwargs: None),
    )
    monkeypatch.setattr(
        module.Variable,
        "set",
        staticmethod(
            lambda name, payload, **_kwargs: saved.append((name, payload))
        ),
    )
    monkeypatch.setattr(
        module,
        "resolve_admin_dong_crosswalk_snapshot_id",
        lambda: 11,
    )
    monkeypatch.setattr(
        module,
        "resolve_weather_table_snapshot_id",
        lambda table_name: snapshots[table_name],
    )
    monkeypatch.setattr(
        module,
        "resolve_staged_recovery_baseline",
        lambda _pins: baseline,
    )
    monkeypatch.setattr(
        module,
        "publishable_window_indexes",
        lambda _windows: {0},
    )

    payload = module._load_or_initialize_staged_checkpoint(
        variable_name=(
            "ask_seoul.weather.w2_observation_recovery."
            "yongsin-0711-0724-light-v3"
        ),
        checkpoint_id="yongsin-0711-0724-light-v3",
        windows=windows,
    )

    assert payload["pins"] == {
        "admin_dong_crosswalk_snapshot_id": 11,
        "weather_bridge_snapshot_id": 12,
        "silver_grid_snapshot_id": 13,
        "gold_baseline_snapshot_id": 14,
    }
    assert payload["selected_windows"] == [windows[0].label]
    assert payload["completed_windows"] == []
    assert saved == [
        (
            "ask_seoul.weather.w2_observation_recovery."
            "yongsin-0711-0724-light-v3",
            payload,
        )
    ]


def test_recovery_executes_selector_phases_in_order_and_checkpoints_before_final(
    monkeypatch,
):
    module = load_recovery_module()
    events = []
    calls = []
    _pin_crosswalk_snapshot(module, monkeypatch)
    monkeypatch.setattr(module, "publishable_window_indexes", lambda _windows: {0})
    monkeypatch.setattr(
        module.Variable,
        "get",
        staticmethod(lambda *_args, **_kwargs: None),
    )
    monkeypatch.setattr(
        module.Variable,
        "set",
        staticmethod(lambda *_args, **_kwargs: events.append("checkpoint")),
    )

    def execute_dbt_phase(**kwargs):
        calls.append(kwargs)
        events.append(kwargs["invocation_id"])
        return _successful_execution()

    monkeypatch.setattr(module.weather_dbt, "execute_dbt_phase", execute_dbt_phase)

    result = module.recover_observation_windows(**_recovery_context())

    assert [call["selector"] for call in calls] == [
        None,
        "ask_seoul_weather_w1_inputs",
        "ask_seoul_weather_transform_common_admin",
        "ask_seoul_weather_w1_bridge",
        "ask_seoul_weather_w1_bridge",
        "ask_seoul_weather_w2_recovery_window_models",
        "ask_seoul_weather_w2_recovery_window_contracts",
        *["ask_seoul_weather_w2_recovery_winner_contract"] * 8,
        *["ask_seoul_weather_w2_recovery_lineage_contract"] * 4,
        "ask_seoul_weather_w2_recovery_final_contract",
    ]
    assert [call["invocation_id"] for call in calls] == [
        "prepare-dependencies",
        "prepare-w1-inputs",
        "prepare-common-admin",
        "prepare-w1-bridge",
        "prepare-w1-contract",
        "window-0000-models",
        "window-0000-contracts",
        "window-0000-winner-0",
        "window-0000-winner-1",
        "window-0000-winner-2",
        "window-0000-winner-3",
        "window-0000-winner-4",
        "window-0000-winner-5",
        "window-0000-winner-6",
        "window-0000-winner-7",
        "window-0000-lineage-0",
        "window-0000-lineage-1",
        "window-0000-lineage-2",
        "window-0000-lineage-3",
        "final-contract",
    ]
    assert events[-2:] == ["checkpoint", "final-contract"]
    assert all(call["threads"] == 1 for call in calls)
    assert all("project_dir" not in call for call in calls)
    assert all("executable" not in call for call in calls)
    assert all(call["pipeline"] == "weather-w2-observation-recovery" for call in calls)
    assert all(call["run_id"] == "manual__w2" for call in calls)
    assert all(call["task_id"] == "recover_observation_windows" for call in calls)
    assert all(call["try_number"] == 2 for call in calls)
    assert all("runner" not in call for call in calls)
    assert all(isinstance(json.loads(call["variables"]), dict) for call in calls)
    assert all(
        json.loads(call["variables"])["admin_dong_crosswalk_pin_snapshot_id"]
        == 8738321387624398062
        for call in calls
    )
    winner_calls = calls[7:15]
    assert [
        json.loads(call["variables"])["weather_w2_winner_bucket_index"]
        for call in winner_calls
    ] == [str(bucket_index) for bucket_index in range(8)]
    assert all(
        json.loads(call["variables"])["weather_w2_winner_bucket_count"] == "8"
        for call in winner_calls
    )
    lineage_calls = calls[15:19]
    assert [
        json.loads(call["variables"])["weather_w2_lineage_run_bucket_index"]
        for call in lineage_calls
    ] == [
        "0",
        "1",
        "2",
        "3",
    ]
    assert result["completed_windows"] == 1


def test_recovery_revalidates_unversioned_checkpoint_before_skip(monkeypatch):
    module = load_recovery_module()
    events = []
    saved_payloads = []
    _pin_crosswalk_snapshot(module, monkeypatch)
    legacy_payload = {
        "range": {
            "start_at": "2026-07-02 00:00:00.000000",
            "cutoff_at": "2026-07-02 05:59:59.999999",
        },
        "completed_windows": [
            "2026-07-02 00:00:00.000000__2026-07-02 05:59:59.999999"
        ],
    }
    monkeypatch.setattr(module, "publishable_window_indexes", lambda _windows: {0})
    monkeypatch.setattr(
        module.Variable,
        "get",
        staticmethod(lambda *_args, **_kwargs: legacy_payload),
    )
    monkeypatch.setattr(
        module.Variable,
        "set",
        staticmethod(
            lambda _name, payload, **_kwargs: saved_payloads.append(payload)
        ),
    )

    def execute_dbt_phase(**kwargs):
        events.append(kwargs["invocation_id"])
        return _successful_execution()

    monkeypatch.setattr(module.weather_dbt, "execute_dbt_phase", execute_dbt_phase)

    result = module.recover_observation_windows(**_recovery_context())

    assert "window-0000-models" in events
    assert "window-0000-winner-0" in events
    assert "window-0000-lineage-0" in events
    assert saved_payloads[-1]["contract_version"] == CHECKPOINT_CONTRACT_VERSION
    assert result["completed_windows"] == 1


def test_recovery_failure_stops_later_buckets_checkpoint_and_final(monkeypatch):
    module = load_recovery_module()
    events = []
    _pin_crosswalk_snapshot(module, monkeypatch)
    monkeypatch.setattr(module, "publishable_window_indexes", lambda _windows: {0})
    monkeypatch.setattr(
        module.Variable, "get", staticmethod(lambda *_args, **_kwargs: None)
    )
    monkeypatch.setattr(
        module.Variable,
        "set",
        staticmethod(lambda *_args, **_kwargs: events.append("checkpoint")),
    )

    def execute_dbt_phase(**kwargs):
        events.append(kwargs["invocation_id"])
        if kwargs["invocation_id"] == "window-0000-lineage-2":
            completed = subprocess.CompletedProcess(
                args=["dbt"], returncode=1, stdout="", stderr="contract failed"
            )
            return types.SimpleNamespace(
                attempts=(completed,),
                completed=completed,
                missing_expected_artifacts=(),
                existing_run_results_path=None,
                existing_sources_path=None,
                existing_manifest_path=None,
                selected_unique_ids=("test.asac_seoul.lineage",),
            )
        return _successful_execution()

    monkeypatch.setattr(module.weather_dbt, "execute_dbt_phase", execute_dbt_phase)

    with pytest.raises(FakeAirflowFailException, match="data-contract-violation"):
        module.recover_observation_windows(**_recovery_context())

    assert events[-1] == "window-0000-lineage-2"
    assert "window-0000-lineage-3" not in events
    assert "checkpoint" not in events
    assert "final-contract" not in events


def test_recovery_winner_failure_stops_later_validation_and_checkpoint(monkeypatch):
    module = load_recovery_module()
    events = []
    _pin_crosswalk_snapshot(module, monkeypatch)
    monkeypatch.setattr(module, "publishable_window_indexes", lambda _windows: {0})
    monkeypatch.setattr(
        module.Variable, "get", staticmethod(lambda *_args, **_kwargs: None)
    )
    monkeypatch.setattr(
        module.Variable,
        "set",
        staticmethod(lambda *_args, **_kwargs: events.append("checkpoint")),
    )

    def execute_dbt_phase(**kwargs):
        events.append(kwargs["invocation_id"])
        if kwargs["invocation_id"] == "window-0000-winner-5":
            completed = subprocess.CompletedProcess(
                args=["dbt"], returncode=1, stdout="", stderr="contract failed"
            )
            return types.SimpleNamespace(
                attempts=(completed,),
                completed=completed,
                missing_expected_artifacts=(),
                existing_run_results_path=None,
                existing_sources_path=None,
                existing_manifest_path=None,
                selected_unique_ids=("test.asac_seoul.winner",),
            )
        return _successful_execution()

    monkeypatch.setattr(module.weather_dbt, "execute_dbt_phase", execute_dbt_phase)

    with pytest.raises(FakeAirflowFailException, match="data-contract-violation"):
        module.recover_observation_windows(**_recovery_context())

    assert events[-1] == "window-0000-winner-5"
    assert "window-0000-winner-6" not in events
    assert "window-0000-lineage-0" not in events
    assert "checkpoint" not in events
    assert "final-contract" not in events


def test_recovery_fails_the_airflow_task_when_crosswalk_snapshot_is_unavailable(
    monkeypatch,
):
    module = load_recovery_module()
    calls = []
    monkeypatch.setattr(module, "publishable_window_indexes", lambda _windows: {0})
    monkeypatch.setattr(
        module.Variable,
        "get",
        staticmethod(lambda *_args, **_kwargs: None),
    )
    monkeypatch.setattr(
        module,
        "resolve_admin_dong_crosswalk_snapshot_id",
        lambda: (_ for _ in ()).throw(
            module.AdminDongCrosswalkSnapshotUnavailableError("snapshot unavailable")
        ),
    )
    monkeypatch.setattr(
        module.weather_dbt,
        "execute_dbt_phase",
        lambda **kwargs: calls.append(kwargs) or _successful_execution(),
    )

    with pytest.raises(FakeAirflowFailException, match="snapshot unavailable"):
        module.recover_observation_windows(**_recovery_context())

    assert calls == []


def test_recovery_dag_contains_no_raw_dbt_or_node_selection_ownership():
    source = (
        Path(__file__).resolve().parents[1] / "weather_w2_observation_recovery.py"
    ).read_text(encoding="utf-8")

    for forbidden in (
        "_".join(("DBT", "BIN")),
        "_".join(("DBT", "PROJECT")),
        ".".join(("subprocess", "run")),
        "--" + "select",
        "tag" + ":",
        "_".join(("silver", "kma", "vilage", "fcst", "observation")),
        "_".join(("gold", "weather", "forecast", "by", "admin", "dong")),
        "_".join(("assert", "gold", "weather")),
        "/".join(("", "opt", "airflow", "dbt", "domains", "weather")),
    ):
        assert forbidden not in source
