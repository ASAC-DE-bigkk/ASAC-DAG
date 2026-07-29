import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest import reliability_report as report  # noqa: E402
from traffic_ingest.reliability import report as composition  # noqa: E402
from traffic_reliability_test_support import (  # noqa: E402
    RecordingCursor,
    stub_report_dependencies,
)


@pytest.fixture(autouse=True)
def stub_reliability_dependencies(monkeypatch):
    stub_report_dependencies(monkeypatch)


def test_build_traffic_report_passes_for_fresh_complete_data(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_SCHEMA", "weather_traffic_bronze")
    cursor = RecordingCursor(
        rows=[
            (1, 25, 25, 1000, 0, datetime(2026, 7, 2, 8, 55, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "PASS"
    assert result["dag_runs"] == {
        "dag_id": "traffic_incident_bronze",
        "success": 3,
        "failed": 0,
        "running": 1,
        "last_success_at": "2026-07-02 08:55:00+00:00",
        "last_publishable_at": "2026-07-02 08:55:00+00:00",
        "latest_dag_run_id": "scheduled__2026-07-02T08:55:00+00:00",
        "latest_status": "SUCCESS",
        "latest_is_publishable": True,
        "latest_event_at": "2026-07-02 08:55:00+00:00",
        "latest_terminal_dag_run_id": "scheduled__2026-07-02T08:55:00+00:00",
        "latest_terminal_status": "SUCCESS",
        "latest_terminal_is_publishable": True,
        "latest_terminal_event_at": "2026-07-02 08:55:00+00:00",
        "publishability_ok": True,
    }
    assert result["traffic"]["parsed_row_count"] == 25
    assert "bronze_seoul_traffic_incident_request_audit" in result["blast_radius"][1]
    assert "load_date >= '2026-06-30'" in cursor.statements[0]
    assert (
        "collected_at >= TIMESTAMP '2026-07-01 09:00:00.000000'" in cursor.statements[0]
    )
    assert result["traffic"]["freshness_status"] == "PASS"
    assert result["publishability_ok"] is True
    assert result["materialization_backlog"] == {
        "count": 0,
        "oldest_snapshot_at": None,
        "oldest_age_minutes": None,
        "status": "PASS",
    }
    assert result["late_publishability"] == {
        "status": "NOT_EVALUATED",
        "reason": "bounded late-repair contract is owned by ASAC-DBT #117",
    }


def test_traffic_report_uses_landing_ledger_and_bronze_manifest(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    captured = {"manifest_dag_ids": []}
    monkeypatch.setattr(
        composition,
        "collect_scheduled_run_summary",
        lambda dag_id, *_args: captured.update(cadence_dag_id=dag_id)
        or {
            "expected": 1,
            "success": 1,
            "failed": 0,
            "running": 0,
            "grace": 0,
            "failures": [],
        },
    )
    monkeypatch.setattr(
        composition,
        "collect_dag_run_summary",
        lambda _cursor, _config, dag_id, _detected_at: captured[
            "manifest_dag_ids"
        ].append(dag_id)
        or {
            "dag_id": dag_id,
            "latest_terminal_status": "SUCCESS",
            "latest_terminal_is_publishable": True,
        },
    )
    cursor = RecordingCursor(
        rows=[
            (1, 25, 25, 1000, 0, datetime(2026, 7, 2, 8, 55, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "PASS"
    assert captured == {
        "manifest_dag_ids": ["traffic_incident_bronze", "traffic_flow_bronze"],
        "cadence_dag_id": "traffic_incident_landing",
    }


@pytest.mark.parametrize(
    ("latest_status", "latest_is_publishable"),
    [("FAILED", False), ("SUCCESS", False)],
)
def test_traffic_report_does_not_borrow_older_publishable_run(
    monkeypatch,
    latest_status,
    latest_is_publishable,
):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setattr(
        composition,
        "collect_dag_run_summary",
        lambda *_args: {
            "dag_id": "traffic_incident_bronze",
            "success": 1,
            "failed": int(latest_status == "FAILED"),
            "running": 0,
            "last_success_at": "2026-07-04 08:45:00+00:00",
            "last_publishable_at": "2026-07-04 08:45:00+00:00",
            "latest_dag_run_id": "scheduled__2026-07-04T08:55:00+00:00",
            "latest_status": latest_status,
            "latest_is_publishable": latest_is_publishable,
            "latest_event_at": "2026-07-04 08:59:00+00:00",
            "latest_terminal_dag_run_id": "scheduled__2026-07-04T08:55:00+00:00",
            "latest_terminal_status": latest_status,
            "latest_terminal_is_publishable": latest_is_publishable,
            "latest_terminal_event_at": "2026-07-04 08:59:00+00:00",
        },
    )
    cursor = RecordingCursor(
        rows=[
            (1, 25, 25, 1000, 0, datetime(2026, 7, 4, 8, 59, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 4, 9, 0, tzinfo=timezone.utc),
    )

    assert result["publishability_ok"] is False
    assert result["status"] == "FAIL"
    assert (
        result["dag_runs"]["latest_dag_run_id"]
        == "scheduled__2026-07-04T08:55:00+00:00"
    )
    assert result["dag_runs"]["latest_status"] == latest_status
    assert result["dag_runs"]["latest_is_publishable"] is latest_is_publishable
    assert result["dag_runs"]["latest_event_at"] == "2026-07-04 08:59:00+00:00"


def test_traffic_report_fails_when_total_exceeds_requested_range(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (1, 1000, 1500, 1000, 0, datetime(2026, 7, 2, 8, 55, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "FAIL"
    assert result["traffic"]["coverage_ok"] is False


def test_traffic_report_warns_after_fifteen_minutes(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (1, 0, 0, 0, 1, datetime(2026, 7, 2, 8, 44, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "WARN"
    assert result["traffic"]["freshness_status"] == "WARN"
    assert result["traffic"]["zero_row_success_count"] == 1
    assert result["traffic"]["coverage_ok"] is True


def test_traffic_report_fails_after_thirty_minutes(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (1, 0, 0, 0, 1, datetime(2026, 7, 2, 8, 29, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "FAIL"
    assert result["traffic"]["freshness_status"] == "FAIL"


def test_traffic_report_has_logical_load_date_bound_without_claiming_partition_pruning(
    monkeypatch,
):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (1, 25, 25, 1000, 0, datetime(2026, 7, 2, 8, 55, tzinfo=timezone.utc)),
        ]
    )

    report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert "load_date >= '2026-06-30'" in cursor.statements[0]


def test_traffic_report_fails_safely_when_run_ledger_query_fails(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")

    def fail_summary(*_args):
        raise RuntimeError("credential=must-not-be-in-report")

    monkeypatch.setattr(
        composition, "collect_scheduled_run_summary", fail_summary
    )
    cursor = RecordingCursor(
        rows=[
            (1, 25, 25, 1000, 0, datetime(2026, 7, 2, 8, 55, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 13, 9, 0, tzinfo=report.KST),
    )
    message = report.format_traffic_discord_message(result)

    assert result["status"] == "FAIL"
    assert result["scheduled_runs"]["reason"] == "run_ledger_query_failed"
    assert result["scheduled_runs"]["error_type"] == "RuntimeError"
    assert "must-not-be-in-report" not in json.dumps(result, ensure_ascii=False)
    assert "run_ledger_query_failed" in message


def test_traffic_report_keeps_publishability_when_latest_manifest_is_running(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setattr(
        composition,
        "collect_dag_run_summary",
        lambda *_args: {
            "dag_id": "traffic_incident_bronze",
            "success": 3,
            "failed": 0,
            "running": 1,
            "last_success_at": "2026-07-15 04:20:00+00:00",
            "last_publishable_at": "2026-07-15 04:20:00+00:00",
            "latest_dag_run_id": "scheduled__2026-07-15T04:25:00+00:00",
            "latest_status": "STARTED",
            "latest_is_publishable": False,
            "latest_event_at": "2026-07-15 04:25:00+00:00",
            "latest_terminal_dag_run_id": "scheduled__2026-07-15T04:20:00+00:00",
            "latest_terminal_status": "SUCCESS",
            "latest_terminal_is_publishable": True,
            "latest_terminal_event_at": "2026-07-15 04:20:00+00:00",
        },
    )
    cursor = RecordingCursor(
        rows=[
            (1, 25, 25, 1000, 0, datetime(2026, 7, 15, 4, 25, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 15, 4, 30, tzinfo=timezone.utc),
    )

    assert result["status"] == "PASS"
    assert result["publishability_ok"] is True
    assert result["dag_runs"]["latest_status"] == "STARTED"
    assert result["dag_runs"]["latest_terminal_status"] == "SUCCESS"


def _pipeline_data_plane(status="PASS"):
    return {
        "report_name": "traffic_bronze_reliability",
        "detected_at": "2026-07-19T09:00:00+09:00",
        "catalog": "iceberg_dev",
        "schema": "ask_seoul",
        "lookback_hours": 24,
        "status": status,
        "traffic": {
            "status": status,
            "freshness_minutes": 3,
            "coverage_ok": status != "FAIL",
        },
        "dag_runs": {
            "dag_id": "traffic_incident_bronze",
            "publishability_ok": True,
        },
        "flow_dag_runs": {
            "dag_id": "traffic_flow_bronze",
            "publishability_ok": True,
        },
        "scheduled_runs": {
            "expected": 288,
            "success": 288,
            "failed": 0,
            "running": 0,
        },
        "materialization_backlog": {
            "count": 0,
            "oldest_age_minutes": None,
            "status": "PASS",
        },
        "publishability_ok": True,
        "blast_radius": [],
    }


def _pipeline_stages(status="PASS", stage_status="PASS", observed=1):
    return {
        "source": "marquez",
        "status": status,
        "stages": [
            {
                "key": "gold",
                "label": "Traffic Gold",
                "status": stage_status,
                "reason": None,
                "observed": observed,
                "age_minutes": 20,
                "duration_ms": {"p50": 80_000, "p95": 120_000},
            },
            {
                "key": "flow_silver",
                "label": "Flow Silver",
                "status": "PASS",
                "reason": None,
                "observed": 1,
                "age_minutes": 12,
                "duration_ms": {"p50": 20_000, "p95": 30_000},
            },
        ],
    }


def test_collect_traffic_data_plane_includes_incident_and_flow_manifests(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    observed_dag_ids = []
    monkeypatch.setattr(
        composition,
        "collect_dag_run_summary",
        lambda _cursor, _config, dag_id, _detected_at: observed_dag_ids.append(dag_id)
        or {
            "dag_id": dag_id,
            "latest_terminal_status": "SUCCESS",
            "latest_terminal_is_publishable": True,
        },
    )
    cursor = RecordingCursor(
        rows=[
            (1, 25, 25, 1000, 0, datetime(2026, 7, 19, 8, 59, tzinfo=timezone.utc)),
        ]
    )

    result = composition.collect_traffic_data_plane(
        cursor=cursor,
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=timezone.utc),
    )

    assert observed_dag_ids == ["traffic_incident_bronze", "traffic_flow_bronze"]
    assert result["dag_runs"]["dag_id"] == "traffic_incident_bronze"
    assert result["flow_dag_runs"]["dag_id"] == "traffic_flow_bronze"
    assert result["publishability_ok"] is True


def test_pipeline_data_failure_wins_over_control_plane_pass():
    result = composition.compose_traffic_pipeline_report(
        data_plane=_pipeline_data_plane("FAIL"),
        stages=_pipeline_stages(),
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["status"] == "FAIL"
    assert result["data_plane_status"] == "FAIL"


def test_pipeline_control_plane_unavailable_degrades_pass_to_warn():
    result = composition.compose_traffic_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages={"status": "UNKNOWN", "stages": []},
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["status"] == "WARN"
    assert result["control_plane_status"] == "UNKNOWN"


def test_pipeline_latest_stage_failure_is_fail():
    result = composition.compose_traffic_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages=_pipeline_stages(status="FAIL", stage_status="FAIL"),
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["status"] == "FAIL"


def test_pipeline_daily_contract_failure_is_visible_and_forces_red():
    audit = {
        "status": "FAIL",
        "selector": "ask_seoul_traffic_daily_assurance",
        "elapsed_seconds": 12.5,
        "selected_count": 212,
        "failure": "dbt_exit_1",
    }

    result = composition.compose_traffic_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages=_pipeline_stages(),
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
        contract_audit=audit,
    )

    assert result["status"] == "FAIL"
    assert result["contract_audit"] == audit


def test_pipeline_recovered_stage_failure_is_warn():
    stages = _pipeline_stages(status="WARN", stage_status="WARN")
    stages["stages"][0]["reason"] = "recovered_failure"

    result = composition.compose_traffic_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages=stages,
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["status"] == "WARN"


def test_asset_stage_is_not_compared_with_landing_expected_count():
    result = composition.compose_traffic_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages=_pipeline_stages(observed=1),
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["status"] == "PASS"
    assert result["scheduled_runs"]["expected"] == 288
    assert result["stages"][0]["observed"] == 1


def test_pipeline_bottleneck_is_largest_observed_stage_p95_not_pool_wait():
    result = composition.compose_traffic_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages=_pipeline_stages(),
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["bottleneck"] == {
        "key": "gold",
        "label": "Traffic Gold",
        "status": "PASS",
        "p95_ms": 120_000,
    }
    assert "pool_wait" not in json.dumps(result["bottleneck"])


def test_pipeline_trend_uses_current_day_and_six_prior_observations():
    history = [
        {"report_date": f"2026-07-{day:02d}", "status": "PASS"}
        for day in range(12, 19)
    ]

    result = composition.compose_traffic_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages=_pipeline_stages(),
        history=history,
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert len(result["trend"]) == 7
    assert result["trend"][0]["report_date"] == "2026-07-13"
    assert result["trend"][-1] == {"report_date": "2026-07-19", "status": "PASS"}
