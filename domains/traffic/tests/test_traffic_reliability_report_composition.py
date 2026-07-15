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
    assert result["late_publishability"] == {
        "status": "NOT_EVALUATED",
        "reason": "bounded late-repair contract is owned by ASAC-DBT #117",
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
