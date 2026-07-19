import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest import reliability_report as report  # noqa: E402
from weather_ingest.reliability import report as composition  # noqa: E402
from weather_reliability_test_support import RecordingCursor, stub_report_dependencies  # noqa: E402


@pytest.fixture(autouse=True)
def stub_reliability_dependencies(monkeypatch):
    stub_report_dependencies(monkeypatch)


def test_build_weather_report_passes_for_fresh_complete_data(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_SCHEMA", "weather_traffic_bronze")
    cursor = RecordingCursor(
        rows=[
            (
                8,
                640,
                640,
                512000,
                80,
                80,
                8,
                "20260702",
                "0800",
                datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc),
            ),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "PASS"
    assert result["dag_runs"] == {
        "dag_id": "weather_vilage_fcst_bronze",
        "success": 2,
        "failed": 1,
        "running": 0,
        "expected_raw_objects": 160,
        "actual_raw_objects": 160,
        "last_success_at": "2026-07-02 08:20:00+00:00",
        "last_publishable_at": "2026-07-02 08:20:00+00:00",
        "latest_dag_run_id": "scheduled__2026-07-02T08:00:00+00:00",
        "latest_status": "SUCCESS",
        "latest_is_publishable": True,
        "latest_event_at": "2026-07-02 08:20:00+00:00",
        "publishability_ok": True,
    }
    assert result["weather"]["base_time_count"] == 8
    assert result["weather"]["grid_slot_count"] == 640
    assert result["weather"]["raw_object_count"] == 640
    assert result["weather"]["latest_base_time"] == "0800"
    assert result["blast_radius"] == [
        "iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst"
    ]
    assert "load_date >= '2026-06-30'" in cursor.statements[0]
    assert (
        "collected_at >= TIMESTAMP '2026-07-01 09:00:00.000000'" in cursor.statements[0]
    )
    assert "FROM by_base" in cursor.statements[0]
    assert result["weather"]["freshness_status"] == "PASS"
    assert result["publishability_ok"] is True
    assert result["late_publishability"] == {
        "status": "NOT_EVALUATED",
        "reason": "bounded late-repair contract is owned by ASAC-DBT #165",
    }


@pytest.mark.parametrize(
    ("latest_status", "latest_is_publishable"),
    [("FAILED", False), ("SUCCESS", False)],
)
def test_weather_report_does_not_borrow_older_publishable_run(
    monkeypatch,
    latest_status,
    latest_is_publishable,
):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setattr(
        composition,
        "collect_dag_run_summary",
        lambda *_args: {
            "dag_id": "weather_vilage_fcst_bronze",
            "success": 1,
            "failed": int(latest_status == "FAILED"),
            "running": 0,
            "expected_raw_objects": 160,
            "actual_raw_objects": 160,
            "last_success_at": "2026-07-04 07:20:00+00:00",
            "last_publishable_at": "2026-07-04 07:20:00+00:00",
            "latest_dag_run_id": "scheduled__2026-07-04T08:00:00+00:00",
            "latest_status": latest_status,
            "latest_is_publishable": latest_is_publishable,
            "latest_event_at": "2026-07-04 08:20:00+00:00",
        },
    )
    cursor = RecordingCursor(
        rows=[
            (
                8,
                640,
                640,
                512000,
                80,
                80,
                8,
                "20260704",
                "0800",
                datetime(2026, 7, 4, 8, 20, tzinfo=timezone.utc),
            ),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 4, 9, 0, tzinfo=timezone.utc),
    )

    assert result["publishability_ok"] is False
    assert result["status"] == "FAIL"
    assert (
        result["dag_runs"]["latest_dag_run_id"]
        == "scheduled__2026-07-04T08:00:00+00:00"
    )
    assert result["dag_runs"]["latest_status"] == latest_status
    assert result["dag_runs"]["latest_is_publishable"] is latest_is_publishable
    assert result["dag_runs"]["latest_event_at"] == "2026-07-04 08:20:00+00:00"


def test_weather_report_fails_when_grid_coverage_is_incomplete(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (
                8,
                639,
                639,
                511200,
                79,
                80,
                7,
                "20260702",
                "0800",
                datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc),
            ),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "FAIL"
    assert result["weather"]["coverage_ok"] is False


def test_weather_report_fails_when_24h_base_time_coverage_is_incomplete(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (
                6,
                480,
                480,
                426800,
                80,
                80,
                6,
                "20260706",
                "0800",
                datetime(2026, 7, 6, 8, 20, tzinfo=timezone.utc),
            ),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 6, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "FAIL"
    assert result["weather"]["coverage_ok"] is False
    assert result["weather"]["base_time_count"] == 6
    assert result["weather"]["expected_base_time_count"] == 8


def test_weather_report_warns_after_four_hours_but_before_six(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (
                8,
                640,
                640,
                512000,
                80,
                80,
                8,
                "20260702",
                "0800",
                datetime(2026, 7, 2, 4, 30, tzinfo=timezone.utc),
            ),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "WARN"
    assert result["weather"]["freshness_status"] == "WARN"


def test_weather_report_fails_after_six_hours(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (
                8,
                640,
                640,
                512000,
                80,
                80,
                8,
                "20260702",
                "0800",
                datetime(2026, 7, 2, 2, 59, tzinfo=timezone.utc),
            ),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "FAIL"
    assert result["weather"]["freshness_status"] == "FAIL"


def test_weather_summary_uses_load_date_and_collected_at_bounds(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (
                8,
                640,
                640,
                512000,
                80,
                80,
                8,
                "20260702",
                "0800",
                datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc),
            ),
        ]
    )

    report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert "load_date >= '2026-06-30'" in cursor.statements[0]
    assert (
        "collected_at >= TIMESTAMP '2026-07-01 09:00:00.000000'" in cursor.statements[0]
    )


def _pipeline_data_plane(status="PASS"):
    return {
        "report_name": "weather_bronze_reliability",
        "detected_at": "2026-07-19T09:00:00+09:00",
        "catalog": "iceberg_dev",
        "schema": "ask_seoul",
        "lookback_hours": 24,
        "status": status,
        "weather": {
            "status": status,
            "freshness_minutes": 20,
            "coverage_ok": status != "FAIL",
            "grid_slot_count": 640,
            "expected_grid_slot_count": 640,
        },
        "dag_runs": {
            "dag_id": "weather_vilage_fcst_bronze",
            "publishability_ok": True,
        },
        "publishability_ok": True,
        "blast_radius": [],
    }


def _pipeline_stages(status="PASS", transform_status="PASS"):
    return {
        "source": "marquez",
        "status": status,
        "stages": [
            {
                "key": "transform",
                "label": "Weather Silver/Gold",
                "status": transform_status,
                "reason": None,
                "observed": 8,
                "age_minutes": 30,
                "duration_ms": {"p50": 100_000, "p95": 180_000},
            },
            {
                "key": "maintenance",
                "label": "Iceberg maintenance",
                "status": "UNKNOWN",
                "reason": "unobserved",
                "required": False,
                "observed": 0,
                "age_minutes": None,
                "duration_ms": {"p50": None, "p95": None},
            },
        ],
    }


def test_pipeline_data_failure_wins_over_control_plane_pass():
    result = composition.compose_weather_pipeline_report(
        data_plane=_pipeline_data_plane("FAIL"),
        stages=_pipeline_stages(),
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["status"] == "FAIL"
    assert result["data_plane_status"] == "FAIL"


def test_pipeline_control_plane_unavailable_degrades_pass_to_warn():
    result = composition.compose_weather_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages={"status": "UNKNOWN", "stages": []},
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["status"] == "WARN"
    assert result["control_plane_status"] == "UNKNOWN"


def test_pipeline_transform_failure_is_fail():
    result = composition.compose_weather_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages=_pipeline_stages(status="FAIL", transform_status="FAIL"),
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["status"] == "FAIL"


def test_pipeline_recovered_transform_failure_is_warn():
    stages = _pipeline_stages(status="WARN", transform_status="WARN")
    stages["stages"][0]["reason"] = "recovered_failure"

    result = composition.compose_weather_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages=stages,
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["status"] == "WARN"


def test_unobserved_optional_maintenance_is_informational():
    result = composition.compose_weather_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages=_pipeline_stages(),
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["status"] == "PASS"
    assert result["stages"][-1]["status"] == "UNKNOWN"


def test_observed_maintenance_failure_is_fail():
    stages = _pipeline_stages(status="FAIL")
    stages["stages"][-1].update(
        status="FAIL", reason="latest_terminal_failed", observed=1
    )

    result = composition.compose_weather_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages=stages,
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["status"] == "FAIL"


def test_weather_pipeline_source_exposes_exact_coverage_percentage():
    data_plane = _pipeline_data_plane()
    data_plane["weather"]["grid_slot_count"] = 600

    result = composition.compose_weather_pipeline_report(
        data_plane=data_plane,
        stages=_pipeline_stages(),
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["source"]["coverage_percent"] == 93.75


def test_weather_pipeline_bottleneck_is_largest_observed_stage_p95():
    result = composition.compose_weather_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages=_pipeline_stages(),
        history=[],
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert result["bottleneck"] == {
        "key": "transform",
        "label": "Weather Silver/Gold",
        "status": "PASS",
        "p95_ms": 180_000,
    }


def test_weather_pipeline_trend_uses_current_day_and_six_prior_observations():
    history = [
        {"report_date": f"2026-07-{day:02d}", "status": "PASS"}
        for day in range(12, 19)
    ]

    result = composition.compose_weather_pipeline_report(
        data_plane=_pipeline_data_plane(),
        stages=_pipeline_stages(),
        history=history,
        detected_at=datetime(2026, 7, 19, 9, 0, tzinfo=report.KST),
    )

    assert len(result["trend"]) == 7
    assert result["trend"][0]["report_date"] == "2026-07-13"
    assert result["trend"][-1] == {"report_date": "2026-07-19", "status": "PASS"}
