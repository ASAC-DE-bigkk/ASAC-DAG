import sys
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest import reliability_report as report  # noqa: E402
from weather_ingest.reliability import trino_repository as repository  # noqa: E402
from weather_reliability_test_support import RecordingCursor  # noqa: E402


ORIGINAL_COLLECT_DAG_RUN_SUMMARY = repository.collect_dag_run_summary


def test_weather_product_profile_measures_latest_issue_categories_places_and_horizon(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(rows=[(4, 80, 72)])

    result = repository.collect_weather_product_profile(
        cursor,
        report.report_config(),
        datetime(2026, 7, 30, 9, 0, tzinfo=timezone.utc),
    )

    assert result == {
        "core_category_count": 4,
        "mapped_place_count": 80,
        "forecast_horizon_hours": 72,
    }
    statement = cursor.statements[0]
    assert "category IN ('TMP', 'POP', 'SKY', 'PTY')" in statement
    assert "count(DISTINCT place_id)" in statement
    assert "date_diff(" in statement and "'hour'" in statement


def test_weather_dag_run_summary_uses_manifest_table(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_SCHEMA", "weather_traffic_bronze")
    cursor = RecordingCursor(
        rows=[
            (
                2,
                1,
                0,
                160,
                120,
                datetime(2026, 7, 4, 8, 0, tzinfo=timezone.utc),
                datetime(2026, 7, 4, 8, 0, tzinfo=timezone.utc),
                "scheduled__2026-07-04T08:00:00+00:00",
                "SUCCESS",
                True,
                datetime(2026, 7, 4, 8, 0, tzinfo=timezone.utc),
            )
        ]
    )
    config = report.report_config()

    result = ORIGINAL_COLLECT_DAG_RUN_SUMMARY(
        cursor,
        config,
        "weather_vilage_fcst_bronze",
        datetime(2026, 7, 4, 9, 0, tzinfo=timezone.utc),
    )

    assert result == {
        "dag_id": "weather_vilage_fcst_bronze",
        "success": 2,
        "failed": 1,
        "running": 0,
        "expected_raw_objects": 160,
        "actual_raw_objects": 120,
        "last_success_at": "2026-07-04 08:00:00+00:00",
        "last_publishable_at": "2026-07-04 08:00:00+00:00",
        "latest_dag_run_id": "scheduled__2026-07-04T08:00:00+00:00",
        "latest_status": "SUCCESS",
        "latest_is_publishable": True,
        "latest_event_at": "2026-07-04 08:00:00+00:00",
    }
    statement = cursor.statements[0]
    assert "bronze_collection_run_manifest" in statement
    assert "dag_id = 'weather_vilage_fcst_bronze'" in statement
    assert (
        "max_by(dag_run_id, ROW(latest_event_at, dag_run_id)) AS latest_dag_run_id"
        in statement
    )
    assert (
        "max_by(latest_status, ROW(latest_event_at, dag_run_id)) AS latest_status"
        in statement
    )
    assert (
        "max_by(latest_is_publishable, ROW(latest_event_at, dag_run_id)) "
        "AS latest_is_publishable"
    ) in statement
