import sys
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest import reliability_report as report  # noqa: E402
from traffic_ingest.reliability import trino_repository as repository  # noqa: E402
from traffic_reliability_test_support import RecordingCursor  # noqa: E402


ORIGINAL_COLLECT_DAG_RUN_SUMMARY = repository.collect_dag_run_summary


def test_traffic_dag_run_summary_uses_manifest_table(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_SCHEMA", "weather_traffic_bronze")
    cursor = RecordingCursor(
        rows=[
            (
                3,
                0,
                1,
                datetime(2026, 7, 4, 8, 0, tzinfo=timezone.utc),
                datetime(2026, 7, 4, 8, 0, tzinfo=timezone.utc),
                "scheduled__2026-07-04T08:00:00+00:00",
                "SUCCESS",
                True,
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
        "traffic_incident_bronze",
        datetime(2026, 7, 4, 9, 0, tzinfo=timezone.utc),
    )

    assert result == {
        "dag_id": "traffic_incident_bronze",
        "success": 3,
        "failed": 0,
        "running": 1,
        "last_success_at": "2026-07-04 08:00:00+00:00",
        "last_publishable_at": "2026-07-04 08:00:00+00:00",
        "latest_dag_run_id": "scheduled__2026-07-04T08:00:00+00:00",
        "latest_status": "SUCCESS",
        "latest_is_publishable": True,
        "latest_event_at": "2026-07-04 08:00:00+00:00",
        "latest_terminal_dag_run_id": "scheduled__2026-07-04T08:00:00+00:00",
        "latest_terminal_status": "SUCCESS",
        "latest_terminal_is_publishable": True,
        "latest_terminal_event_at": "2026-07-04 08:00:00+00:00",
    }
    statement = cursor.statements[0]
    assert "bronze_collection_run_manifest" in statement
    assert "dag_id = 'traffic_incident_bronze'" in statement
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
    assert "terminal AS" in statement
