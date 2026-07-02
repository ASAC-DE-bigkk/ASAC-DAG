import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _shared.bronze_run_manifest import (  # noqa: E402
    STATUS_SUCCESS,
    record_bronze_run_event,
)


class RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql):
        self.statements.append(" ".join(sql.split()))


def test_success_manifest_event_marks_run_publishable():
    cursor = RecordingCursor()

    qualified_table = record_bronze_run_event(
        cursor,
        "iceberg_dev",
        "dev_masondev1024",
        source_id="kma_vilage_fcst",
        dag_id="weather_vilage_fcst_bronze",
        dag_run_id="manual__weather_success",
        status=STATUS_SUCCESS,
        is_publishable=True,
        expected_rows=798,
        actual_rows=798,
        expected_raw_objects=1,
        actual_raw_objects=1,
    )

    assert qualified_table == "iceberg_dev.dev_masondev1024.bronze_collection_run_manifest"
    assert len(cursor.statements) == 4
    assert "CREATE TABLE IF NOT EXISTS iceberg_dev.dev_masondev1024.bronze_collection_run_manifest" in cursor.statements[1]
    assert "status = 'SUCCESS'" in cursor.statements[2]
    assert "'weather_vilage_fcst_bronze'" in cursor.statements[3]
    assert "true" in cursor.statements[3]
    assert "798, 798, 1, 1" in cursor.statements[3]
