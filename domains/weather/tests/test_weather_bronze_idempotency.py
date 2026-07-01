import sys
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.bronze import insert_kma_bronze_rows  # noqa: E402


class RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql):
        self.statements.append(" ".join(sql.split()))


def test_kma_insert_replaces_same_retry_scope_before_append():
    cursor = RecordingCursor()

    inserted = insert_kma_bronze_rows(
        cursor=cursor,
        qualified_table="iceberg_dev.dev_masondev1024.bronze_kma_vilage_fcst",
        rows=[
            {
                "baseDate": "20260701",
                "baseTime": "0800",
                "nx": "60",
                "ny": "127",
                "category": "TMP",
                "fcstDate": "20260701",
                "fcstTime": "0900",
                "fcstValue": "25",
            }
        ],
        metadata={"result_code": "00", "result_msg": "NORMAL_SERVICE", "total_count": 1, "row_count": 1},
        request_id="request-1",
        place_id="seoul-test-grid",
        base_date="20260701",
        base_time="0800",
        nx=60,
        ny=127,
        raw_object_key="bronze/weather/kma/request-1.json",
        raw_hash="abc123",
        http_status=200,
        collected_at=datetime(2026, 7, 1, 0, 20, tzinfo=timezone.utc),
        dag_run_id="scheduled__2026-07-01T08:20:00+09:00",
    )

    assert inserted == 1
    assert len(cursor.statements) == 2
    delete_sql, insert_sql = cursor.statements
    assert delete_sql.startswith("DELETE FROM iceberg_dev.dev_masondev1024.bronze_kma_vilage_fcst WHERE")
    assert "source_id = 'kma_vilage_fcst'" in delete_sql
    assert "dag_run_id = 'scheduled__2026-07-01T08:20:00+09:00'" in delete_sql
    assert "base_date = '20260701'" in delete_sql
    assert "base_time = '0800'" in delete_sql
    assert "nx = 60" in delete_sql
    assert "ny = 127" in delete_sql
    assert insert_sql.startswith("INSERT INTO iceberg_dev.dev_masondev1024.bronze_kma_vilage_fcst")
