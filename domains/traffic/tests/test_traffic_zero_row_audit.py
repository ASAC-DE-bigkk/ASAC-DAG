import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest import bronze  # noqa: E402
from traffic_ingest.acc_info import SOURCE_ID  # noqa: E402


class RecordingCursor:
    def __init__(self, rows=None):
        self.statements = []
        self.rows = list(rows or [])

    def execute(self, sql):
        self.statements.append(" ".join(sql.split()))

    def fetchone(self):
        return self.rows.pop(0)


def test_create_bronze_table_also_creates_request_audit_table():
    cursor = RecordingCursor()

    qualified_table = bronze.create_seoul_traffic_bronze_table(
        cursor=cursor,
        catalog="iceberg_dev",
        schema="dev_masondev1024",
    )

    assert qualified_table == "iceberg_dev.dev_masondev1024.bronze_seoul_traffic_incident"
    assert any(
        "CREATE TABLE IF NOT EXISTS iceberg_dev.dev_masondev1024.bronze_seoul_traffic_incident_request_audit"
        in statement
        for statement in cursor.statements
    )


def test_zero_row_insert_writes_request_audit_without_incident_rows():
    cursor = RecordingCursor()

    inserted = bronze.insert_seoul_traffic_bronze_rows(
        cursor=cursor,
        qualified_table="iceberg_dev.dev_masondev1024.bronze_seoul_traffic_incident",
        rows=[],
        metadata={"result_code": "INFO-000", "result_msg": "OK", "list_total_count": 0, "row_count": 0},
        request_id="request-1",
        start_index=1,
        end_index=1000,
        raw_object_key="raw/traffic/accinfo/request-1.xml",
        raw_hash="abc123",
        http_status=200,
        collected_at=datetime(2026, 7, 1, 0, 15, tzinfo=timezone.utc),
        dag_run_id="scheduled__2026-07-01T09:15:00+09:00",
    )

    assert inserted == 0
    assert len(cursor.statements) == 3
    audit_delete_sql, audit_sql, bronze_delete_sql = cursor.statements
    assert audit_delete_sql.startswith(
        "DELETE FROM iceberg_dev.dev_masondev1024.bronze_seoul_traffic_incident_request_audit WHERE"
    )
    assert audit_sql.startswith(
        "INSERT INTO iceberg_dev.dev_masondev1024.bronze_seoul_traffic_incident_request_audit"
    )
    assert bronze_delete_sql.startswith(
        "DELETE FROM iceberg_dev.dev_masondev1024.bronze_seoul_traffic_incident WHERE"
    )
    assert f"'{SOURCE_ID}'" in audit_sql
    assert "'raw/traffic/accinfo/request-1.xml'" in audit_sql
    assert "'scheduled__2026-07-01T09:15:00+09:00'" in audit_sql


def test_reported_total_with_no_parsed_rows_fails():
    cursor = RecordingCursor()

    with pytest.raises(RuntimeError, match="list_total_count=3"):
        bronze.insert_seoul_traffic_bronze_rows(
            cursor=cursor,
            qualified_table="iceberg_dev.dev_masondev1024.bronze_seoul_traffic_incident",
            rows=[],
            metadata={"result_code": "INFO-000", "result_msg": "OK", "list_total_count": 3, "row_count": 0},
            request_id="request-1",
            start_index=1,
            end_index=1000,
            raw_object_key="raw/traffic/accinfo/request-1.xml",
            raw_hash="abc123",
            http_status=200,
            collected_at=datetime(2026, 7, 1, 0, 15, tzinfo=timezone.utc),
            dag_run_id="scheduled__2026-07-01T09:15:00+09:00",
        )


def test_verify_zero_rows_requires_request_audit(monkeypatch):
    cursor = RecordingCursor(rows=[(0, 0, None), (1,)])
    monkeypatch.setattr(
        bronze,
        "trino_cursor",
        lambda: (cursor, "iceberg_dev", "dev_masondev1024"),
    )

    row_count = bronze.verify_seoul_traffic_bronze_runtime(
        raw_object_key="raw/traffic/accinfo/request-1.xml",
        dag_run_id="scheduled__2026-07-01T09:15:00+09:00",
        expected_rows=0,
    )

    assert row_count == 0
    assert any(
        "FROM iceberg_dev.dev_masondev1024.bronze_seoul_traffic_incident_request_audit" in statement
        for statement in cursor.statements
    )
