import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.acc_info import SOURCE_ID  # noqa: E402
from traffic_ingest.bronze import (  # noqa: E402
    insert_seoul_traffic_bronze_rows,
    replace_seoul_traffic_bronze_snapshots,
)
from traffic_ingest.errors import TrafficSourceSchemaError  # noqa: E402


class RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql):
        self.statements.append(" ".join(sql.split()))


def test_traffic_insert_replaces_same_retry_scope_before_append():
    cursor = RecordingCursor()

    inserted = insert_seoul_traffic_bronze_rows(
        cursor=cursor,
        qualified_table="iceberg_dev.weather_traffic_bronze.bronze_seoul_traffic_incident",
        rows=[
            {
                "acc_id": "ACC-1",
                "occr_date": "20260701",
                "occr_time": "0815",
                "exp_clr_date": "20260701",
                "exp_clr_time": "0915",
                "acc_type": "A",
                "acc_dtype": "D",
                "link_id": "L1",
                "grs80tm_x": "198000.1",
                "grs80tm_y": "451000.2",
                "acc_info": "test incident",
                "acc_road_code": "ROAD-1",
            }
        ],
        metadata={
            "result_code": "INFO-000",
            "result_msg": "OK",
            "list_total_count": 1,
            "row_count": 1,
        },
        request_id="request-1",
        start_index=1,
        end_index=1000,
        raw_object_key="raw/traffic/accinfo/request-1.xml",
        raw_hash="abc123",
        http_status=200,
        collected_at=datetime(2026, 7, 1, 0, 15, tzinfo=timezone.utc),
        dag_run_id="scheduled__2026-07-01T09:15:00+09:00",
    )

    assert inserted == 1
    assert len(cursor.statements) == 4
    audit_delete_sql, audit_insert_sql, delete_sql, insert_sql = cursor.statements
    assert audit_delete_sql.startswith(
        "DELETE FROM iceberg_dev.weather_traffic_bronze.bronze_seoul_traffic_incident_request_audit WHERE"
    )
    assert audit_insert_sql.startswith(
        "INSERT INTO iceberg_dev.weather_traffic_bronze.bronze_seoul_traffic_incident_request_audit"
    )
    assert delete_sql.startswith(
        "DELETE FROM iceberg_dev.weather_traffic_bronze.bronze_seoul_traffic_incident WHERE"
    )
    assert f"source_id = '{SOURCE_ID}'" in delete_sql
    assert "dag_run_id = 'scheduled__2026-07-01T09:15:00+09:00'" in delete_sql
    assert "start_index = 1" in delete_sql
    assert "end_index = 1000" in delete_sql
    assert insert_sql.startswith(
        "INSERT INTO iceberg_dev.weather_traffic_bronze.bronze_seoul_traffic_incident"
    )


def test_set_based_replacement_uses_four_statements_for_multiple_receipts():
    cursor = RecordingCursor()
    common_page = {
        "rows": [
            {
                "acc_id": "ACC-1",
                "occr_date": "20260701",
                "occr_time": "0815",
                "exp_clr_date": "20260701",
                "exp_clr_time": "0915",
                "acc_type": "A",
                "acc_dtype": "D",
                "link_id": "L1",
                "grs80tm_x": "198000.1",
                "grs80tm_y": "451000.2",
                "acc_info": "test incident",
                "acc_road_code": "ROAD-1",
            }
        ],
        "metadata": {
            "result_code": "INFO-000",
            "result_msg": "OK",
            "list_total_count": 1,
            "row_count": 1,
        },
        "request_id": "request-1",
        "start_index": 1,
        "end_index": 1000,
        "raw_object_key": "raw/traffic/accinfo/request-1.xml",
        "raw_hash": "abc123",
        "http_status": 200,
        "collected_at": datetime(2026, 7, 1, 0, 15, tzinfo=timezone.utc),
    }

    counts = replace_seoul_traffic_bronze_snapshots(
        cursor=cursor,
        qualified_table="iceberg_dev.weather_traffic_bronze.bronze_seoul_traffic_incident",
        snapshots=[
            {"dag_run_id": "scheduled__first", "pages": [common_page]},
            {
                "dag_run_id": "scheduled__second",
                "pages": [
                    {
                        **common_page,
                        "request_id": "request-2",
                        "raw_object_key": "raw/traffic/accinfo/request-2.xml",
                    }
                ],
            },
        ],
    )

    assert counts == {"scheduled__first": 1, "scheduled__second": 1}
    assert len(cursor.statements) == 4
    audit_delete_sql, audit_insert_sql, bronze_delete_sql, bronze_insert_sql = (
        cursor.statements
    )
    assert "dag_run_id IN ('scheduled__first', 'scheduled__second')" in audit_delete_sql
    assert audit_insert_sql.count("scheduled__first") == 1
    assert audit_insert_sql.count("scheduled__second") == 1
    assert "dag_run_id IN ('scheduled__first', 'scheduled__second')" in bronze_delete_sql
    assert bronze_insert_sql.count("scheduled__first") == 1
    assert bronze_insert_sql.count("scheduled__second") == 1


def test_set_based_replacement_validates_all_pages_before_first_statement():
    cursor = RecordingCursor()

    with pytest.raises(TrafficSourceSchemaError, match="collected_at"):
        replace_seoul_traffic_bronze_snapshots(
            cursor=cursor,
            qualified_table="iceberg_dev.ask_seoul.bronze_seoul_traffic_incident",
            snapshots=[
                {
                    "dag_run_id": "scheduled__invalid",
                    "pages": [
                        {
                            "rows": [],
                            "metadata": {
                                "result_code": "INFO-000",
                                "list_total_count": 0,
                            },
                            "request_id": "request-invalid",
                            "start_index": 1,
                            "end_index": 1,
                            "raw_object_key": "raw/invalid.xml",
                            "raw_hash": "abc123",
                            "http_status": 200,
                            "collected_at": "not-a-datetime",
                        }
                    ],
                }
            ],
        )

    assert cursor.statements == []
