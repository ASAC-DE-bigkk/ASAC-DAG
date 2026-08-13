from __future__ import annotations

from dataclasses import dataclass

import pytest

from common.collection_slots.iceberg_sink import (
    EVENT_COLUMNS,
    EVENT_TABLE,
    EXPECTED_COLUMNS,
    EXPECTED_TABLE,
    TrinoCollectionSlotSink,
)
from common.collection_slots.materializer import MaterializationError


def _row() -> dict[str, object]:
    return {
        "contract_version": "v1",
        "expected_slot_id": "a" * 64,
        "domain": "traffic",
        "collection_contract_id": "traffic.incident.v1",
        "source_id": "seoul_traffic_incident",
        "collection_slot_at": "2026-08-08T00:00:00+00:00",
        "grain_key": "b" * 64,
        "grain_json": '{"source_id":"seoul_traffic_incident"}',
        "schedule_version": "traffic-v1",
        "scheduled_at": "2026-08-08T00:00:00+00:00",
        "deadline_at": "2026-08-08T00:15:00+00:00",
        "is_scheduled": True,
        "recovery_boundary_type": "raw_retention",
        "recovery_boundary": "r2-control",
        "declared_at": "2026-08-08T00:00:01+00:00",
        "declared_by": "traffic_incident_landing",
    }


def _event_row() -> dict[str, object]:
    return {
        "event_id": "c" * 64,
        "expected_slot_id": "a" * 64,
        "event_type": "terminal",
        "collection_state": "collection_failed",
        "recovery_state": "recovered",
        "recovery_class": "raw_replay",
        "gap_reason_code": "bronze_load_failed",
        "dag_id": "traffic_incident_bronze",
        "dag_run_id": "run-1",
        "task_id": None,
        "raw_manifest_key": None,
        "raw_object_count": None,
        "row_count": None,
        "source_result_code": None,
        "recovery_run_id": "replay-run",
        "recovered_at": "2026-08-08T00:20:00+00:00",
        "recovery_evidence_code": "raw_manifest_verified",
        "event_at": "2026-08-08T00:06:00+00:00",
    }


@dataclass
class FakeConnection:
    closed: bool = False

    def close(self):
        self.closed = True


class FakeCursor:
    def __init__(self, existing_rows=()):
        self.connection = FakeConnection()
        self.existing_rows = list(existing_rows)
        self.sql: list[str] = []

    def execute(self, sql: str):
        self.sql.append(sql)

    def fetchall(self):
        if self.sql[-1].lstrip().upper().startswith("SELECT"):
            return self.existing_rows
        return []


def test_ddl_is_additive_and_partitioned_by_domain_source():
    sql = TrinoCollectionSlotSink._create_expected_sql("iceberg_dev", "weather_traffic_bronze")
    assert f"CREATE TABLE IF NOT EXISTS iceberg_dev.weather_traffic_bronze.{EXPECTED_TABLE}" in sql
    assert "expected_slot_id VARCHAR" in sql
    assert "partitioning = ARRAY['domain', 'source_id']" in sql


def test_event_ddl_merge_columns_and_ensure_tables_include_recovery_evidence_migration():
    sql = TrinoCollectionSlotSink._create_event_sql("iceberg_dev", "weather_traffic_bronze")
    assert f"CREATE TABLE IF NOT EXISTS iceberg_dev.weather_traffic_bronze.{EVENT_TABLE}" in sql
    assert "recovery_evidence_code VARCHAR" in sql
    assert "recovery_evidence_code" in EVENT_COLUMNS

    created: list[FakeCursor] = []

    def factory():
        cursor = FakeCursor()
        created.append(cursor)
        return cursor, "iceberg_dev", "weather_traffic_bronze"

    TrinoCollectionSlotSink(cursor_factory=factory).ensure_tables()

    statements = created[0].sql
    event_create_index = next(
        index for index, statement in enumerate(statements) if f".{EVENT_TABLE}" in statement
    )
    migration_index = next(
        index
        for index, statement in enumerate(statements)
        if statement
        == "ALTER TABLE iceberg_dev.weather_traffic_bronze."
        f"{EVENT_TABLE} ADD COLUMN IF NOT EXISTS recovery_evidence_code VARCHAR"
    )
    assert migration_index > event_create_index

    created.clear()
    written = TrinoCollectionSlotSink(cursor_factory=factory).write_events([_event_row()])

    assert written == 1
    merge_sql = next(sql for cursor in created for sql in cursor.sql if sql.startswith("MERGE"))
    assert "recovery_evidence_code" in merge_sql


def test_sink_merges_new_rows_without_interpolating_untrusted_identifiers():
    created: list[FakeCursor] = []

    def factory():
        cursor = FakeCursor()
        created.append(cursor)
        return cursor, "iceberg_dev", "weather_traffic_bronze"

    written = TrinoCollectionSlotSink(cursor_factory=factory).write_expected([_row()])

    assert written == 1
    merge_sql = next(sql for cursor in created for sql in cursor.sql if sql.startswith("MERGE"))
    assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
    assert "iceberg_dev.weather_traffic_bronze" in merge_sql


def test_sink_rejects_existing_conflict_before_merge():
    row = _row()
    existing = list(row[column] for column in EXPECTED_COLUMNS)
    existing = tuple(existing[:5] + [existing[5].replace("+00:00", "")] + existing[6:])

    def factory():
        return FakeCursor([existing]), "iceberg_dev", "weather_traffic_bronze"

    with pytest.raises(MaterializationError, match="conflicting Iceberg row"):
        TrinoCollectionSlotSink(cursor_factory=factory).write_expected([row])
