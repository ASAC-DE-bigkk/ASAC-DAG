"""Trino/Iceberg sink for validated collection-slot receipt rows."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Callable

from common.collection_slots.contract import canonical_json
from common.collection_slots.materializer import CollectionSlotSink, MaterializationError


EXPECTED_TABLE = "bronze_collection_expected_slot"
EVENT_TABLE = "bronze_collection_slot_event"
DEFAULT_SCHEMA = "weather_traffic_bronze"
MAX_MERGE_ROWS = 200

EXPECTED_COLUMNS = (
    "contract_version",
    "expected_slot_id",
    "domain",
    "collection_contract_id",
    "source_id",
    "collection_slot_at",
    "grain_key",
    "grain_json",
    "schedule_version",
    "scheduled_at",
    "deadline_at",
    "is_scheduled",
    "recovery_boundary_type",
    "recovery_boundary",
    "declared_at",
    "declared_by",
)
EVENT_COLUMNS = (
    "event_id",
    "expected_slot_id",
    "event_type",
    "collection_state",
    "recovery_state",
    "recovery_class",
    "gap_reason_code",
    "dag_id",
    "dag_run_id",
    "task_id",
    "raw_manifest_key",
    "raw_object_count",
    "row_count",
    "source_result_code",
    "recovery_run_id",
    "recovered_at",
    "event_at",
)
_TIMESTAMP_COLUMNS = frozenset(
    {
        "collection_slot_at",
        "scheduled_at",
        "deadline_at",
        "declared_at",
        "recovered_at",
        "event_at",
    }
)
_INTEGER_COLUMNS = frozenset({"raw_object_count", "row_count"})
_BOOLEAN_COLUMNS = frozenset({"is_scheduled"})


def _identifier(value: str) -> str:
    if not value or not value.replace("_", "a").isalnum() or value[0].isdigit():
        raise MaterializationError(f"unsafe Iceberg identifier: {value!r}")
    return value


def _sql_string(value: object) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _sql_timestamp(value: object) -> str:
    if value is None:
        return "NULL"
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise MaterializationError("timestamp value must include timezone")
    utc = parsed.astimezone(timezone.utc)
    return "TIMESTAMP " + _sql_string(utc.strftime("%Y-%m-%d %H:%M:%S.%f"))


def _sql_value(column: str, value: object) -> str:
    if column in _TIMESTAMP_COLUMNS:
        return _sql_timestamp(value)
    if value is None:
        return "NULL"
    if column in _BOOLEAN_COLUMNS:
        if not isinstance(value, bool):
            raise MaterializationError(f"{column} must be boolean")
        return "true" if value else "false"
    if column in _INTEGER_COLUMNS:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise MaterializationError(f"{column} must be a non-negative integer")
        return str(value)
    return _sql_string(value)


def _normalize(value: object, column: str) -> object:
    if value is None:
        return None
    if column in _TIMESTAMP_COLUMNS and isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    return value


class TrinoCollectionSlotSink(CollectionSlotSink):
    """Idempotent Iceberg sink using Trino MERGE as the commit boundary."""

    def __init__(
        self,
        *,
        cursor_factory: Callable[[], tuple[Any, str, str]] | None = None,
        schema: str | None = None,
    ) -> None:
        self._cursor_factory = cursor_factory or self._default_cursor
        self._schema = _identifier(schema or os.environ.get("ASK_SEOUL_SCHEMA", DEFAULT_SCHEMA))

    def write_expected(self, rows: list[dict[str, object]]) -> int:
        return self._write_batched(rows, table=EXPECTED_TABLE, columns=EXPECTED_COLUMNS)

    def write_events(self, rows: list[dict[str, object]]) -> int:
        return self._write_batched(rows, table=EVENT_TABLE, columns=EVENT_COLUMNS)

    def ensure_tables(self) -> None:
        cursor, catalog, schema = self._cursor_factory()
        try:
            catalog = _identifier(catalog)
            schema = _identifier(schema or self._schema)
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            cursor.execute(self._create_expected_sql(catalog, schema))
            cursor.execute(self._create_event_sql(catalog, schema))
        finally:
            connection = getattr(cursor, "connection", None)
            if connection is not None:
                connection.close()

    def _write(
        self,
        rows: list[dict[str, object]],
        *,
        table: str,
        columns: Sequence[str],
    ) -> int:
        self.ensure_tables()
        if not rows:
            return 0
        key = columns[1] if table == EXPECTED_TABLE else columns[0]
        cursor, catalog, schema = self._cursor_factory()
        qualified = f"{_identifier(catalog)}.{_identifier(schema or self._schema)}.{_identifier(table)}"
        try:
            existing = self._existing(cursor, qualified, rows, columns, key)
            new_rows: list[dict[str, object]] = []
            for row in rows:
                identity = str(row[key])
                prior = existing.get(identity)
                if prior is None:
                    new_rows.append(row)
                    continue
                if canonical_json(row) != canonical_json(prior):
                    raise MaterializationError(f"{key} conflicting Iceberg row: {identity}")
            if not new_rows:
                return 0
            values = ",\n".join(
                "(" + ", ".join(_sql_value(column, row.get(column)) for column in columns) + ")"
                for row in new_rows
            )
            source_columns = ", ".join(columns)
            cursor.execute(
                f"MERGE INTO {qualified} AS target "
                f"USING (VALUES {values}) AS source ({source_columns}) "
                f"ON target.{key} = source.{key} "
                "WHEN NOT MATCHED THEN INSERT ("
                f"{source_columns}) VALUES ({', '.join('source.' + column for column in columns)})"
            )
            return len(new_rows)
        finally:
            connection = getattr(cursor, "connection", None)
            if connection is not None:
                connection.close()

    def _write_batched(
        self,
        rows: list[dict[str, object]],
        *,
        table: str,
        columns: Sequence[str],
    ) -> int:
        total = 0
        for start in range(0, len(rows), MAX_MERGE_ROWS):
            total += self._write(
                rows[start : start + MAX_MERGE_ROWS],
                table=table,
                columns=columns,
            )
        return total

    def _existing(
        self,
        cursor: Any,
        qualified: str,
        rows: list[dict[str, object]],
        columns: Sequence[str],
        key: str,
    ) -> dict[str, dict[str, object]]:
        ids = sorted({str(row[key]) for row in rows})
        values = ", ".join(_sql_string(value) for value in ids)
        cursor.execute(
            f"SELECT {', '.join(columns)} FROM {qualified} WHERE {key} IN ({values})"
        )
        result: dict[str, dict[str, object]] = {}
        for raw_row in cursor.fetchall():
            record = {
                column: _normalize(value, column)
                for column, value in zip(columns, raw_row, strict=True)
            }
            result[str(record[key])] = record
        return result

    @staticmethod
    def _create_expected_sql(catalog: str, schema: str) -> str:
        qualified = f"{catalog}.{schema}.{EXPECTED_TABLE}"
        return f"""CREATE TABLE IF NOT EXISTS {qualified} (
            contract_version VARCHAR,
            expected_slot_id VARCHAR,
            domain VARCHAR,
            collection_contract_id VARCHAR,
            source_id VARCHAR,
            collection_slot_at TIMESTAMP(6),
            grain_key VARCHAR,
            grain_json VARCHAR,
            schedule_version VARCHAR,
            scheduled_at TIMESTAMP(6),
            deadline_at TIMESTAMP(6),
            is_scheduled BOOLEAN,
            recovery_boundary_type VARCHAR,
            recovery_boundary VARCHAR,
            declared_at TIMESTAMP(6),
            declared_by VARCHAR
        ) WITH (format = 'PARQUET', partitioning = ARRAY['domain', 'source_id'])"""

    @staticmethod
    def _create_event_sql(catalog: str, schema: str) -> str:
        qualified = f"{catalog}.{schema}.{EVENT_TABLE}"
        return f"""CREATE TABLE IF NOT EXISTS {qualified} (
            event_id VARCHAR,
            expected_slot_id VARCHAR,
            event_type VARCHAR,
            collection_state VARCHAR,
            recovery_state VARCHAR,
            recovery_class VARCHAR,
            gap_reason_code VARCHAR,
            dag_id VARCHAR,
            dag_run_id VARCHAR,
            task_id VARCHAR,
            raw_manifest_key VARCHAR,
            raw_object_count BIGINT,
            row_count BIGINT,
            source_result_code VARCHAR,
            recovery_run_id VARCHAR,
            recovered_at TIMESTAMP(6),
            event_at TIMESTAMP(6)
        ) WITH (format = 'PARQUET', partitioning = ARRAY['collection_state'])"""

    @staticmethod
    def _default_cursor() -> tuple[Any, str, str]:
        import trino.dbapi

        catalog = os.environ.get("TRINO_ICEBERG_CATALOG", "")
        schema = os.environ.get("ASK_SEOUL_SCHEMA", DEFAULT_SCHEMA)
        if not catalog:
            raise MaterializationError("TRINO_ICEBERG_CATALOG is required")
        connection = trino.dbapi.connect(
            host=os.environ.get("TRINO_HOST", "trino"),
            port=int(os.environ.get("TRINO_PORT", "8080")),
            user=os.environ.get("TRINO_USER", "airflow"),
            catalog=catalog,
            schema=schema,
            http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
        )
        return connection.cursor(), catalog, schema


__all__ = [
    "EVENT_TABLE",
    "EXPECTED_TABLE",
    "TrinoCollectionSlotSink",
]
