"""Traffic-owned Bronze run-manifest Module."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Protocol


MANIFEST_TABLE = "bronze_collection_run_manifest"
SOURCE_ID = "seoul_traffic_incident"
STATUS_STARTED = "STARTED"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_COALESCED = "COALESCED"


class RunNotPublishableError(RuntimeError):
    """The requested Traffic Bronze snapshot is not publishable."""


class Cursor(Protocol):
    def execute(self, statement: str) -> None: ...

    def fetchone(self): ...


CursorFactory = Callable[[], tuple[Cursor, str, str]]


@dataclass(frozen=True)
class TrafficRun:
    dag_id: str
    run_id: str


class TrafficRunManifest:
    def __init__(self, cursor_factory: CursorFactory) -> None:
        self._cursor_factory = cursor_factory

    def start(
        self,
        run: TrafficRun,
        *,
        expected_raw_objects: int | None = None,
    ) -> str:
        return self._record(
            run,
            status=STATUS_STARTED,
            is_publishable=False,
            expected_rows=None,
            actual_rows=None,
            expected_raw_objects=expected_raw_objects,
            actual_raw_objects=None,
            failure_reason=None,
        )

    def publish(
        self,
        run: TrafficRun,
        *,
        expected_rows: int,
        actual_rows: int,
        expected_raw_objects: int,
        actual_raw_objects: int,
        is_publishable: bool = True,
    ) -> str:
        return self._record(
            run,
            status=STATUS_SUCCESS,
            is_publishable=is_publishable,
            expected_rows=expected_rows,
            actual_rows=actual_rows,
            expected_raw_objects=expected_raw_objects,
            actual_raw_objects=actual_raw_objects,
            failure_reason=None,
        )

    def fail(
        self,
        run: TrafficRun,
        *,
        task_id: str,
        error: BaseException,
        expected_raw_objects: int | None = None,
        actual_raw_objects: int | None = None,
    ) -> str:
        return self._record(
            run,
            status=STATUS_FAILED,
            is_publishable=False,
            expected_rows=None,
            actual_rows=None,
            expected_raw_objects=expected_raw_objects,
            actual_raw_objects=actual_raw_objects,
            failure_reason=f"{type(error).__name__} in {task_id}",
        )

    def coalesce(self, run_id: str, *, replacement_run_id: str) -> str:
        return self._record(
            TrafficRun("traffic_incident_bronze", run_id),
            status=STATUS_COALESCED,
            is_publishable=False,
            expected_rows=None,
            actual_rows=None,
            expected_raw_objects=None,
            actual_raw_objects=None,
            failure_reason=f"replaced_by={replacement_run_id}",
        )

    def require_publishable(self, run_id: str) -> str:
        cursor, catalog, schema = self._cursor_factory()
        cursor.execute(
            f"""
            SELECT dag_run_id
            FROM {catalog}.{schema}.{MANIFEST_TABLE}
            WHERE source_id = {_sql_string(SOURCE_ID)}
              AND status = {_sql_string(STATUS_SUCCESS)}
              AND is_publishable
              AND dag_run_id = {_sql_string(run_id)}
              AND NOT EXISTS (
                  SELECT 1
                  FROM {catalog}.{schema}.{MANIFEST_TABLE} AS coalesced
                  WHERE coalesced.source_id = {_sql_string(SOURCE_ID)}
                    AND coalesced.dag_run_id = {_sql_string(run_id)}
                    AND coalesced.status = {_sql_string(STATUS_COALESCED)}
              )
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if row is None:
            raise RunNotPublishableError(
                f"Traffic snapshot is not publishable: {run_id}"
            )
        return str(row[0])

    def latest_publishable_run_id(self) -> str:
        cursor, catalog, schema = self._cursor_factory()
        cursor.execute(
            f"""
            SELECT successful.dag_run_id
            FROM {catalog}.{schema}.{MANIFEST_TABLE} AS successful
            WHERE successful.source_id = {_sql_string(SOURCE_ID)}
              AND successful.status = {_sql_string(STATUS_SUCCESS)}
              AND successful.is_publishable
              AND NOT EXISTS (
                  SELECT 1
                  FROM {catalog}.{schema}.{MANIFEST_TABLE} AS coalesced
                  WHERE coalesced.source_id = successful.source_id
                    AND coalesced.dag_run_id = successful.dag_run_id
                    AND coalesced.status = {_sql_string(STATUS_COALESCED)}
              )
            ORDER BY successful.event_at DESC, successful.dag_run_id DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if row is None:
            raise RunNotPublishableError(
                "No publishable Traffic Bronze run is available"
            )
        return str(row[0])

    def _record(
        self,
        run: TrafficRun,
        *,
        status: str,
        is_publishable: bool,
        expected_rows: int | None,
        actual_rows: int | None,
        expected_raw_objects: int | None,
        actual_raw_objects: int | None,
        failure_reason: str | None,
    ) -> str:
        cursor, catalog, schema = self._cursor_factory()
        qualified_schema = f"{catalog}.{schema}"
        qualified_table = f"{qualified_schema}.{MANIFEST_TABLE}"
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
        except Exception as exc:
            if "Namespace already exists" not in str(exc):
                raise
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {qualified_table} (
                source_id varchar,
                dag_id varchar,
                dag_run_id varchar,
                status varchar,
                is_publishable boolean,
                event_at timestamp(6),
                expected_rows integer,
                actual_rows integer,
                expected_raw_objects integer,
                actual_raw_objects integer,
                failure_reason varchar
            )
            WITH (format = 'PARQUET')
            """
        )
        event_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        values = ", ".join(
            (
                _sql_string(SOURCE_ID),
                _sql_string(run.dag_id),
                _sql_string(run.run_id),
                _sql_string(status),
                "true" if is_publishable else "false",
                f"TIMESTAMP {_sql_string(event_at)}",
                _sql_int(expected_rows),
                _sql_int(actual_rows),
                _sql_int(expected_raw_objects),
                _sql_int(actual_raw_objects),
                _sql_string(failure_reason),
            )
        )
        columns = (
            "source_id, dag_id, dag_run_id, status, is_publishable, event_at, "
            "expected_rows, actual_rows, expected_raw_objects, actual_raw_objects, failure_reason"
        )
        cursor.execute(
            f"""
            MERGE INTO {qualified_table} AS target
            USING (VALUES ({values})) AS incoming ({columns})
              ON target.source_id = incoming.source_id
             AND target.dag_run_id = incoming.dag_run_id
             AND target.status = incoming.status
            WHEN MATCHED THEN UPDATE SET
                dag_id = incoming.dag_id,
                is_publishable = incoming.is_publishable,
                event_at = incoming.event_at,
                expected_rows = incoming.expected_rows,
                actual_rows = incoming.actual_rows,
                expected_raw_objects = incoming.expected_raw_objects,
                actual_raw_objects = incoming.actual_raw_objects,
                failure_reason = incoming.failure_reason
            WHEN NOT MATCHED THEN INSERT ({columns})
            VALUES (
                incoming.source_id, incoming.dag_id, incoming.dag_run_id, incoming.status,
                incoming.is_publishable, incoming.event_at, incoming.expected_rows,
                incoming.actual_rows, incoming.expected_raw_objects, incoming.actual_raw_objects,
                incoming.failure_reason
            )
            """
        )
        return qualified_table


def _sql_string(value: object | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _sql_int(value: int | None) -> str:
    return "NULL" if value is None else str(int(value))
