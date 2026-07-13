# shared-domains: weather,traffic
"""Read-only Trino query telemetry for cost-proxy benchmarks.

``system.runtime.queries`` varies across Trino releases and retains only a
bounded query history.  This module therefore discovers exposed columns at
runtime and falls back to the completed DB-API cursor's statistics.  Missing
data is deliberately represented as ``None``—never as a zero-cost query.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import re
from typing import Any


METRIC_COLUMNS = (
    "query_id",
    "state",
    "queued_time_ms",
    "analysis_time_ms",
    "distributed_planning_time_ms",
    "cpu_time_ms",
    "wall_time_ms",
    "peak_user_memory_bytes",
    "input_rows",
    "input_bytes",
    "output_rows",
    "output_bytes",
    "physical_input_bytes",
    "physical_written_bytes",
    "spilled_bytes",
)

CURSOR_STAT_MAP = {
    "queryId": "query_id",
    "state": "state",
    "queuedTimeMillis": "queued_time_ms",
    "analysisTimeMillis": "analysis_time_ms",
    "cpuTimeMillis": "cpu_time_ms",
    # The Trino DB-API exposes elapsed time reliably in the local dev runtime;
    # prefer it over the older wallTimeMillis spelling when both are present.
    "elapsedTimeMillis": "wall_time_ms",
    "wallTimeMillis": "wall_time_ms",
    "peakMemoryBytes": "peak_user_memory_bytes",
    "processedRows": "input_rows",
    "processedBytes": "input_bytes",
    "physicalInputBytes": "physical_input_bytes",
    "physicalWrittenBytes": "physical_written_bytes",
    "spilledBytes": "spilled_bytes",
}

_QUALIFIED_TABLE_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sql_string(value: str) -> str:
    """Return a safely quoted SQL string literal for a query identifier."""
    return "'" + value.replace("'", "''") + "'"


def query_id_from_cursor(cursor: Any) -> str | None:
    """Read the completed Trino query ID from either common cursor stat spelling."""
    stats = getattr(cursor, "stats", None) or {}
    if not isinstance(stats, Mapping):
        return None
    query_id = stats.get("queryId") or stats.get("query_id")
    return str(query_id) if query_id else None


def _json_value(value: Any, *, metric_name: str | None = None) -> Any:
    """Convert DB-API values to stable JSON-safe primitives.

    Trino returns Python ``timedelta`` values for some older interval-typed
    runtime columns.  Our public metric contract names those values in
    milliseconds, so convert only those duration fields to milliseconds.
    """
    if isinstance(value, timedelta):
        milliseconds = value.total_seconds() * 1000
        return int(milliseconds) if milliseconds.is_integer() else milliseconds
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _blank(query_id: str | None) -> dict[str, Any]:
    result = {name: None for name in METRIC_COLUMNS}
    result.update(
        query_id=query_id,
        metric_source="unavailable",
        unavailable_metrics=list(METRIC_COLUMNS[1:]),
        unavailable_reasons=[],
    )
    return result


def _runtime_columns(cursor: Any) -> set[str]:
    cursor.execute("DESCRIBE system.runtime.queries")
    return {str(row[0]) for row in cursor.fetchall() if row}


def _mark_unavailable_metrics(result: dict[str, Any]) -> None:
    result["unavailable_metrics"] = [
        name for name in METRIC_COLUMNS[1:] if result.get(name) is None
    ]


def _apply_cursor_stats(result: dict[str, Any], fallback_stats: Mapping[str, Any] | None) -> bool:
    """Fill only unavailable metrics from DB-API statistics.

    System connector values remain authoritative when present.  The return
    value distinguishes a pure system-runtime record from a blended record.
    """
    applied_metric = False
    for source_name, target_name in CURSOR_STAT_MAP.items():
        value = (fallback_stats or {}).get(source_name)
        if value is not None and result.get(target_name) is None:
            result[target_name] = _json_value(value, metric_name=target_name)
            applied_metric = applied_metric or target_name != "query_id"
    return applied_metric


def collect_query_metrics(
    cursor: Any,
    query_id: str,
    *,
    fallback_stats: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Collect available metrics for one completed query without writing state.

    A missing System connector row is expected when the query history is short;
    cursor statistics retain the useful portable subset in that case.
    """
    result = _blank(query_id)
    try:
        available = _runtime_columns(cursor)
    except Exception as exc:  # runtime connector/version is optional evidence
        result["unavailable_reasons"].append(
            f"system_runtime_queries_unavailable:{type(exc).__name__}"
        )
        _apply_cursor_stats(result, fallback_stats)
        if any(result[name] is not None for name in METRIC_COLUMNS[1:]):
            result["metric_source"] = "cursor.stats"
        else:
            result["unavailable_reasons"].append("no_cursor_stats_available")
        _mark_unavailable_metrics(result)
        return result

    selected = [name for name in METRIC_COLUMNS if name in available]
    if "query_id" in selected:
        try:
            cursor.execute(
                "SELECT "
                + ", ".join(selected)
                + " FROM system.runtime.queries WHERE query_id = "
                + sql_string(query_id)
            )
            row = cursor.fetchone()
        except Exception as exc:  # query history may disappear between statements
            row = None
            result["unavailable_reasons"].append(
                f"system_runtime_query_lookup_failed:{type(exc).__name__}"
            )
        if row is not None:
            for name, value in zip(selected, row, strict=True):
                result[name] = _json_value(value, metric_name=name)
            if result["query_id"] is None:
                result["query_id"] = query_id
            result["metric_source"] = "system.runtime.queries"
            if _apply_cursor_stats(result, fallback_stats):
                result["metric_source"] = "system.runtime.queries+cursor.stats"
            _mark_unavailable_metrics(result)
            return result
        result["unavailable_reasons"].append("query_not_found_in_system_runtime")
    else:
        result["unavailable_reasons"].append("query_id_column_unavailable")

    _apply_cursor_stats(result, fallback_stats)
    if any(result[name] is not None for name in METRIC_COLUMNS[1:]):
        result["metric_source"] = "cursor.stats"
    else:
        result["unavailable_reasons"].append("no_cursor_stats_available")
    _mark_unavailable_metrics(result)
    return result


def _metadata_table(qualified_table: str, suffix: str) -> str:
    parts = qualified_table.split(".")
    if len(parts) != 3 or not all(_QUALIFIED_TABLE_PART.fullmatch(part) for part in parts):
        raise ValueError("qualified_table must be catalog.schema.table with safe identifiers")
    catalog, schema, table = parts
    return f'{catalog}.{schema}."{table}${suffix}"'


def collect_iceberg_fingerprint(cursor: Any, qualified_table: str) -> dict[str, Any]:
    """Read lightweight Iceberg snapshot/files metadata for a suite fingerprint."""
    snapshots_table = _metadata_table(qualified_table, "snapshots")
    cursor.execute(
        "SELECT snapshot_id, committed_at FROM "
        + snapshots_table
        + " ORDER BY committed_at DESC LIMIT 1"
    )
    snapshot = cursor.fetchone() or (None, None)

    files_table = _metadata_table(qualified_table, "files")
    cursor.execute(
        "SELECT count(*), coalesce(sum(file_size_in_bytes), 0), "
        "coalesce(sum(record_count), 0) FROM "
        + files_table
    )
    files = cursor.fetchone() or (0, 0, 0)

    return {
        "table": qualified_table,
        "snapshot_id": _json_value(snapshot[0]),
        "snapshot_committed_at": _json_value(snapshot[1]),
        "file_count": int(files[0] or 0),
        "file_bytes": int(files[1] or 0),
        "record_count": int(files[2] or 0),
    }


class TelemetryCursor:
    """Proxy a workload cursor and append one cost record after each fetch.

    The metadata cursor must be a separate Trino cursor: querying
    ``system.runtime.queries`` through the workload cursor would replace the
    workload result set before its caller reads it.
    """

    def __init__(self, workload_cursor: Any, metadata_cursor: Any) -> None:
        self._workload_cursor = workload_cursor
        self._metadata_cursor = metadata_cursor
        self.records: list[dict[str, Any]] = []

    @property
    def stats(self) -> Any:
        return getattr(self._workload_cursor, "stats", None)

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        return self._workload_cursor.execute(*args, **kwargs)

    def fetchone(self, *args: Any, **kwargs: Any) -> Any:
        value = self._workload_cursor.fetchone(*args, **kwargs)
        self._capture_metrics()
        return value

    def fetchall(self, *args: Any, **kwargs: Any) -> Any:
        values = self._workload_cursor.fetchall(*args, **kwargs)
        self._capture_metrics()
        return values

    def _capture_metrics(self) -> None:
        query_id = query_id_from_cursor(self._workload_cursor)
        if query_id is None:
            result = _blank(None)
            result["unavailable_reasons"] = ["missing_query_id"]
        else:
            stats = getattr(self._workload_cursor, "stats", None)
            result = collect_query_metrics(
                self._metadata_cursor,
                query_id,
                fallback_stats=stats if isinstance(stats, Mapping) else None,
            )
        self.records.append(result)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._workload_cursor, name)
