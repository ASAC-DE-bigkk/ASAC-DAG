"""Daily bounded refresh for missing or stale TOPIS road-link references."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

from traffic_ingest.common.runtime import (
    sql_identifier,
    sql_string,
    sql_timestamp,
    trino_cursor,
)
from traffic_ingest.errors import TrafficBronzeConfigurationError
from traffic_ingest.flow_info import SOURCE_ID as FLOW_SOURCE_ID
from traffic_ingest.flow_info import normalize_link_ids
from traffic_ingest.link_reference_backfill import (
    MAX_BATCH_SIZE,
    land_backfill_batch,
    materialize_backfill_batch,
)
from traffic_ingest.link_reference_bronze import (
    LINK_INFO_TABLE,
    LINK_VERTEX_TABLE,
    REQUEST_AUDIT_TABLE,
)
from traffic_ingest.link_reference_info import (
    LINK_INFO_SERVICE,
    LINK_VERTEX_SERVICE,
    SOURCE_ID,
)
from traffic_ingest.run_manifest import (
    MANIFEST_TABLE,
    SOURCE_ID as INCIDENT_SOURCE_ID,
    STATUS_COALESCED,
    STATUS_SUCCESS,
)


DEFAULT_SYNC_BATCH_SIZE = 100
DEFAULT_STALE_AFTER_DAYS = 30
MAX_STALE_AFTER_DAYS = 365


def _bounded_integer(value: object, *, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TrafficBronzeConfigurationError(f"{name} must be an integer")
    if not 1 <= value <= maximum:
        raise TrafficBronzeConfigurationError(
            f"{name} must be between 1 and {maximum}"
        )
    return value


def validate_incremental_sync_request(
    *,
    batch_size: object,
    stale_after_days: object,
) -> tuple[int, int]:
    """Validate the two bounded controls shared by scheduled and manual runs."""
    return (
        _bounded_integer(
            batch_size,
            name="batch_size",
            maximum=MAX_BATCH_SIZE,
        ),
        _bounded_integer(
            stale_after_days,
            name="stale_after_days",
            maximum=MAX_STALE_AFTER_DAYS,
        ),
    )


def build_incremental_sync_link_sql(
    *,
    catalog: str,
    schema: str,
    stale_before: datetime,
    batch_size: int,
) -> str:
    """Select missing links first, then the oldest complete reference pairs."""
    safe_batch_size, _ = validate_incremental_sync_request(
        batch_size=batch_size,
        stale_after_days=DEFAULT_STALE_AFTER_DAYS,
    )
    if not isinstance(stale_before, datetime) or stale_before.tzinfo is None:
        raise TrafficBronzeConfigurationError(
            "stale_before must be a timezone-aware datetime"
        )
    qualified = f"{sql_identifier(catalog)}.{sql_identifier(schema)}"
    incident_table = f"{qualified}.bronze_seoul_traffic_incident"
    flow_table = f"{qualified}.bronze_seoul_traffic_flow"
    manifest_table = f"{qualified}.{MANIFEST_TABLE}"
    audit_table = f"{qualified}.{REQUEST_AUDIT_TABLE}"
    info_table = f"{qualified}.{LINK_INFO_TABLE}"
    vertex_table = f"{qualified}.{LINK_VERTEX_TABLE}"
    source_id = sql_string(SOURCE_ID)
    info_service = sql_string(LINK_INFO_SERVICE)
    vertex_service = sql_string(LINK_VERTEX_SERVICE)
    incident_source_id = sql_string(INCIDENT_SOURCE_ID)
    flow_source_id = sql_string(FLOW_SOURCE_ID)
    success_status = sql_string(STATUS_SUCCESS)
    coalesced_status = sql_string(STATUS_COALESCED)
    cutoff = sql_timestamp(stale_before)

    return f"""
        WITH active_runs_ranked AS (
            SELECT
                successful.source_id,
                successful.dag_run_id,
                row_number() OVER (
                    PARTITION BY successful.source_id
                    ORDER BY successful.event_at DESC,
                             successful.dag_run_id DESC
                ) AS active_run_num
            FROM {manifest_table} AS successful
            WHERE successful.source_id IN ({incident_source_id}, {flow_source_id})
              AND successful.status = {success_status}
              AND successful.is_publishable
              AND NOT EXISTS (
                  SELECT 1
                  FROM {manifest_table} AS coalesced
                  WHERE coalesced.source_id = successful.source_id
                    AND coalesced.dag_run_id = successful.dag_run_id
                    AND coalesced.status = {coalesced_status}
              )
        ),
        latest_active_runs AS (
            SELECT source_id, dag_run_id
            FROM active_runs_ranked
            WHERE active_run_num = 1
        ),
        link_universe AS (
            SELECT cast(incident.link_id AS varchar) AS link_id
            FROM {incident_table} AS incident
            JOIN latest_active_runs AS active_run
              ON active_run.source_id = {incident_source_id}
             AND incident.dag_run_id = active_run.dag_run_id
            WHERE incident.link_id IS NOT NULL
              AND trim(cast(incident.link_id AS varchar)) <> ''
            UNION
            SELECT cast(flow.link_id AS varchar) AS link_id
            FROM {flow_table} AS flow
            JOIN latest_active_runs AS active_run
              ON active_run.source_id = {flow_source_id}
             AND flow.dag_run_id = active_run.dag_run_id
            WHERE flow.link_id IS NOT NULL
              AND trim(cast(flow.link_id AS varchar)) <> ''
        ),
        audit_runs AS (
            SELECT
                link_id,
                dag_run_id,
                count_if(service_name = {info_service}
                         AND result_code = 'INFO-000') AS info_success_count,
                count_if(service_name = {vertex_service}
                         AND result_code = 'INFO-000') AS vertex_success_count,
                max(CASE WHEN service_name = {info_service}
                         THEN row_count END) AS info_audit_row_count,
                max(CASE WHEN service_name = {info_service}
                         THEN list_total_count END) AS info_audit_total_count,
                max(CASE WHEN service_name = {vertex_service}
                         THEN row_count END) AS vertex_audit_row_count,
                max(CASE WHEN service_name = {vertex_service}
                         THEN list_total_count END) AS vertex_audit_total_count,
                max(collected_at) AS reference_collected_at
            FROM {audit_table}
            WHERE source_id = {source_id}
            GROUP BY link_id, dag_run_id
        ),
        info_actual AS (
            SELECT link_id, dag_run_id, count(*) AS info_actual_count
            FROM {info_table}
            WHERE source_id = {source_id}
            GROUP BY link_id, dag_run_id
        ),
        vertex_actual AS (
            SELECT
                link_id,
                dag_run_id,
                count(*) AS vertex_actual_count,
                count(DISTINCT try_cast(vertex_sequence AS integer))
                    AS vertex_sequence_distinct_count
            FROM {vertex_table}
            WHERE source_id = {source_id}
            GROUP BY link_id, dag_run_id
        ),
        complete_runs AS (
            SELECT audit_runs.link_id, audit_runs.reference_collected_at
            FROM audit_runs
            JOIN info_actual
              ON audit_runs.link_id = info_actual.link_id
             AND audit_runs.dag_run_id = info_actual.dag_run_id
            JOIN vertex_actual
              ON audit_runs.link_id = vertex_actual.link_id
             AND audit_runs.dag_run_id = vertex_actual.dag_run_id
            WHERE info_success_count = 1
              AND vertex_success_count = 1
              AND info_audit_row_count = 1
              AND info_audit_total_count = 1
              AND info_actual_count = 1
              AND vertex_audit_row_count >= 1
              AND vertex_audit_row_count = vertex_audit_total_count
              AND vertex_actual_count = vertex_audit_row_count
              AND vertex_sequence_distinct_count = vertex_actual_count
        ),
        latest_complete AS (
            SELECT
                link_id,
                max(reference_collected_at) AS latest_complete_collected_at
            FROM complete_runs
            GROUP BY link_id
        )
        SELECT link_universe.link_id
        FROM link_universe
        LEFT JOIN latest_complete
          ON link_universe.link_id = latest_complete.link_id
        WHERE latest_complete_collected_at IS NULL
           OR latest_complete_collected_at < {cutoff}
        ORDER BY
            CASE WHEN latest_complete_collected_at IS NULL THEN 0 ELSE 1 END,
            latest_complete_collected_at ASC,
            link_universe.link_id ASC
        LIMIT {safe_batch_size}
    """


def resolve_incremental_sync_link_ids(
    *,
    batch_size: int = DEFAULT_SYNC_BATCH_SIZE,
    stale_after_days: int = DEFAULT_STALE_AFTER_DAYS,
    now: datetime | None = None,
    cursor_factory=trino_cursor,
) -> list[str]:
    safe_batch_size, safe_stale_days = validate_incremental_sync_request(
        batch_size=batch_size,
        stale_after_days=stale_after_days,
    )
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise TrafficBronzeConfigurationError(
            "now must be a timezone-aware datetime"
        )
    cursor, catalog, schema = cursor_factory()
    cursor.execute(
        build_incremental_sync_link_sql(
            catalog=catalog,
            schema=schema,
            stale_before=current - timedelta(days=safe_stale_days),
            batch_size=safe_batch_size,
        )
    )
    return normalize_link_ids([row[0] for row in cursor.fetchall()])[
        :safe_batch_size
    ]


def land_incremental_sync_batch(
    *,
    link_ids: list[str],
    dag_run_id: str,
    land_batch: Callable[..., dict[str, object]] = land_backfill_batch,
) -> dict[str, object]:
    normalized = normalize_link_ids(link_ids)
    return land_batch(
        conf={
            "link_ids": normalized,
            "batch_size": max(1, len(normalized)),
            "force_refresh": True,
        },
        dag_run_id=dag_run_id,
    )


def materialize_incremental_sync_batch(
    *,
    raw_result: dict[str, object],
    dag_run_id: str,
    materialize_batch: Callable[..., dict[str, object]] = (
        materialize_backfill_batch
    ),
) -> dict[str, object]:
    return materialize_batch(
        raw_result=raw_result,
        dag_run_id=dag_run_id,
    )


__all__ = [
    "DEFAULT_STALE_AFTER_DAYS",
    "DEFAULT_SYNC_BATCH_SIZE",
    "MAX_STALE_AFTER_DAYS",
    "build_incremental_sync_link_sql",
    "land_incremental_sync_batch",
    "materialize_incremental_sync_batch",
    "resolve_incremental_sync_link_ids",
    "validate_incremental_sync_request",
]
