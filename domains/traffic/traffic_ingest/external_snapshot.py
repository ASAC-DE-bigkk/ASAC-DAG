import os
from typing import Mapping

from traffic_ingest.common.runtime import sql_identifier, trino_cursor


class ExternalSnapshotUnavailableError(RuntimeError):
    """An external Iceberg table has no usable snapshot to pin."""


def traffic_gold_anchor_exists(cursor_factory=trino_cursor) -> bool:
    cursor, catalog, _ = cursor_factory()
    cursor.execute(
        "SELECT count(*) "
        f"FROM {catalog}.information_schema.tables "
        "WHERE table_schema = 'traffic' "
        "AND table_name = 'gold_traffic_incident_current_by_admin_dong_hourly'"
    )
    row = cursor.fetchone()
    try:
        relation_count = row[0]
    except (TypeError, IndexError) as exc:
        raise ExternalSnapshotUnavailableError(
            "Traffic Gold anchor telemetry is unavailable"
        ) from exc
    if (
        isinstance(relation_count, bool)
        or not isinstance(relation_count, int)
        or relation_count not in {0, 1}
    ):
        raise ExternalSnapshotUnavailableError(
            "Traffic Gold anchor telemetry must be zero or one"
        )
    return relation_count == 1


def resolve_citydata_crowding_snapshot_id(
    cursor_factory=trino_cursor,
    env: Mapping[str, str] = os.environ,
) -> int:
    cursor, catalog, _ = cursor_factory()
    target = env.get("ASK_SEOUL_TARGET", env.get("DBT_TARGET", "prod"))
    default_schema = "seoul_citydata" if target == "dev" else "citydata"
    schema = sql_identifier(env.get("SEOUL_CITYDATA_SCHEMA", default_schema))
    table = sql_identifier("gold_citydata_ppltn_by_time")
    cursor.execute(
        "SELECT snapshot_id "
        f'FROM {catalog}.{schema}."{table}$snapshots" '
        "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
    )
    row = cursor.fetchone()
    try:
        raw_snapshot_id = row[0]
    except (TypeError, IndexError) as exc:
        raise ExternalSnapshotUnavailableError(
            "Citydata crowding Iceberg snapshot is unavailable"
        ) from exc
    if (
        isinstance(raw_snapshot_id, bool)
        or not isinstance(raw_snapshot_id, int)
        or raw_snapshot_id <= 0
    ):
        raise ExternalSnapshotUnavailableError(
            "Citydata crowding Iceberg snapshot ID must be a positive integer"
        )
    return raw_snapshot_id


def resolve_admin_dong_crosswalk_snapshot_id(
    cursor_factory=trino_cursor,
    env: Mapping[str, str] = os.environ,
) -> int:
    cursor, catalog, _ = cursor_factory()
    schema = sql_identifier(env.get("COMMON_SCHEMA", "common"))
    table = sql_identifier("seoul_admin_dong_crosswalk")
    cursor.execute(
        "SELECT snapshot_id "
        f'FROM {catalog}.{schema}."{table}$snapshots" '
        "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
    )
    row = cursor.fetchone()
    try:
        raw_snapshot_id = row[0]
    except (TypeError, IndexError) as exc:
        raise ExternalSnapshotUnavailableError(
            "admin_dong crosswalk Iceberg snapshot is unavailable"
        ) from exc
    if (
        isinstance(raw_snapshot_id, bool)
        or not isinstance(raw_snapshot_id, int)
        or raw_snapshot_id <= 0
    ):
        raise ExternalSnapshotUnavailableError(
            "admin_dong crosswalk Iceberg snapshot ID must be a positive integer"
        )
    return raw_snapshot_id
