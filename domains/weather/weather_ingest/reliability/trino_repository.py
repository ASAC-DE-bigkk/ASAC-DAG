from __future__ import annotations

import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from .config import (
    KMA_BASE_INTERVAL_HOURS,
    KST,
    MANIFEST_TABLE,
    WEATHER_TABLE,
    WeatherReportConfig,
    sql_identifier,
    trino_catalog,
)


def trino_cursor():
    import trino.dbapi

    catalog = sql_identifier(trino_catalog())
    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return connection.cursor()


def _fetch_one(cursor, sql: str) -> tuple[Any, ...]:
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return ()
    return tuple(row)


def _qualified(config: WeatherReportConfig, table: str) -> str:
    return f"{config.catalog}.{config.schema}.{table}"


def _sql_string(value: object) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _sql_timestamp_utc(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return "TIMESTAMP " + _sql_string(utc_value.strftime("%Y-%m-%d %H:%M:%S.%f"))


def _age_minutes(collected_at: Any, detected_at: datetime) -> int | None:
    if collected_at is None:
        return None
    if isinstance(collected_at, str):
        collected_at = datetime.fromisoformat(collected_at.replace("Z", "+00:00"))
    if collected_at.tzinfo is None:
        collected_at = collected_at.replace(tzinfo=ZoneInfo("UTC"))
    return int(
        (detected_at.astimezone(collected_at.tzinfo) - collected_at).total_seconds()
        // 60
    )


def freshness_status(
    age_minutes: int | None, warn_minutes: int, error_minutes: int
) -> str:
    if age_minutes is None or age_minutes > error_minutes:
        return "FAIL"
    if age_minutes > warn_minutes:
        return "WARN"
    return "PASS"


def _weather_cutoffs(
    config: WeatherReportConfig, detected_at: datetime
) -> tuple[datetime, str]:
    cutoff = detected_at.astimezone(timezone.utc) - timedelta(
        hours=config.lookback_hours
    )
    partition_days = max(1, math.ceil(config.lookback_hours / 24) + 1)
    load_date_floor = (
        detected_at.astimezone(KST).date() - timedelta(days=partition_days)
    ).isoformat()
    return cutoff, load_date_floor


def collect_weather_summary(
    cursor, config: WeatherReportConfig, detected_at: datetime
) -> dict[str, Any]:
    table = _qualified(config, WEATHER_TABLE)
    cutoff, load_date_floor = _weather_cutoffs(config, detected_at)
    expected_base_time_count = max(1, config.lookback_hours // KMA_BASE_INTERVAL_HOURS)
    expected_grid_slot_count = config.expected_kma_grids * expected_base_time_count
    expected_raw_object_count = expected_grid_slot_count
    row = _fetch_one(
        cursor,
        f"""
        WITH by_base AS (
            SELECT
                base_date,
                base_time,
                count(DISTINCT concat(cast(nx AS varchar), ':', cast(ny AS varchar))) AS grid_count,
                count(DISTINCT raw_object_key) AS raw_object_count,
                count(*) AS row_count,
                max(collected_at) AS last_collected_at
            FROM {table}
            WHERE source_id = 'kma_vilage_fcst'
              AND load_date >= {_sql_string(load_date_floor)}
              AND collected_at >= {_sql_timestamp_utc(cutoff)}
            GROUP BY base_date, base_time
        )
        SELECT
            count(*) AS base_time_count,
            coalesce(sum(grid_count), 0) AS grid_slot_count,
            coalesce(sum(raw_object_count), 0) AS raw_object_count,
            coalesce(sum(row_count), 0) AS row_count,
            coalesce(min(grid_count), 0) AS min_grid_count,
            coalesce(max(grid_count), 0) AS max_grid_count,
            coalesce(sum(CASE WHEN grid_count >= {config.expected_kma_grids} THEN 1 ELSE 0 END), 0) AS complete_base_time_count,
            max_by(base_date, concat(base_date, base_time)) AS latest_base_date,
            max_by(base_time, concat(base_date, base_time)) AS latest_base_time,
            max(last_collected_at) AS last_collected_at
        FROM by_base
        """,
    )
    if not row or int(row[0] or 0) == 0:
        return {
            "status": "FAIL",
            "reason": "no_weather_rows",
            "table": table,
            "grid_count": 0,
            "expected_grid_count": config.expected_kma_grids,
            "base_time_count": 0,
            "expected_base_time_count": expected_base_time_count,
            "grid_slot_count": 0,
            "expected_grid_slot_count": expected_grid_slot_count,
            "raw_object_count": 0,
            "expected_raw_object_count": expected_raw_object_count,
            "additional_raw_page_count": 0,
            "freshness_minutes": None,
            "freshness_status": "FAIL",
            "freshness_warn_minutes": config.freshness_warn_minutes,
            "freshness_error_minutes": config.freshness_error_minutes,
            "freshness_slo_minutes": config.freshness_error_minutes,
            "freshness_ok": False,
            "coverage_ok": False,
        }

    (
        base_time_count,
        grid_slot_count,
        raw_object_count,
        row_count,
        min_grid_count,
        max_grid_count,
        complete_base_time_count,
        latest_base_date,
        latest_base_time,
        last_collected_at,
    ) = row
    base_time_count = int(base_time_count or 0)
    complete_base_time_count = int(complete_base_time_count or 0)
    grid_slot_count = int(grid_slot_count or 0)
    raw_object_count = int(raw_object_count or 0)
    additional_raw_page_count = max(0, raw_object_count - grid_slot_count)
    freshness_minutes = _age_minutes(last_collected_at, detected_at)
    raw_pages_ok = raw_object_count >= grid_slot_count and raw_object_count > 0
    coverage_ok = (
        base_time_count >= expected_base_time_count
        and complete_base_time_count >= expected_base_time_count
        and grid_slot_count >= expected_grid_slot_count
        and raw_pages_ok
    )
    freshness = freshness_status(
        freshness_minutes,
        config.freshness_warn_minutes,
        config.freshness_error_minutes,
    )
    freshness_ok = freshness == "PASS"
    status = "FAIL" if not coverage_ok or freshness == "FAIL" else freshness
    return {
        "status": status,
        "table": table,
        "base_date": latest_base_date,
        "base_time": latest_base_time,
        "latest_base_date": latest_base_date,
        "latest_base_time": latest_base_time,
        "base_time_count": base_time_count,
        "expected_base_time_count": expected_base_time_count,
        "complete_base_time_count": complete_base_time_count,
        "grid_count": int(min_grid_count or 0),
        "min_grid_count": int(min_grid_count or 0),
        "max_grid_count": int(max_grid_count or 0),
        "expected_grid_count": config.expected_kma_grids,
        "grid_slot_count": grid_slot_count,
        "expected_grid_slot_count": expected_grid_slot_count,
        "raw_object_count": raw_object_count,
        "expected_raw_object_count": expected_raw_object_count,
        "additional_raw_page_count": additional_raw_page_count,
        "raw_pages_ok": raw_pages_ok,
        "row_count": int(row_count),
        "last_collected_at": str(last_collected_at),
        "freshness_minutes": freshness_minutes,
        "freshness_status": freshness,
        "freshness_warn_minutes": config.freshness_warn_minutes,
        "freshness_error_minutes": config.freshness_error_minutes,
        "freshness_slo_minutes": config.freshness_error_minutes,
        "coverage_ok": coverage_ok,
        "freshness_ok": freshness_ok,
    }


def collect_weather_product_profile(
    cursor, config: WeatherReportConfig, detected_at: datetime
) -> dict[str, int | None] | None:
    """Measure the latest KMA issue for product-health, independent of status."""
    table = _qualified(config, WEATHER_TABLE)
    cutoff, load_date_floor = _weather_cutoffs(config, detected_at)
    row = _fetch_one(
        cursor,
        f"""
        WITH latest_issue AS (
            SELECT max(concat(base_date, lpad(base_time, 4, '0'))) AS issue_key
            FROM {table}
            WHERE source_id = 'kma_vilage_fcst'
              AND load_date >= {_sql_string(load_date_floor)}
              AND collected_at >= {_sql_timestamp_utc(cutoff)}
        )
        SELECT
            count(DISTINCT CASE WHEN category IN ('TMP', 'POP', 'SKY', 'PTY') THEN category END),
            count(DISTINCT place_id),
            max(
                date_diff(
                    'hour',
                    date_parse(concat(base_date, lpad(base_time, 4, '0')), '%Y%m%d%H%i'),
                    date_parse(concat(fcst_date, lpad(fcst_time, 4, '0')), '%Y%m%d%H%i')
                )
            )
        FROM {table}
        CROSS JOIN latest_issue
        WHERE source_id = 'kma_vilage_fcst'
          AND load_date >= {_sql_string(load_date_floor)}
          AND collected_at >= {_sql_timestamp_utc(cutoff)}
          AND concat(base_date, lpad(base_time, 4, '0')) = latest_issue.issue_key
        """,
    )
    if not row:
        return None
    return {
        "core_category_count": int(row[0]) if row[0] is not None else None,
        "mapped_place_count": int(row[1]) if row[1] is not None else None,
        "forecast_horizon_hours": int(row[2]) if row[2] is not None else None,
    }


def collect_dag_run_summary(
    cursor,
    config: WeatherReportConfig,
    dag_id: str,
    detected_at: datetime,
) -> dict[str, Any]:
    manifest_table = _qualified(config, MANIFEST_TABLE)
    cutoff = detected_at.astimezone(timezone.utc).replace(tzinfo=timezone.utc)
    cutoff = cutoff.replace(microsecond=0) - timedelta(hours=config.lookback_hours)
    row = _fetch_one(
        cursor,
        f"""
        WITH latest AS (
            SELECT
                dag_run_id,
                max_by(status, event_at) AS latest_status,
                max_by(is_publishable, event_at) AS latest_is_publishable,
                max(event_at) AS latest_event_at,
                max_by(expected_raw_objects, event_at) AS expected_raw_objects,
                max_by(actual_raw_objects, event_at) AS actual_raw_objects
            FROM {manifest_table}
            WHERE dag_id = {_sql_string(dag_id)}
              AND event_at >= {_sql_timestamp_utc(cutoff)}
            GROUP BY dag_run_id
        )
        SELECT
            coalesce(sum(CASE WHEN latest_status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
            coalesce(sum(CASE WHEN latest_status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
            coalesce(sum(CASE WHEN latest_status = 'STARTED' THEN 1 ELSE 0 END), 0) AS running,
            coalesce(sum(expected_raw_objects), 0) AS expected_raw_objects,
            coalesce(sum(actual_raw_objects), 0) AS actual_raw_objects,
            max(CASE WHEN latest_status = 'SUCCESS' THEN latest_event_at END) AS last_success_at,
            max(
                CASE WHEN latest_status = 'SUCCESS' AND latest_is_publishable THEN latest_event_at END
            ) AS last_publishable_at,
            max_by(dag_run_id, ROW(latest_event_at, dag_run_id)) AS latest_dag_run_id,
            max_by(latest_status, ROW(latest_event_at, dag_run_id)) AS latest_status,
            max_by(latest_is_publishable, ROW(latest_event_at, dag_run_id)) AS latest_is_publishable,
            max_by(latest_event_at, ROW(latest_event_at, dag_run_id)) AS latest_event_at
        FROM latest
        """,
    )
    summary = {
        "dag_id": dag_id,
        "success": 0,
        "failed": 0,
        "running": 0,
        "expected_raw_objects": 0,
        "actual_raw_objects": 0,
        "last_success_at": None,
        "last_publishable_at": None,
        "latest_dag_run_id": None,
        "latest_status": None,
        "latest_is_publishable": None,
        "latest_event_at": None,
    }
    if row:
        summary.update(
            success=int(row[0] or 0),
            failed=int(row[1] or 0),
            running=int(row[2] or 0),
            expected_raw_objects=int(row[3] or 0),
            actual_raw_objects=int(row[4] or 0),
            last_success_at=str(row[5]) if row[5] is not None else None,
            last_publishable_at=str(row[6]) if row[6] is not None else None,
            latest_dag_run_id=str(row[7]) if row[7] is not None else None,
            latest_status=str(row[8]) if row[8] is not None else None,
            latest_is_publishable=bool(row[9]) if row[9] is not None else None,
            latest_event_at=str(row[10]) if row[10] is not None else None,
        )
    return summary
