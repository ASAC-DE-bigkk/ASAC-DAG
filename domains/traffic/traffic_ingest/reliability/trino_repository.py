from __future__ import annotations

import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from .config import (
    KST,
    MANIFEST_TABLE,
    TRAFFIC_AUDIT_TABLE,
    TRAFFIC_TABLE,
    TrafficReportConfig,
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


def _qualified(config: TrafficReportConfig, table: str) -> str:
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


def _traffic_cutoffs(
    config: TrafficReportConfig, detected_at: datetime
) -> tuple[datetime, str]:
    cutoff = detected_at.astimezone(timezone.utc) - timedelta(
        hours=config.lookback_hours
    )
    load_date_days = max(1, math.ceil(config.lookback_hours / 24) + 1)
    load_date_floor = (
        detected_at.astimezone(KST).date() - timedelta(days=load_date_days)
    ).isoformat()
    return cutoff, load_date_floor


def collect_traffic_summary(
    cursor, config: TrafficReportConfig, detected_at: datetime
) -> dict[str, Any]:
    audit_table = _qualified(config, TRAFFIC_AUDIT_TABLE)
    cutoff, load_date_floor = _traffic_cutoffs(config, detected_at)
    # Traffic Bronze/audit are not physically partitioned today. This bounds report semantics
    # but is not asserted to reduce physical input bytes; the benchmark determines that.
    row = _fetch_one(
        cursor,
        f"""
        SELECT
            count(*) AS request_count,
            coalesce(sum(row_count), 0) AS parsed_row_count,
            coalesce(max(list_total_count), 0) AS max_list_total_count,
            coalesce(max(end_index), 0) AS max_end_index,
            sum(CASE WHEN row_count = 0 AND list_total_count = 0 THEN 1 ELSE 0 END) AS zero_row_success_count,
            max(collected_at) AS last_collected_at
        FROM {audit_table}
        WHERE source_id = 'seoul_traffic_incident'
          AND load_date >= {_sql_string(load_date_floor)}
          AND collected_at >= {_sql_timestamp_utc(cutoff)}
        """,
    )
    if not row:
        return {
            "status": "FAIL",
            "reason": "no_traffic_audit_rows",
            "table": _qualified(config, TRAFFIC_TABLE),
            "audit_table": audit_table,
            "request_count": 0,
            "freshness_minutes": None,
            "freshness_status": "FAIL",
            "freshness_warn_minutes": config.freshness_warn_minutes,
            "freshness_error_minutes": config.freshness_error_minutes,
            "freshness_slo_minutes": config.freshness_error_minutes,
            "coverage_ok": False,
            "freshness_ok": False,
        }

    (
        request_count,
        parsed_row_count,
        list_total_count,
        max_end_index,
        zero_row_success_count,
        last_collected_at,
    ) = row
    request_count = int(request_count or 0)
    parsed_row_count = int(parsed_row_count or 0)
    list_total_count = int(list_total_count or 0)
    max_end_index = int(max_end_index or 0)
    zero_row_success_count = int(zero_row_success_count or 0)
    freshness_minutes = _age_minutes(last_collected_at, detected_at)
    freshness = freshness_status(
        freshness_minutes,
        config.freshness_warn_minutes,
        config.freshness_error_minutes,
    )
    freshness_ok = freshness == "PASS"
    coverage_ok = request_count > 0 and (
        list_total_count == 0
        or parsed_row_count >= list_total_count
        or max_end_index >= list_total_count
    )
    return {
        "status": "FAIL" if not coverage_ok or freshness == "FAIL" else freshness,
        "table": _qualified(config, TRAFFIC_TABLE),
        "audit_table": audit_table,
        "request_count": request_count,
        "parsed_row_count": parsed_row_count,
        "list_total_count": list_total_count,
        "max_end_index": max_end_index,
        "zero_row_success_count": zero_row_success_count,
        "last_collected_at": str(last_collected_at),
        "freshness_minutes": freshness_minutes,
        "freshness_status": freshness,
        "freshness_warn_minutes": config.freshness_warn_minutes,
        "freshness_error_minutes": config.freshness_error_minutes,
        "freshness_slo_minutes": config.freshness_error_minutes,
        "coverage_ok": coverage_ok,
        "freshness_ok": freshness_ok,
    }


def collect_dag_run_summary(
    cursor,
    config: TrafficReportConfig,
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
                max(event_at) AS latest_event_at
            FROM {manifest_table}
            WHERE dag_id = {_sql_string(dag_id)}
              AND event_at >= {_sql_timestamp_utc(cutoff)}
            GROUP BY dag_run_id
        ),
        terminal AS (
            SELECT *
            FROM latest
            WHERE latest_status IN ('SUCCESS', 'FAILED')
        )
        SELECT
            coalesce(sum(CASE WHEN latest_status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
            coalesce(sum(CASE WHEN latest_status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
            coalesce(sum(CASE WHEN latest_status = 'STARTED' THEN 1 ELSE 0 END), 0) AS running,
            max(CASE WHEN latest_status = 'SUCCESS' THEN latest_event_at END) AS last_success_at,
            max(
                CASE WHEN latest_status = 'SUCCESS' AND latest_is_publishable THEN latest_event_at END
            ) AS last_publishable_at,
            max_by(dag_run_id, ROW(latest_event_at, dag_run_id)) AS latest_dag_run_id,
            max_by(latest_status, ROW(latest_event_at, dag_run_id)) AS latest_status,
            max_by(latest_is_publishable, ROW(latest_event_at, dag_run_id)) AS latest_is_publishable,
            max_by(latest_event_at, ROW(latest_event_at, dag_run_id)) AS latest_event_at,
            (SELECT max_by(dag_run_id, ROW(latest_event_at, dag_run_id)) FROM terminal)
                AS latest_terminal_dag_run_id,
            (SELECT max_by(latest_status, ROW(latest_event_at, dag_run_id)) FROM terminal)
                AS latest_terminal_status,
            (SELECT max_by(latest_is_publishable, ROW(latest_event_at, dag_run_id)) FROM terminal)
                AS latest_terminal_is_publishable,
            (SELECT max_by(latest_event_at, ROW(latest_event_at, dag_run_id)) FROM terminal)
                AS latest_terminal_event_at
        FROM latest
        """,
    )
    summary = {
        "dag_id": dag_id,
        "success": 0,
        "failed": 0,
        "running": 0,
        "last_success_at": None,
        "last_publishable_at": None,
        "latest_dag_run_id": None,
        "latest_status": None,
        "latest_is_publishable": None,
        "latest_event_at": None,
        "latest_terminal_dag_run_id": None,
        "latest_terminal_status": None,
        "latest_terminal_is_publishable": None,
        "latest_terminal_event_at": None,
    }
    if row:
        summary.update(
            success=int(row[0] or 0),
            failed=int(row[1] or 0),
            running=int(row[2] or 0),
            last_success_at=str(row[3]) if row[3] is not None else None,
            last_publishable_at=str(row[4]) if row[4] is not None else None,
            latest_dag_run_id=str(row[5]) if row[5] is not None else None,
            latest_status=str(row[6]) if row[6] is not None else None,
            latest_is_publishable=bool(row[7]) if row[7] is not None else None,
            latest_event_at=str(row[8]) if row[8] is not None else None,
            latest_terminal_dag_run_id=str(row[9]) if row[9] is not None else None,
            latest_terminal_status=str(row[10]) if row[10] is not None else None,
            latest_terminal_is_publishable=bool(row[11]) if row[11] is not None else None,
            latest_terminal_event_at=str(row[12]) if row[12] is not None else None,
        )
    return summary
