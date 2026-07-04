from __future__ import annotations

import json
import logging
import os
import re
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KST = ZoneInfo("Asia/Seoul")
LOGGER = logging.getLogger(__name__)

TRAFFIC_BRONZE_DAG_ID = "traffic_incident_bronze"
TRAFFIC_TABLE = "bronze_seoul_traffic_incident"
TRAFFIC_AUDIT_TABLE = "bronze_seoul_traffic_incident_request_audit"
MANIFEST_TABLE = "bronze_collection_run_manifest"
WEBHOOK_ENVS = ("ASK_SEOUL_DISCORD_WEBHOOK_URL", "TRAFFIC_DISCORD_WEBHOOK_URL")
SCHEDULE_ENV = "ASK_SEOUL_TRAFFIC_REPORT_DAG_SCHEDULE"
GLOBAL_SCHEDULE_ENV = "ASK_SEOUL_REPORT_DAG_SCHEDULE"
DISCORD_GREEN = 3066993
DISCORD_RED = 15158332


@dataclass(frozen=True)
class TrafficReportConfig:
    catalog: str
    schema: str
    lookback_hours: int
    freshness_minutes: int


def is_dev_target(env: Mapping[str, str] = os.environ) -> bool:
    return env.get("ASK_SEOUL_TARGET", env.get("DBT_TARGET", "prod")) == "dev"


def discord_webhook_url(env: Mapping[str, str] = os.environ) -> str | None:
    for key in WEBHOOK_ENVS:
        value = (env.get(key) or "").strip()
        if value:
            return value
    return None


def report_dag_schedule(env: Mapping[str, str] = os.environ) -> str | None:
    if SCHEDULE_ENV in env:
        return env[SCHEDULE_ENV] or None
    if GLOBAL_SCHEDULE_ENV in env:
        return env[GLOBAL_SCHEDULE_ENV] or None
    if not is_dev_target(env):
        return None
    if not discord_webhook_url(env):
        return None
    return "0 9 * * *"


def sql_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def trino_catalog(env: Mapping[str, str] = os.environ) -> str:
    if is_dev_target(env):
        return env.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    return env.get("TRINO_ICEBERG_CATALOG", "iceberg")


def ask_seoul_schema(env: Mapping[str, str] = os.environ) -> str:
    return env.get("ASK_SEOUL_SCHEMA", "ask_seoul")


def report_config(env: Mapping[str, str] = os.environ) -> TrafficReportConfig:
    return TrafficReportConfig(
        catalog=sql_identifier(trino_catalog(env)),
        schema=sql_identifier(ask_seoul_schema(env)),
        lookback_hours=int(env.get("ASK_SEOUL_REPORT_LOOKBACK_HOURS", "24")),
        freshness_minutes=int(env.get("ASK_SEOUL_REPORT_TRAFFIC_FRESHNESS_MINUTES", "15")),
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
    return int((detected_at.astimezone(collected_at.tzinfo) - collected_at).total_seconds() // 60)


def collect_traffic_summary(cursor, config: TrafficReportConfig, detected_at: datetime) -> dict[str, Any]:
    audit_table = _qualified(config, TRAFFIC_AUDIT_TABLE)
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
          AND collected_at >= current_timestamp - INTERVAL '{config.lookback_hours}' HOUR
        """,
    )
    if not row:
        return {
            "status": "FAIL",
            "reason": "no_traffic_audit_rows",
            "table": _qualified(config, TRAFFIC_TABLE),
            "audit_table": audit_table,
            "request_count": 0,
        }

    request_count, parsed_row_count, list_total_count, max_end_index, zero_row_success_count, last_collected_at = row
    request_count = int(request_count or 0)
    parsed_row_count = int(parsed_row_count or 0)
    list_total_count = int(list_total_count or 0)
    max_end_index = int(max_end_index or 0)
    zero_row_success_count = int(zero_row_success_count or 0)
    freshness_minutes = _age_minutes(last_collected_at, detected_at)
    freshness_ok = freshness_minutes is not None and freshness_minutes <= config.freshness_minutes
    coverage_ok = request_count > 0 and (
        list_total_count == 0 or parsed_row_count >= list_total_count or max_end_index >= list_total_count
    )
    return {
        "status": "PASS" if coverage_ok and freshness_ok else "FAIL",
        "table": _qualified(config, TRAFFIC_TABLE),
        "audit_table": audit_table,
        "request_count": request_count,
        "parsed_row_count": parsed_row_count,
        "list_total_count": list_total_count,
        "max_end_index": max_end_index,
        "zero_row_success_count": zero_row_success_count,
        "last_collected_at": str(last_collected_at),
        "freshness_minutes": freshness_minutes,
        "freshness_slo_minutes": config.freshness_minutes,
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
                max_by(status, event_at) AS latest_status
            FROM {manifest_table}
            WHERE dag_id = {_sql_string(dag_id)}
              AND event_at >= {_sql_timestamp_utc(cutoff)}
            GROUP BY dag_run_id
        )
        SELECT
            coalesce(sum(CASE WHEN latest_status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
            coalesce(sum(CASE WHEN latest_status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
            coalesce(sum(CASE WHEN latest_status = 'STARTED' THEN 1 ELSE 0 END), 0) AS running
        FROM latest
        """,
    )
    summary = {"dag_id": dag_id, "success": 0, "failed": 0, "running": 0}
    if row:
        summary.update(success=int(row[0] or 0), failed=int(row[1] or 0), running=int(row[2] or 0))
    return summary


def build_traffic_reliability_report(cursor=None, detected_at: datetime | None = None) -> dict[str, Any]:
    config = report_config()
    cursor = cursor or trino_cursor()
    detected_at = detected_at or datetime.now(KST)
    try:
        traffic = collect_traffic_summary(cursor, config, detected_at)
    except Exception as exc:
        traffic = {
            "status": "FAIL",
            "reason": "traffic_query_failed",
            "error": str(exc),
            "table": _qualified(config, TRAFFIC_TABLE),
            "audit_table": _qualified(config, TRAFFIC_AUDIT_TABLE),
        }
    try:
        dag_runs = collect_dag_run_summary(cursor, config, TRAFFIC_BRONZE_DAG_ID, detected_at)
    except Exception as exc:
        dag_runs = {
            "dag_id": TRAFFIC_BRONZE_DAG_ID,
            "success": 0,
            "failed": 0,
            "running": 0,
            "reason": "dag_run_query_failed",
            "error": str(exc),
        }
    status = traffic["status"] if not dag_runs.get("reason") else "FAIL"

    return {
        "report_name": "traffic_bronze_reliability",
        "detected_at": detected_at.isoformat(),
        "catalog": config.catalog,
        "schema": config.schema,
        "lookback_hours": config.lookback_hours,
        "status": status,
        "traffic": traffic,
        "dag_runs": dag_runs,
        "blast_radius": [
            _qualified(config, TRAFFIC_TABLE),
            _qualified(config, TRAFFIC_AUDIT_TABLE),
        ],
    }


def _format_bool(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _format_minutes(value: int | None) -> str:
    if value is None:
        return "unknown"
    return f"{value}m"


def _icon(value: bool) -> str:
    return "✅" if value else "❌"


def format_traffic_discord_message(report: dict[str, Any]) -> str:
    traffic = report["traffic"]
    detected_date = str(report["detected_at"])[:10]
    target = os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod"))
    status_ok = report["status"] == "PASS"
    coverage_ok = bool(traffic.get("coverage_ok"))
    freshness_ok = bool(traffic.get("freshness_ok"))
    dag_ok = not bool(report["dag_runs"].get("reason"))
    lines = [
        f"서울시 돌발정보 Bronze 신뢰성 리포트 - {detected_date} (target={target})",
        f"{_icon(status_ok)} 리포트 상태: {'성공' if status_ok else '실패'}",
        f"{_icon(freshness_ok)} Freshness: {_format_minutes(traffic.get('freshness_minutes'))} / SLO {traffic.get('freshness_slo_minutes', 'n/a')}m",
        f"{_icon(traffic.get('request_count', 0) > 0)} API 호출건수: {traffic.get('request_count', 0)}회",
        f"{_icon(traffic.get('parsed_row_count', 0) > 0)} 파싱 행수: {int(traffic.get('parsed_row_count', 0)):,}행",
        f"{_icon(traffic.get('list_total_count', 0) >= 0)} 최신 응답 전체 건수: {traffic.get('list_total_count', 0)}건",
        f"{_icon(coverage_ok)} requested_end: {traffic.get('max_end_index', 0)}",
        f"{_icon(traffic.get('zero_row_success_count', 0) == 0)} zero-row 정상 응답: {traffic.get('zero_row_success_count', 0)}건",
        f"{_icon(traffic.get('reason', '-') == '-')} reason: {traffic.get('reason', '-')}",
        "",
        f"DAG runs / last {report['lookback_hours']}h:",
        (
            f"`dag_id={report['dag_runs'].get('dag_id')}` "
            f"success={report['dag_runs'].get('success', 0)} "
            f"failed={report['dag_runs'].get('failed', 0)} "
            f"running={report['dag_runs'].get('running', 0)} "
            f"reason={report['dag_runs'].get('reason', '-')}"
        ),
        "",
        "Checks:",
        f"{_icon(coverage_ok)} traffic_coverage={_format_bool(coverage_ok)}",
        f"{_icon(freshness_ok)} traffic_freshness={_format_bool(freshness_ok)}",
        f"{_icon(dag_ok)} dag_run_summary={_format_bool(dag_ok)}",
        "",
        "Blast radius:",
    ]
    lines.extend(f"`{table}`" for table in report["blast_radius"])
    lines.extend(["", f"detected_at: `{report['detected_at']}`", f"catalog/schema: `{report['catalog']}.{report['schema']}`"])
    message = "\n".join(lines)
    if len(message) > 1900:
        return message[:1890] + "\n...(truncated)"
    return message


def _discord_payload(message: str) -> bytes:
    lines = message.splitlines()
    title = lines[0].strip("*") if lines else "Traffic Bronze reliability report"
    description = "\n".join(lines[1:]).strip() or title
    color = DISCORD_RED if "FAIL" in title or "리포트 상태: 실패" in message else DISCORD_GREEN
    payload = {
        "embeds": [{
            "title": title[:256],
            "description": description[:4096],
            "color": color,
        }]
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def send_discord_message(message: str, webhook_url: str | None = None) -> bool:
    webhook_url = webhook_url or discord_webhook_url()
    if not webhook_url:
        LOGGER.info("Discord webhook is not configured; skip traffic report notification.")
        return False
    request = urllib.request.Request(
        webhook_url,
        data=_discord_payload(message),
        headers={"Content-Type": "application/json", "User-Agent": "ask-seoul-traffic-report/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status >= 400:
                LOGGER.warning("Traffic report Discord notification failed: status=%s", response.status)
                return False
    except urllib.error.HTTPError as exc:
        LOGGER.warning(
            "Traffic report Discord notification failed: status=%s error_type=%s",
            exc.code,
            type(exc).__name__,
        )
        return False
    except Exception as exc:
        LOGGER.warning("Traffic report Discord notification failed: error_type=%s", type(exc).__name__)
        return False
    return True
