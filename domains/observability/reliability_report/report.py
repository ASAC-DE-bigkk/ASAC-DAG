from __future__ import annotations

import json
import os
import re
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KST = ZoneInfo("Asia/Seoul")

WEATHER_TABLE = "bronze_kma_vilage_fcst"
TRAFFIC_TABLE = "bronze_seoul_traffic_incident"
TRAFFIC_AUDIT_TABLE = "bronze_seoul_traffic_incident_request_audit"


@dataclass(frozen=True)
class ReportConfig:
    catalog: str
    schema: str
    lookback_hours: int
    expected_kma_grids: int
    weather_freshness_minutes: int
    traffic_freshness_minutes: int


def is_dev_target() -> bool:
    return os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"


def report_dag_schedule() -> str | None:
    if "ASK_SEOUL_REPORT_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_REPORT_DAG_SCHEDULE"] or None
    if not is_dev_target():
        return None
    if not os.environ.get("ASK_SEOUL_DISCORD_WEBHOOK_URL"):
        return None
    return "0 9 * * *"


def sql_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def trino_catalog() -> str:
    if is_dev_target():
        return os.environ.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    return os.environ.get("TRINO_ICEBERG_CATALOG", "iceberg")


def ask_seoul_schema() -> str:
    return os.environ.get("ASK_SEOUL_SCHEMA", "ask_seoul")


def report_config() -> ReportConfig:
    return ReportConfig(
        catalog=sql_identifier(trino_catalog()),
        schema=sql_identifier(ask_seoul_schema()),
        lookback_hours=int(os.environ.get("ASK_SEOUL_REPORT_LOOKBACK_HOURS", "24")),
        expected_kma_grids=int(os.environ.get("ASK_SEOUL_REPORT_EXPECTED_KMA_GRIDS", "80")),
        weather_freshness_minutes=int(os.environ.get("ASK_SEOUL_REPORT_WEATHER_FRESHNESS_MINUTES", "240")),
        traffic_freshness_minutes=int(os.environ.get("ASK_SEOUL_REPORT_TRAFFIC_FRESHNESS_MINUTES", "15")),
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


def _qualified(config: ReportConfig, table: str) -> str:
    return f"{config.catalog}.{config.schema}.{table}"


def _age_minutes(collected_at: Any, detected_at: datetime) -> int | None:
    if collected_at is None:
        return None
    if isinstance(collected_at, str):
        collected_at = datetime.fromisoformat(collected_at.replace("Z", "+00:00"))
    if collected_at.tzinfo is None:
        collected_at = collected_at.replace(tzinfo=ZoneInfo("UTC"))
    return int((detected_at.astimezone(collected_at.tzinfo) - collected_at).total_seconds() // 60)


def collect_weather_summary(cursor, config: ReportConfig, detected_at: datetime) -> dict[str, Any]:
    table = _qualified(config, WEATHER_TABLE)
    row = _fetch_one(
        cursor,
        f"""
        SELECT
            base_date,
            base_time,
            count(DISTINCT concat(cast(nx AS varchar), ':', cast(ny AS varchar))) AS grid_count,
            count(DISTINCT raw_object_key) AS raw_object_count,
            count(*) AS row_count,
            max(collected_at) AS last_collected_at
        FROM {table}
        WHERE source_id = 'kma_vilage_fcst'
          AND collected_at >= current_timestamp - INTERVAL '{config.lookback_hours}' HOUR
        GROUP BY base_date, base_time
        ORDER BY base_date DESC, base_time DESC
        LIMIT 1
        """,
    )
    if not row:
        return {
            "status": "FAIL",
            "reason": "no_weather_rows",
            "table": table,
            "grid_count": 0,
            "expected_grid_count": config.expected_kma_grids,
        }

    base_date, base_time, grid_count, raw_object_count, row_count, last_collected_at = row
    freshness_minutes = _age_minutes(last_collected_at, detected_at)
    coverage_ok = int(grid_count) >= config.expected_kma_grids
    freshness_ok = freshness_minutes is not None and freshness_minutes <= config.weather_freshness_minutes
    return {
        "status": "PASS" if coverage_ok and freshness_ok else "FAIL",
        "table": table,
        "base_date": base_date,
        "base_time": base_time,
        "grid_count": int(grid_count),
        "expected_grid_count": config.expected_kma_grids,
        "raw_object_count": int(raw_object_count),
        "row_count": int(row_count),
        "last_collected_at": str(last_collected_at),
        "freshness_minutes": freshness_minutes,
        "freshness_slo_minutes": config.weather_freshness_minutes,
        "coverage_ok": coverage_ok,
        "freshness_ok": freshness_ok,
    }


def collect_traffic_summary(cursor, config: ReportConfig, detected_at: datetime) -> dict[str, Any]:
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
    freshness_ok = freshness_minutes is not None and freshness_minutes <= config.traffic_freshness_minutes
    coverage_ok = request_count > 0 and (list_total_count == 0 or parsed_row_count >= list_total_count or max_end_index >= list_total_count)
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
        "freshness_slo_minutes": config.traffic_freshness_minutes,
        "coverage_ok": coverage_ok,
        "freshness_ok": freshness_ok,
    }


def build_reliability_report(cursor=None, detected_at: datetime | None = None) -> dict[str, Any]:
    config = report_config()
    cursor = cursor or trino_cursor()
    detected_at = detected_at or datetime.now(KST)
    try:
        weather = collect_weather_summary(cursor, config, detected_at)
    except Exception as exc:
        weather = {
            "status": "FAIL",
            "reason": "weather_query_failed",
            "error": str(exc),
            "table": _qualified(config, WEATHER_TABLE),
        }
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
    failures = []
    for domain, summary in (("weather", weather), ("traffic", traffic)):
        if summary["status"] != "PASS":
            failures.append(domain)
    return {
        "report_name": "weather_traffic_bronze_reliability",
        "detected_at": detected_at.isoformat(),
        "catalog": config.catalog,
        "schema": config.schema,
        "lookback_hours": config.lookback_hours,
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "weather": weather,
        "traffic": traffic,
        "blast_radius": [
            _qualified(config, WEATHER_TABLE),
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


def format_discord_message(report: dict[str, Any]) -> str:
    weather = report["weather"]
    traffic = report["traffic"]
    lines = [
        f"**서울 도시 데이터 Bronze 신뢰성 리포트: {report['status']}**",
        f"- detected_at: `{report['detected_at']}`",
        f"- catalog/schema: `{report['catalog']}.{report['schema']}`",
        "",
        "**Weather / KMA**",
        (
            f"- status={weather['status']} freshness={_format_minutes(weather.get('freshness_minutes'))}"
            f"/{weather.get('freshness_slo_minutes', 'n/a')}m "
            f"coverage={weather.get('grid_count', 0)}/{weather.get('expected_grid_count', 0)} grids "
            f"rows={weather.get('row_count', 0)} raw_objects={weather.get('raw_object_count', 0)} "
            f"base={weather.get('base_date', '-')}{weather.get('base_time', '')} "
            f"reason={weather.get('reason', '-')}"
        ),
        "",
        "**Traffic / TOPIS AccInfo**",
        (
            f"- status={traffic['status']} freshness={_format_minutes(traffic.get('freshness_minutes'))}"
            f"/{traffic.get('freshness_slo_minutes', 'n/a')}m "
            f"requests={traffic.get('request_count', 0)} rows={traffic.get('parsed_row_count', 0)}"
            f"/{traffic.get('list_total_count', 0)} "
            f"requested_end={traffic.get('max_end_index', 0)} zero_row_ok={traffic.get('zero_row_success_count', 0)} "
            f"reason={traffic.get('reason', '-')}"
        ),
        "",
        "**Checks**",
        f"- weather_coverage={_format_bool(bool(weather.get('coverage_ok')))}",
        f"- weather_freshness={_format_bool(bool(weather.get('freshness_ok')))}",
        f"- traffic_coverage={_format_bool(bool(traffic.get('coverage_ok')))}",
        f"- traffic_freshness={_format_bool(bool(traffic.get('freshness_ok')))}",
        "",
        "**Blast radius**",
    ]
    lines.extend(f"- `{table}`" for table in report["blast_radius"])
    message = "\n".join(lines)
    if len(message) > 1900:
        return message[:1890] + "\n...(truncated)"
    return message


def discord_webhook_url() -> str | None:
    return os.environ.get("ASK_SEOUL_DISCORD_WEBHOOK_URL")


def send_discord_message(message: str, webhook_url: str | None = None) -> bool:
    webhook_url = webhook_url or discord_webhook_url()
    if not webhook_url:
        print("ASK_SEOUL_DISCORD_WEBHOOK_URL is not configured; skip Discord notification.")
        return False
    payload = json.dumps({"content": message}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "ask-seoul-reliability-report/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status >= 400:
            raise RuntimeError(f"Discord notification failed: status={response.status}")
    return True
