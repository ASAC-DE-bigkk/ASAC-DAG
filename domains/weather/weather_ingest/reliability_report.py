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

WEATHER_BRONZE_DAG_ID = "weather_vilage_fcst_bronze"
WEATHER_TABLE = "bronze_kma_vilage_fcst"
WEBHOOK_ENVS = ("ASK_SEOUL_DISCORD_WEBHOOK_URL", "WEATHER_DISCORD_WEBHOOK_URL")
SCHEDULE_ENV = "ASK_SEOUL_WEATHER_REPORT_DAG_SCHEDULE"
GLOBAL_SCHEDULE_ENV = "ASK_SEOUL_REPORT_DAG_SCHEDULE"


@dataclass(frozen=True)
class WeatherReportConfig:
    catalog: str
    schema: str
    lookback_hours: int
    expected_kma_grids: int
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


def report_config(env: Mapping[str, str] = os.environ) -> WeatherReportConfig:
    return WeatherReportConfig(
        catalog=sql_identifier(trino_catalog(env)),
        schema=sql_identifier(ask_seoul_schema(env)),
        lookback_hours=int(env.get("ASK_SEOUL_REPORT_LOOKBACK_HOURS", "24")),
        expected_kma_grids=int(env.get("ASK_SEOUL_REPORT_EXPECTED_KMA_GRIDS", "80")),
        freshness_minutes=int(env.get("ASK_SEOUL_REPORT_WEATHER_FRESHNESS_MINUTES", "240")),
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


def _age_minutes(collected_at: Any, detected_at: datetime) -> int | None:
    if collected_at is None:
        return None
    if isinstance(collected_at, str):
        collected_at = datetime.fromisoformat(collected_at.replace("Z", "+00:00"))
    if collected_at.tzinfo is None:
        collected_at = collected_at.replace(tzinfo=ZoneInfo("UTC"))
    return int((detected_at.astimezone(collected_at.tzinfo) - collected_at).total_seconds() // 60)


def collect_weather_summary(cursor, config: WeatherReportConfig, detected_at: datetime) -> dict[str, Any]:
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
    freshness_ok = freshness_minutes is not None and freshness_minutes <= config.freshness_minutes
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
        "freshness_slo_minutes": config.freshness_minutes,
        "coverage_ok": coverage_ok,
        "freshness_ok": freshness_ok,
    }


def collect_dag_run_summary(dag_id: str, detected_at: datetime, lookback_hours: int) -> dict[str, Any]:
    from airflow.models.dagrun import DagRun
    from airflow.settings import Session
    from sqlalchemy import func

    cutoff = detected_at.astimezone(timezone.utc) - timedelta(hours=lookback_hours)
    session = Session()
    try:
        rows = (
            session.query(DagRun.state, func.count())
            .filter(DagRun.dag_id == dag_id, DagRun.run_after >= cutoff)
            .group_by(DagRun.state)
            .all()
        )
    finally:
        session.close()
    summary = {"dag_id": dag_id, "success": 0, "failed": 0, "running": 0}
    for state, count in rows:
        if state in summary:
            summary[state] = int(count)
    return summary


def build_weather_reliability_report(cursor=None, detected_at: datetime | None = None) -> dict[str, Any]:
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
        dag_runs = collect_dag_run_summary(WEATHER_BRONZE_DAG_ID, detected_at, config.lookback_hours)
    except Exception as exc:
        dag_runs = {
            "dag_id": WEATHER_BRONZE_DAG_ID,
            "success": 0,
            "failed": 0,
            "running": 0,
            "reason": "dag_run_query_failed",
            "error": str(exc),
        }

    return {
        "report_name": "weather_bronze_reliability",
        "detected_at": detected_at.isoformat(),
        "catalog": config.catalog,
        "schema": config.schema,
        "lookback_hours": config.lookback_hours,
        "status": weather["status"],
        "weather": weather,
        "dag_runs": dag_runs,
        "blast_radius": [_qualified(config, WEATHER_TABLE)],
    }


def _format_bool(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _format_minutes(value: int | None) -> str:
    if value is None:
        return "unknown"
    return f"{value}m"


def format_weather_discord_message(report: dict[str, Any]) -> str:
    weather = report["weather"]
    lines = [
        f"**기상청 단기예보 Bronze 신뢰성 리포트: {report['status']}**",
        f"- detected_at: `{report['detected_at']}`",
        f"- catalog/schema: `{report['catalog']}.{report['schema']}`",
        "",
        "**Weather / KMA getVilageFcst**",
        (
            f"- status={weather['status']} freshness={_format_minutes(weather.get('freshness_minutes'))}"
            f"/{weather.get('freshness_slo_minutes', 'n/a')}m "
            f"coverage={weather.get('grid_count', 0)}/{weather.get('expected_grid_count', 0)} grids "
            f"rows={weather.get('row_count', 0)} raw_objects={weather.get('raw_object_count', 0)} "
            f"base={weather.get('base_date', '-')}{weather.get('base_time', '')} "
            f"reason={weather.get('reason', '-')}"
        ),
        "",
        f"**DAG runs / last {report['lookback_hours']}h**",
        (
            f"- dag_id=`{report['dag_runs'].get('dag_id')}` "
            f"success={report['dag_runs'].get('success', 0)} "
            f"failed={report['dag_runs'].get('failed', 0)} "
            f"running={report['dag_runs'].get('running', 0)} "
            f"reason={report['dag_runs'].get('reason', '-')}"
        ),
        "",
        "**Checks**",
        f"- weather_coverage={_format_bool(bool(weather.get('coverage_ok')))}",
        f"- weather_freshness={_format_bool(bool(weather.get('freshness_ok')))}",
        "",
        "**Blast radius**",
    ]
    lines.extend(f"- `{table}`" for table in report["blast_radius"])
    message = "\n".join(lines)
    if len(message) > 1900:
        return message[:1890] + "\n...(truncated)"
    return message


def send_discord_message(message: str, webhook_url: str | None = None) -> bool:
    webhook_url = webhook_url or discord_webhook_url()
    if not webhook_url:
        LOGGER.info("Discord webhook is not configured; skip weather report notification.")
        return False
    payload = json.dumps({"content": message}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "ask-seoul-weather-report/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status >= 400:
                LOGGER.warning("Weather report Discord notification failed: status=%s", response.status)
                return False
    except urllib.error.HTTPError as exc:
        LOGGER.warning(
            "Weather report Discord notification failed: status=%s error_type=%s",
            exc.code,
            type(exc).__name__,
        )
        return False
    except Exception as exc:
        LOGGER.warning("Weather report Discord notification failed: error_type=%s", type(exc).__name__)
        return False
    return True
