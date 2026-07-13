from __future__ import annotations

import json
import logging
import math
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
MANIFEST_TABLE = "bronze_collection_run_manifest"
WEBHOOK_ENVS = ("ASK_SEOUL_DISCORD_WEBHOOK_URL", "WEATHER_DISCORD_WEBHOOK_URL")
SCHEDULE_ENV = "ASK_SEOUL_WEATHER_REPORT_DAG_SCHEDULE"
GLOBAL_SCHEDULE_ENV = "ASK_SEOUL_REPORT_DAG_SCHEDULE"
DISCORD_GREEN = 3066993
DISCORD_YELLOW = 16776960
DISCORD_RED = 15158332
KMA_BASE_INTERVAL_HOURS = 3


@dataclass(frozen=True)
class WeatherReportConfig:
    catalog: str
    schema: str
    lookback_hours: int
    expected_kma_grids: int
    freshness_warn_minutes: int
    freshness_error_minutes: int


def is_dev_target(env: Mapping[str, str] = os.environ) -> bool:
    return env.get("ASK_SEOUL_TARGET", env.get("DBT_TARGET", "prod")) == "dev"


def discord_webhook_url(env: Mapping[str, str] = os.environ) -> str | None:
    for key in WEBHOOK_ENVS:
        value = (env.get(key) or "").strip()
        if value:
            return value
    return None


def report_dag_schedule(env: Mapping[str, str] = os.environ) -> str | None:
    if not is_dev_target(env):
        return None
    if SCHEDULE_ENV in env:
        return env[SCHEDULE_ENV] or None
    if GLOBAL_SCHEDULE_ENV in env:
        return env[GLOBAL_SCHEDULE_ENV] or None
    if not discord_webhook_url(env):
        return None
    return "0 * * * *"


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
        freshness_warn_minutes=int(
            env.get("ASK_SEOUL_REPORT_WEATHER_FRESHNESS_WARN_MINUTES", "240")
        ),
        freshness_error_minutes=int(
            env.get("ASK_SEOUL_REPORT_WEATHER_FRESHNESS_ERROR_MINUTES", "360")
        ),
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
    return int((detected_at.astimezone(collected_at.tzinfo) - collected_at).total_seconds() // 60)


def freshness_status(age_minutes: int | None, warn_minutes: int, error_minutes: int) -> str:
    if age_minutes is None or age_minutes > error_minutes:
        return "FAIL"
    if age_minutes > warn_minutes:
        return "WARN"
    return "PASS"


def _weather_cutoffs(config: WeatherReportConfig, detected_at: datetime) -> tuple[datetime, str]:
    cutoff = detected_at.astimezone(timezone.utc) - timedelta(hours=config.lookback_hours)
    partition_days = max(1, math.ceil(config.lookback_hours / 24) + 1)
    load_date_floor = (
        detected_at.astimezone(KST).date() - timedelta(days=partition_days)
    ).isoformat()
    return cutoff, load_date_floor


def collect_weather_summary(cursor, config: WeatherReportConfig, detected_at: datetime) -> dict[str, Any]:
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
            ) AS last_publishable_at
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
        )
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
        dag_runs = collect_dag_run_summary(cursor, config, WEATHER_BRONZE_DAG_ID, detected_at)
    except Exception as exc:
        dag_runs = {
            "dag_id": WEATHER_BRONZE_DAG_ID,
            "success": 0,
            "failed": 0,
            "running": 0,
            "reason": "dag_run_query_failed",
            "error": str(exc),
        }
    publishability_ok = bool(dag_runs.get("last_publishable_at"))
    dag_runs["publishability_ok"] = publishability_ok
    late_publishability = {
        "status": "NOT_EVALUATED",
        "reason": "bounded late-repair contract is owned by ASAC-DBT #165",
    }
    if dag_runs.get("reason") or not publishability_ok:
        status = "FAIL"
    else:
        status = weather["status"]

    return {
        "report_name": "weather_bronze_reliability",
        "detected_at": detected_at.isoformat(),
        "catalog": config.catalog,
        "schema": config.schema,
        "lookback_hours": config.lookback_hours,
        "status": status,
        "weather": weather,
        "dag_runs": dag_runs,
        "publishability_ok": publishability_ok,
        "late_publishability": late_publishability,
        "blast_radius": [_qualified(config, WEATHER_TABLE)],
    }


def _format_bool(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _format_minutes(value: int | None) -> str:
    if value is None:
        return "unknown"
    return f"{value}m"


def _icon(value: bool) -> str:
    return "✅" if value else "❌"


def _status_icon(status: str) -> str:
    return {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(status, "❌")


def _status_label(status: str) -> str:
    return {"PASS": "성공", "WARN": "경고", "FAIL": "실패"}.get(status, "실패")


def format_weather_discord_message(report: dict[str, Any]) -> str:
    weather = report["weather"]
    detected_date = str(report["detected_at"])[:10]
    target = os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod"))
    report_status = str(report["status"])
    coverage_ok = bool(weather.get("coverage_ok"))
    freshness = str(weather.get("freshness_status", "FAIL"))
    freshness_ok = freshness == "PASS"
    publishability_ok = bool(report.get("publishability_ok"))
    dag_ok = not bool(report["dag_runs"].get("reason"))
    lines = [
        f"기상청 단기예보 Bronze 신뢰성 리포트 - {detected_date} (target={target})",
        f"{_status_icon(report_status)} 리포트 상태: {_status_label(report_status)}",
        (
            f"{_status_icon(freshness)} Freshness: {_format_minutes(weather.get('freshness_minutes'))} "
            f"/ WARN {weather.get('freshness_warn_minutes', 'n/a')}m "
            f"/ FAIL {weather.get('freshness_error_minutes', weather.get('freshness_slo_minutes', 'n/a'))}m"
        ),
        f"{_icon(coverage_ok)} 발표시각 커버리지: {weather.get('base_time_count', 0)}/{weather.get('expected_base_time_count', 0)}회",
        f"{_icon(coverage_ok)} 서울 격자 커버리지: {weather.get('complete_base_time_count', 0)}/{weather.get('expected_base_time_count', 0)}회 complete ({weather.get('expected_grid_count', 0)}개 grid 기준)",
        f"{_icon(coverage_ok)} Bronze grid slots(커버리지): {weather.get('grid_slot_count', 0)}/{weather.get('expected_grid_slot_count', 0)}개",
        f"{_icon(weather.get('raw_pages_ok', False))} Bronze raw API pages(실제 응답): {weather.get('raw_object_count', 0)}개",
        f"{_icon(weather.get('raw_pages_ok', False))} 추가 pagination pages(page 2+): {weather.get('additional_raw_page_count', 0)}개",
        f"{_icon(weather.get('row_count', 0) > 0)} Bronze 적재: {int(weather.get('row_count', 0)):,}행",
        f"{_icon(bool(weather.get('latest_base_date')))} 최신 예보 발표시각: {weather.get('latest_base_date', 'N/A')} {weather.get('latest_base_time', '')}",
        f"{_icon(weather.get('reason', '-') == '-')} reason: {weather.get('reason', '-')}",
        "",
        f"DAG runs / last {report['lookback_hours']}h:",
        (
            f"`dag_id={report['dag_runs'].get('dag_id')}` "
            f"success={report['dag_runs'].get('success', 0)} "
            f"failed={report['dag_runs'].get('failed', 0)} "
            f"running={report['dag_runs'].get('running', 0)} "
            f"raw_pages={report['dag_runs'].get('actual_raw_objects', 0)}/{report['dag_runs'].get('expected_raw_objects', 0)} "
            f"last_success={report['dag_runs'].get('last_success_at', 'N/A')} "
            f"last_publishable={report['dag_runs'].get('last_publishable_at', 'N/A')} "
            f"reason={report['dag_runs'].get('reason', '-')}"
        ),
        (
            f"{_icon(publishability_ok)} publishability="
            f"{_format_bool(publishability_ok)}"
        ),
        (
            "late_publishability="
            f"{report.get('late_publishability', {}).get('status', 'NOT_EVALUATED')} "
            f"({report.get('late_publishability', {}).get('reason', 'unknown')})"
        ),
        "",
        "Checks:",
        f"{_icon(coverage_ok)} weather_coverage={_format_bool(coverage_ok)}",
        f"{_icon(freshness_ok)} weather_freshness={_format_bool(freshness_ok)}",
        f"{_icon(publishability_ok)} weather_publishability={_format_bool(publishability_ok)}",
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
    title = lines[0].strip("*") if lines else "Weather Bronze reliability report"
    description = "\n".join(lines[1:]).strip() or title
    if "FAIL" in title or "리포트 상태: 실패" in message:
        color = DISCORD_RED
    elif "리포트 상태: 경고" in message:
        color = DISCORD_YELLOW
    else:
        color = DISCORD_GREEN
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
        LOGGER.info("Discord webhook is not configured; skip weather report notification.")
        return False
    request = urllib.request.Request(
        webhook_url,
        data=_discord_payload(message),
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
