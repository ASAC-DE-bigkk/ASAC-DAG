from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

from .card import build_weather_discord_payload
from .config import (
    DISCORD_GREEN,
    DISCORD_RED,
    DISCORD_YELLOW,
    LOGGER,
    discord_webhook_url,
)


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
    lines.extend(
        [
            "",
            f"detected_at: `{report['detected_at']}`",
            f"catalog/schema: `{report['catalog']}.{report['schema']}`",
        ]
    )
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
        "embeds": [
            {
                "title": title[:256],
                "description": description[:4096],
                "color": color,
            }
        ]
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def send_discord_message(message: str, webhook_url: str | None = None) -> bool:
    webhook_url = webhook_url or discord_webhook_url()
    if not webhook_url:
        LOGGER.info(
            "Discord webhook is not configured; skip weather report notification."
        )
        return False
    request = urllib.request.Request(
        webhook_url,
        data=_discord_payload(message),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "ask-seoul-weather-report/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status >= 400:
                LOGGER.warning(
                    "Weather report Discord notification failed: status=%s",
                    response.status,
                )
                return False
    except urllib.error.HTTPError as exc:
        LOGGER.warning(
            "Weather report Discord notification failed: status=%s error_type=%s",
            exc.code,
            type(exc).__name__,
        )
        return False
    except Exception as exc:
        LOGGER.warning(
            "Weather report Discord notification failed: error_type=%s",
            type(exc).__name__,
        )
        return False
    return True


def send_discord_report(report: dict[str, Any], webhook_url: str | None = None) -> bool:
    webhook_url = webhook_url or discord_webhook_url()
    if not webhook_url:
        LOGGER.info(
            "Discord webhook is not configured; skip weather report notification."
        )
        return False
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(
            build_weather_discord_payload(report), ensure_ascii=False
        ).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "ask-seoul-weather-report/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status >= 400:
                LOGGER.warning(
                    "Weather report Discord notification failed: status=%s",
                    response.status,
                )
                return False
    except urllib.error.HTTPError as exc:
        LOGGER.warning(
            "Weather report Discord notification failed: status=%s error_type=%s",
            exc.code,
            type(exc).__name__,
        )
        return False
    except Exception as exc:
        LOGGER.warning(
            "Weather report Discord notification failed: error_type=%s",
            type(exc).__name__,
        )
        return False
    return True
