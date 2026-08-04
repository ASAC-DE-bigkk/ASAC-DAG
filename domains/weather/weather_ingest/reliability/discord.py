from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

from common.discord.notify import send_payload

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


def _discord_payload(message: str) -> dict:
    lines = message.splitlines()
    title = lines[0].strip("*") if lines else "Weather Bronze reliability report"
    description = "\n".join(lines[1:]).strip() or title
    if "FAIL" in title or "리포트 상태: 실패" in message:
        color = DISCORD_RED
    elif "리포트 상태: 경고" in message:
        color = DISCORD_YELLOW
    else:
        color = DISCORD_GREEN
    return {
        "embeds": [
            {
                "title": title[:256],
                "description": description[:4096],
                "color": color,
            }
        ]
    }


def send_discord_message(message: str, webhook_url: str | None = None) -> bool:
    """리포트 메시지 1건 전송 — 조립은 여기서, 전송은 공용 모듈이(ASAC-DAG#692).

    메시지 내용·색 판정은 그대로 두고 전송 계층만 공용으로 옮겼다. 그래서 출력은 이전과
    같고, 앞에 환경 표식(`[PROD]`/`[DEV]`)과 footer 출처가 더해진다 — 여러 인스턴스가 같은
    채널을 쓰기 때문에 어느 환경 결과인지 메시지만 보고 가릴 수 있어야 한다.
    """
    return send_payload(_discord_payload(message),
                        domain="weather", webhook=webhook_url or discord_webhook_url() or None)


def send_discord_report(report: dict[str, Any], webhook_url: str | None = None) -> bool:
    """카드 payload 전송 — `build_weather_discord_payload` 결과를 그대로 보낸다(#692).

    `fields`·`footer` 를 쓰는 모양이라 공용 `send_embed` 로는 담기지 않는다. `send_payload` 가
    payload 를 건드리지 않고 표식·출처만 주입한다.
    """
    return send_payload(build_weather_discord_payload(report),
                        domain="weather", webhook=webhook_url or discord_webhook_url() or None)
