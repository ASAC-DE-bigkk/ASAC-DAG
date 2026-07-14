from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any

from .airflow_evidence import _as_utc_datetime
from .config import (
    AIRFLOW_FAILURE_REASON_FALLBACK,
    DISCORD_GREEN,
    DISCORD_RED,
    DISCORD_YELLOW,
    KST,
    LOGGER,
    TRAFFIC_SCHEDULE_INTERVAL_MINUTES,
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


def _airflow_failure_time(value: Any) -> str:
    timestamp = _as_utc_datetime(value)
    if timestamp is None:
        return "unknown time"
    return timestamp.astimezone(KST).strftime("%H:%M KST")


def _airflow_failure_window(
    failures: list[Mapping[str, Any]],
    schedule_interval_minutes: int = TRAFFIC_SCHEDULE_INTERVAL_MINUTES,
) -> str | None:
    """Format a KST failure window, including the final scheduled slot.

    Traffic Bronze's five-minute schedule means failures at 11:30 and 11:50
    cover 25 minutes (20 minutes between timestamps plus the final 5-minute
    slot). Keeping the interval as an argument makes the calculation testable
    and allows a future schedule contract to override it without changing the
    timestamp logic.
    """
    timestamps = [
        timestamp.astimezone(KST)
        for failure in failures
        if (timestamp := _as_utc_datetime(failure.get("logical_date"))) is not None
    ]
    if not timestamps:
        return None
    first = min(timestamps)
    last = max(timestamps)
    if first.date() == last.date():
        window = f"{first:%Y-%m-%d %H:%M}~{last:%H:%M} KST"
    else:
        window = f"{first:%Y-%m-%d %H:%M}~{last:%Y-%m-%d %H:%M} KST"
    try:
        interval_minutes = max(0, int(schedule_interval_minutes))
    except (TypeError, ValueError):
        interval_minutes = TRAFFIC_SCHEDULE_INTERVAL_MINUTES
    elapsed_minutes = max(0, int((last - first).total_seconds() // 60))
    return f"{window} ({elapsed_minutes + interval_minutes}분)"


def format_traffic_discord_message(report: dict[str, Any]) -> str:
    traffic = report["traffic"]
    airflow_runs = report.get("airflow_runs") or {}
    detected_date = str(report["detected_at"])[:10]
    target = os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod"))
    report_status = str(report["status"])
    coverage_ok = bool(traffic.get("coverage_ok"))
    freshness = str(traffic.get("freshness_status", "FAIL"))
    freshness_ok = freshness == "PASS"
    publishability_ok = bool(report.get("publishability_ok"))
    dag_ok = not bool(report["dag_runs"].get("reason"))
    airflow_query_ok = not bool(airflow_runs.get("reason"))
    airflow_failures_ok = int(airflow_runs.get("failed") or 0) == 0
    scheduled_ok = airflow_query_ok and airflow_failures_ok
    expected = airflow_runs.get("expected")
    success = int(airflow_runs.get("success") or 0)
    failed = int(airflow_runs.get("failed") or 0)
    expected_text = str(expected) if expected is not None else "unknown"
    lines = [
        f"서울시 돌발정보 Bronze 신뢰성 리포트 - {detected_date} (target={target})",
        f"{_status_icon(report_status)} 리포트 상태: {_status_label(report_status)}",
        (
            f"{_status_icon(freshness)} Freshness: {_format_minutes(traffic.get('freshness_minutes'))} "
            f"/ WARN {traffic.get('freshness_warn_minutes', 'n/a')}m "
            f"/ FAIL {traffic.get('freshness_error_minutes', traffic.get('freshness_slo_minutes', 'n/a'))}m"
        ),
        f"{_icon(traffic.get('request_count', 0) > 0)} API 호출건수: {traffic.get('request_count', 0)}회",
        f"{_icon(traffic.get('parsed_row_count', 0) > 0)} 파싱 행수: {int(traffic.get('parsed_row_count', 0)):,}행",
        f"{_icon(traffic.get('list_total_count', 0) >= 0)} 최신 응답 전체 건수: {traffic.get('list_total_count', 0)}건",
        f"{_icon(coverage_ok)} requested_end: {traffic.get('max_end_index', 0)}",
        f"{_icon(traffic.get('zero_row_success_count', 0) >= 0)} zero-row 정상 응답: {traffic.get('zero_row_success_count', 0)}건",
        f"{_icon(traffic.get('reason', '-') == '-')} reason: {traffic.get('reason', '-')}",
        "",
        f"DAG runs / last {report['lookback_hours']}h:",
        (
            f"`dag_id={report['dag_runs'].get('dag_id')}` "
            f"success={report['dag_runs'].get('success', 0)} "
            f"failed={report['dag_runs'].get('failed', 0)} "
            f"running={report['dag_runs'].get('running', 0)} "
            f"last_success={report['dag_runs'].get('last_success_at', 'N/A')} "
            f"last_publishable={report['dag_runs'].get('last_publishable_at', 'N/A')} "
            f"reason={report['dag_runs'].get('reason', '-')}"
        ),
        f"{_icon(publishability_ok)} publishability={_format_bool(publishability_ok)}",
        (
            "late_publishability="
            f"{report.get('late_publishability', {}).get('status', 'NOT_EVALUATED')} "
            f"({report.get('late_publishability', {}).get('reason', 'unknown')})"
        ),
        f"{_icon(scheduled_ok)} 스케줄 수집 상태: {success}/{expected_text} 성공, {failed} 실패",
    ]
    failure_window = _airflow_failure_window(list(airflow_runs.get("failures") or []))
    if failure_window:
        lines.extend([f"실패 수집 공백: {failure_window}", "실패 내역:"])
        for failure in airflow_runs.get("failures") or []:
            time_text = _airflow_failure_time(failure.get("logical_date"))
            task_id = str(failure.get("task_id") or "unknown")
            reason = str(failure.get("reason") or AIRFLOW_FAILURE_REASON_FALLBACK)
            run_id = str(failure.get("run_id") or "unknown")
            lines.append(f"- {time_text} | task={task_id} | {reason} | run_id={run_id}")
    elif airflow_runs.get("reason"):
        lines.append(
            f"스케줄 수집 상태 조회 실패: {airflow_runs.get('reason')}"
            f" (error_type={airflow_runs.get('error_type', 'unknown')})"
        )
        diagnostic_log_url = airflow_runs.get("diagnostic_log_url")
        if diagnostic_log_url:
            lines.append(f"diagnostic_log_url={diagnostic_log_url}")
    lines.extend(
        [
            "",
            "Checks:",
            f"{_icon(coverage_ok)} traffic_coverage={_format_bool(coverage_ok)}",
            f"{_icon(freshness_ok)} traffic_freshness={_format_bool(freshness_ok)}",
            f"{_icon(publishability_ok)} traffic_publishability={_format_bool(publishability_ok)}",
            f"{_icon(dag_ok)} dag_run_summary={_format_bool(dag_ok)}",
            f"{_icon(scheduled_ok)} airflow_scheduled_runs={_format_bool(scheduled_ok)}",
            "",
            "Blast radius:",
        ]
    )
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
    title = lines[0].strip("*") if lines else "Traffic Bronze reliability report"
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
            "Discord webhook is not configured; skip traffic report notification."
        )
        return False
    request = urllib.request.Request(
        webhook_url,
        data=_discord_payload(message),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "ask-seoul-traffic-report/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status >= 400:
                LOGGER.warning(
                    "Traffic report Discord notification failed: status=%s",
                    response.status,
                )
                return False
    except urllib.error.HTTPError as exc:
        LOGGER.warning(
            "Traffic report Discord notification failed: status=%s error_type=%s",
            exc.code,
            type(exc).__name__,
        )
        return False
    except Exception as exc:
        LOGGER.warning(
            "Traffic report Discord notification failed: error_type=%s",
            type(exc).__name__,
        )
        return False
    return True
