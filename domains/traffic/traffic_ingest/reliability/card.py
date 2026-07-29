from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .config import DISCORD_GREEN, DISCORD_RED, DISCORD_YELLOW


STATUS_COLORS = {
    "PASS": DISCORD_GREEN,
    "WARN": DISCORD_YELLOW,
    "FAIL": DISCORD_RED,
    "UNKNOWN": DISCORD_YELLOW,
}
STATUS_ICONS = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌", "UNKNOWN": "◻️"}


def _truncate(value: object, limit: int) -> str:
    text = str(value)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"


def _duration(value: object) -> str:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return "unknown"
    seconds = float(value) / 1_000
    return f"{seconds:.1f}s" if seconds < 60 else f"{seconds / 60:.1f}m"


def _field(name: str, value: object, *, inline: bool = False) -> dict[str, object]:
    return {
        "name": _truncate(name, 256),
        "value": _truncate(value or "관측값 없음", 1_024),
        "inline": inline,
    }


def _pipeline_value(report: Mapping[str, Any]) -> str:
    stages = report.get("stages")
    if not isinstance(stages, list):
        return "관측값 없음"
    lines = []
    for stage in stages:
        if not isinstance(stage, Mapping):
            continue
        status = str(stage.get("status") or "UNKNOWN")
        duration = stage.get("duration_ms")
        p95 = duration.get("p95") if isinstance(duration, Mapping) else None
        age = stage.get("age_minutes")
        age_text = f"{age}m" if age is not None else "unknown"
        lines.append(
            f"{STATUS_ICONS.get(status, '◻️')} {stage.get('label') or stage.get('key')}"
            f" · age {age_text} · p95 {_duration(p95)}"
        )
    return "\n".join(lines) or "관측값 없음"


def _trend_value(report: Mapping[str, Any]) -> str:
    trend = report.get("trend")
    if not isinstance(trend, list):
        return "관측값 없음"
    parts = []
    for item in trend:
        if not isinstance(item, Mapping):
            continue
        report_date = str(item.get("report_date") or "unknown")
        status = str(item.get("status") or "UNKNOWN")
        parts.append(f"{STATUS_ICONS.get(status, '◻️')} {report_date[-5:]}")
    return "  ".join(parts) or "관측값 없음"


def build_traffic_discord_payload(report: Mapping[str, Any]) -> dict[str, Any]:
    status = str(report.get("status") or "UNKNOWN").upper()
    source = report.get("source") if isinstance(report.get("source"), Mapping) else {}
    scheduled = (
        report.get("scheduled_runs")
        if isinstance(report.get("scheduled_runs"), Mapping)
        else {}
    )
    bottleneck = (
        report.get("bottleneck")
        if isinstance(report.get("bottleneck"), Mapping)
        else {}
    )
    overview = (
        f"{STATUS_ICONS.get(status, '◻️')} **{status}**\n"
        f"Data {report.get('data_plane_status', 'UNKNOWN')} · "
        f"Control {report.get('control_plane_status', 'UNKNOWN')} · "
        f"Contracts {(report.get('contract_audit') or {}).get('status', 'UNKNOWN')} · "
        f"Window {report.get('lookback_hours', 24)}h"
    )
    source_value = (
        f"Landing {scheduled.get('success', 0)}/{scheduled.get('expected', 'unknown')} "
        f"· failed {scheduled.get('failed', 0)} · running {scheduled.get('running', 0)}\n"
        f"Freshness {source.get('freshness_minutes', 'unknown')}m · "
        f"coverage {source.get('coverage_percent', 'unknown')}% · "
        f"pending {source.get('pending_count', 'unknown')}\n"
        f"Incident/Flow publishable={source.get('publishability_ok', False)}"
    )
    if bottleneck:
        bottleneck_value = (
            f"{STATUS_ICONS.get(str(bottleneck.get('status')), '◻️')} "
            f"{bottleneck.get('label') or bottleneck.get('key')} · "
            f"task runtime p95 {_duration(bottleneck.get('p95_ms'))}"
        )
    else:
        bottleneck_value = "관측값 없음"
    embed = {
        "title": _truncate(
            f"Traffic Pipeline Reliability · {report.get('report_date', 'unknown')}",
            256,
        ),
        "description": "서울시 도로 돌발·소통정보 end-to-end 24시간 SLO",
        "color": STATUS_COLORS.get(status, DISCORD_YELLOW),
        "fields": [
            _field("상태", overview, inline=True),
            _field("수집 품질", source_value, inline=True),
            _field("파이프라인", _pipeline_value(report)),
            _field("관측 병목", bottleneck_value),
            _field("7일 추세", _trend_value(report)),
        ],
        "footer": {
            "text": "매일 09:00 KST · DAG 실패는 즉시 별도 알림 · dev"
        },
    }
    if report.get("detected_at"):
        embed["timestamp"] = str(report["detected_at"])
    return {"embeds": [embed]}
