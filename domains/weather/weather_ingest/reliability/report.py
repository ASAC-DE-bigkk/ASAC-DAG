from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .config import (
    KST,
    MARQUEZ_BASE_URL,
    MARQUEZ_NAMESPACE,
    WEATHER_BRONZE_DAG_ID,
    WEATHER_PIPELINE_STAGE_POLICIES,
    WEATHER_TABLE,
    report_config,
)
from .history import load_recent_history
from .lineage import collect_pipeline_stages
from .trino_repository import (
    _qualified,
    collect_dag_run_summary,
    collect_weather_summary,
    trino_cursor,
)


def collect_weather_data_plane(
    cursor=None, detected_at: datetime | None = None
) -> dict[str, Any]:
    config = report_config()
    cursor = cursor or trino_cursor()
    detected_at = detected_at or datetime.now(KST)
    try:
        weather = collect_weather_summary(cursor, config, detected_at)
    except Exception as exc:
        weather = {
            "status": "FAIL",
            "reason": "weather_query_failed",
            "error_type": type(exc).__name__,
            "table": _qualified(config, WEATHER_TABLE),
        }
    try:
        dag_runs = collect_dag_run_summary(
            cursor, config, WEATHER_BRONZE_DAG_ID, detected_at
        )
    except Exception as exc:
        dag_runs = {
            "dag_id": WEATHER_BRONZE_DAG_ID,
            "success": 0,
            "failed": 0,
            "running": 0,
            "reason": "dag_run_query_failed",
            "error_type": type(exc).__name__,
        }
    publishability_ok = (
        dag_runs.get("latest_status") == "SUCCESS"
        and dag_runs.get("latest_is_publishable") is True
    )
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


def _pipeline_status(data_status: str, control_status: str) -> str:
    if "FAIL" in {data_status, control_status}:
        return "FAIL"
    if data_status in {"WARN", "UNKNOWN"} or control_status in {
        "WARN",
        "UNKNOWN",
    }:
        return "WARN"
    return "PASS"


def _select_bottleneck(stages: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates: list[tuple[int, dict[str, Any]]] = []
    for stage in stages:
        duration = stage.get("duration_ms")
        p95 = duration.get("p95") if isinstance(duration, Mapping) else None
        if isinstance(p95, (int, float)) and not isinstance(p95, bool):
            candidates.append((int(p95), stage))
    if not candidates:
        return None
    p95_ms, stage = max(candidates, key=lambda item: item[0])
    return {
        "key": stage.get("key"),
        "label": stage.get("label"),
        "status": stage.get("status"),
        "p95_ms": p95_ms,
    }


def _trend(
    history: list[dict[str, Any]], report_date: str, status: str
) -> list[dict[str, str]]:
    prior = [
        {
            "report_date": str(item.get("report_date") or "unknown"),
            "status": str(item.get("status") or "UNKNOWN"),
        }
        for item in history
        if isinstance(item, Mapping)
    ]
    return (prior + [{"report_date": report_date, "status": status}])[-7:]


def _coverage_percent(weather: Mapping[str, Any]) -> float:
    actual = int(weather.get("grid_slot_count") or 0)
    expected = int(weather.get("expected_grid_slot_count") or 0)
    if expected <= 0:
        return 0.0
    return round(min(100.0, actual * 100.0 / expected), 2)


def compose_weather_pipeline_report(
    *,
    data_plane: dict[str, Any],
    stages: dict[str, Any],
    history: list[dict[str, Any]],
    detected_at: datetime,
) -> dict[str, Any]:
    data_status = str(data_plane.get("status") or "FAIL").upper()
    control_status = str(stages.get("status") or "UNKNOWN").upper()
    status = _pipeline_status(data_status, control_status)
    stage_items = stages.get("stages")
    if not isinstance(stage_items, list):
        stage_items = []
    stage_items = [dict(item) for item in stage_items if isinstance(item, Mapping)]
    report_date = detected_at.astimezone(KST).date().isoformat()
    weather = data_plane.get("weather") or {}
    source = {
        "status": data_status,
        "freshness_minutes": weather.get("freshness_minutes"),
        "coverage_percent": _coverage_percent(weather),
        "publishability_ok": data_plane.get("publishability_ok"),
    }
    result = {
        **data_plane,
        "report_name": "weather_pipeline_reliability_v2",
        "domain": "weather",
        "report_date": report_date,
        "detected_at": detected_at.isoformat(),
        "status": status,
        "data_plane_status": data_status,
        "control_plane_status": control_status,
        "source": source,
        "stages": stage_items,
        "bottleneck": _select_bottleneck(stage_items),
    }
    result["trend"] = _trend(history, report_date, status)
    return result


def build_weather_reliability_report(
    cursor=None, detected_at: datetime | None = None
) -> dict[str, Any]:
    detected_at = detected_at or datetime.now(KST)
    data_plane = collect_weather_data_plane(cursor=cursor, detected_at=detected_at)
    stages = collect_pipeline_stages(
        policies=WEATHER_PIPELINE_STAGE_POLICIES,
        detected_at=detected_at,
        lookback_hours=int(data_plane["lookback_hours"]),
        namespace=MARQUEZ_NAMESPACE,
        base_url=MARQUEZ_BASE_URL,
    )
    try:
        history = load_recent_history(detected_at.astimezone(KST).date())
    except Exception as exc:
        history = [
            {
                "report_date": "unknown",
                "status": "UNKNOWN",
                "reason": "history_unavailable",
                "error_type": type(exc).__name__,
            }
        ]
    return compose_weather_pipeline_report(
        data_plane=data_plane,
        stages=stages,
        history=history,
        detected_at=detected_at,
    )
