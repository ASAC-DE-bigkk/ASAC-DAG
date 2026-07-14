from __future__ import annotations

from datetime import datetime
from typing import Any

from .config import KST, WEATHER_BRONZE_DAG_ID, WEATHER_TABLE, report_config
from .trino_repository import (
    _qualified,
    collect_dag_run_summary,
    collect_weather_summary,
    trino_cursor,
)


def build_weather_reliability_report(
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
            "error": str(exc),
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
            "error": str(exc),
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
