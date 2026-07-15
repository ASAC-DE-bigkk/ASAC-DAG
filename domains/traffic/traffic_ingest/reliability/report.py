from __future__ import annotations

from datetime import datetime
from typing import Any

from .airflow_evidence import (
    _redacted_airflow_metadata_log_url,
    collect_airflow_scheduled_run_summary,
)
from .config import (
    KST,
    TRAFFIC_AUDIT_TABLE,
    TRAFFIC_BRONZE_DAG_ID,
    TRAFFIC_TABLE,
    report_config,
)
from .trino_repository import (
    _qualified,
    collect_dag_run_summary,
    collect_traffic_summary,
    trino_cursor,
)


def build_traffic_reliability_report(
    cursor=None,
    detected_at: datetime | None = None,
    *,
    airflow_metadata_log_url: str | None = None,
) -> dict[str, Any]:
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
        dag_runs = collect_dag_run_summary(
            cursor, config, TRAFFIC_BRONZE_DAG_ID, detected_at
        )
    except Exception as exc:
        dag_runs = {
            "dag_id": TRAFFIC_BRONZE_DAG_ID,
            "success": 0,
            "failed": 0,
            "running": 0,
            "reason": "dag_run_query_failed",
            "error": str(exc),
        }
    try:
        airflow_runs = collect_airflow_scheduled_run_summary(
            TRAFFIC_BRONZE_DAG_ID,
            detected_at,
            config.lookback_hours,
        )
    except Exception as exc:
        # The failure itself is reportable, but exception details may contain
        # credentials or connection URLs and must not reach Discord.
        airflow_runs = {
            "expected": None,
            "success": 0,
            "failed": 0,
            "running": 0,
            "failures": [],
            "reason": "airflow_metadata_query_failed",
            "error_type": type(exc).__name__,
        }
        diagnostic_log_url = _redacted_airflow_metadata_log_url(
            airflow_metadata_log_url
        )
        if diagnostic_log_url:
            airflow_runs["diagnostic_log_url"] = diagnostic_log_url

    manifest_query_ok = not dag_runs.get("reason")
    airflow_query_ok = not airflow_runs.get("reason")
    airflow_failures_ok = int(airflow_runs.get("failed") or 0) == 0
    publishability_ok = (
        dag_runs.get("latest_status") == "SUCCESS"
        and dag_runs.get("latest_is_publishable") is True
    )
    dag_runs["publishability_ok"] = publishability_ok
    late_publishability = {
        "status": "NOT_EVALUATED",
        "reason": "bounded late-repair contract is owned by ASAC-DBT #117",
    }
    if (
        not manifest_query_ok
        or not airflow_query_ok
        or not airflow_failures_ok
        or not publishability_ok
    ):
        status = "FAIL"
    else:
        status = str(traffic.get("status") or "FAIL")

    return {
        "report_name": "traffic_bronze_reliability",
        "detected_at": detected_at.isoformat(),
        "catalog": config.catalog,
        "schema": config.schema,
        "lookback_hours": config.lookback_hours,
        "status": status,
        "traffic": traffic,
        "dag_runs": dag_runs,
        "airflow_runs": airflow_runs,
        "publishability_ok": publishability_ok,
        "late_publishability": late_publishability,
        "blast_radius": [
            _qualified(config, TRAFFIC_TABLE),
            _qualified(config, TRAFFIC_AUDIT_TABLE),
        ],
    }
