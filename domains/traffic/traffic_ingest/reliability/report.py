from __future__ import annotations

from datetime import datetime
from typing import Any

from .config import (
    KST,
    TRAFFIC_AUDIT_TABLE,
    TRAFFIC_BRONZE_DAG_ID,
    TRAFFIC_LANDING_DAG_ID,
    TRAFFIC_TABLE,
    report_config,
)
from .backlog import collect_materialization_backlog
from .ledger import collect_scheduled_run_summary
from .trino_repository import (
    _qualified,
    collect_dag_run_summary,
    collect_traffic_summary,
    trino_cursor,
)


def build_traffic_reliability_report(
    cursor=None,
    detected_at: datetime | None = None,
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
        scheduled_runs = collect_scheduled_run_summary(
            TRAFFIC_LANDING_DAG_ID,
            detected_at,
            config,
        )
    except Exception as exc:
        scheduled_runs = {
            "expected": None,
            "success": 0,
            "failed": 0,
            "running": 0,
            "grace": 0,
            "failures": [],
            "reason": "run_ledger_query_failed",
            "error_type": type(exc).__name__,
        }
    try:
        materialization_backlog = collect_materialization_backlog(
            detected_at,
            config,
        )
    except Exception as exc:
        materialization_backlog = {
            "count": None,
            "oldest_snapshot_at": None,
            "oldest_age_minutes": None,
            "status": "FAIL",
            "reason": "receipt_backlog_query_failed",
            "error_type": type(exc).__name__,
        }

    manifest_query_ok = not dag_runs.get("reason")
    scheduled_reason = scheduled_runs.get("reason")
    scheduled_query_ok = scheduled_reason in {None, "run_ledger_bootstrapping"}
    scheduled_failures_ok = int(scheduled_runs.get("failed") or 0) == 0
    publishability_ok = (
        dag_runs.get("latest_terminal_status") == "SUCCESS"
        and dag_runs.get("latest_terminal_is_publishable") is True
    )
    dag_runs["publishability_ok"] = publishability_ok
    late_publishability = {
        "status": "NOT_EVALUATED",
        "reason": "bounded late-repair contract is owned by ASAC-DBT #117",
    }
    if (
        not manifest_query_ok
        or not scheduled_query_ok
        or not scheduled_failures_ok
        or not publishability_ok
        or materialization_backlog.get("status") == "FAIL"
    ):
        status = "FAIL"
    else:
        status = str(traffic.get("status") or "FAIL")
        if status == "PASS" and scheduled_reason == "run_ledger_bootstrapping":
            status = "WARN"
        if status == "PASS" and materialization_backlog.get("status") == "WARN":
            status = "WARN"

    return {
        "report_name": "traffic_bronze_reliability",
        "detected_at": detected_at.isoformat(),
        "catalog": config.catalog,
        "schema": config.schema,
        "lookback_hours": config.lookback_hours,
        "status": status,
        "traffic": traffic,
        "dag_runs": dag_runs,
        "scheduled_runs": scheduled_runs,
        "materialization_backlog": materialization_backlog,
        "publishability_ok": publishability_ok,
        "late_publishability": late_publishability,
        "blast_radius": [
            _qualified(config, TRAFFIC_TABLE),
            _qualified(config, TRAFFIC_AUDIT_TABLE),
        ],
    }
