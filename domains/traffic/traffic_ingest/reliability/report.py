from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .config import (
    KST,
    TRAFFIC_AUDIT_TABLE,
    TRAFFIC_BRONZE_DAG_ID,
    TRAFFIC_FLOW_AUDIT_TABLE,
    TRAFFIC_FLOW_BRONZE_DAG_ID,
    TRAFFIC_FLOW_TABLE,
    TRAFFIC_LANDING_DAG_ID,
    TRAFFIC_PIPELINE_STAGE_POLICIES,
    TRAFFIC_TABLE,
    MARQUEZ_BASE_URL,
    MARQUEZ_NAMESPACE,
    report_config,
)
from .backlog import collect_materialization_backlog
from .history import load_recent_history
from .ledger import collect_scheduled_run_summary
from .lineage import collect_pipeline_stages
from .trino_repository import (
    _qualified,
    collect_dag_run_summary,
    collect_traffic_summary,
    trino_cursor,
)


def collect_traffic_data_plane(
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
            "error_type": type(exc).__name__,
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
            "error_type": type(exc).__name__,
        }
    try:
        flow_dag_runs = collect_dag_run_summary(
            cursor, config, TRAFFIC_FLOW_BRONZE_DAG_ID, detected_at
        )
    except Exception as exc:
        flow_dag_runs = {
            "dag_id": TRAFFIC_FLOW_BRONZE_DAG_ID,
            "success": 0,
            "failed": 0,
            "running": 0,
            "reason": "dag_run_query_failed",
            "error_type": type(exc).__name__,
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

    manifest_query_ok = not dag_runs.get("reason") and not flow_dag_runs.get(
        "reason"
    )
    scheduled_reason = scheduled_runs.get("reason")
    scheduled_query_ok = scheduled_reason in {None, "run_ledger_bootstrapping"}
    scheduled_failures_ok = int(scheduled_runs.get("failed") or 0) == 0
    publishability_ok = (
        dag_runs.get("latest_terminal_status") == "SUCCESS"
        and dag_runs.get("latest_terminal_is_publishable") is True
    )
    flow_publishability_ok = (
        flow_dag_runs.get("latest_terminal_status") == "SUCCESS"
        and flow_dag_runs.get("latest_terminal_is_publishable") is True
    )
    dag_runs["publishability_ok"] = publishability_ok
    flow_dag_runs["publishability_ok"] = flow_publishability_ok
    all_publishable = publishability_ok and flow_publishability_ok
    late_publishability = {
        "status": "NOT_EVALUATED",
        "reason": "bounded late-repair contract is owned by ASAC-DBT #117",
    }
    if (
        not manifest_query_ok
        or not scheduled_query_ok
        or not scheduled_failures_ok
        or not all_publishable
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
        "flow_dag_runs": flow_dag_runs,
        "scheduled_runs": scheduled_runs,
        "materialization_backlog": materialization_backlog,
        "publishability_ok": all_publishable,
        "incident_publishability_ok": publishability_ok,
        "flow_publishability_ok": flow_publishability_ok,
        "late_publishability": late_publishability,
        "blast_radius": [
            _qualified(config, TRAFFIC_TABLE),
            _qualified(config, TRAFFIC_AUDIT_TABLE),
            _qualified(config, TRAFFIC_FLOW_TABLE),
            _qualified(config, TRAFFIC_FLOW_AUDIT_TABLE),
        ],
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


def compose_traffic_pipeline_report(
    *,
    data_plane: dict[str, Any],
    stages: dict[str, Any],
    history: list[dict[str, Any]],
    detected_at: datetime,
    contract_audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data_status = str(data_plane.get("status") or "FAIL").upper()
    control_status = str(stages.get("status") or "UNKNOWN").upper()
    status = _pipeline_status(data_status, control_status)
    if contract_audit is not None:
        audit_status = str(contract_audit.get("status") or "FAIL").upper()
        status = _pipeline_status(status, audit_status)
    stage_items = stages.get("stages")
    if not isinstance(stage_items, list):
        stage_items = []
    stage_items = [dict(item) for item in stage_items if isinstance(item, Mapping)]
    report_date = detected_at.astimezone(KST).date().isoformat()
    traffic = data_plane.get("traffic") or {}
    backlog = data_plane.get("materialization_backlog") or {}
    source = {
        "status": data_status,
        "freshness_minutes": traffic.get("freshness_minutes"),
        "coverage_percent": 100.0 if traffic.get("coverage_ok") else 0.0,
        "pending_count": backlog.get("count"),
        "duplicate_keys": data_plane.get("duplicate_keys"),
        "publishability_ok": data_plane.get("publishability_ok"),
    }
    result = {
        **data_plane,
        "report_name": "traffic_pipeline_reliability_v2",
        "domain": "traffic",
        "report_date": report_date,
        "detected_at": detected_at.isoformat(),
        "status": status,
        "data_plane_status": data_status,
        "control_plane_status": control_status,
        "source": source,
        "stages": stage_items,
        "bottleneck": _select_bottleneck(stage_items),
    }
    if contract_audit is not None:
        result["contract_audit"] = dict(contract_audit)
    result["trend"] = _trend(history, report_date, status)
    return result


def build_traffic_reliability_report(
    cursor=None,
    detected_at: datetime | None = None,
) -> dict[str, Any]:
    detected_at = detected_at or datetime.now(KST)
    data_plane = collect_traffic_data_plane(cursor=cursor, detected_at=detected_at)
    stages = collect_pipeline_stages(
        policies=TRAFFIC_PIPELINE_STAGE_POLICIES,
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
    return compose_traffic_pipeline_report(
        data_plane=data_plane,
        stages=stages,
        history=history,
        detected_at=detected_at,
    )
