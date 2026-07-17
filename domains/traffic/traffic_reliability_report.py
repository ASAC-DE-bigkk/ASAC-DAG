import hashlib
import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
DAGS_ROOT_DIR = os.path.dirname(os.path.dirname(DAG_DIR))
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runmetrics import track  # noqa: E402
from traffic_lineage import enable_lineage_if_configured  # noqa: E402

from traffic_ingest.reliability_report import (  # noqa: E402
    KST,
    build_traffic_reliability_report,
    format_traffic_discord_message,
    report_dag_schedule,
    scheduled_failure_identities,
    send_discord_message,
)


# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_traffic_problem = problem_failure_callback(domain="traffic")
DELIVERY_FINGERPRINT_VARIABLE = (
    "ask_seoul.traffic.bronze_reliability.delivery_fingerprint"
)


def notification_fingerprint(report: dict) -> str:
    """Hash status and stable failure identity, excluding observation timestamps."""
    traffic = report.get("traffic") or {}
    dag_runs = report.get("dag_runs") or {}
    scheduled_runs = report.get("scheduled_runs") or {}
    identity = {"status": str(report.get("status") or "FAIL")}
    if traffic.get("status") != "PASS":
        identity["traffic"] = {
            "status": traffic.get("status"),
            "reason": traffic.get("reason"),
            "coverage_ok": traffic.get("coverage_ok"),
            "freshness_status": traffic.get("freshness_status"),
        }
    if dag_runs.get("reason") or not bool(report.get("publishability_ok")):
        identity["manifest"] = {
            "reason": dag_runs.get("reason"),
            "dag_run_id": dag_runs.get("latest_terminal_dag_run_id"),
            "status": dag_runs.get("latest_terminal_status"),
            "is_publishable": dag_runs.get("latest_terminal_is_publishable"),
        }
    scheduled_incidents = scheduled_failure_identities(
        list(scheduled_runs.get("failures") or [])
    )
    scheduled_failed_count = int(scheduled_runs.get("failed") or 0)
    if scheduled_runs.get("reason") or scheduled_failed_count:
        identity["scheduled_runs"] = {
            "reason": scheduled_runs.get("reason"),
            "error_type": scheduled_runs.get("error_type"),
            "incidents": scheduled_incidents,
        }
        if not scheduled_incidents:
            identity["scheduled_runs"]["failed_count"] = scheduled_failed_count
    payload = json.dumps(
        identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def daily_delivery_fingerprint(
    _report: dict,
    *,
    logical_date: datetime | None,
    run_id: str | None,
) -> str:
    """Return one idempotency key per scheduled KST day, independent of status."""
    if logical_date is not None:
        delivery_key = f"date:{logical_date.astimezone(KST).date().isoformat()}"
    else:
        delivery_key = f"run:{run_id or 'unknown'}"
    payload = json.dumps(
        {"delivery_key": delivery_key, "report_contract": "bronze-reliability-daily-v1"},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def should_notify_fingerprint(fingerprint: str, *, get=Variable.get) -> bool:
    """Check the delivered daily fingerprint history; Variable reads fail open."""
    try:
        raw = get(DELIVERY_FINGERPRINT_VARIABLE, "[]")
        try:
            decoded = json.loads(raw)
        except (TypeError, ValueError):
            # Previous releases stored one plain fingerprint. Preserve that entry.
            decoded = [raw] if raw and raw != "UNKNOWN" else []
        if not isinstance(decoded, list):
            return True
        return fingerprint not in {value for value in decoded if isinstance(value, str)}
    except Exception:  # state tracking must never suppress an alert
        return True


def record_delivered_fingerprint(
    fingerprint: str, *, get=Variable.get, set=Variable.set
) -> bool:
    """Persist every confirmed daily delivery; Variable writes fail open.

    This deliberately provides at-least-once delivery if state storage fails:
    claiming before Discord confirms delivery could silently lose that day's report.
    """
    try:
        raw = get(DELIVERY_FINGERPRINT_VARIABLE, "[]")
        try:
            decoded = json.loads(raw)
        except (TypeError, ValueError):
            decoded = [raw] if raw and raw != "UNKNOWN" else []
        history = {value for value in decoded if isinstance(value, str)}
        history.add(fingerprint)
        # max_active_runs=1 serializes normal report runs, so this read-modify-write
        # ledger retains every logical day's idempotency key without a claim race.
        set(
            DELIVERY_FINGERPRINT_VARIABLE,
            json.dumps(sorted(history), ensure_ascii=False, separators=(",", ":")),
        )
        return True
    except Exception:
        return False


@track(layer="bronze", domain="traffic")
def collect_and_notify(**context) -> dict:
    report = build_traffic_reliability_report()
    notification = notification_fingerprint(report)
    fingerprint = daily_delivery_fingerprint(
        report,
        logical_date=context.get("logical_date"),
        run_id=context.get("run_id"),
    )
    should_notify = should_notify_fingerprint(fingerprint)
    discord_sent = False
    state_recorded = False
    if should_notify:
        message = format_traffic_discord_message(report)
        try:
            discord_sent = send_discord_message(message) is True
        except Exception:
            discord_sent = False
        if discord_sent:
            state_recorded = record_delivered_fingerprint(fingerprint)
    if not should_notify:
        notification_reason = "fingerprint_unchanged"
    elif not discord_sent:
        notification_reason = "delivery_failed"
    elif state_recorded:
        notification_reason = "delivery_succeeded"
    else:
        notification_reason = "delivery_succeeded_state_unavailable"
    report["discord_sent"] = discord_sent
    report["notification_fingerprint"] = notification
    report["delivery_fingerprint"] = fingerprint
    report["notification_state_recorded"] = state_recorded
    report["notification_reason"] = notification_reason
    report["dag_run_id"] = context.get("run_id")
    return report


with DAG(
    dag_id="traffic_bronze_reliability_report",
    description="Scheduled traffic Bronze freshness, coverage, and Discord reliability report.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=report_dag_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["ask_seoul", "traffic", "bronze", "reliability", "discord"],
) as dag:
    send_report = PythonOperator(
        task_id="send_traffic_bronze_reliability_report",
        python_callable=collect_and_notify,
        on_failure_callback=record_traffic_problem,
    )


enable_lineage_if_configured(dag)
