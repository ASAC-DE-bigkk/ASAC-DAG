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
from weather_lineage import enable_lineage_if_configured  # noqa: E402

from weather_ingest.reliability_report import (  # noqa: E402
    KST,
    build_weather_reliability_report,
    format_weather_discord_message,
    report_dag_schedule,
    send_discord_message,
)


# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
# 리포트 DAG 은 외부 소스 API 를 호출하지 않으므로 source_system 은 생략한다.
record_weather_problem = problem_failure_callback(domain="weather")
DELIVERY_FINGERPRINT_VARIABLE = (
    "ask_seoul.weather.bronze_reliability.delivery_fingerprint"
)


def notification_fingerprint(report: dict) -> str:
    """Hash status and stable failure identity, excluding observation timestamps."""
    weather = report.get("weather") or {}
    dag_runs = report.get("dag_runs") or {}
    identity = {"status": str(report.get("status") or "FAIL")}
    if weather.get("status") != "PASS":
        identity["weather"] = {
            "status": weather.get("status"),
            "reason": weather.get("reason"),
            "coverage_ok": weather.get("coverage_ok"),
            "freshness_status": weather.get("freshness_status"),
        }
    if dag_runs.get("reason") or not bool(report.get("publishability_ok")):
        identity["manifest"] = {
            "reason": dag_runs.get("reason"),
            "dag_run_id": dag_runs.get("latest_dag_run_id"),
            "status": dag_runs.get("latest_status"),
            "is_publishable": dag_runs.get("latest_is_publishable"),
        }
    payload = json.dumps(
        identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def should_notify_fingerprint(fingerprint: str, *, get=Variable.get) -> bool:
    """Compare with the last delivered fingerprint; Variable reads fail open."""
    try:
        return get(DELIVERY_FINGERPRINT_VARIABLE, "UNKNOWN") != fingerprint
    except Exception:  # state tracking must never suppress an alert
        return True


def record_delivered_fingerprint(fingerprint: str, *, set=Variable.set) -> bool:
    """Persist only a confirmed delivery; Variable writes fail open."""
    try:
        set(DELIVERY_FINGERPRINT_VARIABLE, fingerprint)
        return True
    except Exception:
        return False


@track(layer="bronze", domain="weather")
def collect_and_notify(**context) -> dict:
    report = build_weather_reliability_report()
    fingerprint = notification_fingerprint(report)
    should_notify = should_notify_fingerprint(fingerprint)
    discord_sent = False
    state_recorded = False
    if should_notify:
        message = format_weather_discord_message(report)
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
    report["notification_fingerprint"] = fingerprint
    report["notification_state_recorded"] = state_recorded
    report["notification_reason"] = notification_reason
    report["dag_run_id"] = context.get("run_id")
    return report


with DAG(
    dag_id="weather_bronze_reliability_report",
    description="Scheduled weather Bronze freshness, coverage, and Discord reliability report.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=report_dag_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["ask_seoul", "weather", "bronze", "reliability", "discord"],
) as dag:
    send_report = PythonOperator(
        task_id="send_weather_bronze_reliability_report",
        python_callable=collect_and_notify,
        on_failure_callback=record_weather_problem,
    )


enable_lineage_if_configured(dag)
