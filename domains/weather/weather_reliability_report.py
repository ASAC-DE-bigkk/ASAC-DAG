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
STATUS_VARIABLE = "ask_seoul.weather.bronze_reliability.status"


def should_notify_status_change(status: str, *, get=Variable.get, set=Variable.set) -> bool:
    """Notify on a state transition; fail open if Airflow Variables are unavailable."""
    try:
        previous = get(STATUS_VARIABLE, "UNKNOWN")
        if previous == status:
            return False
        set(STATUS_VARIABLE, status)
        return True
    except Exception:  # state tracking must never suppress an alert
        return True


@track(layer="bronze", domain="weather")
def collect_and_notify(**context) -> dict:
    report = build_weather_reliability_report()
    status_changed = should_notify_status_change(report["status"])
    report["discord_sent"] = (
        send_discord_message(format_weather_discord_message(report)) if status_changed else False
    )
    report["notification_reason"] = "status_changed" if status_changed else "status_unchanged"
    report["dag_run_id"] = context.get("run_id")
    return report


with DAG(
    dag_id="weather_bronze_reliability_report",
    description="Daily weather Bronze freshness, coverage, and Discord reliability report.",
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
