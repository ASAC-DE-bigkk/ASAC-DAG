import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)

from reliability_report.report import (  # noqa: E402
    KST,
    build_reliability_report,
    format_discord_message,
    report_dag_schedule,
    send_discord_message,
)


def collect_and_notify(**context) -> dict:
    report = build_reliability_report()
    message = format_discord_message(report)
    sent = send_discord_message(message)
    print(message)
    report["discord_sent"] = sent
    report["dag_run_id"] = context["run_id"]
    return report


with DAG(
    dag_id="weather_traffic_bronze_reliability_report",
    description="Daily weather/traffic Bronze freshness, coverage, and Discord reliability report.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=report_dag_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["ask_seoul", "observability", "weather", "traffic", "discord"],
) as dag:
    send_report = PythonOperator(
        task_id="send_weather_traffic_reliability_report",
        python_callable=collect_and_notify,
    )
