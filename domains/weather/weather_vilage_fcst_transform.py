"""Airflow DAG: weather silver/gold transform via dbt.

The bronze DAG stores KMA raw payloads in R2 and publishes verified Iceberg
bronze runs. This transform DAG consumes only publishable bronze runs through
the dbt models and keeps silver/gold retries independent from API collection.
"""

from __future__ import annotations

import os
import shlex
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
DAGS_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/weather"
DEFAULT_PARAMS = {"target": "dev"}
DEFAULT_TRANSFORM_CRON_KST = "35 2,5,8,11,14,17,20,23 * * *"

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
# dbt transform 은 외부 소스 API 를 호출하지 않으므로 source_system 은 생략한다.
record_weather_problem = problem_failure_callback(domain="weather")


def is_dev_target() -> bool:
    return os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"


def transform_schedule() -> str | None:
    if "ASK_SEOUL_WEATHER_TRANSFORM_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_WEATHER_TRANSFORM_DAG_SCHEDULE"] or None
    return DEFAULT_TRANSFORM_CRON_KST if is_dev_target() else None


def dbt_command(args: str) -> str:
    project = shlex.quote(DBT_PROJECT)
    return (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"{shlex.quote(DBT_BIN)} {args} --target {{{{ params.target }}}} --no-use-colors"
    )


with DAG(
    dag_id="weather_vilage_fcst_transform",
    description="Transform weather bronze -> silver/gold via dbt.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=transform_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "transform", "silver", "gold", "dbt"],
) as dag:
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=dbt_command("run --select silver_kma_vilage_fcst"),
        on_failure_callback=record_weather_problem,
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=dbt_command(
            "test --select "
            "silver_kma_vilage_fcst "
            "assert_silver_kma_vilage_fcst_grain_unique "
            "assert_silver_kma_vilage_fcst_grid_coverage "
            "assert_silver_kma_uses_publishable_runs"
        ),
        on_failure_callback=record_weather_problem,
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=dbt_command("run --select gold_weather_forecast_summary"),
        on_failure_callback=record_weather_problem,
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=dbt_command(
            "test --select "
            "gold_weather_forecast_summary "
            "assert_gold_weather_counts_match_silver "
            "assert_gold_weather_row_counts_positive"
        ),
        on_failure_callback=record_weather_problem,
    )

    dbt_run_silver >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold
