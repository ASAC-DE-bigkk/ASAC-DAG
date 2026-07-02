"""Airflow DAG: traffic silver/gold transform via dbt.

The bronze DAG stores TOPIS AccInfo raw XML in R2 and publishes verified
Iceberg bronze runs. This transform DAG consumes only publishable bronze runs
through the dbt models and keeps transform retries independent from API calls.
"""

from __future__ import annotations

import os
import shlex
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


KST = ZoneInfo("Asia/Seoul")
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/traffic"
DEFAULT_PARAMS = {"target": "dev"}
DEFAULT_TRANSFORM_CRON_KST = "*/15 * * * *"


def is_dev_target() -> bool:
    return os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"


def transform_schedule() -> str | None:
    if "ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE"] or None
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
    dag_id="traffic_incident_transform",
    description="Transform traffic bronze -> silver/gold via dbt.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=transform_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "traffic", "transform", "silver", "gold", "dbt"],
) as dag:
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=dbt_command("run --select silver_seoul_traffic_incident"),
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=dbt_command(
            "test --select "
            "silver_seoul_traffic_incident "
            "assert_silver_traffic_uses_publishable_runs "
            "assert_silver_traffic_location_contract "
            "assert_traffic_audit_covers_latest_total_count "
            "assert_silver_seoul_traffic_incident_grain_unique"
        ),
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=dbt_command("run --select gold_traffic_incident_summary"),
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=dbt_command(
            "test --select "
            "gold_traffic_incident_summary "
            "assert_gold_traffic_counts_match_silver "
            "assert_gold_traffic_row_counts_positive"
        ),
    )

    dbt_run_silver >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold
