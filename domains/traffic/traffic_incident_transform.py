"""Airflow DAG: traffic silver/gold transform via dbt.

The bronze DAG stores TOPIS AccInfo raw XML in R2 and publishes verified
Iceberg bronze runs. This transform DAG consumes only publishable bronze runs
through the dbt models and keeps transform retries independent from API calls.
"""

from __future__ import annotations

import os
import shlex
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
DAGS_ROOT_DIR = os.path.dirname(os.path.dirname(DAG_DIR))
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.assets import TRAFFIC_BRONZE_ASSET  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/traffic"
# Bronze runs every five minutes. Keep the hourly transform outside that boundary
# so a run consumes one stable, publishable Bronze snapshot.
TRAFFIC_TRANSFORM_CRON_KST = "12 * * * *"
DEFAULT_PARAMS = {
    "target": Param(
        default="dev",
        type="string",
        enum=["dev"],
        description="dbt target profile name (dev only until production rollout).",
    )
}
# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_traffic_problem = problem_failure_callback(domain="traffic")


def transform_schedule() -> str | None:
    if "ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE"] or None
    return TRAFFIC_TRANSFORM_CRON_KST


def dbt_command(args: str) -> str:
    project = shlex.quote(DBT_PROJECT)
    return (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"{shlex.quote(DBT_BIN)} {args} --target '{{{{ params.target }}}}' --no-use-colors"
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
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "traffic", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_traffic_problem,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_command("deps"),
        on_failure_callback=record_traffic_problem,
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=dbt_command("source freshness"),
        on_failure_callback=record_traffic_problem,
    )

    dbt_test_traffic_incident_availability = BashOperator(
        task_id="dbt_test_traffic_incident_availability",
        bash_command=dbt_command("test --select assert_traffic_incident_row_availability"),
        on_failure_callback=record_traffic_problem,
    )

    dbt_seed_asac_axes = BashOperator(
        task_id="dbt_seed_asac_axes",
        bash_command=dbt_command("seed --select asac_axes"),
        on_failure_callback=record_traffic_problem,
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=dbt_command(
            "run --select silver_seoul_traffic_incident silver_seoul_traffic_incident_current"
        ),
        on_failure_callback=record_traffic_problem,
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=dbt_command(
            "test --select "
            "silver_seoul_traffic_incident "
            "silver_seoul_traffic_incident_current "
            "assert_traffic_current_latest_publishable_run "
            "assert_silver_traffic_uses_publishable_runs "
            "assert_silver_traffic_location_contract "
            "assert_traffic_audit_covers_latest_total_count "
            "assert_silver_seoul_traffic_incident_grain_unique "
            "assert_silver_traffic_event_at_matches_occurred_at "
            "assert_silver_traffic_wgs84_required_when_source_coordinate_available "
            "assert_silver_traffic_admin_axis_consistent "
            "assert_silver_traffic_admin_axis_coverage "
            "assert_silver_traffic_latest_publishable_record "
            # gold-silver 교차 카운트 테스트는 silver 모델명 셀렉터가 참조 테스트로
            # 끌어오지만, 이 단계에서는 gold가 직전 사이클 상태라 silver 가 갱신된
            # 사이클마다 구조적으로 FAIL 한다. gold 재빌드 후 dbt_test_gold 에서만 돌린다.
            "--exclude assert_gold_traffic_counts_match_silver"
        ),
        on_failure_callback=record_traffic_problem,
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=dbt_command("run --select gold_traffic_incident_summary"),
        on_failure_callback=record_traffic_problem,
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=dbt_command(
            "test --select "
            "gold_traffic_incident_summary "
            "assert_gold_traffic_counts_match_silver "
            "assert_gold_traffic_row_counts_positive"
        ),
        on_failure_callback=record_traffic_problem,
    )

    (
        validate_runtime
        >> dbt_deps
        >> dbt_source_freshness
        >> dbt_test_traffic_incident_availability
        >> dbt_seed_asac_axes
        >> dbt_run_silver
        >> dbt_test_silver
        >> dbt_run_gold
        >> dbt_test_gold
    )
