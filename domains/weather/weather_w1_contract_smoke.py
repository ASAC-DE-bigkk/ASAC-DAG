"""Manual, isolated smoke validation for the Weather W1 bridge contract."""

from __future__ import annotations

import hashlib
import os
import re
import shlex
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


WEATHER_DIR = os.path.dirname(os.path.abspath(__file__))
if WEATHER_DIR not in sys.path:
    sys.path.insert(0, WEATHER_DIR)
DAGS_ROOT_DIR = os.path.dirname(os.path.dirname(WEATHER_DIR))
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from weather_ingest.common.runtime import trino_cursor  # noqa: E402
from weather_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/weather"
SMOKE_CATALOG = "iceberg_dev"
SMOKE_SCHEMA_PATTERN = re.compile(r"^dev_weather_w1_weather_contract_test_[0-9a-f]{24}$")
SMOKE_SCHEMA_TASK_ID = "create_isolated_schema"
DEFAULT_PARAMS = {
    "target": Param(
        default="dev",
        type="string",
        enum=["dev"],
        description="dbt target profile name for the isolated dev smoke only.",
    )
}
record_weather_problem = problem_failure_callback(domain="weather")


def smoke_schema_name(run_id: str) -> str:
    digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:24]
    return f"dev_weather_w1_weather_contract_test_{digest}"


def _safe_smoke_schema(schema: str) -> str:
    if not SMOKE_SCHEMA_PATTERN.fullmatch(schema):
        raise ValueError(f"Unsafe smoke schema: {schema!r}")
    return schema


def _smoke_cursor():
    cursor, catalog, _schema = trino_cursor()
    if catalog != SMOKE_CATALOG:
        raise RuntimeError(f"Weather W1 smoke requires {SMOKE_CATALOG}, got {catalog}.")
    return cursor, catalog


def create_weather_w1_smoke_schema(**context) -> str:
    schema = _safe_smoke_schema(smoke_schema_name(str(context["run_id"])))
    cursor, catalog = _smoke_cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"[weather-w1-smoke] created isolated schema {catalog}.{schema}")
    return schema


def cleanup_weather_w1_smoke_schema(*, ti, run_id: str | None = None, **_context) -> None:
    schema = ti.xcom_pull(task_ids=SMOKE_SCHEMA_TASK_ID)
    if not schema and run_id:
        schema = smoke_schema_name(run_id)
        print("[weather-w1-smoke] schema XCom missing; recomputed from run_id")
    if not schema:
        print("[weather-w1-smoke] no schema XCom or run_id; cleanup skipped")
        return
    schema = _safe_smoke_schema(str(schema))
    cursor, catalog = _smoke_cursor()
    cursor.execute(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
    print(f"[weather-w1-smoke] dropped isolated schema {catalog}.{schema}")


def dbt_smoke_command(args: str) -> str:
    project = shlex.quote(DBT_PROJECT)
    schema = "{{ ti.xcom_pull(task_ids='create_isolated_schema') }}"
    return (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"export WEATHER_SCHEMA='{schema}' ASK_SEOUL_SCHEMA='{schema}'\n"
        f"{shlex.quote(DBT_BIN)} {args} --target '{{{{ params.target }}}}' "
        "--vars '{\"weather_w1_initial_build_mode\": \"bounded_isolated_smoke\"}' "
        "--no-use-colors"
    )


with DAG(
    dag_id="weather_w1_contract_smoke",
    description="Run Weather W1 bridge contracts in an isolated schema and always remove it.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "dbt", "smoke", "isolated", "w1"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "weather", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_weather_problem,
    )

    create_isolated_schema = PythonOperator(
        task_id=SMOKE_SCHEMA_TASK_ID,
        python_callable=create_weather_w1_smoke_schema,
        pool=TRINO_HEAVY_POOL,
        on_failure_callback=record_weather_problem,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_smoke_command("deps"),
        pool=TRINO_HEAVY_POOL,
        on_failure_callback=record_weather_problem,
    )

    dbt_seed_bridge_inputs = BashOperator(
        task_id="dbt_seed_bridge_inputs",
        pool=TRINO_HEAVY_POOL,
        bash_command=dbt_smoke_command(
            "seed --select asac_axes weather_place_grid_mapping weather_admin_dong_grid_bridge_history"
        ),
        on_failure_callback=record_weather_problem,
    )

    dbt_run_common_admin_dong_dimension = BashOperator(
        task_id="dbt_run_common_admin_dong_dimension",
        bash_command=dbt_smoke_command("run --select asac_axes.dim_admin_dong"),
        pool=TRINO_HEAVY_POOL,
        on_failure_callback=record_weather_problem,
    )

    dbt_run_bridge = BashOperator(
        task_id="dbt_run_bridge",
        bash_command=dbt_smoke_command("run --select bridge_weather_admin_dong_grid"),
        pool=TRINO_HEAVY_POOL,
        on_failure_callback=record_weather_problem,
    )

    dbt_test_bridge_contract = BashOperator(
        task_id="dbt_test_bridge_contract",
        pool=TRINO_HEAVY_POOL,
        bash_command=dbt_smoke_command(
            "test --select "
            "assert_weather_bridge_candidate_grain_unique "
            "assert_weather_bridge_canonical_stamp_exact "
            "assert_weather_bridge_legacy_mapping_reconciles "
            "assert_weather_bridge_temporal_evidence "
            "assert_weather_bridge_validity_non_overlapping"
        ),
        on_failure_callback=record_weather_problem,
    )

    cleanup_isolated_schema = PythonOperator(
        task_id="cleanup_isolated_schema",
        python_callable=cleanup_weather_w1_smoke_schema,
        pool=TRINO_HEAVY_POOL,
        trigger_rule=TriggerRule.ALL_DONE,
        on_failure_callback=record_weather_problem,
    )

    (
        validate_runtime
        >> create_isolated_schema
        >> dbt_deps
        >> dbt_seed_bridge_inputs
        >> dbt_run_common_admin_dong_dimension
        >> dbt_run_bridge
        >> dbt_test_bridge_contract
        >> cleanup_isolated_schema
    )
