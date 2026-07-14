"""Manual, isolated smoke validation for the Weather W1 bridge contract."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
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
import weather_dbt_execution as weather_dbt  # noqa: E402
from weather_lineage import enable_lineage_if_configured  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
DBT_BIN = weather_dbt.dbt_bin()
DBT_PROJECT = weather_dbt.dbt_project_dir()
SMOKE_CATALOG = "iceberg_dev"
SMOKE_SCHEMA_PATTERN = re.compile(
    r"^dev_weather_w1_weather_contract_test_[0-9a-f]{24}$"
)
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


def cleanup_weather_w1_smoke_schema(
    *, ti, run_id: str | None = None, **_context
) -> None:
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


def run_dbt_smoke_phase(
    *, dbt_command: str, selection: str | None, **context
) -> dict[str, object]:
    """Run one W1 phase in the current run's isolated schema and artifacts."""
    ti = context["ti"]
    schema = _safe_smoke_schema(str(ti.xcom_pull(task_ids=SMOKE_SCHEMA_TASK_ID)))
    environ = os.environ.copy()
    environ["WEATHER_SCHEMA"] = schema
    environ["ASK_SEOUL_SCHEMA"] = schema
    environ["ASAC_AXES_SCHEMA"] = schema
    execution = weather_dbt.execute_dbt_phase(
        dbt_command=dbt_command,
        selection=selection,
        pipeline="weather-w1-contract-smoke",
        run_id=context.get("run_id"),
        task_id=getattr(ti, "task_id", None),
        try_number=getattr(ti, "try_number", None),
        target=(context.get("params") or {}).get("target", "dev"),
        variables=json.dumps(
            {"weather_w1_initial_build_mode": "bounded_isolated_smoke"},
            separators=(",", ":"),
        ),
        project_dir=DBT_PROJECT,
        executable=DBT_BIN,
        runner=subprocess.run,
        environ=environ,
    )
    for completed in execution.attempts:
        if completed.stdout:
            print(completed.stdout, end="")
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr)
    completed = execution.completed
    if completed.returncode != 0 or execution.missing_expected_artifacts:
        missing = (
            "; missing expected dbt artifacts: "
            + ", ".join(execution.missing_expected_artifacts)
            if execution.missing_expected_artifacts
            else ""
        )
        raise AirflowException(
            f"weather W1 dbt command failed with exit code {completed.returncode}{missing}"
        )
    return {
        "status": "success",
        "run_results_path": execution.existing_run_results_path,
        "sources_path": execution.existing_sources_path,
        "manifest_path": execution.existing_manifest_path,
        "selected_unique_ids": list(execution.selected_unique_ids),
    }


def dbt_smoke_task(
    task_id: str, dbt_command: str, selection: str | None = None
) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=run_dbt_smoke_phase,
        op_kwargs={"dbt_command": dbt_command, "selection": selection},
        pool=TRINO_HEAVY_POOL,
        on_failure_callback=record_weather_problem,
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

    dbt_deps = dbt_smoke_task("dbt_deps", "deps")

    dbt_seed_bridge_inputs = dbt_smoke_task(
        "dbt_seed_bridge_inputs", "seed", "tag:ask_seoul_weather_w1_inputs"
    )

    dbt_run_common_admin_dong_dimension = dbt_smoke_task(
        "dbt_run_common_admin_dong_dimension",
        "run",
        "tag:ask_seoul_weather_transform_common_admin",
    )

    dbt_run_bridge = dbt_smoke_task(
        "dbt_run_bridge", "run", "tag:ask_seoul_weather_w1_bridge"
    )

    dbt_test_bridge_contract = dbt_smoke_task(
        "dbt_test_bridge_contract", "test", "tag:ask_seoul_weather_w1_bridge"
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


enable_lineage_if_configured(dag)
