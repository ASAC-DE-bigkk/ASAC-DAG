"""Manual, resumable Weather W2 observation recovery for historical gaps."""

from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
DAGS_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(DAG_DIR)))
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from weather_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402
from weather_ingest.w2_recovery import (  # noqa: E402
    checkpoint_payload,
    completed_window_labels,
    dbt_cli_options,
    preparation_dbt_vars,
    split_repair_windows,
    window_dbt_vars,
)


LOGGER = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")
DAG_ID = "weather_w2_observation_recovery"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/weather"
CHECKPOINT_PREFIX = "ask_seoul.weather.w2_observation_recovery"
CHECKPOINT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")

PREPARE_DBT_ARGS = (
    ("deps",),
    (
        "seed",
        "--select",
        "asac_axes weather_place_grid_mapping weather_admin_dong_grid_bridge_history",
    ),
    ("run", "--select", "asac_axes.dim_admin_dong"),
    ("run", "--select", "bridge_weather_admin_dong_grid"),
    (
        "test",
        "--select",
        "assert_weather_bridge_candidate_grain_unique "
        "assert_weather_bridge_canonical_stamp_exact "
        "assert_weather_bridge_legacy_mapping_reconciles "
        "assert_weather_bridge_temporal_evidence "
        "assert_weather_bridge_validity_non_overlapping",
    ),
)
WINDOW_DBT_ARGS = (
    ("run", "--select", "silver_kma_vilage_fcst_observation"),
    ("run", "--select", "silver_kma_vilage_fcst_grid"),
    ("run", "--select", "gold_weather_forecast_by_admin_dong"),
    (
        "test",
        "--select",
        "assert_gold_weather_forecast_by_admin_dong_repair_reconciles "
        "assert_gold_weather_forecast_by_admin_dong_repair_window_no_extra_rows "
        "assert_gold_weather_forecast_by_admin_dong_repair_window_lineage",
    ),
)
FINAL_DBT_ARGS = (
    "test",
    "--select",
    "assert_weather_observation_publishable_and_counts_reconcile",
)
DEFAULT_PARAMS = {
    "target": Param(
        default="dev",
        type="string",
        enum=["dev"],
        description="dbt target profile name. Historical recovery is dev-only.",
    ),
    "repair_start_at": Param(
        default="2026-07-02 00:00:00.000000",
        type="string",
        description="Inclusive KST start for the historical W2 repair.",
    ),
    "repair_cutoff_at": Param(
        default="2026-07-14 23:59:59.999999",
        type="string",
        description="Inclusive KST cutoff for the historical W2 repair.",
    ),
    "checkpoint_id": Param(
        default="issue-196",
        type="string",
        description="Stable recovery checkpoint identity used to resume the same range.",
    ),
}
record_weather_problem = problem_failure_callback(domain="weather")


def checkpoint_variable_name(checkpoint_id: str) -> str:
    if not CHECKPOINT_ID_PATTERN.fullmatch(checkpoint_id):
        raise ValueError("checkpoint_id must be 1-80 letters, numbers, dot, dash, or underscore")
    return f"{CHECKPOINT_PREFIX}.{checkpoint_id}"


def dbt_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment.update({"DBT_PROJECT_DIR": DBT_PROJECT, "DBT_PROFILES_DIR": DBT_PROJECT})
    return environment


def run_dbt(args: tuple[str, ...], *, target: str, variables: dict[str, str]) -> None:
    command = [
        DBT_BIN,
        *args,
        *dbt_cli_options(args[0], target=target, variables=variables),
    ]
    LOGGER.info("[weather-w2-recovery] dbt command=%s", " ".join(command[:-3]))
    subprocess.run(command, cwd=DBT_PROJECT, env=dbt_environment(), check=True)


def _checkpoint_for_windows(variable_name: str, windows) -> set[str]:
    stored = Variable.get(variable_name, default=None, deserialize_json=True)
    return completed_window_labels(stored, windows)


def _save_checkpoint(variable_name: str, windows, completed: set[str]) -> None:
    Variable.set(
        variable_name,
        checkpoint_payload(windows, completed_labels=sorted(completed)),
        serialize_json=True,
    )


def recover_observation_windows(**context) -> dict[str, object]:
    params = context["params"]
    target = str(params["target"])
    if target != "dev":
        raise ValueError("Weather W2 historical recovery is dev-only")

    windows = split_repair_windows(
        str(params["repair_start_at"]),
        str(params["repair_cutoff_at"]),
    )
    now_kst = datetime.now(KST).replace(tzinfo=None)
    if windows[-1].cutoff_at > now_kst:
        raise ValueError("repair_cutoff_at must not be in the future")

    variable_name = checkpoint_variable_name(str(params["checkpoint_id"]))
    completed = _checkpoint_for_windows(variable_name, windows)
    preparation_variables = preparation_dbt_vars(windows[0], windows[-1])
    for args in PREPARE_DBT_ARGS:
        run_dbt(args, target=target, variables=preparation_variables)

    for window in windows:
        if window.label in completed:
            LOGGER.info("[weather-w2-recovery] checkpoint skip window=%s", window.label)
            continue
        variables = window_dbt_vars(window)
        LOGGER.info("[weather-w2-recovery] recover window=%s", window.label)
        for args in WINDOW_DBT_ARGS:
            run_dbt(args, target=target, variables=variables)
        completed.add(window.label)
        _save_checkpoint(variable_name, windows, completed)

    run_dbt(FINAL_DBT_ARGS, target=target, variables=preparation_variables)
    return {
        "checkpoint_variable": variable_name,
        "completed_windows": len(completed),
        "total_windows": len(windows),
    }


with DAG(
    dag_id=DAG_ID,
    description="Recover Weather W2 historical observations in resumable <=6h KST windows.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "dbt", "recovery", "w2", "manual"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "weather", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_weather_problem,
    )

    recover_windows = PythonOperator(
        task_id="recover_observation_windows",
        python_callable=recover_observation_windows,
        pool=TRINO_HEAVY_POOL,
        pool_slots=1,
        on_failure_callback=record_weather_problem,
    )

    validate_runtime >> recover_windows
