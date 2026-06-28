import os
import shlex
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.task.trigger_rule import TriggerRule


DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/elt_smoke")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
DBT_BIN = os.environ.get("DBT_BIN", "dbt")


def dbt_command(args: str) -> str:
    dbt_bin = shlex.quote(DBT_BIN)
    project_dir = shlex.quote(DBT_PROJECT_DIR)
    profiles_dir = shlex.quote(DBT_PROFILES_DIR)
    return (
        "set -euo pipefail\n"
        f"cd {project_dir}\n"
        f"DBT_PROFILES_DIR={profiles_dir} "
        f"{dbt_bin} --no-use-colors {args}"
    )


with DAG(
    dag_id="dbt_trino_iceberg_smoke",
    description="Runs dbt seed/model/test against Trino and Cloudflare R2 Data Catalog Iceberg.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=2,
    tags=["dbt", "trino", "iceberg", "smoke"],
) as dag:
    prepare_schema = BashOperator(
        task_id="prepare_smoke_schema",
        bash_command=dbt_command("run-operation prepare_smoke_schema"),
    )

    seed_sample_events = BashOperator(
        task_id="seed_sample_events",
        bash_command=dbt_command("seed --full-refresh --select sample_events"),
    )

    run_smoke_model = BashOperator(
        task_id="run_smoke_model",
        bash_command=dbt_command("run --select smoke_event_counts"),
    )

    test_smoke_relations = BashOperator(
        task_id="test_smoke_relations",
        bash_command=dbt_command("test --select sample_events smoke_event_counts"),
    )

    cleanup_smoke = BashOperator(
        task_id="cleanup_smoke",
        bash_command=dbt_command("run-operation cleanup_smoke"),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    prepare_schema >> seed_sample_events >> run_smoke_model >> test_smoke_relations >> cleanup_smoke
