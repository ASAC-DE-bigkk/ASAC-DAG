"""Manual, serial Airflow DAG for historical TOPIS link-reference catch-up."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
DOMAINS_DIR = os.path.dirname(DAG_DIR)
if DOMAINS_DIR not in sys.path:
    sys.path.insert(0, DOMAINS_DIR)
DAGS_ROOT_DIR = os.path.dirname(DOMAINS_DIR)
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from traffic_ingest.bronze_dag_support import (  # noqa: E402
    dag_run_conf,
    fail_fast_traffic_bronze,
)
from traffic_ingest.common.resources import TRINO_INGEST_POOL  # noqa: E402
from traffic_ingest.flow_info import KST  # noqa: E402
from traffic_ingest.link_reference_backfill import (  # noqa: E402
    land_backfill_batch,
    materialize_backfill_batch,
)
from traffic_lineage import enable_lineage_if_configured  # noqa: E402


DAG_ID = "traffic_link_reference_backfill"
LAND_TASK_ID = "land_link_reference_backfill"
MATERIALIZE_TASK_ID = "materialize_link_reference_backfill"
record_traffic_problem = problem_failure_callback(
    domain="traffic",
    source_system="seoul_topis",
)


@fail_fast_traffic_bronze
def land_link_reference_backfill(**context) -> dict[str, object]:
    return land_backfill_batch(
        conf=dag_run_conf(context),
        dag_run_id=str(context["run_id"]),
    )


@fail_fast_traffic_bronze
def materialize_link_reference_backfill(**context) -> dict[str, object]:
    raw_result = context["ti"].xcom_pull(task_ids=LAND_TASK_ID) or {}
    return materialize_backfill_batch(
        raw_result=raw_result,
        dag_run_id=str(context["run_id"]),
    )


with DAG(
    dag_id=DAG_ID,
    description=(
        "Manually catches up bounded TOPIS LinkInfo and LinkVerInfo pairs."
    ),
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ask_seoul", "traffic", "road", "reference", "backfill", "manual"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "traffic"},
        on_failure_callback=record_traffic_problem,
    )
    land_reference = PythonOperator(
        task_id=LAND_TASK_ID,
        python_callable=land_link_reference_backfill,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        on_failure_callback=record_traffic_problem,
    )
    materialize_reference = PythonOperator(
        task_id=MATERIALIZE_TASK_ID,
        python_callable=materialize_link_reference_backfill,
        pool=TRINO_INGEST_POOL,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        on_failure_callback=record_traffic_problem,
    )
    validate_runtime >> land_reference >> materialize_reference


enable_lineage_if_configured(dag)
