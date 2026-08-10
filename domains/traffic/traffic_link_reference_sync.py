"""Daily incremental synchronization for TOPIS road-link reference pairs."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param


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
    fail_fast_traffic_bronze,
)
from traffic_ingest.common.resources import TRINO_INGEST_POOL  # noqa: E402
from traffic_ingest.flow_info import KST  # noqa: E402
from traffic_ingest.link_reference_backfill import MAX_BATCH_SIZE  # noqa: E402
from traffic_ingest.link_reference_sync import (  # noqa: E402
    DEFAULT_STALE_AFTER_DAYS,
    DEFAULT_SYNC_BATCH_SIZE,
    MAX_STALE_AFTER_DAYS,
    land_incremental_sync_batch,
    materialize_incremental_sync_batch,
    resolve_incremental_sync_link_ids,
)
from traffic_lineage import enable_lineage_if_configured  # noqa: E402


DAG_ID = "traffic_link_reference_sync"
SYNC_SCHEDULE = os.environ.get(
    "ASK_SEOUL_TRAFFIC_LINK_REFERENCE_SYNC_SCHEDULE",
    "37 3 * * *",
) or None
RESOLVE_TASK_ID = "resolve_link_reference_sync_candidates"
LAND_TASK_ID = "land_link_reference_sync"
MATERIALIZE_TASK_ID = "materialize_link_reference_sync"
record_traffic_problem = problem_failure_callback(
    domain="traffic",
    source_system="seoul_topis",
)


@fail_fast_traffic_bronze
def resolve_link_reference_sync_candidates(**context) -> list[str]:
    params = context.get("params") or {}
    return resolve_incremental_sync_link_ids(
        batch_size=params.get("batch_size", DEFAULT_SYNC_BATCH_SIZE),
        stale_after_days=params.get(
            "stale_after_days",
            DEFAULT_STALE_AFTER_DAYS,
        ),
    )


@fail_fast_traffic_bronze
def land_link_reference_sync(**context) -> dict[str, object]:
    link_ids = context["ti"].xcom_pull(task_ids=RESOLVE_TASK_ID) or []
    return land_incremental_sync_batch(
        link_ids=link_ids,
        dag_run_id=str(context["run_id"]),
    )


@fail_fast_traffic_bronze
def materialize_link_reference_sync(**context) -> dict[str, object]:
    raw_result = context["ti"].xcom_pull(task_ids=LAND_TASK_ID) or {}
    return materialize_incremental_sync_batch(
        raw_result=raw_result,
        dag_run_id=str(context["run_id"]),
    )


with DAG(
    dag_id=DAG_ID,
    description=(
        "Synchronize missing or stale TOPIS LinkInfo and LinkVerInfo once daily."
    ),
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=SYNC_SCHEDULE,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params={
        "batch_size": Param(
            default=DEFAULT_SYNC_BATCH_SIZE,
            type="integer",
            minimum=1,
            maximum=MAX_BATCH_SIZE,
        ),
        "stale_after_days": Param(
            default=DEFAULT_STALE_AFTER_DAYS,
            type="integer",
            minimum=1,
            maximum=MAX_STALE_AFTER_DAYS,
        ),
    },
    tags=["ask_seoul", "traffic", "road", "reference", "daily", "iceberg"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "traffic"},
        on_failure_callback=record_traffic_problem,
    )
    resolve_candidates = PythonOperator(
        task_id=RESOLVE_TASK_ID,
        python_callable=resolve_link_reference_sync_candidates,
        pool=TRINO_INGEST_POOL,
        on_failure_callback=record_traffic_problem,
    )
    land_reference = PythonOperator(
        task_id=LAND_TASK_ID,
        python_callable=land_link_reference_sync,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        on_failure_callback=record_traffic_problem,
    )
    materialize_reference = PythonOperator(
        task_id=MATERIALIZE_TASK_ID,
        python_callable=materialize_link_reference_sync,
        pool=TRINO_INGEST_POOL,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        on_failure_callback=record_traffic_problem,
    )

    validate_runtime >> resolve_candidates >> land_reference >> materialize_reference


enable_lineage_if_configured(dag)
