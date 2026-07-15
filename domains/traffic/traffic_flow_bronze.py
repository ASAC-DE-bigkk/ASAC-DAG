"""Airflow DAG for Seoul TOPIS per-link real-time traffic flow Bronze."""

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
    traffic_dag_schedule,
)
from traffic_ingest.common.runtime import download_raw_object, trino_cursor  # noqa: E402
from traffic_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402
from traffic_ingest.flow_bronze import (  # noqa: E402
    load_traffic_flow_batch,
    verify_seoul_traffic_flow_bronze_runtime,
)
from traffic_ingest.flow_info import KST, resolve_flow_link_ids  # noqa: E402
from traffic_ingest.flow_ingest import (  # noqa: E402
    build_traffic_flow_landing,
    build_traffic_flow_manifest,
)
from traffic_ingest.run_manifest import TrafficRun  # noqa: E402
from traffic_lineage import enable_lineage_if_configured  # noqa: E402


record_traffic_problem = problem_failure_callback(domain="traffic", source_system="seoul_topis")
DAG_ID = "traffic_flow_bronze"
RESOLVE_LINKS_TASK_ID = "resolve_traffic_flow_link_ids"
LAND_TASK_ID = "land_seoul_traffic_flow_raw"
LOAD_TASK_ID = "load_seoul_traffic_flow_bronze"


def start_traffic_flow_run(**context) -> str:
    return build_traffic_flow_manifest().start(
        TrafficRun(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
        )
    )


def fail_traffic_flow_run(context) -> None:
    try:
        build_traffic_flow_manifest().fail(
            TrafficRun(
                dag_id=getattr(context.get("dag"), "dag_id", DAG_ID),
                run_id=context["run_id"],
            ),
            task_id=getattr(context.get("ti"), "task_id", "unknown"),
            error=context.get("exception") or RuntimeError("Airflow task failed"),
        )
    except Exception as exc:  # pragma: no cover - failure callback must not mask task failure
        print(f"traffic flow manifest failure recording failed: {type(exc).__name__}")


@fail_fast_traffic_bronze
def resolve_traffic_flow_links(**context) -> list[str]:
    return resolve_flow_link_ids(conf=dag_run_conf(context))


@fail_fast_traffic_bronze
def land_seoul_traffic_flow_raw(**context) -> dict:
    link_ids = context["ti"].xcom_pull(task_ids=RESOLVE_LINKS_TASK_ID) or []
    if not isinstance(link_ids, list):
        raise ValueError("resolved traffic flow link ids must be a list")
    return build_traffic_flow_landing().collect(
        link_ids=link_ids,
        dag_run_id=context["run_id"],
    )


@fail_fast_traffic_bronze
def load_seoul_traffic_flow_bronze(**context) -> dict:
    raw_result = context["ti"].xcom_pull(task_ids=LAND_TASK_ID) or {}
    return load_traffic_flow_batch(
        raw_result=raw_result,
        dag_run_id=context["run_id"],
        cursor_factory=trino_cursor,
        download_raw_object=download_raw_object,
    )


@fail_fast_traffic_bronze
def verify_seoul_traffic_flow_bronze(**context) -> int:
    load_result = context["ti"].xcom_pull(task_ids=LOAD_TASK_ID) or {}
    expected_rows = int(
        load_result.get("expected_rows", load_result.get("inserted", 0))
    )
    expected_raw_objects = int(load_result.get("page_count", 0))
    verified_rows = verify_seoul_traffic_flow_bronze_runtime(
        dag_run_id=context["run_id"],
        expected_rows=expected_rows,
        expected_raw_objects=expected_raw_objects,
    )
    build_traffic_flow_manifest().publish(
        TrafficRun(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
        ),
        expected_rows=expected_rows,
        actual_rows=verified_rows,
        expected_raw_objects=expected_raw_objects,
        actual_raw_objects=len(load_result.get("raw_object_keys") or []),
        is_publishable=bool(load_result.get("is_publishable", True)),
    )
    return verified_rows


with DAG(
    dag_id=DAG_ID,
    description="Loads Seoul TOPIS TrafficInfo per-link XML into R2 and Iceberg Bronze.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=traffic_dag_schedule(),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=fail_traffic_flow_run,
    tags=["ask_seoul", "traffic", "flow", "bronze", "r2", "iceberg"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "traffic"},
        on_failure_callback=record_traffic_problem,
    )
    start_manifest = PythonOperator(
        task_id="record_traffic_flow_run_started",
        python_callable=start_traffic_flow_run,
        on_failure_callback=record_traffic_problem,
    )
    resolve_links = PythonOperator(
        task_id=RESOLVE_LINKS_TASK_ID,
        python_callable=resolve_traffic_flow_links,
        on_failure_callback=record_traffic_problem,
    )
    land_raw = PythonOperator(
        task_id=LAND_TASK_ID,
        python_callable=land_seoul_traffic_flow_raw,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        on_failure_callback=record_traffic_problem,
    )
    load_bronze = PythonOperator(
        task_id=LOAD_TASK_ID,
        python_callable=load_seoul_traffic_flow_bronze,
        pool=TRINO_HEAVY_POOL,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        on_failure_callback=record_traffic_problem,
    )
    verify_bronze = PythonOperator(
        task_id="verify_seoul_traffic_flow_bronze",
        python_callable=verify_seoul_traffic_flow_bronze,
        pool=TRINO_HEAVY_POOL,
        on_failure_callback=record_traffic_problem,
    )

    validate_runtime >> start_manifest >> resolve_links >> land_raw >> load_bronze >> verify_bronze

enable_lineage_if_configured(dag)


__all__ = ["dag"]
