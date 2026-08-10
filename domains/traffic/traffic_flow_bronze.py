"""Airflow DAG for exact-parent Seoul TOPIS TrafficInfo Bronze snapshots."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowFailException


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
from common.ops.product_observability import (  # noqa: E402
    record_domain_stage_event,
    record_product_event,
)
from traffic_ingest.assets import (  # noqa: E402
    TRAFFIC_FLOW_MATERIALIZED_ALIAS,
    TRAFFIC_FLOW_BRONZE_ASSET_REF,
    TRAFFIC_INCIDENT_BRONZE_ASSET,
    latest_incident_bronze_event,
    publish_through_alias,
    schedule_asset,
)
from traffic_ingest.bronze_dag_support import (  # noqa: E402
    dag_run_conf,
    fail_fast_traffic_bronze,
)
from traffic_ingest.common.resources import TRINO_INGEST_POOL  # noqa: E402
from traffic_ingest.errors import TrafficSourceEmptyResponseError  # noqa: E402
from traffic_ingest.flow_info import KST  # noqa: E402
from traffic_ingest.flow_ingest import build_traffic_flow_pipeline  # noqa: E402
from traffic_ingest.flow_pipeline import FLOW_MATERIALIZE_TASK_ID  # noqa: E402
from traffic_lineage import enable_lineage_if_configured  # noqa: E402


DAG_ID = "traffic_flow_bronze"
LAND_TASK_ID = "land_traffic_flow_snapshot"
record_traffic_problem = problem_failure_callback(
    domain="traffic", source_system="seoul_topis"
)
record_traffic_raw_product_failure = record_domain_stage_event(
    "traffic", "raw", status="failed"
)
record_traffic_bronze_product_failure = record_domain_stage_event(
    "traffic", "bronze", status="failed"
)


def _should_notify_flow_land_failure(exception: BaseException | None) -> bool:
    """TrafficInfo 빈 응답(known-transient)만 Discord 알림에서 제외한다.

    링크별 격리 없이 첫 실패로 전체 batch 가 죽는 구조라(#481) 이 패턴은
    하루 산발적으로 실패하지만 실제 데이터 결손 없이 다음 5분 asset 트리거로
    자연 재수렴한다. R2 problem 문서는 그대로 남아 감시 가능성은 유지된다.
    """
    return not isinstance(getattr(exception, "__cause__", None), TrafficSourceEmptyResponseError)


record_traffic_flow_land_problem = problem_failure_callback(
    domain="traffic",
    source_system="seoul_topis",
    should_notify=_should_notify_flow_land_failure,
)


def _record_traffic_flow_rows(
    context: dict,
    *,
    layer: str,
    task_id: str,
    field: str,
    rows_source: str,
) -> dict:
    row_count = None
    try:
        task_instance = context.get("ti") or context.get("task_instance")
        result = task_instance.xcom_pull(task_ids=task_id)
        value = result.get(field) if isinstance(result, dict) else None
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            row_count = value
    except Exception:
        row_count = None
    return record_product_event(
        context,
        domain="traffic",
        layer=layer,
        row_count=row_count,
        rows_source=rows_source if row_count is not None else "not_observed",
    )


def record_traffic_raw_product_event(context: dict) -> dict:
    return _record_traffic_flow_rows(
        context,
        layer="raw",
        task_id=LAND_TASK_ID,
        field="expected_rows",
        rows_source="raw_manifest",
    )


def record_traffic_bronze_product_event(context: dict) -> dict:
    return _record_traffic_flow_rows(
        context,
        layer="bronze",
        task_id=FLOW_MATERIALIZE_TASK_ID,
        field="row_count",
        rows_source="bronze_run_manifest",
    )


def _incident_parent_from_context(context: dict) -> str:
    event = latest_incident_bronze_event(context)
    if event is not None:
        return str(event["bronze_dag_run_id"])
    configured = dag_run_conf(context).get("incident_run_id")
    if not str(configured or "").strip():
        raise AirflowFailException(
            "traffic_flow_bronze requires an Incident Bronze Asset or incident_run_id"
        )
    return str(configured)


@fail_fast_traffic_bronze
def land_traffic_flow_snapshot(**context) -> dict[str, object]:
    return build_traffic_flow_pipeline().land(
        parent_incident_run_id=_incident_parent_from_context(context),
        flow_run_id=str(context["run_id"]),
        conf=dag_run_conf(context),
    )


@fail_fast_traffic_bronze
def materialize_verify_publish_traffic_flow(**context) -> dict[str, object]:
    raw_result = context["ti"].xcom_pull(task_ids=LAND_TASK_ID) or {}
    outcome = build_traffic_flow_pipeline().materialize(
        raw_result=raw_result,
        flow_run_id=str(context["run_id"]),
    )
    if outcome.asset_metadata is not None:
        publish_through_alias(
            context,
            alias=TRAFFIC_FLOW_MATERIALIZED_ALIAS,
            asset=TRAFFIC_FLOW_BRONZE_ASSET_REF,
            metadata=outcome.asset_metadata,
        )
    return {
        "row_count": outcome.row_count,
        "asset_published": outcome.asset_metadata is not None,
    }


with DAG(
    dag_id=DAG_ID,
    description="Lands and materializes TrafficInfo for one exact Incident snapshot.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=[schedule_asset(TRAFFIC_INCIDENT_BRONZE_ASSET)],
    catchup=False,
    max_active_runs=1,
    tags=["ask_seoul", "traffic", "flow", "bronze", "asset", "iceberg"],
) as dag:
    land_flow = PythonOperator(
        task_id=LAND_TASK_ID,
        python_callable=land_traffic_flow_snapshot,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        on_failure_callback=[
            record_traffic_flow_land_problem,
            record_traffic_raw_product_failure,
        ],
        on_success_callback=record_traffic_raw_product_event,
    )
    materialize_flow = PythonOperator(
        task_id=FLOW_MATERIALIZE_TASK_ID,
        python_callable=materialize_verify_publish_traffic_flow,
        pool=TRINO_INGEST_POOL,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        outlets=[TRAFFIC_FLOW_MATERIALIZED_ALIAS],
        on_failure_callback=[
            record_traffic_problem,
            record_traffic_bronze_product_failure,
        ],
        on_success_callback=record_traffic_bronze_product_event,
    )
    land_flow >> materialize_flow


enable_lineage_if_configured(dag)
