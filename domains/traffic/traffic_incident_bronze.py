"""Airflow entrypoint for TOPIS raw landing and Bronze publication."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from hashlib import sha256

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset
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

from common.assets import TRAFFIC_BRONZE_ASSET  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from traffic_ingest.acc_info import (  # noqa: E402
    KST,
    SOURCE_ID,
    resolve_acc_info_page_window,
)
from traffic_ingest.bronze import (  # noqa: E402
    create_seoul_traffic_bronze_table,
    insert_seoul_traffic_bronze_rows,
    verify_seoul_traffic_bronze_runtime as verify_seoul_traffic_bronze_rows,
)
from traffic_ingest.bronze_batch import load_traffic_bronze_batch  # noqa: E402
from traffic_ingest.bronze_dag_support import (  # noqa: E402
    BACKFILL_DAG_ID,
    DAG_ID,
    DISCORD_GREEN as DISCORD_GREEN,
    DISCORD_RED as DISCORD_RED,
    LOAD_TRAFFIC_BRONZE_TASK_ID,
    RECOLLECT_DAG_ID,
    TRAFFIC_DISCORD_WEBHOOK_ENV as TRAFFIC_DISCORD_WEBHOOK_ENV,
    current_dag_id,
    dag_run_conf,
    discord_report_date as discord_report_date,
    fail_fast_traffic_bronze,
    fail_traffic_run,
    notify_traffic_bronze_failure,
    notify_traffic_bronze_success as notify_traffic_bronze_success,
    raw_object_keys_from_conf,
    send_traffic_discord as send_traffic_discord,
    short_text as short_text,
    stage_name as stage_name,
    start_traffic_backfill_run,
    start_traffic_run,
    target_name as target_name,
    traffic_dag_schedule,
)
from traffic_ingest.common.runtime import (  # noqa: E402
    download_raw_object,
    trino_cursor,
)
from traffic_ingest.errors import TrafficBronzeConfigurationError  # noqa: E402
from traffic_ingest.landing import (  # noqa: E402
    RunIdentity,
    TrafficCollectionMode,
    TrafficLandingRequest,
)
from traffic_ingest.run_manifest import TrafficRun  # noqa: E402
from traffic_ingest.runtime import (  # noqa: E402
    build_traffic_landing,
    build_traffic_manifest,
)
from traffic_lineage import enable_lineage_if_configured  # noqa: E402


record_traffic_problem = problem_failure_callback(
    domain="traffic", source_system="seoul_topis"
)
TRAFFIC_BRONZE_ASSET_REF = Asset(TRAFFIC_BRONZE_ASSET)


@fail_fast_traffic_bronze
def land_seoul_traffic_raw(**context) -> dict:
    conf = dag_run_conf(context)
    start_index, end_index, page_size = resolve_acc_info_page_window(conf)
    configured_mode = conf.get(
        "collection_mode", TrafficCollectionMode.FULL_SNAPSHOT.value
    )
    if configured_mode not in {
        TrafficCollectionMode.FULL_SNAPSHOT.value,
        TrafficCollectionMode.WINDOW.value,
    }:
        raise TrafficBronzeConfigurationError(
            "dag_run.conf.collection_mode must be full_snapshot or window"
        )
    batch = build_traffic_landing().collect(
        RunIdentity(current_dag_id(context), context["run_id"]),
        TrafficLandingRequest(
            start_index,
            end_index,
            page_size,
            mode=TrafficCollectionMode(configured_mode),
        ),
    )
    return batch.to_xcom()


@fail_fast_traffic_bronze
def land_seoul_traffic_raw_object_keys(**context) -> dict:
    raw_object_keys = raw_object_keys_from_conf(context)
    return build_traffic_landing().replay(raw_object_keys).to_xcom()


@fail_fast_traffic_bronze
def load_seoul_traffic_bronze(**context) -> dict:
    raw_result = context["ti"].xcom_pull(task_ids="land_seoul_traffic_raw") or {}
    return load_traffic_bronze_batch(
        raw_result=raw_result,
        dag_run_id=context["run_id"],
        cursor_factory=trino_cursor,
        create_table=create_seoul_traffic_bronze_table,
        download_raw_object=download_raw_object,
        insert_rows=insert_seoul_traffic_bronze_rows,
    )


def record_seoul_traffic_run_started(**context) -> str:
    return start_traffic_run(context, manifest_factory=build_traffic_manifest)


def record_seoul_traffic_backfill_run_started(**context) -> str:
    return start_traffic_backfill_run(context, manifest_factory=build_traffic_manifest)


def record_seoul_traffic_run_failed(context) -> None:
    fail_traffic_run(context, manifest_factory=build_traffic_manifest)


def record_and_notify_seoul_traffic_run_failed(context) -> None:
    record_seoul_traffic_run_failed(context)
    notify_traffic_bronze_failure(context)


@fail_fast_traffic_bronze
def verify_seoul_traffic_bronze_runtime(**context) -> int:
    ingest_result = context["ti"].xcom_pull(task_ids=LOAD_TRAFFIC_BRONZE_TASK_ID) or {}
    verified_rows = verify_seoul_traffic_bronze_rows(
        raw_object_keys=ingest_result["raw_object_keys"],
        dag_run_id=context["run_id"],
        expected_rows=int(ingest_result["inserted"]),
        expected_raw_objects=int(ingest_result["page_count"]),
    )
    build_traffic_manifest().publish(
        TrafficRun(current_dag_id(context), context["run_id"]),
        expected_rows=int(
            ingest_result.get("expected_rows", ingest_result["list_total_count"])
        ),
        actual_rows=verified_rows,
        expected_raw_objects=int(ingest_result["page_count"]),
        actual_raw_objects=len(ingest_result["raw_object_keys"]),
        is_publishable=bool(ingest_result.get("is_publishable", True)),
    )
    return verified_rows


def publish_traffic_bronze_asset(**context) -> str:
    ingest_result = context["ti"].xcom_pull(task_ids=LOAD_TRAFFIC_BRONZE_TASK_ID) or {}
    if not bool(ingest_result.get("is_publishable", True)):
        raise AirflowSkipException("traffic Bronze run is not publishable")

    raw_result = context["ti"].xcom_pull(task_ids="land_seoul_traffic_raw") or {}
    raw_objects = raw_result.get("raw_objects") or []
    if not raw_objects:
        raise AirflowFailException(
            "traffic Bronze asset event requires at least one raw object"
        )

    collected_at_values = [str(item["collected_at"]) for item in raw_objects]
    event_at = max(
        collected_at_values,
        key=lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")),
    )
    payload_hashes = sorted(
        str(item["raw_hash"])
        for item in raw_objects
        if item.get("raw_hash")
    )
    if not payload_hashes:
        raise AirflowFailException(
            "traffic Bronze asset event requires raw payload hashes"
        )
    payload_hash = (
        payload_hashes[0]
        if len(payload_hashes) == 1
        else sha256("|".join(payload_hashes).encode("utf-8")).hexdigest()
    )
    outlet_events = context.get("outlet_events")
    if outlet_events is None:
        raise AirflowFailException("traffic Bronze outlet event is unavailable")
    outlet_event = outlet_events[TRAFFIC_BRONZE_ASSET_REF]

    event_datetime = datetime.fromisoformat(event_at.replace("Z", "+00:00"))
    outlet_event.extra = {
        "source_id": SOURCE_ID,
        "bronze_run_id": context["run_id"],
        "bronze_dag_run_id": context["run_id"],
        "event_at": event_at,
        "load_date": event_datetime.astimezone(KST).date().isoformat(),
        "row_count": int(ingest_result["inserted"]),
        "payload_hash": payload_hash,
        "is_publishable": True,
    }
    return context["run_id"]


def build_traffic_bronze_dag(
    dag_id: str, schedule: str | None, description: str, tags: list[str]
):
    with DAG(
        dag_id=dag_id,
        description=description,
        start_date=datetime(2026, 1, 1, tzinfo=KST),
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        on_failure_callback=record_seoul_traffic_run_failed,
        tags=tags,
    ) as built_dag:
        validate_runtime = PythonOperator(
            task_id="validate_dev_runtime",
            python_callable=validate_dev_runtime,
            op_kwargs={"domain": "traffic"},
            on_failure_callback=[record_traffic_problem],
        )
        start_manifest = PythonOperator(
            task_id="record_seoul_traffic_run_started",
            python_callable=record_seoul_traffic_run_started,
            on_failure_callback=[record_traffic_problem],
        )
        land_raw = PythonOperator(
            task_id="land_seoul_traffic_raw",
            python_callable=land_seoul_traffic_raw,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[
                record_and_notify_seoul_traffic_run_failed,
                record_traffic_problem,
            ],
        )
        load_bronze = PythonOperator(
            task_id=LOAD_TRAFFIC_BRONZE_TASK_ID,
            python_callable=load_seoul_traffic_bronze,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[
                record_and_notify_seoul_traffic_run_failed,
                record_traffic_problem,
            ],
        )
        verify_bronze = PythonOperator(
            task_id="verify_seoul_traffic_bronze_runtime",
            python_callable=verify_seoul_traffic_bronze_runtime,
            on_failure_callback=[
                record_and_notify_seoul_traffic_run_failed,
                record_traffic_problem,
            ],
        )
        publish_bronze_asset = PythonOperator(
            task_id="publish_traffic_bronze_asset",
            python_callable=publish_traffic_bronze_asset,
            outlets=[TRAFFIC_BRONZE_ASSET_REF],
        )
        (
            validate_runtime
            >> start_manifest
            >> land_raw
            >> load_bronze
            >> verify_bronze
            >> publish_bronze_asset
        )
    return enable_lineage_if_configured(built_dag)


def build_traffic_bronze_backfill_dag():
    with DAG(
        dag_id=BACKFILL_DAG_ID,
        description="Loads existing TOPIS raw objects into Bronze without API calls.",
        start_date=datetime(2026, 1, 1, tzinfo=KST),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        on_failure_callback=record_seoul_traffic_run_failed,
        tags=["ask_seoul", "traffic", "bronze", "backfill", "r2", "iceberg"],
    ) as built_dag:
        validate_runtime = PythonOperator(
            task_id="validate_dev_runtime",
            python_callable=validate_dev_runtime,
            op_kwargs={"domain": "traffic"},
            on_failure_callback=[record_traffic_problem],
        )
        start_manifest = PythonOperator(
            task_id="record_seoul_traffic_run_started",
            python_callable=record_seoul_traffic_backfill_run_started,
            on_failure_callback=[record_traffic_problem],
        )
        land_raw = PythonOperator(
            task_id="land_seoul_traffic_raw",
            python_callable=land_seoul_traffic_raw_object_keys,
            on_failure_callback=[
                record_and_notify_seoul_traffic_run_failed,
                record_traffic_problem,
            ],
        )
        load_bronze = PythonOperator(
            task_id=LOAD_TRAFFIC_BRONZE_TASK_ID,
            python_callable=load_seoul_traffic_bronze,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[
                record_and_notify_seoul_traffic_run_failed,
                record_traffic_problem,
            ],
        )
        verify_bronze = PythonOperator(
            task_id="verify_seoul_traffic_bronze_runtime",
            python_callable=verify_seoul_traffic_bronze_runtime,
            on_failure_callback=[
                record_and_notify_seoul_traffic_run_failed,
                record_traffic_problem,
            ],
        )
        publish_bronze_asset = PythonOperator(
            task_id="publish_traffic_bronze_asset",
            python_callable=publish_traffic_bronze_asset,
            outlets=[TRAFFIC_BRONZE_ASSET_REF],
        )
        (
            validate_runtime
            >> start_manifest
            >> land_raw
            >> load_bronze
            >> verify_bronze
            >> publish_bronze_asset
        )
    return enable_lineage_if_configured(built_dag)


dag = build_traffic_bronze_dag(
    DAG_ID,
    traffic_dag_schedule(),
    "Loads Seoul TOPIS AccInfo XML into R2 and validates the Iceberg bronze runtime.",
    ["ask_seoul", "traffic", "bronze", "r2", "iceberg"],
)
recollect_dag = build_traffic_bronze_dag(
    RECOLLECT_DAG_ID,
    None,
    "Manually recollects TOPIS page windows through the Bronze contract.",
    ["ask_seoul", "traffic", "bronze", "recollect", "r2", "iceberg"],
)
backfill_dag = build_traffic_bronze_backfill_dag()
