import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)

from traffic_ingest.acc_info import (  # noqa: E402
    KST,
    SOURCE_ID,
    build_raw_object_key,
    build_seoul_acc_info_url,
    parse_seoul_acc_info_response,
)
from traffic_ingest.bronze import (  # noqa: E402
    create_seoul_traffic_bronze_table,
    insert_seoul_traffic_bronze_rows,
    verify_seoul_traffic_bronze_runtime as verify_seoul_traffic_bronze_rows,
)
from traffic_ingest.common.runtime import (  # noqa: E402
    fetch_url,
    is_dev_target,
    sha256_hex,
    trino_cursor,
    upload_raw_object,
)


def traffic_dag_schedule() -> str | None:
    if "ASK_SEOUL_TRAFFIC_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_TRAFFIC_DAG_SCHEDULE"] or None
    return "* * * * *" if is_dev_target() else None


def ingest_seoul_traffic_incident(**context) -> dict:
    collected_at = datetime.now(timezone.utc)
    request_id = str(uuid.uuid4())
    start_index = int(os.environ.get("SEOUL_ACC_INFO_START_INDEX", "1"))
    end_index = int(os.environ.get("SEOUL_ACC_INFO_END_INDEX", "1000"))

    url = build_seoul_acc_info_url(start_index=start_index, end_index=end_index)
    http_status, raw_bytes = fetch_url(url, "ask-seoul-traffic-bronze/1.0")
    metadata, rows = parse_seoul_acc_info_response(raw_bytes)
    raw_hash = sha256_hex(raw_bytes)
    raw_object_key = build_raw_object_key(
        collected_at=collected_at,
        request_id=request_id,
        start_index=start_index,
        end_index=end_index,
    )
    upload_raw_object(
        raw_bytes=raw_bytes,
        object_key=raw_object_key,
        content_type="application/xml; charset=utf-8",
        log_label="Seoul traffic raw payload",
    )

    cursor, catalog, schema = trino_cursor()
    qualified_table = create_seoul_traffic_bronze_table(cursor, catalog, schema)
    inserted = insert_seoul_traffic_bronze_rows(
        cursor=cursor,
        qualified_table=qualified_table,
        rows=rows,
        metadata=metadata,
        request_id=request_id,
        start_index=start_index,
        end_index=end_index,
        raw_object_key=raw_object_key,
        raw_hash=raw_hash,
        http_status=http_status,
        collected_at=collected_at,
        dag_run_id=context["run_id"],
    )
    print(f"Inserted {inserted} Seoul traffic rows into {qualified_table}")
    return {
        "source_id": SOURCE_ID,
        "raw_object_key": raw_object_key,
        "inserted": inserted,
    }


def verify_seoul_traffic_bronze_runtime(**context) -> int:
    ingest_result = context["ti"].xcom_pull(task_ids="ingest_seoul_traffic_incident") or {}
    return verify_seoul_traffic_bronze_rows(
        raw_object_key=ingest_result["raw_object_key"],
        dag_run_id=context["run_id"],
        expected_rows=int(ingest_result["inserted"]),
    )


with DAG(
    dag_id="seoul_traffic_incident_bronze",
    description="Loads Seoul TOPIS AccInfo XML into R2 and validates the Iceberg bronze runtime.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=traffic_dag_schedule(),
    catchup=False,
    max_active_runs=1,
    tags=["ask_seoul", "traffic", "bronze", "r2", "iceberg"],
) as dag:
    ingest_traffic = PythonOperator(
        task_id="ingest_seoul_traffic_incident",
        python_callable=ingest_seoul_traffic_incident,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
    )

    verify_bronze = PythonOperator(
        task_id="verify_seoul_traffic_bronze_runtime",
        python_callable=verify_seoul_traffic_bronze_runtime,
    )

    ingest_traffic >> verify_bronze
