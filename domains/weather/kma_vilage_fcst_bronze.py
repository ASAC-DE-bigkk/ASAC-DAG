import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)

from weather_ingest.bronze import (  # noqa: E402
    create_kma_bronze_table,
    insert_kma_bronze_rows,
    verify_kma_bronze_runtime as verify_kma_bronze_rows,
)
from weather_ingest.common.runtime import (  # noqa: E402
    fetch_url,
    is_dev_target,
    sha256_hex,
    trino_cursor,
    upload_raw_object,
)
from weather_ingest.kma import (  # noqa: E402
    KST,
    SOURCE_ID,
    build_kma_url,
    build_raw_object_key,
    parse_kma_response,
    resolve_kma_base_datetime,
)


KMA_PUBLISH_CRON_KST = "20 2,5,8,11,14,17,20,23 * * *"


def kma_dag_schedule() -> str | None:
    if "ASK_SEOUL_KMA_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_KMA_DAG_SCHEDULE"] or None
    return KMA_PUBLISH_CRON_KST if is_dev_target() else None


def ingest_kma_vilage_fcst(**context) -> dict:
    collected_at = datetime.now(timezone.utc)
    request_id = str(uuid.uuid4())
    place_id = os.environ.get("ASK_SEOUL_KMA_PLACE_ID", "seoul_station")
    nx = int(os.environ.get("ASK_SEOUL_KMA_NX", "60"))
    ny = int(os.environ.get("ASK_SEOUL_KMA_NY", "127"))
    base_date, base_time = resolve_kma_base_datetime()

    url = build_kma_url(base_date=base_date, base_time=base_time, nx=nx, ny=ny)
    http_status, raw_bytes = fetch_url(url, "ask-seoul-kma-bronze/1.0")
    metadata, rows = parse_kma_response(raw_bytes)
    raw_hash = sha256_hex(raw_bytes)
    raw_object_key = build_raw_object_key(
        collected_at=collected_at,
        request_id=request_id,
        base_date=base_date,
        base_time=base_time,
    )
    upload_raw_object(
        raw_bytes=raw_bytes,
        object_key=raw_object_key,
        content_type="application/json; charset=utf-8",
        log_label="KMA raw payload",
    )

    cursor, catalog, schema = trino_cursor()
    qualified_table = create_kma_bronze_table(cursor, catalog, schema)
    inserted = insert_kma_bronze_rows(
        cursor=cursor,
        qualified_table=qualified_table,
        rows=rows,
        metadata=metadata,
        request_id=request_id,
        place_id=place_id,
        base_date=base_date,
        base_time=base_time,
        nx=nx,
        ny=ny,
        raw_object_key=raw_object_key,
        raw_hash=raw_hash,
        http_status=http_status,
        collected_at=collected_at,
        dag_run_id=context["run_id"],
    )
    print(f"Inserted {inserted} KMA rows into {qualified_table}")
    return {
        "source_id": SOURCE_ID,
        "raw_object_key": raw_object_key,
        "inserted": inserted,
    }


def verify_kma_bronze_runtime(**context) -> int:
    ingest_result = context["ti"].xcom_pull(task_ids="ingest_kma_vilage_fcst") or {}
    return verify_kma_bronze_rows(
        raw_object_key=ingest_result["raw_object_key"],
        dag_run_id=context["run_id"],
        expected_rows=int(ingest_result["inserted"]),
    )


with DAG(
    dag_id="kma_vilage_fcst_bronze",
    description="Loads KMA getVilageFcst raw JSON into R2 and validates the Iceberg bronze runtime.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=kma_dag_schedule(),
    catchup=False,
    max_active_runs=1,
    tags=["ask_seoul", "kma", "bronze", "r2", "iceberg"],
) as dag:
    ingest_kma = PythonOperator(
        task_id="ingest_kma_vilage_fcst",
        python_callable=ingest_kma_vilage_fcst,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
    )

    verify_bronze = PythonOperator(
        task_id="verify_kma_bronze_runtime",
        python_callable=verify_kma_bronze_runtime,
    )

    ingest_kma >> verify_bronze
