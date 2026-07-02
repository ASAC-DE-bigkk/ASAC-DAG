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
    metadata_total_count,
    next_acc_info_page_ranges,
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
    return "*/5 * * * *" if is_dev_target() else None


def ingest_seoul_traffic_incident(**context) -> dict:
    start_index = int(os.environ.get("SEOUL_ACC_INFO_START_INDEX", "1"))
    end_index = int(os.environ.get("SEOUL_ACC_INFO_END_INDEX", "1000"))
    page_size = int(os.environ.get("SEOUL_ACC_INFO_PAGE_SIZE", str(end_index - start_index + 1)))

    cursor, catalog, schema = trino_cursor()
    qualified_table = create_seoul_traffic_bronze_table(cursor, catalog, schema)

    inserted = 0
    parsed_rows = 0
    raw_object_keys = []
    page_ranges = [(start_index, end_index)]
    page_summaries = []
    list_total_count = 0
    page_index = 0
    while page_index < len(page_ranges):
        page_start, page_end = page_ranges[page_index]
        page_index += 1
        collected_at = datetime.now(timezone.utc)
        request_id = str(uuid.uuid4())
        url = build_seoul_acc_info_url(start_index=page_start, end_index=page_end)
        http_status, raw_bytes = fetch_url(url, "ask-seoul-traffic-bronze/1.0")
        metadata, rows = parse_seoul_acc_info_response(raw_bytes)
        raw_hash = sha256_hex(raw_bytes)
        raw_object_key = build_raw_object_key(
            collected_at=collected_at,
            request_id=request_id,
            start_index=page_start,
            end_index=page_end,
        )
        upload_raw_object(
            raw_bytes=raw_bytes,
            object_key=raw_object_key,
            content_type="application/xml; charset=utf-8",
            log_label="Seoul traffic raw payload",
        )
        page_inserted = insert_seoul_traffic_bronze_rows(
            cursor=cursor,
            qualified_table=qualified_table,
            rows=rows,
            metadata=metadata,
            request_id=request_id,
            start_index=page_start,
            end_index=page_end,
            raw_object_key=raw_object_key,
            raw_hash=raw_hash,
            http_status=http_status,
            collected_at=collected_at,
            dag_run_id=context["run_id"],
        )
        inserted += page_inserted
        parsed_rows += len(rows)
        raw_object_keys.append(raw_object_key)
        list_total_count = max(list_total_count, metadata_total_count(metadata))
        page_summaries.append(
            {
                "start_index": page_start,
                "end_index": page_end,
                "row_count": len(rows),
                "list_total_count": metadata_total_count(metadata),
                "raw_object_key": raw_object_key,
            }
        )
        if page_index == 1:
            page_ranges.extend(
                next_acc_info_page_ranges(
                    start_index=page_start,
                    end_index=page_end,
                    list_total_count=list_total_count,
                    page_size=page_size,
                )
            )

    if parsed_rows < list_total_count:
        raise RuntimeError(
            "Seoul traffic pagination incomplete: "
            f"list_total_count={list_total_count}, parsed_rows={parsed_rows}, "
            f"requested_end_index={max(end for _, end in page_ranges)}"
        )

    print(
        f"Inserted {inserted} Seoul traffic rows from {len(page_ranges)} pages "
        f"into {qualified_table}"
    )
    return {
        "source_id": SOURCE_ID,
        "raw_object_keys": raw_object_keys,
        "inserted": inserted,
        "list_total_count": list_total_count,
        "page_count": len(page_ranges),
        "requested_end_index": max(end for _, end in page_ranges),
        "pages": page_summaries,
    }


def verify_seoul_traffic_bronze_runtime(**context) -> int:
    ingest_result = context["ti"].xcom_pull(task_ids="ingest_seoul_traffic_incident") or {}
    return verify_seoul_traffic_bronze_rows(
        raw_object_keys=ingest_result["raw_object_keys"],
        dag_run_id=context["run_id"],
        expected_rows=int(ingest_result["inserted"]),
        expected_raw_objects=int(ingest_result["page_count"]),
    )


with DAG(
    dag_id="traffic_incident_bronze",
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
