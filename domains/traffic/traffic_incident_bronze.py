import json
import logging
import os
import sys
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
DOMAINS_DIR = os.path.dirname(DAG_DIR)
if DOMAINS_DIR not in sys.path:
    sys.path.insert(0, DOMAINS_DIR)

from _shared.bronze_run_manifest import (  # noqa: E402
    STATUS_FAILED,
    STATUS_STARTED,
    STATUS_SUCCESS,
    failure_reason_from_context,
    record_bronze_run_event,
)
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


TRAFFIC_DISCORD_WEBHOOK_ENV = "TRAFFIC_DISCORD_WEBHOOK_URL"
DISCORD_GREEN = 3066993
DISCORD_RED = 15158332
LOGGER = logging.getLogger(__name__)
DAG_ID = "traffic_incident_bronze"


def discord_report_date(context) -> str:
    logical_date = context.get("logical_date")
    if logical_date:
        return logical_date.astimezone(KST).strftime("%Y-%m-%d")
    return datetime.now(KST).strftime("%Y-%m-%d")


def target_name() -> str:
    return os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod"))


def short_text(value: object, limit: int = 130) -> str:
    text = str(value or "N/A")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def stage_name(task_id: str) -> str:
    if "ingest" in task_id:
        return "API 수집/R2 적재"
    if "verify" in task_id:
        return "Bronze 검증"
    return "알 수 없음"


def send_traffic_discord(title: str, description: str, color: int, footer: str) -> None:
    webhook_url = (os.environ.get(TRAFFIC_DISCORD_WEBHOOK_ENV) or "").strip()
    if not webhook_url:
        LOGGER.info("[traffic notify:noop] %s (webhook url not configured)", title)
        return
    payload = {
        "embeds": [{
            "title": title,
            "description": description[:4096],
            "color": color,
            "footer": {"text": footer[:2048]},
        }]
    }
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception as exc:
        LOGGER.warning("[traffic notify] Discord send failed: %s", type(exc).__name__)


def notify_traffic_bronze_success(context) -> None:
    ti = context["ti"]
    ingest_result = ti.xcom_pull(task_ids="ingest_seoul_traffic_incident") or {}
    inserted = int(ingest_result.get("inserted", 0))
    total_count = ingest_result.get("list_total_count", "N/A")
    raw_keys = ingest_result.get("raw_object_keys") or []
    page_count = ingest_result.get("page_count") or len(raw_keys)
    incident_line = "현재 돌발정보: 0건 (정상 응답)" if inserted == 0 else f"현재 돌발정보: {inserted:,}건"
    run_id = context["run_id"]
    send_traffic_discord(
        f"서울시 돌발정보 수집 리포트 - {discord_report_date(context)} (target={target_name()})",
        "\n".join(
            [
                "✅ 수집 상태: 성공",
                f"✅ TOPIS 응답: {ingest_result.get('result_code', 'N/A')}",
                f"✅ {incident_line}",
                f"✅ API 전체 건수: {total_count}건",
                f"✅ raw XML: {len(raw_keys)}개 / 페이지 {page_count}개",
                f"✅ Bronze 적재: {inserted:,}행",
                "",
                f"테이블: `bronze_seoul_traffic_incident`",
                f"raw 샘플: `{short_text(raw_keys[0] if raw_keys else 'N/A')}`",
            ]
        ),
        DISCORD_GREEN,
        f"dag_id={context['dag'].dag_id} · run_id={short_text(run_id, 180)}",
    )


def notify_traffic_bronze_failure(context) -> None:
    ti = context.get("ti") or context.get("task_instance")
    task_id = getattr(ti, "task_id", "N/A")
    exc = context.get("exception")
    run_id = context.get("run_id", "N/A")
    send_traffic_discord(
        f"서울시 돌발정보 수집 실패 - {discord_report_date(context)} (target={target_name()})",
        "\n".join(
            [
                "❌ 수집 상태: 실패",
                f"❌ 실패 단계: {stage_name(task_id)}",
                f"❌ 실패 task: `{task_id}`",
                f"❌ 오류 유형: `{type(exc).__name__ if exc else 'N/A'}`",
                "",
                f"Airflow 로그: {getattr(ti, 'log_url', 'N/A')}",
            ]
        ),
        DISCORD_RED,
        f"dag_id={context['dag'].dag_id} · run_id={short_text(run_id, 180)}",
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
    result_code = "N/A"
    page_index = 0
    while page_index < len(page_ranges):
        page_start, page_end = page_ranges[page_index]
        page_index += 1
        collected_at = datetime.now(timezone.utc)
        request_id = str(uuid.uuid4())
        url = build_seoul_acc_info_url(start_index=page_start, end_index=page_end)
        http_status, raw_bytes = fetch_url(url, "ask-seoul-traffic-bronze/1.0")
        metadata, rows = parse_seoul_acc_info_response(raw_bytes)
        result_code = metadata.get("result_code") or result_code
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
        "result_code": result_code,
        "list_total_count": list_total_count,
        "page_count": len(page_ranges),
        "requested_end_index": max(end for _, end in page_ranges),
        "pages": page_summaries,
    }


def record_seoul_traffic_run_started(**context) -> str:
    cursor, catalog, schema = trino_cursor()
    return record_bronze_run_event(
        cursor,
        catalog,
        schema,
        source_id=SOURCE_ID,
        dag_id=DAG_ID,
        dag_run_id=context["run_id"],
        status=STATUS_STARTED,
    )


def record_seoul_traffic_run_failed(context) -> None:
    try:
        cursor, catalog, schema = trino_cursor()
        record_bronze_run_event(
            cursor,
            catalog,
            schema,
            source_id=SOURCE_ID,
            dag_id=DAG_ID,
            dag_run_id=context["run_id"],
            status=STATUS_FAILED,
            failure_reason=failure_reason_from_context(context),
        )
    except Exception as exc:
        print(f"Failed to record Seoul traffic run manifest failure: {type(exc).__name__}")


def record_and_notify_seoul_traffic_run_failed(context) -> None:
    record_seoul_traffic_run_failed(context)
    notify_traffic_bronze_failure(context)


def verify_seoul_traffic_bronze_runtime(**context) -> int:
    ingest_result = context["ti"].xcom_pull(task_ids="ingest_seoul_traffic_incident") or {}
    verified_rows = verify_seoul_traffic_bronze_rows(
        raw_object_keys=ingest_result["raw_object_keys"],
        dag_run_id=context["run_id"],
        expected_rows=int(ingest_result["inserted"]),
        expected_raw_objects=int(ingest_result["page_count"]),
    )
    cursor, catalog, schema = trino_cursor()
    record_bronze_run_event(
        cursor,
        catalog,
        schema,
        source_id=SOURCE_ID,
        dag_id=DAG_ID,
        dag_run_id=context["run_id"],
        status=STATUS_SUCCESS,
        is_publishable=True,
        expected_rows=int(ingest_result["list_total_count"]),
        actual_rows=verified_rows,
        expected_raw_objects=int(ingest_result["page_count"]),
        actual_raw_objects=len(ingest_result["raw_object_keys"]),
    )
    return verified_rows


with DAG(
    dag_id=DAG_ID,
    description="Loads Seoul TOPIS AccInfo XML into R2 and validates the Iceberg bronze runtime.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=traffic_dag_schedule(),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=record_seoul_traffic_run_failed,
    tags=["ask_seoul", "traffic", "bronze", "r2", "iceberg"],
) as dag:
    start_manifest = PythonOperator(
        task_id="record_seoul_traffic_run_started",
        python_callable=record_seoul_traffic_run_started,
    )

    ingest_traffic = PythonOperator(
        task_id="ingest_seoul_traffic_incident",
        python_callable=ingest_seoul_traffic_incident,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        on_failure_callback=record_and_notify_seoul_traffic_run_failed,
    )

    verify_bronze = PythonOperator(
        task_id="verify_seoul_traffic_bronze_runtime",
        python_callable=verify_seoul_traffic_bronze_runtime,
        on_success_callback=notify_traffic_bronze_success,
        on_failure_callback=record_and_notify_seoul_traffic_run_failed,
    )

    start_manifest >> ingest_traffic >> verify_bronze
