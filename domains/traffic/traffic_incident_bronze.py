import json
import logging
import os
import re
import sys
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.sdk import Asset
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
from common.assets import TRAFFIC_BRONZE_ASSET  # noqa: E402

from weather.bronze_run_manifest import (  # noqa: E402
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
    resolve_acc_info_page_window,
)
from traffic_ingest.bronze import (  # noqa: E402
    create_seoul_traffic_bronze_table,
    insert_seoul_traffic_bronze_rows,
    verify_seoul_traffic_bronze_runtime as verify_seoul_traffic_bronze_rows,
)
from traffic_ingest.common.runtime import (  # noqa: E402
    download_raw_object,
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
RECOLLECT_DAG_ID = "traffic_incident_recollect"
BACKFILL_DAG_ID = "traffic_incident_bronze_backfill"
TRAFFIC_RAW_KEY_RE = re.compile(
    r"/load_date=(?P<load_date>\d{4}-\d{2}-\d{2})/"
    r"(?P<collected>\d{8}T\d{6})KST_AccInfo-(?P<start_index>\d+)-(?P<end_index>\d+)_"
    r"(?P<request_id>[^/]+)\.xml$"
)

# 공통 에러 모듈 파일럿(#77) — 태스크 최종 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
# 기존 콜백(manifest 기록·Discord 알림)과 리스트로 나란히 걸어 기존 동작은 바꾸지 않는다.
record_traffic_problem = problem_failure_callback(
    domain="traffic", source_system="seoul_topis")


def dag_run_conf(context: dict) -> dict:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return conf if isinstance(conf, dict) else {}


def raw_object_keys_from_conf(context: dict) -> list[str]:
    raw_keys = dag_run_conf(context).get("raw_object_keys")
    if isinstance(raw_keys, str):
        raw_keys = [raw_keys]
    if not isinstance(raw_keys, list):
        raise RuntimeError("dag_run.conf.raw_object_keys must be a non-empty string or list.")
    cleaned = [str(key).strip() for key in raw_keys if str(key).strip()]
    if not cleaned:
        raise RuntimeError("dag_run.conf.raw_object_keys must not be empty.")
    return cleaned


def current_dag_id(context: dict) -> str:
    return getattr(context.get("dag"), "dag_id", DAG_ID)


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
    if "land" in task_id or "ingest" in task_id:
        return "API 수집/R2 적재"
    if "load" in task_id:
        return "Bronze 적재"
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
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": "ask-seoul-airflow/1.0"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception as exc:
        LOGGER.warning("[traffic notify] Discord send failed: %s", type(exc).__name__)


def notify_traffic_bronze_success(context) -> None:
    ti = context["ti"]
    ingest_result = ti.xcom_pull(task_ids="load_seoul_traffic_bronze") or {}
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


def land_seoul_traffic_raw(**context) -> dict:
    start_index, end_index, page_size = resolve_acc_info_page_window(dag_run_conf(context))

    parsed_rows = 0
    raw_objects = []
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
        raw_objects.append(
            {
                "request_id": request_id,
                "raw_object_key": raw_object_key,
                "raw_hash": raw_hash,
                "http_status": http_status,
                "collected_at": collected_at.isoformat(),
                "start_index": page_start,
                "end_index": page_end,
            }
        )
        parsed_rows += len(rows)
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

    print(f"Landed {len(raw_objects)} Seoul traffic raw objects from {len(page_ranges)} pages")
    return {
        "source_id": SOURCE_ID,
        "raw_objects": raw_objects,
        "raw_object_keys": [item["raw_object_key"] for item in raw_objects],
        "result_code": result_code,
        "list_total_count": list_total_count,
        "page_count": len(page_ranges),
        "requested_end_index": max(end for _, end in page_ranges),
        "pages": page_summaries,
    }


def land_seoul_traffic_raw_object_keys(**context) -> dict:
    raw_objects = []
    page_summaries = []
    list_total_count = 0
    result_code = "N/A"
    for raw_object_key in raw_object_keys_from_conf(context):
        match = TRAFFIC_RAW_KEY_RE.search(raw_object_key)
        if not match:
            raise RuntimeError(f"Unsupported Seoul traffic raw_object_key format: {raw_object_key}")
        raw_bytes = download_raw_object(raw_object_key, "Seoul traffic raw payload")
        metadata, rows = parse_seoul_acc_info_response(raw_bytes)
        start_index = int(match.group("start_index"))
        end_index = int(match.group("end_index"))
        result_code = metadata.get("result_code") or result_code
        list_total_count = max(list_total_count, metadata_total_count(metadata))
        collected_at = datetime.strptime(match.group("collected"), "%Y%m%dT%H%M%S").replace(tzinfo=KST)
        raw_objects.append(
            {
                "request_id": match.group("request_id"),
                "raw_object_key": raw_object_key,
                "raw_hash": sha256_hex(raw_bytes),
                "http_status": 200,
                "collected_at": collected_at.isoformat(),
                "start_index": start_index,
                "end_index": end_index,
            }
        )
        page_summaries.append(
            {
                "start_index": start_index,
                "end_index": end_index,
                "row_count": len(rows),
                "list_total_count": metadata_total_count(metadata),
                "raw_object_key": raw_object_key,
            }
        )
    return {
        "source_id": SOURCE_ID,
        "raw_objects": raw_objects,
        "raw_object_keys": [item["raw_object_key"] for item in raw_objects],
        "result_code": result_code,
        "list_total_count": list_total_count,
        "page_count": len(raw_objects),
        "requested_end_index": max(item["end_index"] for item in raw_objects),
        "pages": page_summaries,
    }


def load_seoul_traffic_bronze(**context) -> dict:
    raw_result = context["ti"].xcom_pull(task_ids="land_seoul_traffic_raw") or {}
    raw_objects = raw_result.get("raw_objects") or []
    if not raw_objects:
        raise RuntimeError("Seoul traffic raw landing result is empty; cannot load bronze rows.")
    cursor, catalog, schema = trino_cursor()
    qualified_table = create_seoul_traffic_bronze_table(cursor, catalog, schema)

    inserted = 0
    parsed_rows = 0
    raw_object_keys = []
    result_code = raw_result.get("result_code") or "N/A"
    list_total_count = int(raw_result.get("list_total_count", 0))
    for raw_object in raw_objects:
        raw_bytes = download_raw_object(raw_object["raw_object_key"], "Seoul traffic raw payload")
        metadata, rows = parse_seoul_acc_info_response(raw_bytes)
        result_code = metadata.get("result_code") or result_code
        parsed_rows += len(rows)
        collected_at = datetime.fromisoformat(raw_object["collected_at"])
        inserted += insert_seoul_traffic_bronze_rows(
            cursor=cursor,
            qualified_table=qualified_table,
            rows=rows,
            metadata=metadata,
            request_id=raw_object["request_id"],
            start_index=int(raw_object["start_index"]),
            end_index=int(raw_object["end_index"]),
            raw_object_key=raw_object["raw_object_key"],
            raw_hash=raw_object["raw_hash"],
            http_status=int(raw_object["http_status"]),
            collected_at=collected_at,
            dag_run_id=context["run_id"],
        )
        raw_object_keys.append(raw_object["raw_object_key"])

    if parsed_rows < list_total_count:
        raise RuntimeError(
            "Seoul traffic bronze load incomplete: "
            f"list_total_count={list_total_count}, parsed_rows={parsed_rows}, "
            f"requested_end_index={raw_result.get('requested_end_index', 'N/A')}"
        )

    print(
        f"Inserted {inserted} Seoul traffic rows from {len(raw_objects)} raw objects "
        f"into {qualified_table}"
    )
    return {
        "source_id": SOURCE_ID,
        "raw_object_keys": raw_object_keys,
        "inserted": inserted,
        "result_code": result_code,
        "list_total_count": list_total_count,
        "page_count": int(raw_result.get("page_count", len(raw_objects))),
        "requested_end_index": raw_result.get("requested_end_index"),
        "pages": raw_result.get("pages") or [],
    }


def record_seoul_traffic_run_started(**context) -> str:
    cursor, catalog, schema = trino_cursor()
    return record_bronze_run_event(
        cursor,
        catalog,
        schema,
        source_id=SOURCE_ID,
        dag_id=current_dag_id(context),
        dag_run_id=context["run_id"],
        status=STATUS_STARTED,
    )


def record_seoul_traffic_backfill_run_started(**context) -> str:
    cursor, catalog, schema = trino_cursor()
    return record_bronze_run_event(
        cursor,
        catalog,
        schema,
        source_id=SOURCE_ID,
        dag_id=current_dag_id(context),
        dag_run_id=context["run_id"],
        status=STATUS_STARTED,
        expected_raw_objects=len(raw_object_keys_from_conf(context)),
    )


def record_seoul_traffic_run_failed(context) -> None:
    try:
        cursor, catalog, schema = trino_cursor()
        record_bronze_run_event(
            cursor,
            catalog,
            schema,
            source_id=SOURCE_ID,
            dag_id=current_dag_id(context),
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
    ingest_result = context["ti"].xcom_pull(task_ids="load_seoul_traffic_bronze") or {}
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
        dag_id=current_dag_id(context),
        dag_run_id=context["run_id"],
        status=STATUS_SUCCESS,
        is_publishable=True,
        expected_rows=int(ingest_result["list_total_count"]),
        actual_rows=verified_rows,
        expected_raw_objects=int(ingest_result["page_count"]),
        actual_raw_objects=len(ingest_result["raw_object_keys"]),
    )
    return verified_rows


def build_traffic_bronze_dag(dag_id: str, schedule: str | None, description: str, tags: list[str]):
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
            on_failure_callback=[record_and_notify_seoul_traffic_run_failed, record_traffic_problem],
        )

        load_bronze = PythonOperator(
            task_id="load_seoul_traffic_bronze",
            python_callable=load_seoul_traffic_bronze,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[record_and_notify_seoul_traffic_run_failed, record_traffic_problem],
        )

        verify_bronze = PythonOperator(
            task_id="verify_seoul_traffic_bronze_runtime",
            python_callable=verify_seoul_traffic_bronze_runtime,
            on_failure_callback=[record_and_notify_seoul_traffic_run_failed, record_traffic_problem],
            outlets=[Asset(TRAFFIC_BRONZE_ASSET)],
        )

        validate_runtime >> start_manifest >> land_raw >> load_bronze >> verify_bronze
    return built_dag


def build_traffic_bronze_backfill_dag():
    with DAG(
        dag_id=BACKFILL_DAG_ID,
        description="Loads existing Seoul TOPIS AccInfo raw_object_keys into Iceberg bronze without API calls.",
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
            on_failure_callback=[record_and_notify_seoul_traffic_run_failed, record_traffic_problem],
        )

        load_bronze = PythonOperator(
            task_id="load_seoul_traffic_bronze",
            python_callable=load_seoul_traffic_bronze,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[record_and_notify_seoul_traffic_run_failed, record_traffic_problem],
        )

        verify_bronze = PythonOperator(
            task_id="verify_seoul_traffic_bronze_runtime",
            python_callable=verify_seoul_traffic_bronze_runtime,
            on_failure_callback=[record_and_notify_seoul_traffic_run_failed, record_traffic_problem],
            outlets=[Asset(TRAFFIC_BRONZE_ASSET)],
        )

        validate_runtime >> start_manifest >> land_raw >> load_bronze >> verify_bronze
    return built_dag


dag = build_traffic_bronze_dag(
    DAG_ID,
    traffic_dag_schedule(),
    "Loads Seoul TOPIS AccInfo XML into R2 and validates the Iceberg bronze runtime.",
    ["ask_seoul", "traffic", "bronze", "r2", "iceberg"],
)

recollect_dag = build_traffic_bronze_dag(
    RECOLLECT_DAG_ID,
    None,
    "Manually recollects Seoul TOPIS AccInfo page windows through the Bronze contract.",
    ["ask_seoul", "traffic", "bronze", "recollect", "r2", "iceberg"],
)

backfill_dag = build_traffic_bronze_backfill_dag()
