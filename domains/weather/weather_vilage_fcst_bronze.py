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
    load_kma_grids,
    parse_kma_response,
    resolve_kma_base_datetime,
)


KMA_PUBLISH_CRON_KST = "20 2,5,8,11,14,17,20,23 * * *"
WEATHER_DISCORD_WEBHOOK_ENV = "WEATHER_DISCORD_WEBHOOK_URL"
DISCORD_GREEN = 3066993
DISCORD_RED = 15158332
LOGGER = logging.getLogger(__name__)
DAG_ID = "weather_vilage_fcst_bronze"


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


def send_weather_discord(title: str, description: str, color: int, footer: str) -> None:
    webhook_url = (os.environ.get(WEATHER_DISCORD_WEBHOOK_ENV) or "").strip()
    if not webhook_url:
        LOGGER.info("[weather notify:noop] %s (webhook url not configured)", title)
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
        headers={"Content-Type": "application/json", "User-Agent": "ask-seoul-airflow/1.0"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception as exc:
        LOGGER.warning("[weather notify] Discord send failed: %s", type(exc).__name__)


def notify_weather_bronze_success(context) -> None:
    ti = context["ti"]
    ingest_result = ti.xcom_pull(task_ids="ingest_kma_vilage_fcst") or {}
    raw_keys = ingest_result.get("raw_object_keys") or []
    run_id = context["run_id"]
    send_weather_discord(
        f"기상청 단기예보 수집 리포트 - {discord_report_date(context)} (target={target_name()})",
        "\n".join(
            [
                "✅ 수집 상태: 성공",
                f"✅ 예보 발표시각: {ingest_result.get('base_date', 'N/A')} {ingest_result.get('base_time', 'N/A')}",
                f"✅ 서울 격자 커버리지: {ingest_result.get('grid_count', 'N/A')}개 grid / raw {len(raw_keys)}개",
                f"✅ Bronze 적재: {int(ingest_result.get('inserted', 0)):,}행",
                "",
                f"테이블: `bronze_kma_vilage_fcst`",
                f"raw 샘플: `{short_text(raw_keys[0] if raw_keys else 'N/A')}`",
            ]
        ),
        DISCORD_GREEN,
        f"dag_id={context['dag'].dag_id} · run_id={short_text(run_id, 180)}",
    )


def notify_weather_bronze_failure(context) -> None:
    ti = context.get("ti") or context.get("task_instance")
    task_id = getattr(ti, "task_id", "N/A")
    exc = context.get("exception")
    run_id = context.get("run_id", "N/A")
    send_weather_discord(
        f"기상청 단기예보 수집 실패 - {discord_report_date(context)} (target={target_name()})",
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


def kma_dag_schedule() -> str | None:
    if "ASK_SEOUL_KMA_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_KMA_DAG_SCHEDULE"] or None
    return KMA_PUBLISH_CRON_KST if is_dev_target() else None


def ingest_kma_vilage_fcst(**context) -> dict:
    base_date, base_time = resolve_kma_base_datetime()
    grids = load_kma_grids()
    cursor, catalog, schema = trino_cursor()
    qualified_table = create_kma_bronze_table(cursor, catalog, schema)
    inserted = 0
    expected_rows = 0
    raw_object_keys = []
    for grid in grids:
        collected_at = datetime.now(timezone.utc)
        request_id = str(uuid.uuid4())
        nx = int(grid["nx"])
        ny = int(grid["ny"])
        url = build_kma_url(base_date=base_date, base_time=base_time, nx=nx, ny=ny)
        http_status, raw_bytes = fetch_url(url, "ask-seoul-kma-bronze/1.0")
        metadata, rows = parse_kma_response(raw_bytes)
        expected_rows += int(metadata.get("total_count") or len(rows))
        raw_hash = sha256_hex(raw_bytes)
        raw_object_key = build_raw_object_key(
            collected_at=collected_at,
            request_id=request_id,
            base_date=base_date,
            base_time=base_time,
            nx=nx,
            ny=ny,
        )
        upload_raw_object(
            raw_bytes=raw_bytes,
            object_key=raw_object_key,
            content_type="application/json; charset=utf-8",
            log_label="KMA raw payload",
        )
        inserted += insert_kma_bronze_rows(
            cursor=cursor,
            qualified_table=qualified_table,
            rows=rows,
            metadata=metadata,
            request_id=request_id,
            place_id=str(grid["place_id"]),
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
        raw_object_keys.append(raw_object_key)
    print(f"Inserted {inserted} KMA rows for {len(grids)} grids into {qualified_table}")
    return {
        "source_id": SOURCE_ID,
        "raw_object_keys": raw_object_keys,
        "inserted": inserted,
        "expected_rows": expected_rows,
        "grid_count": len(grids),
        "base_date": base_date,
        "base_time": base_time,
    }


def record_kma_run_started(**context) -> str:
    cursor, catalog, schema = trino_cursor()
    return record_bronze_run_event(
        cursor,
        catalog,
        schema,
        source_id=SOURCE_ID,
        dag_id=DAG_ID,
        dag_run_id=context["run_id"],
        status=STATUS_STARTED,
        expected_raw_objects=len(load_kma_grids()),
    )


def record_kma_run_failed(context) -> None:
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
        print(f"Failed to record KMA run manifest failure: {type(exc).__name__}")


def record_and_notify_kma_run_failed(context) -> None:
    record_kma_run_failed(context)
    notify_weather_bronze_failure(context)


def verify_kma_bronze_runtime(**context) -> int:
    ingest_result = context["ti"].xcom_pull(task_ids="ingest_kma_vilage_fcst") or {}
    verified_rows = verify_kma_bronze_rows(
        raw_object_keys=ingest_result["raw_object_keys"],
        dag_run_id=context["run_id"],
        expected_rows=int(ingest_result["inserted"]),
        expected_raw_objects=int(ingest_result["grid_count"]),
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
        expected_rows=int(ingest_result["expected_rows"]),
        actual_rows=verified_rows,
        expected_raw_objects=int(ingest_result["grid_count"]),
        actual_raw_objects=len(ingest_result["raw_object_keys"]),
    )
    return verified_rows


with DAG(
    dag_id=DAG_ID,
    description="Loads KMA getVilageFcst raw JSON into R2 and validates the Iceberg bronze runtime.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=kma_dag_schedule(),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=record_kma_run_failed,
    tags=["ask_seoul", "kma", "bronze", "r2", "iceberg"],
) as dag:
    start_manifest = PythonOperator(
        task_id="record_kma_run_started",
        python_callable=record_kma_run_started,
    )

    ingest_kma = PythonOperator(
        task_id="ingest_kma_vilage_fcst",
        python_callable=ingest_kma_vilage_fcst,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        on_failure_callback=record_and_notify_kma_run_failed,
    )

    verify_bronze = PythonOperator(
        task_id="verify_kma_bronze_runtime",
        python_callable=verify_kma_bronze_runtime,
        on_success_callback=notify_weather_bronze_success,
        on_failure_callback=record_and_notify_kma_run_failed,
    )

    start_manifest >> ingest_kma >> verify_bronze
