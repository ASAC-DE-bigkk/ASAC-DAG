import json
import logging
import os
import re
import sys
import time
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
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
DAGS_ROOT_DIR = os.path.dirname(DOMAINS_DIR)
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.assets import WEATHER_BRONZE_ASSET  # noqa: E402

from _shared.bronze_run_manifest import (  # noqa: E402
    STATUS_FAILED,
    STATUS_STARTED,
    STATUS_SUCCESS,
    failure_reason_from_context,
    record_bronze_run_event,
)
from weather_ingest.bronze import (  # noqa: E402
    append_kma_bronze_row_batches_pyiceberg,
    create_kma_bronze_table,
    verify_kma_bronze_runtime as verify_kma_bronze_rows,
)
from weather_ingest.common.runtime import (  # noqa: E402
    download_raw_object,
    fetch_url,
    is_dev_target,
    raw_prefix,
    sha256_hex,
    trino_cursor,
    upload_raw_object,
)
from weather_ingest.kma import (  # noqa: E402
    KST,
    SOURCE_ID,
    build_kma_url,
    build_raw_object_key,
    kma_base_datetime_from_conf,
    kma_num_of_rows,
    kma_page_numbers,
    load_kma_grids,
    parse_kma_response,
    resolve_kma_base_datetime,
)


KMA_PUBLISH_CRON_KST = "20 2,5,8,11,14,17,20,23 * * *"
WEATHER_DISCORD_WEBHOOK_ENV = "WEATHER_DISCORD_WEBHOOK_URL"
KMA_REQUEST_DELAY_SECONDS = 5
KMA_RETRY_STATUSES = (429, 500, 502, 503, 504)
DISCORD_GREEN = 3066993
DISCORD_RED = 15158332
LOGGER = logging.getLogger(__name__)
DAG_ID = "weather_vilage_fcst_bronze"
RECOLLECT_DAG_ID = "weather_vilage_fcst_recollect"
BACKFILL_DAG_ID = "weather_vilage_fcst_bronze_backfill"
KMA_RAW_KEY_RE = re.compile(
    r"/load_date=(?P<load_date>\d{4}-\d{2}-\d{2})/nx=(?P<nx>\d+)/ny=(?P<ny>\d+)/"
    r"(?P<collected>\d{8}T\d{6})KST_base-(?P<base_date>\d{8})(?P<base_time>\d{4})_"
    r"(?P<request_id>[^/]+)\.json$"
)

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_weather_problem = problem_failure_callback(domain="weather", source_system=SOURCE_ID)


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


def safe_object_key_segment(value: object) -> str:
    return "".join(ch if ch.isalnum() or ch in "._=-" else "_" for ch in str(value or "unknown"))


def kma_landing_checkpoint_key(context: dict, base_date: str, base_time: str) -> str:
    dag_id = safe_object_key_segment(current_dag_id(context))
    run_id = safe_object_key_segment(context["run_id"])
    return (
        f"{raw_prefix().rstrip('/')}"
        f"/_checkpoints/{SOURCE_ID}/dag_id={dag_id}/run_id={run_id}/base-{base_date}{base_time}.json"
    )


def is_missing_r2_object_error(exc: Exception) -> bool:
    error = (getattr(exc, "response", {}) or {}).get("Error", {})
    return str(error.get("Code", "")) in {"NoSuchKey", "NotFound", "404"}


def raw_object_grid_key(raw_object: dict) -> tuple[int, int]:
    return int(raw_object["nx"]), int(raw_object["ny"])


def raw_object_page_no(raw_object: dict) -> int:
    return int(raw_object.get("page_no") or 1)


def kma_response_page_info(raw_bytes: bytes) -> tuple[int, int]:
    payload = json.loads(raw_bytes.decode("utf-8"))
    body = (payload.get("response") or {}).get("body") or {}
    return int(body.get("pageNo") or 1), int(body.get("numOfRows") or kma_num_of_rows())


def checkpoint_by_grid_page(raw_objects: list[dict]) -> dict[tuple[int, int], dict[int, dict]]:
    grouped: dict[tuple[int, int], dict[int, dict]] = {}
    for raw_object in raw_objects:
        grouped.setdefault(raw_object_grid_key(raw_object), {})[raw_object_page_no(raw_object)] = raw_object
    return grouped


def load_kma_landing_checkpoint(checkpoint_key: str, base_date: str, base_time: str) -> list[dict]:
    try:
        raw_bytes = download_raw_object(checkpoint_key, "KMA landing checkpoint")
    except Exception as exc:
        if is_missing_r2_object_error(exc):
            return []
        raise
    payload = json.loads(raw_bytes.decode("utf-8"))
    if payload.get("base_date") != base_date or payload.get("base_time") != base_time:
        raise RuntimeError(f"KMA landing checkpoint base mismatch: {checkpoint_key}")
    raw_objects = payload.get("raw_objects") or []
    if not isinstance(raw_objects, list):
        raise RuntimeError(f"KMA landing checkpoint raw_objects must be a list: {checkpoint_key}")
    return raw_objects


def save_kma_landing_checkpoint(
    checkpoint_key: str,
    *,
    context: dict,
    base_date: str,
    base_time: str,
    raw_objects: list[dict],
) -> None:
    payload = {
        "source_id": SOURCE_ID,
        "dag_id": current_dag_id(context),
        "dag_run_id": context["run_id"],
        "base_date": base_date,
        "base_time": base_time,
        "raw_objects": raw_objects,
    }
    upload_raw_object(
        raw_bytes=json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8"),
        object_key=checkpoint_key,
        content_type="application/json; charset=utf-8",
        log_label="KMA landing checkpoint",
    )


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
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": "ask-seoul-airflow/1.0"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception as exc:
        LOGGER.warning("[weather notify] Discord send failed: %s", type(exc).__name__)


def notify_weather_bronze_success(context) -> None:
    ti = context["ti"]
    ingest_result = ti.xcom_pull(task_ids="load_kma_bronze") or {}
    raw_keys = ingest_result.get("raw_object_keys") or []
    api_call_count = ingest_result.get("api_call_count", len(raw_keys))
    api_request_count = ingest_result.get("api_request_count", "N/A")
    run_id = context["run_id"]
    send_weather_discord(
        f"기상청 단기예보 수집 리포트 - {discord_report_date(context)} (target={target_name()})",
        "\n".join(
            [
                "✅ 수집 상태: 성공",
                f"✅ 예보 발표시각: {ingest_result.get('base_date', 'N/A')} {ingest_result.get('base_time', 'N/A')}",
                f"✅ raw page: {api_call_count}개",
                f"✅ actual API requests: {api_request_count}회",
                f"✅ 서울 격자 커버리지: {ingest_result.get('grid_count', 'N/A')}개 grid",
                f"✅ raw JSON: {len(raw_keys)}개",
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


def land_kma_raw(**context) -> dict:
    base_date, base_time = (
        kma_base_datetime_from_conf(dag_run_conf(context))
        or resolve_kma_base_datetime()
    )
    grids = load_kma_grids()
    num_of_rows = kma_num_of_rows()
    checkpoint_key = kma_landing_checkpoint_key(context, base_date, base_time)
    checkpoint_raw_objects = load_kma_landing_checkpoint(checkpoint_key, base_date, base_time)
    checkpoint_pages = checkpoint_by_grid_page(checkpoint_raw_objects)
    raw_objects = []
    api_request_count = 0
    reused_raw_object_count = 0

    def fetch_raw_page(grid: dict, page_no: int) -> dict:
        nonlocal api_request_count
        nx = int(grid["nx"])
        ny = int(grid["ny"])
        if api_request_count:
            time.sleep(KMA_REQUEST_DELAY_SECONDS)
        collected_at = datetime.now(timezone.utc)
        request_id = str(uuid.uuid4())
        url = build_kma_url(
            base_date=base_date,
            base_time=base_time,
            nx=nx,
            ny=ny,
            page_no=page_no,
            num_of_rows=num_of_rows,
        )
        http_status, raw_bytes = fetch_url(
            url,
            "ask-seoul-kma-bronze/1.0",
            max_attempts=4,
            retry_statuses=KMA_RETRY_STATUSES,
            retry_base_delay_seconds=30,
        )
        metadata, rows = parse_kma_response(raw_bytes)
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
        api_request_count += 1
        return {
            "request_id": request_id,
            "raw_object_key": raw_object_key,
            "raw_hash": raw_hash,
            "http_status": http_status,
            "collected_at": collected_at.isoformat(),
            "place_id": str(grid["place_id"]),
            "base_date": base_date,
            "base_time": base_time,
            "nx": nx,
            "ny": ny,
            "page_no": page_no,
            "num_of_rows": num_of_rows,
            "total_count": int(metadata.get("total_count") or len(rows)),
            "row_count": len(rows),
        }

    def ensure_page_metadata(raw_object: dict, default_page_no: int) -> dict:
        raw_object.setdefault("page_no", default_page_no)
        raw_object.setdefault("num_of_rows", num_of_rows)
        if raw_object.get("total_count") is None or raw_object.get("row_count") is None:
            raw_bytes = download_raw_object(raw_object["raw_object_key"], "KMA raw payload")
            metadata, rows = parse_kma_response(raw_bytes)
            raw_object["total_count"] = int(metadata.get("total_count") or len(rows))
            raw_object["row_count"] = len(rows)
        return raw_object

    for grid in grids:
        nx = int(grid["nx"])
        ny = int(grid["ny"])
        existing_pages = checkpoint_pages.get((nx, ny), {})
        page_one = existing_pages.get(1)
        page_one_reused = page_one is not None
        if page_one_reused:
            page_one = ensure_page_metadata(page_one, 1)
            reused_raw_object_count += 1
        else:
            page_one = fetch_raw_page(grid, 1)
        page_numbers = kma_page_numbers(page_one["total_count"], int(page_one.get("num_of_rows") or num_of_rows))
        page_one["page_count"] = len(page_numbers)
        raw_objects.append(page_one)
        if not page_one_reused:
            save_kma_landing_checkpoint(
                checkpoint_key,
                context=context,
                base_date=base_date,
                base_time=base_time,
                raw_objects=raw_objects,
            )
        for page_no in page_numbers[1:]:
            existing_raw_object = existing_pages.get(page_no)
            if existing_raw_object:
                existing_raw_object = ensure_page_metadata(existing_raw_object, page_no)
                existing_raw_object["page_count"] = len(page_numbers)
                raw_objects.append(existing_raw_object)
                reused_raw_object_count += 1
                continue
            raw_object = fetch_raw_page(grid, page_no)
            raw_object["page_count"] = len(page_numbers)
            raw_objects.append(raw_object)
            save_kma_landing_checkpoint(
                checkpoint_key,
                context=context,
                base_date=base_date,
                base_time=base_time,
                raw_objects=raw_objects,
            )
    print(
        f"Landed {len(raw_objects)} KMA raw objects for {len(grids)} grids "
        f"(reused={reused_raw_object_count}, api_requests={api_request_count})"
    )
    return {
        "source_id": SOURCE_ID,
        "raw_objects": raw_objects,
        "raw_object_keys": [item["raw_object_key"] for item in raw_objects],
        "grid_count": len(grids),
        "api_call_count": len(raw_objects),
        "api_request_count": api_request_count,
        "reused_raw_object_count": reused_raw_object_count,
        "raw_page_count": len(raw_objects),
        "expected_raw_object_count": len(raw_objects),
        "base_date": base_date,
        "base_time": base_time,
    }


def land_kma_raw_object_keys(**context) -> dict:
    grid_place_ids = {(int(grid["nx"]), int(grid["ny"])): str(grid["place_id"]) for grid in load_kma_grids()}
    raw_objects = []
    for raw_object_key in raw_object_keys_from_conf(context):
        match = KMA_RAW_KEY_RE.search(raw_object_key)
        if not match:
            raise RuntimeError(f"Unsupported KMA raw_object_key format: {raw_object_key}")
        raw_bytes = download_raw_object(raw_object_key, "KMA raw payload")
        metadata, rows = parse_kma_response(raw_bytes)
        page_no, num_of_rows = kma_response_page_info(raw_bytes)
        nx = int(match.group("nx"))
        ny = int(match.group("ny"))
        collected_at = datetime.strptime(match.group("collected"), "%Y%m%dT%H%M%S").replace(tzinfo=KST)
        raw_objects.append(
            {
                "request_id": match.group("request_id"),
                "raw_object_key": raw_object_key,
                "raw_hash": sha256_hex(raw_bytes),
                "http_status": 200,
                "collected_at": collected_at.isoformat(),
                "place_id": grid_place_ids.get((nx, ny), f"kma_{nx}_{ny}"),
                "base_date": match.group("base_date"),
                "base_time": match.group("base_time"),
                "nx": nx,
                "ny": ny,
                "page_no": page_no,
                "num_of_rows": num_of_rows,
                "total_count": int(metadata.get("total_count") or len(rows)),
                "row_count": len(rows),
            }
        )
    base_date = raw_objects[0]["base_date"]
    base_time = raw_objects[0]["base_time"]
    return {
        "source_id": SOURCE_ID,
        "raw_objects": raw_objects,
        "raw_object_keys": [item["raw_object_key"] for item in raw_objects],
        "grid_count": len({(item["nx"], item["ny"]) for item in raw_objects}),
        "api_call_count": len(raw_objects),
        "api_request_count": 0,
        "reused_raw_object_count": len(raw_objects),
        "raw_page_count": len(raw_objects),
        "expected_raw_object_count": len(raw_objects),
        "base_date": base_date,
        "base_time": base_time,
    }


def load_kma_bronze(**context) -> dict:
    raw_result = context["ti"].xcom_pull(task_ids="land_kma_raw") or {}
    raw_objects = raw_result.get("raw_objects") or []
    if not raw_objects:
        raise RuntimeError("KMA raw landing result is empty; cannot load bronze rows.")
    cursor, catalog, schema = trino_cursor()
    qualified_table = create_kma_bronze_table(cursor, catalog, schema)
    parsed_pages = []
    grid_summaries = {}
    raw_object_keys = []
    for raw_object in raw_objects:
        raw_bytes = download_raw_object(raw_object["raw_object_key"], "KMA raw payload")
        metadata, rows = parse_kma_response(raw_bytes)
        metadata = dict(metadata)
        page_no = raw_object_page_no(raw_object)
        num_of_rows = int(raw_object.get("num_of_rows") or kma_num_of_rows())
        metadata["page_no"] = page_no
        metadata["num_of_rows"] = num_of_rows
        total_count = int(metadata.get("total_count") or len(rows))
        grid_key = (
            raw_object["base_date"],
            raw_object["base_time"],
            int(raw_object["nx"]),
            int(raw_object["ny"]),
        )
        summary = grid_summaries.setdefault(
            grid_key,
            {"total_count": total_count, "parsed_rows": 0, "pages": set(), "num_of_rows": num_of_rows},
        )
        summary["total_count"] = max(int(summary["total_count"]), total_count)
        summary["parsed_rows"] = int(summary["parsed_rows"]) + len(rows)
        summary["pages"].add(page_no)
        summary["num_of_rows"] = max(int(summary["num_of_rows"]), num_of_rows)
        collected_at = datetime.fromisoformat(raw_object["collected_at"])
        parsed_pages.append(
            {
                "raw_object": raw_object,
                "metadata": metadata,
                "rows": rows,
                "collected_at": collected_at,
                "page_no": page_no,
                "num_of_rows": num_of_rows,
                "grid_key": grid_key,
            }
        )
        raw_object_keys.append(raw_object["raw_object_key"])

    for grid_key, summary in grid_summaries.items():
        expected_pages = set(kma_page_numbers(summary["total_count"], summary["num_of_rows"]))
        parsed_rows = int(summary["parsed_rows"])
        total_count = int(summary["total_count"])
        if not expected_pages.issubset(summary["pages"]) or parsed_rows < total_count:
            base_date, base_time, nx, ny = grid_key
            raise RuntimeError(
                "KMA bronze pagination incomplete: "
                f"base_date={base_date}, base_time={base_time}, nx={nx}, ny={ny}, "
                f"total_count={total_count}, parsed_rows={parsed_rows}, "
                f"expected_pages={sorted(expected_pages)}, actual_pages={sorted(summary['pages'])}"
            )

    batch_inputs = []
    for page in sorted(
        parsed_pages,
        key=lambda item: (
            item["grid_key"][0],
            item["grid_key"][1],
            item["grid_key"][2],
            item["grid_key"][3],
            item["page_no"],
        ),
    ):
        raw_object = page["raw_object"]
        batch_inputs.append(
            {
                "metadata": page["metadata"],
                "rows": page["rows"],
                "request_id": raw_object["request_id"],
                "place_id": raw_object["place_id"],
                "base_date": raw_object["base_date"],
                "base_time": raw_object["base_time"],
                "nx": int(raw_object["nx"]),
                "ny": int(raw_object["ny"]),
                "raw_object_key": raw_object["raw_object_key"],
                "raw_hash": raw_object["raw_hash"],
                "http_status": int(raw_object["http_status"]),
                "collected_at": page["collected_at"],
                "page_no": page["page_no"],
                "num_of_rows": page["num_of_rows"],
            }
        )

    inserted = append_kma_bronze_row_batches_pyiceberg(
        schema=schema,
        row_batches=batch_inputs,
        dag_run_id=context["run_id"],
        delete_existing=True,
    )
    expected_rows = sum(int(summary["total_count"]) for summary in grid_summaries.values())
    print(f"Inserted {inserted} KMA rows for {len(raw_objects)} raw objects into {qualified_table}")
    return {
        "source_id": SOURCE_ID,
        "raw_object_keys": raw_object_keys,
        "inserted": inserted,
        "expected_rows": expected_rows,
        "grid_count": int(raw_result.get("grid_count", len(raw_objects))),
        "api_call_count": int(raw_result.get("api_call_count", len(raw_objects))),
        "api_request_count": int(raw_result.get("api_request_count", 0)),
        "reused_raw_object_count": int(raw_result.get("reused_raw_object_count", 0)),
        "raw_page_count": len(raw_objects),
        "expected_raw_object_count": len(raw_objects),
        "base_date": raw_result.get("base_date"),
        "base_time": raw_result.get("base_time"),
    }


def record_kma_run_started(**context) -> str:
    cursor, catalog, schema = trino_cursor()
    return record_bronze_run_event(
        cursor,
        catalog,
        schema,
        source_id=SOURCE_ID,
        dag_id=current_dag_id(context),
        dag_run_id=context["run_id"],
        status=STATUS_STARTED,
        expected_raw_objects=len(load_kma_grids()),
    )


def record_kma_backfill_run_started(**context) -> str:
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


def record_kma_run_failed(context) -> None:
    try:
        ti = context.get("ti") or context.get("task_instance")
        raw_result = {}
        if ti is not None:
            raw_result = ti.xcom_pull(task_ids="land_kma_raw") or {}
        raw_keys = raw_result.get("raw_object_keys") or []
        cursor, catalog, schema = trino_cursor()
        record_bronze_run_event(
            cursor,
            catalog,
            schema,
            source_id=SOURCE_ID,
            dag_id=current_dag_id(context),
            dag_run_id=context["run_id"],
            status=STATUS_FAILED,
            expected_raw_objects=(
                int(raw_result["expected_raw_object_count"])
                if raw_result.get("expected_raw_object_count") is not None
                else (len(raw_keys) or None)
            ),
            actual_raw_objects=(len(raw_keys) or None),
            failure_reason=failure_reason_from_context(context),
        )
    except Exception as exc:
        print(f"Failed to record KMA run manifest failure: {type(exc).__name__}")


def record_and_notify_kma_run_failed(context) -> None:
    record_kma_run_failed(context)
    notify_weather_bronze_failure(context)


def verify_kma_bronze_runtime(**context) -> int:
    ingest_result = context["ti"].xcom_pull(task_ids="load_kma_bronze") or {}
    verified_rows = verify_kma_bronze_rows(
        raw_object_keys=ingest_result["raw_object_keys"],
        dag_run_id=context["run_id"],
        expected_rows=int(ingest_result["inserted"]),
        expected_raw_objects=int(ingest_result["expected_raw_object_count"]),
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
        expected_rows=int(ingest_result["expected_rows"]),
        actual_rows=verified_rows,
        expected_raw_objects=int(ingest_result["expected_raw_object_count"]),
        actual_raw_objects=len(ingest_result["raw_object_keys"]),
    )
    return verified_rows


def build_kma_bronze_dag(dag_id: str, schedule: str | None, description: str, tags: list[str]):
    with DAG(
        dag_id=dag_id,
        description=description,
        start_date=datetime(2026, 1, 1, tzinfo=KST),
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        on_failure_callback=record_kma_run_failed,
        tags=tags,
    ) as built_dag:
        start_manifest = PythonOperator(
            task_id="record_kma_run_started",
            python_callable=record_kma_run_started,
            on_failure_callback=record_weather_problem,
        )

        land_raw = PythonOperator(
            task_id="land_kma_raw",
            python_callable=land_kma_raw,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[record_and_notify_kma_run_failed, record_weather_problem],
        )

        load_bronze = PythonOperator(
            task_id="load_kma_bronze",
            python_callable=load_kma_bronze,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[record_and_notify_kma_run_failed, record_weather_problem],
        )

        verify_bronze = PythonOperator(
            task_id="verify_kma_bronze_runtime",
            python_callable=verify_kma_bronze_runtime,
            on_success_callback=notify_weather_bronze_success,
            on_failure_callback=[record_and_notify_kma_run_failed, record_weather_problem],
            outlets=[Asset(WEATHER_BRONZE_ASSET)],
        )

        start_manifest >> land_raw >> load_bronze >> verify_bronze
    return built_dag


def build_kma_bronze_backfill_dag():
    with DAG(
        dag_id=BACKFILL_DAG_ID,
        description="Loads existing KMA getVilageFcst raw_object_keys into Iceberg bronze without API calls.",
        start_date=datetime(2026, 1, 1, tzinfo=KST),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        on_failure_callback=record_kma_run_failed,
        tags=["ask_seoul", "kma", "bronze", "backfill", "r2", "iceberg"],
    ) as built_dag:
        start_manifest = PythonOperator(
            task_id="record_kma_run_started",
            python_callable=record_kma_backfill_run_started,
            on_failure_callback=record_weather_problem,
        )

        land_raw = PythonOperator(
            task_id="land_kma_raw",
            python_callable=land_kma_raw_object_keys,
            on_failure_callback=[record_and_notify_kma_run_failed, record_weather_problem],
        )

        load_bronze = PythonOperator(
            task_id="load_kma_bronze",
            python_callable=load_kma_bronze,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[record_and_notify_kma_run_failed, record_weather_problem],
        )

        verify_bronze = PythonOperator(
            task_id="verify_kma_bronze_runtime",
            python_callable=verify_kma_bronze_runtime,
            on_failure_callback=[record_and_notify_kma_run_failed, record_weather_problem],
            outlets=[Asset(WEATHER_BRONZE_ASSET)],
        )

        start_manifest >> land_raw >> load_bronze >> verify_bronze
    return built_dag


dag = build_kma_bronze_dag(
    DAG_ID,
    kma_dag_schedule(),
    "Loads KMA getVilageFcst raw JSON into R2 and validates the Iceberg bronze runtime.",
    ["ask_seoul", "kma", "bronze", "r2", "iceberg"],
)

recollect_dag = build_kma_bronze_dag(
    RECOLLECT_DAG_ID,
    None,
    "Manually recollects a KMA getVilageFcst base_date/base_time through the Bronze contract.",
    ["ask_seoul", "kma", "bronze", "recollect", "r2", "iceberg"],
)

backfill_dag = build_kma_bronze_backfill_dag()
