"""Retry, notification, and manifest support for Traffic Bronze DAGs."""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from collections.abc import Callable
from datetime import datetime
from functools import wraps
from typing import ParamSpec, TypeVar

from airflow.sdk.exceptions import AirflowFailException

from traffic_ingest.acc_info import KST
from traffic_ingest.common.runtime import is_dev_target
from traffic_ingest.errors import TrafficBronzeDeterministicError
from traffic_ingest.errors import TrafficBronzeConfigurationError
from traffic_ingest.run_ledger import (
    STATUS_FAILED,
    STATUS_STARTED,
    STATUS_SUCCESS,
    TrafficRunLedger,
)
from traffic_ingest.run_manifest import TrafficRun


P = ParamSpec("P")
R = TypeVar("R")

TRAFFIC_DISCORD_WEBHOOK_ENV = "TRAFFIC_DISCORD_WEBHOOK_URL"
DISCORD_GREEN = 3066993
DISCORD_RED = 15158332
DAG_ID = "traffic_incident_bronze"
RECOLLECT_DAG_ID = "traffic_incident_recollect"
BACKFILL_DAG_ID = "traffic_incident_bronze_backfill"
LOAD_TRAFFIC_BRONZE_TASK_ID = "load_seoul_traffic_bronze"
LOGGER = logging.getLogger(__name__)


def fail_fast_traffic_bronze(callable_: Callable[P, R]) -> Callable[P, R]:
    """Map only permanent Traffic contract failures to Airflow no-retry errors."""

    @wraps(callable_)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return callable_(*args, **kwargs)
        except TrafficBronzeDeterministicError as exc:
            raise AirflowFailException(str(exc)) from exc

    return wrapped


def dag_run_conf(context: dict) -> dict:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return conf if isinstance(conf, dict) else {}


def raw_object_keys_from_conf(context: dict) -> list[str]:
    raw_keys = dag_run_conf(context).get("raw_object_keys")
    if isinstance(raw_keys, str):
        raw_keys = [raw_keys]
    if not isinstance(raw_keys, list):
        raise TrafficBronzeConfigurationError(
            "dag_run.conf.raw_object_keys must be a non-empty string or list."
        )
    cleaned = [str(key).strip() for key in raw_keys if str(key).strip()]
    if not cleaned:
        raise TrafficBronzeConfigurationError(
            "dag_run.conf.raw_object_keys must not be empty."
        )
    return cleaned


def current_dag_id(context: dict) -> str:
    return getattr(context.get("dag"), "dag_id", DAG_ID)


def discord_report_date(context: dict) -> str:
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
        "embeds": [
            {
                "title": title,
                "description": description[:4096],
                "color": color,
                "footer": {"text": footer[:2048]},
            }
        ]
    }
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "ask-seoul-airflow/1.0",
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception as exc:
        LOGGER.warning("[traffic notify] Discord send failed: %s", type(exc).__name__)


def notify_traffic_bronze_success(context: dict) -> None:
    ingest_result = context["ti"].xcom_pull(task_ids=LOAD_TRAFFIC_BRONZE_TASK_ID) or {}
    inserted = int(ingest_result.get("inserted", 0))
    total_count = ingest_result.get("list_total_count", "N/A")
    raw_keys = ingest_result.get("raw_object_keys") or []
    page_count = ingest_result.get("page_count") or len(raw_keys)
    incident_line = (
        "현재 돌발정보: 0건 (정상 응답)"
        if inserted == 0
        else f"현재 돌발정보: {inserted:,}건"
    )
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
                "테이블: `bronze_seoul_traffic_incident`",
                f"raw 샘플: `{short_text(raw_keys[0] if raw_keys else 'N/A')}`",
            ]
        ),
        DISCORD_GREEN,
        f"dag_id={context['dag'].dag_id} · run_id={short_text(run_id, 180)}",
    )


def notify_traffic_bronze_failure(context: dict) -> None:
    task_instance = context.get("ti") or context.get("task_instance")
    task_id = getattr(task_instance, "task_id", "N/A")
    error = context.get("exception")
    run_id = context.get("run_id", "N/A")
    send_traffic_discord(
        f"서울시 돌발정보 수집 실패 - {discord_report_date(context)} (target={target_name()})",
        "\n".join(
            [
                "❌ 수집 상태: 실패",
                f"❌ 실패 단계: {stage_name(task_id)}",
                f"❌ 실패 task: `{task_id}`",
                f"❌ 오류 유형: `{type(error).__name__ if error else 'N/A'}`",
                "",
                f"Airflow 로그: {getattr(task_instance, 'log_url', 'N/A')}",
            ]
        ),
        DISCORD_RED,
        f"dag_id={context['dag'].dag_id} · run_id={short_text(run_id, 180)}",
    )


def traffic_dag_schedule() -> str | None:
    if "ASK_SEOUL_TRAFFIC_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_TRAFFIC_DAG_SCHEDULE"] or None
    return "*/5 * * * *" if is_dev_target() else None


def start_traffic_run(context: dict, *, manifest_factory: Callable) -> str:
    return manifest_factory().start(
        TrafficRun(current_dag_id(context), context["run_id"])
    )


def start_traffic_backfill_run(context: dict, *, manifest_factory: Callable) -> str:
    return manifest_factory().start(
        TrafficRun(current_dag_id(context), context["run_id"]),
        expected_raw_objects=len(raw_object_keys_from_conf(context)),
    )


def record_traffic_run_ledger_started(context: dict) -> None:
    _record_traffic_run_ledger(context, status=STATUS_STARTED)


def record_traffic_run_ledger_success(context: dict) -> None:
    _record_traffic_run_ledger(context, status=STATUS_SUCCESS)


def _record_traffic_run_ledger(context: dict, *, status: str) -> None:
    try:
        task_instance = context.get("ti") or context.get("task_instance")
        TrafficRunLedger().record(
            dag_id=current_dag_id(context),
            run_id=str(context["run_id"]),
            status=status,
            logical_date=(
                context.get("logical_date")
                or getattr(context.get("dag_run"), "logical_date", None)
            ),
            task_id=getattr(task_instance, "task_id", None),
            error=context.get("exception"),
        )
    except Exception as exc:
        LOGGER.warning(
            "Failed to record Traffic run ledger event: status=%s error_type=%s",
            status,
            type(exc).__name__,
        )


def fail_traffic_run(context: dict, *, manifest_factory: Callable) -> None:
    _record_traffic_run_ledger(context, status=STATUS_FAILED)
    try:
        task_instance = context.get("ti") or context.get("task_instance")
        error = context.get("exception") or RuntimeError("Airflow task failed")
        manifest_factory().fail(
            TrafficRun(current_dag_id(context), context["run_id"]),
            task_id=getattr(task_instance, "task_id", "unknown"),
            error=error,
        )
    except Exception as exc:
        LOGGER.warning(
            "Failed to record Seoul traffic run manifest failure: %s",
            type(exc).__name__,
        )


__all__ = [
    "BACKFILL_DAG_ID",
    "DAG_ID",
    "DISCORD_GREEN",
    "DISCORD_RED",
    "LOAD_TRAFFIC_BRONZE_TASK_ID",
    "RECOLLECT_DAG_ID",
    "TRAFFIC_DISCORD_WEBHOOK_ENV",
    "current_dag_id",
    "dag_run_conf",
    "discord_report_date",
    "fail_fast_traffic_bronze",
    "fail_traffic_run",
    "notify_traffic_bronze_failure",
    "notify_traffic_bronze_success",
    "raw_object_keys_from_conf",
    "record_traffic_run_ledger_started",
    "record_traffic_run_ledger_success",
    "send_traffic_discord",
    "short_text",
    "stage_name",
    "start_traffic_backfill_run",
    "start_traffic_run",
    "target_name",
    "traffic_dag_schedule",
]
