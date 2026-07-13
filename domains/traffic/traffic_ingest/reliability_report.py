from __future__ import annotations

import json
import logging
import math
import os
import re
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KST = ZoneInfo("Asia/Seoul")
LOGGER = logging.getLogger(__name__)

TRAFFIC_BRONZE_DAG_ID = "traffic_incident_bronze"
# ``traffic_incident_bronze`` runs on a five-minute cron in the dev smoke flow.
# The interval is added to the first-to-last failed slot so the reported window
# includes the final slot's collection period.
TRAFFIC_SCHEDULE_INTERVAL_MINUTES = 5
TRAFFIC_TABLE = "bronze_seoul_traffic_incident"
TRAFFIC_AUDIT_TABLE = "bronze_seoul_traffic_incident_request_audit"
MANIFEST_TABLE = "bronze_collection_run_manifest"
WEBHOOK_ENVS = ("ASK_SEOUL_DISCORD_WEBHOOK_URL", "TRAFFIC_DISCORD_WEBHOOK_URL")
SCHEDULE_ENV = "ASK_SEOUL_TRAFFIC_REPORT_DAG_SCHEDULE"
GLOBAL_SCHEDULE_ENV = "ASK_SEOUL_REPORT_DAG_SCHEDULE"
DISCORD_GREEN = 3066993
DISCORD_YELLOW = 16776960
DISCORD_RED = 15158332
AIRFLOW_FAILURE_REASON_FALLBACK = "원인 미확인"
AIRFLOW_FAILURE_REASON_MAX_LENGTH = 240
PROBLEM_DOCUMENT_PREFIX = "errors"


@dataclass(frozen=True)
class TrafficReportConfig:
    catalog: str
    schema: str
    lookback_hours: int
    freshness_warn_minutes: int
    freshness_error_minutes: int


def is_dev_target(env: Mapping[str, str] = os.environ) -> bool:
    return env.get("ASK_SEOUL_TARGET", env.get("DBT_TARGET", "prod")) == "dev"


def discord_webhook_url(env: Mapping[str, str] = os.environ) -> str | None:
    for key in WEBHOOK_ENVS:
        value = (env.get(key) or "").strip()
        if value:
            return value
    return None


def report_dag_schedule(env: Mapping[str, str] = os.environ) -> str | None:
    if not is_dev_target(env):
        return None
    if SCHEDULE_ENV in env:
        return env[SCHEDULE_ENV] or None
    if GLOBAL_SCHEDULE_ENV in env:
        return env[GLOBAL_SCHEDULE_ENV] or None
    if not discord_webhook_url(env):
        return None
    return "*/15 * * * *"


def sql_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def trino_catalog(env: Mapping[str, str] = os.environ) -> str:
    if is_dev_target(env):
        return env.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    return env.get("TRINO_ICEBERG_CATALOG", "iceberg")


def ask_seoul_schema(env: Mapping[str, str] = os.environ) -> str:
    return env.get("ASK_SEOUL_SCHEMA", "ask_seoul")


def report_config(env: Mapping[str, str] = os.environ) -> TrafficReportConfig:
    return TrafficReportConfig(
        catalog=sql_identifier(trino_catalog(env)),
        schema=sql_identifier(ask_seoul_schema(env)),
        lookback_hours=int(env.get("ASK_SEOUL_REPORT_LOOKBACK_HOURS", "24")),
        freshness_warn_minutes=int(
            env.get("ASK_SEOUL_REPORT_TRAFFIC_FRESHNESS_WARN_MINUTES", "15")
        ),
        freshness_error_minutes=int(
            env.get("ASK_SEOUL_REPORT_TRAFFIC_FRESHNESS_ERROR_MINUTES", "30")
        ),
    )


def trino_cursor():
    import trino.dbapi

    catalog = sql_identifier(trino_catalog())
    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return connection.cursor()


def _fetch_one(cursor, sql: str) -> tuple[Any, ...]:
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return ()
    return tuple(row)


def _qualified(config: TrafficReportConfig, table: str) -> str:
    return f"{config.catalog}.{config.schema}.{table}"


def _sql_string(value: object) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _sql_timestamp_utc(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return "TIMESTAMP " + _sql_string(utc_value.strftime("%Y-%m-%d %H:%M:%S.%f"))


def _age_minutes(collected_at: Any, detected_at: datetime) -> int | None:
    if collected_at is None:
        return None
    if isinstance(collected_at, str):
        collected_at = datetime.fromisoformat(collected_at.replace("Z", "+00:00"))
    if collected_at.tzinfo is None:
        collected_at = collected_at.replace(tzinfo=ZoneInfo("UTC"))
    return int((detected_at.astimezone(collected_at.tzinfo) - collected_at).total_seconds() // 60)


def freshness_status(age_minutes: int | None, warn_minutes: int, error_minutes: int) -> str:
    if age_minutes is None or age_minutes > error_minutes:
        return "FAIL"
    if age_minutes > warn_minutes:
        return "WARN"
    return "PASS"


def _traffic_cutoffs(config: TrafficReportConfig, detected_at: datetime) -> tuple[datetime, str]:
    cutoff = detected_at.astimezone(timezone.utc) - timedelta(hours=config.lookback_hours)
    load_date_days = max(1, math.ceil(config.lookback_hours / 24) + 1)
    load_date_floor = (
        detected_at.astimezone(KST).date() - timedelta(days=load_date_days)
    ).isoformat()
    return cutoff, load_date_floor


def collect_traffic_summary(cursor, config: TrafficReportConfig, detected_at: datetime) -> dict[str, Any]:
    audit_table = _qualified(config, TRAFFIC_AUDIT_TABLE)
    cutoff, load_date_floor = _traffic_cutoffs(config, detected_at)
    # Traffic Bronze/audit are not physically partitioned today. This bounds report semantics
    # but is not asserted to reduce physical input bytes; the benchmark determines that.
    row = _fetch_one(
        cursor,
        f"""
        SELECT
            count(*) AS request_count,
            coalesce(sum(row_count), 0) AS parsed_row_count,
            coalesce(max(list_total_count), 0) AS max_list_total_count,
            coalesce(max(end_index), 0) AS max_end_index,
            sum(CASE WHEN row_count = 0 AND list_total_count = 0 THEN 1 ELSE 0 END) AS zero_row_success_count,
            max(collected_at) AS last_collected_at
        FROM {audit_table}
        WHERE source_id = 'seoul_traffic_incident'
          AND load_date >= {_sql_string(load_date_floor)}
          AND collected_at >= {_sql_timestamp_utc(cutoff)}
        """,
    )
    if not row:
        return {
            "status": "FAIL",
            "reason": "no_traffic_audit_rows",
            "table": _qualified(config, TRAFFIC_TABLE),
            "audit_table": audit_table,
            "request_count": 0,
            "freshness_minutes": None,
            "freshness_status": "FAIL",
            "freshness_warn_minutes": config.freshness_warn_minutes,
            "freshness_error_minutes": config.freshness_error_minutes,
            "freshness_slo_minutes": config.freshness_error_minutes,
            "coverage_ok": False,
            "freshness_ok": False,
        }

    request_count, parsed_row_count, list_total_count, max_end_index, zero_row_success_count, last_collected_at = row
    request_count = int(request_count or 0)
    parsed_row_count = int(parsed_row_count or 0)
    list_total_count = int(list_total_count or 0)
    max_end_index = int(max_end_index or 0)
    zero_row_success_count = int(zero_row_success_count or 0)
    freshness_minutes = _age_minutes(last_collected_at, detected_at)
    freshness = freshness_status(
        freshness_minutes,
        config.freshness_warn_minutes,
        config.freshness_error_minutes,
    )
    freshness_ok = freshness == "PASS"
    coverage_ok = request_count > 0 and (
        list_total_count == 0 or parsed_row_count >= list_total_count or max_end_index >= list_total_count
    )
    return {
        "status": "FAIL" if not coverage_ok or freshness == "FAIL" else freshness,
        "table": _qualified(config, TRAFFIC_TABLE),
        "audit_table": audit_table,
        "request_count": request_count,
        "parsed_row_count": parsed_row_count,
        "list_total_count": list_total_count,
        "max_end_index": max_end_index,
        "zero_row_success_count": zero_row_success_count,
        "last_collected_at": str(last_collected_at),
        "freshness_minutes": freshness_minutes,
        "freshness_status": freshness,
        "freshness_warn_minutes": config.freshness_warn_minutes,
        "freshness_error_minutes": config.freshness_error_minutes,
        "freshness_slo_minutes": config.freshness_error_minutes,
        "coverage_ok": coverage_ok,
        "freshness_ok": freshness_ok,
    }


def collect_dag_run_summary(
    cursor,
    config: TrafficReportConfig,
    dag_id: str,
    detected_at: datetime,
) -> dict[str, Any]:
    manifest_table = _qualified(config, MANIFEST_TABLE)
    cutoff = detected_at.astimezone(timezone.utc).replace(tzinfo=timezone.utc)
    cutoff = cutoff.replace(microsecond=0) - timedelta(hours=config.lookback_hours)
    row = _fetch_one(
        cursor,
        f"""
        WITH latest AS (
            SELECT
                dag_run_id,
                max_by(status, event_at) AS latest_status,
                max_by(is_publishable, event_at) AS latest_is_publishable,
                max(event_at) AS latest_event_at
            FROM {manifest_table}
            WHERE dag_id = {_sql_string(dag_id)}
              AND event_at >= {_sql_timestamp_utc(cutoff)}
            GROUP BY dag_run_id
        )
        SELECT
            coalesce(sum(CASE WHEN latest_status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
            coalesce(sum(CASE WHEN latest_status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
            coalesce(sum(CASE WHEN latest_status = 'STARTED' THEN 1 ELSE 0 END), 0) AS running,
            max(CASE WHEN latest_status = 'SUCCESS' THEN latest_event_at END) AS last_success_at,
            max(
                CASE WHEN latest_status = 'SUCCESS' AND latest_is_publishable THEN latest_event_at END
            ) AS last_publishable_at
        FROM latest
        """,
    )
    summary = {
        "dag_id": dag_id,
        "success": 0,
        "failed": 0,
        "running": 0,
        "last_success_at": None,
        "last_publishable_at": None,
    }
    if row:
        summary.update(
            success=int(row[0] or 0),
            failed=int(row[1] or 0),
            running=int(row[2] or 0),
            last_success_at=str(row[3]) if row[3] is not None else None,
            last_publishable_at=str(row[4]) if row[4] is not None else None,
        )
    return summary


def _airflow_state_name(value: Any) -> str:
    """Return an Airflow enum/string state in a stable lower-case form."""
    enum_value = getattr(value, "value", value)
    return str(enum_value or "").lower()


def _is_scheduled_airflow_run(dag_run: Any) -> bool:
    run_type = getattr(dag_run, "run_type", None)
    # A class-level SQLAlchemy attribute can appear on lightweight test doubles;
    # in that case the query already applied the scheduled filter.
    if run_type is not None and (isinstance(run_type, str) or hasattr(run_type, "value")):
        return _airflow_state_name(run_type) in {"scheduled", "dagruntype.scheduled"}
    run_id = str(getattr(dag_run, "run_id", ""))
    return run_id.startswith("scheduled__")


def _as_utc_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _problem_key_segment(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9._=-]", "-", str(value or "unknown"))


def _build_airflow_problem_storage():
    """Build the configured R2 storage lazily so report tests need no credentials."""
    from common.storage import build_storage, r2_env

    return build_storage(
        "r2",
        bucket=r2_env("R2_BUCKET_NAME"),
        endpoint=r2_env("R2_ENDPOINT"),
        key=r2_env("R2_ACCESS_KEY_ID"),
        secret=r2_env("R2_SECRET_ACCESS_KEY"),
        region="auto",
    )


def _normalize_airflow_problem_reason(document: Mapping[str, Any]) -> str:
    """Extract a one-line, redacted reason from an R2 Problem document."""
    title = str(document.get("title") or "").strip()
    detail = str(document.get("detail") or "").strip()
    if title and detail:
        reason = detail if detail.startswith(title) else f"{title}: {detail}"
    else:
        reason = title or detail
    reason = re.split(r"[\r\n]", reason, maxsplit=1)[0].strip()
    if not reason:
        return AIRFLOW_FAILURE_REASON_FALLBACK

    # Problem documents are redacted at write time; redact again before displaying
    # to keep this reporting boundary safe when older documents are inspected.
    try:
        from common.security import redact, refresh_env_secrets

        refresh_env_secrets()
        reason = str(redact(reason))
    except Exception:  # pragma: no cover - a defensive fallback around redaction
        # A redaction failure must never return the original R2 detail.
        return AIRFLOW_FAILURE_REASON_FALLBACK
    reason = re.split(r"[\r\n]", reason, maxsplit=1)[0].strip()
    if not reason:
        return AIRFLOW_FAILURE_REASON_FALLBACK
    return reason[:AIRFLOW_FAILURE_REASON_MAX_LENGTH]


def _lookup_airflow_problem_reason(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    logical_date: Any,
) -> str:
    """Find a failed task's existing redacted R2 Problem reason.

    The lookup is intentionally best-effort. The Airflow metadata result remains
    useful when R2 is unavailable, and callers receive the explicit fallback.
    """
    utc_date = _as_utc_datetime(logical_date)
    if utc_date is None:
        return AIRFLOW_FAILURE_REASON_FALLBACK

    try:
        storage = _build_airflow_problem_storage()
        safe_run_id = _problem_key_segment(run_id)
        # A failure near UTC midnight can be written on the adjacent observed date.
        dates = (
            utc_date.date(),
            (utc_date - timedelta(days=1)).date(),
            (utc_date + timedelta(days=1)).date(),
        )
        for observed_date in dates:
            prefix = (
                f"{PROBLEM_DOCUMENT_PREFIX}/observed_date={observed_date.isoformat()}"
                f"/domain=traffic/dag_id={_problem_key_segment(dag_id)}/"
            )
            for key in storage.list_keys(prefix):
                filename = str(key).rsplit("/", 1)[-1]
                if not filename.startswith(f"{safe_run_id}__"):
                    continue
                try:
                    document = storage.read_json(key)
                except AttributeError:
                    document = json.loads(storage.read_bytes(key).decode("utf-8"))
                if not isinstance(document, Mapping):
                    continue
                if document.get("run_id") != run_id or document.get("task_id") != task_id:
                    continue
                return _normalize_airflow_problem_reason(document)
    except Exception as exc:
        # Never include the exception text: it may contain a credential or URL.
        LOGGER.warning("Unable to resolve Airflow failure reason from R2: error_type=%s", type(exc).__name__)
    return AIRFLOW_FAILURE_REASON_FALLBACK


def collect_airflow_scheduled_run_summary(
    dag_id: str,
    detected_at: datetime,
    lookback_hours: int,
) -> dict[str, Any]:
    """Summarize scheduled Airflow runs and their first failed task.

    This reads Airflow's metadata DB directly because a failed run may never have
    reached the Trino manifest/audit tables. Manual and backfill runs are excluded
    by ``DagRunType.SCHEDULED``.
    """
    from airflow.models.dagrun import DagRun
    from airflow.utils.session import create_session

    try:
        from airflow.utils.types import DagRunType

        scheduled_type = DagRunType.SCHEDULED
    except (ImportError, AttributeError):  # Airflow 2 compatibility
        scheduled_type = "scheduled"

    detected_at_utc = _as_utc_datetime(detected_at) or datetime.now(timezone.utc)
    cutoff = detected_at_utc - timedelta(hours=int(lookback_hours))
    query_end = detected_at_utc
    failures: list[dict[str, Any]] = []
    success = failed = running = 0

    with create_session() as session:
        query = session.query(DagRun).filter(DagRun.dag_id == dag_id)
        run_type_column = getattr(DagRun, "run_type", None)
        if run_type_column is not None:
            query = query.filter(run_type_column == scheduled_type)
        # Airflow 3 exposes ``logical_date`` as a property while the ORM column
        # remains ``execution_date``. Lightweight test doubles may expose only
        # logical_date, so prefer the real column and fall back when unavailable.
        logical_date_column = getattr(DagRun, "execution_date", None)
        if logical_date_column is None:
            logical_date_column = getattr(DagRun, "logical_date", None)
        query = query.filter(logical_date_column >= cutoff, logical_date_column <= query_end)
        if logical_date_column is not None:
            query = query.order_by(logical_date_column)
        runs = [
            dag_run
            for dag_run in query.all()
            if _is_scheduled_airflow_run(dag_run)
            and (
                (logical_date := _as_utc_datetime(getattr(dag_run, "logical_date", None))) is not None
                and cutoff <= logical_date <= query_end
            )
        ]

        for dag_run in runs:
            state = _airflow_state_name(getattr(dag_run, "state", None))
            if state == "success":
                success += 1
                continue
            if state == "running":
                running += 1
                continue
            if state != "failed":
                continue

            failed += 1
            task_instances = list(dag_run.get_task_instances(session=session) or [])
            failed_tasks = [
                task_instance
                for task_instance in task_instances
                if _airflow_state_name(getattr(task_instance, "state", None)) == "failed"
            ]
            def _task_start_key(task_instance: Any) -> datetime:
                return _as_utc_datetime(getattr(task_instance, "start_date", None)) or datetime.max.replace(
                    tzinfo=timezone.utc
                )

            failed_task = min(
                failed_tasks,
                key=_task_start_key,
            ) if failed_tasks else None
            task_id = str(getattr(failed_task, "task_id", None) or "unknown")
            run_id = str(getattr(dag_run, "run_id", None) or "unknown")
            logical_date = getattr(dag_run, "logical_date", None)
            failures.append(
                {
                    "logical_date": logical_date,
                    "run_id": run_id,
                    "task_id": task_id,
                    "reason": _lookup_airflow_problem_reason(
                        dag_id=dag_id,
                        run_id=run_id,
                        task_id=task_id,
                        logical_date=logical_date,
                    ),
                }
            )

    return {
        "expected": len(runs),
        "success": success,
        "failed": failed,
        "running": running,
        "failures": failures,
    }


def build_traffic_reliability_report(cursor=None, detected_at: datetime | None = None) -> dict[str, Any]:
    config = report_config()
    cursor = cursor or trino_cursor()
    detected_at = detected_at or datetime.now(KST)
    try:
        traffic = collect_traffic_summary(cursor, config, detected_at)
    except Exception as exc:
        traffic = {
            "status": "FAIL",
            "reason": "traffic_query_failed",
            "error": str(exc),
            "table": _qualified(config, TRAFFIC_TABLE),
            "audit_table": _qualified(config, TRAFFIC_AUDIT_TABLE),
        }
    try:
        dag_runs = collect_dag_run_summary(cursor, config, TRAFFIC_BRONZE_DAG_ID, detected_at)
    except Exception as exc:
        dag_runs = {
            "dag_id": TRAFFIC_BRONZE_DAG_ID,
            "success": 0,
            "failed": 0,
            "running": 0,
            "reason": "dag_run_query_failed",
            "error": str(exc),
        }
    try:
        airflow_runs = collect_airflow_scheduled_run_summary(
            TRAFFIC_BRONZE_DAG_ID,
            detected_at,
            config.lookback_hours,
        )
    except Exception as exc:
        # The failure itself is reportable, but exception details may contain
        # credentials or connection URLs and must not reach Discord.
        airflow_runs = {
            "expected": None,
            "success": 0,
            "failed": 0,
            "running": 0,
            "failures": [],
            "reason": "airflow_metadata_query_failed",
            "error_type": type(exc).__name__,
        }

    manifest_query_ok = not dag_runs.get("reason")
    airflow_query_ok = not airflow_runs.get("reason")
    airflow_failures_ok = int(airflow_runs.get("failed") or 0) == 0
    publishability_ok = bool(dag_runs.get("last_publishable_at"))
    dag_runs["publishability_ok"] = publishability_ok
    late_publishability = {
        "status": "NOT_EVALUATED",
        "reason": "bounded late-repair contract is owned by ASAC-DBT #117",
    }
    if not manifest_query_ok or not airflow_query_ok or not airflow_failures_ok or not publishability_ok:
        status = "FAIL"
    else:
        status = str(traffic.get("status") or "FAIL")

    return {
        "report_name": "traffic_bronze_reliability",
        "detected_at": detected_at.isoformat(),
        "catalog": config.catalog,
        "schema": config.schema,
        "lookback_hours": config.lookback_hours,
        "status": status,
        "traffic": traffic,
        "dag_runs": dag_runs,
        "airflow_runs": airflow_runs,
        "publishability_ok": publishability_ok,
        "late_publishability": late_publishability,
        "blast_radius": [
            _qualified(config, TRAFFIC_TABLE),
            _qualified(config, TRAFFIC_AUDIT_TABLE),
        ],
    }


def _format_bool(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _format_minutes(value: int | None) -> str:
    if value is None:
        return "unknown"
    return f"{value}m"


def _icon(value: bool) -> str:
    return "✅" if value else "❌"


def _status_icon(status: str) -> str:
    return {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(status, "❌")


def _status_label(status: str) -> str:
    return {"PASS": "성공", "WARN": "경고", "FAIL": "실패"}.get(status, "실패")


def _airflow_failure_time(value: Any) -> str:
    timestamp = _as_utc_datetime(value)
    if timestamp is None:
        return "unknown time"
    return timestamp.astimezone(KST).strftime("%H:%M KST")


def _airflow_failure_window(
    failures: list[Mapping[str, Any]],
    schedule_interval_minutes: int = TRAFFIC_SCHEDULE_INTERVAL_MINUTES,
) -> str | None:
    """Format a KST failure window, including the final scheduled slot.

    Traffic Bronze's five-minute schedule means failures at 11:30 and 11:50
    cover 25 minutes (20 minutes between timestamps plus the final 5-minute
    slot). Keeping the interval as an argument makes the calculation testable
    and allows a future schedule contract to override it without changing the
    timestamp logic.
    """
    timestamps = [
        timestamp.astimezone(KST)
        for failure in failures
        if (timestamp := _as_utc_datetime(failure.get("logical_date"))) is not None
    ]
    if not timestamps:
        return None
    first = min(timestamps)
    last = max(timestamps)
    if first.date() == last.date():
        window = f"{first:%Y-%m-%d %H:%M}~{last:%H:%M} KST"
    else:
        window = f"{first:%Y-%m-%d %H:%M}~{last:%Y-%m-%d %H:%M} KST"
    try:
        interval_minutes = max(0, int(schedule_interval_minutes))
    except (TypeError, ValueError):
        interval_minutes = TRAFFIC_SCHEDULE_INTERVAL_MINUTES
    elapsed_minutes = max(0, int((last - first).total_seconds() // 60))
    return f"{window} ({elapsed_minutes + interval_minutes}분)"


def format_traffic_discord_message(report: dict[str, Any]) -> str:
    traffic = report["traffic"]
    airflow_runs = report.get("airflow_runs") or {}
    detected_date = str(report["detected_at"])[:10]
    target = os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod"))
    report_status = str(report["status"])
    coverage_ok = bool(traffic.get("coverage_ok"))
    freshness = str(traffic.get("freshness_status", "FAIL"))
    freshness_ok = freshness == "PASS"
    publishability_ok = bool(report.get("publishability_ok"))
    dag_ok = not bool(report["dag_runs"].get("reason"))
    airflow_query_ok = not bool(airflow_runs.get("reason"))
    airflow_failures_ok = int(airflow_runs.get("failed") or 0) == 0
    scheduled_ok = airflow_query_ok and airflow_failures_ok
    expected = airflow_runs.get("expected")
    success = int(airflow_runs.get("success") or 0)
    failed = int(airflow_runs.get("failed") or 0)
    expected_text = str(expected) if expected is not None else "unknown"
    lines = [
        f"서울시 돌발정보 Bronze 신뢰성 리포트 - {detected_date} (target={target})",
        f"{_status_icon(report_status)} 리포트 상태: {_status_label(report_status)}",
        (
            f"{_status_icon(freshness)} Freshness: {_format_minutes(traffic.get('freshness_minutes'))} "
            f"/ WARN {traffic.get('freshness_warn_minutes', 'n/a')}m "
            f"/ FAIL {traffic.get('freshness_error_minutes', traffic.get('freshness_slo_minutes', 'n/a'))}m"
        ),
        f"{_icon(traffic.get('request_count', 0) > 0)} API 호출건수: {traffic.get('request_count', 0)}회",
        f"{_icon(traffic.get('parsed_row_count', 0) > 0)} 파싱 행수: {int(traffic.get('parsed_row_count', 0)):,}행",
        f"{_icon(traffic.get('list_total_count', 0) >= 0)} 최신 응답 전체 건수: {traffic.get('list_total_count', 0)}건",
        f"{_icon(coverage_ok)} requested_end: {traffic.get('max_end_index', 0)}",
        f"{_icon(traffic.get('zero_row_success_count', 0) >= 0)} zero-row 정상 응답: {traffic.get('zero_row_success_count', 0)}건",
        f"{_icon(traffic.get('reason', '-') == '-')} reason: {traffic.get('reason', '-')}",
        "",
        f"DAG runs / last {report['lookback_hours']}h:",
        (
            f"`dag_id={report['dag_runs'].get('dag_id')}` "
            f"success={report['dag_runs'].get('success', 0)} "
            f"failed={report['dag_runs'].get('failed', 0)} "
            f"running={report['dag_runs'].get('running', 0)} "
            f"last_success={report['dag_runs'].get('last_success_at', 'N/A')} "
            f"last_publishable={report['dag_runs'].get('last_publishable_at', 'N/A')} "
            f"reason={report['dag_runs'].get('reason', '-')}"
        ),
        f"{_icon(publishability_ok)} publishability={_format_bool(publishability_ok)}",
        (
            "late_publishability="
            f"{report.get('late_publishability', {}).get('status', 'NOT_EVALUATED')} "
            f"({report.get('late_publishability', {}).get('reason', 'unknown')})"
        ),
        f"{_icon(scheduled_ok)} 스케줄 수집 상태: {success}/{expected_text} 성공, {failed} 실패",
    ]
    failure_window = _airflow_failure_window(list(airflow_runs.get("failures") or []))
    if failure_window:
        lines.extend([f"실패 수집 공백: {failure_window}", "실패 내역:"])
        for failure in airflow_runs.get("failures") or []:
            time_text = _airflow_failure_time(failure.get("logical_date"))
            task_id = str(failure.get("task_id") or "unknown")
            reason = str(failure.get("reason") or AIRFLOW_FAILURE_REASON_FALLBACK)
            run_id = str(failure.get("run_id") or "unknown")
            lines.append(f"- {time_text} | task={task_id} | {reason} | run_id={run_id}")
    elif airflow_runs.get("reason"):
        lines.append(
            f"스케줄 수집 상태 조회 실패: {airflow_runs.get('reason')}"
            f" (error_type={airflow_runs.get('error_type', 'unknown')})"
        )
    lines.extend([
        "",
        "Checks:",
        f"{_icon(coverage_ok)} traffic_coverage={_format_bool(coverage_ok)}",
        f"{_icon(freshness_ok)} traffic_freshness={_format_bool(freshness_ok)}",
        f"{_icon(publishability_ok)} traffic_publishability={_format_bool(publishability_ok)}",
        f"{_icon(dag_ok)} dag_run_summary={_format_bool(dag_ok)}",
        f"{_icon(scheduled_ok)} airflow_scheduled_runs={_format_bool(scheduled_ok)}",
        "",
        "Blast radius:",
    ])
    lines.extend(f"`{table}`" for table in report["blast_radius"])
    lines.extend(["", f"detected_at: `{report['detected_at']}`", f"catalog/schema: `{report['catalog']}.{report['schema']}`"])
    message = "\n".join(lines)
    if len(message) > 1900:
        return message[:1890] + "\n...(truncated)"
    return message


def _discord_payload(message: str) -> bytes:
    lines = message.splitlines()
    title = lines[0].strip("*") if lines else "Traffic Bronze reliability report"
    description = "\n".join(lines[1:]).strip() or title
    if "FAIL" in title or "리포트 상태: 실패" in message:
        color = DISCORD_RED
    elif "리포트 상태: 경고" in message:
        color = DISCORD_YELLOW
    else:
        color = DISCORD_GREEN
    payload = {
        "embeds": [{
            "title": title[:256],
            "description": description[:4096],
            "color": color,
        }]
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def send_discord_message(message: str, webhook_url: str | None = None) -> bool:
    webhook_url = webhook_url or discord_webhook_url()
    if not webhook_url:
        LOGGER.info("Discord webhook is not configured; skip traffic report notification.")
        return False
    request = urllib.request.Request(
        webhook_url,
        data=_discord_payload(message),
        headers={"Content-Type": "application/json", "User-Agent": "ask-seoul-traffic-report/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status >= 400:
                LOGGER.warning("Traffic report Discord notification failed: status=%s", response.status)
                return False
    except urllib.error.HTTPError as exc:
        LOGGER.warning(
            "Traffic report Discord notification failed: status=%s error_type=%s",
            exc.code,
            type(exc).__name__,
        )
        return False
    except Exception as exc:
        LOGGER.warning("Traffic report Discord notification failed: error_type=%s", type(exc).__name__)
        return False
    return True
