from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import (
    AIRFLOW_FAILURE_REASON_FALLBACK,
    AIRFLOW_FAILURE_REASON_MAX_LENGTH,
    AIRFLOW_METADATA_QUERY_MAX_ATTEMPTS,
    LOGGER,
    PROBLEM_DOCUMENT_PREFIX,
)


def _airflow_state_name(value: Any) -> str:
    """Return an Airflow enum/string state in a stable lower-case form."""
    enum_value = getattr(value, "value", value)
    return str(enum_value or "").lower()


def _is_scheduled_airflow_run(dag_run: Any) -> bool:
    run_type = getattr(dag_run, "run_type", None)
    # A class-level SQLAlchemy attribute can appear on lightweight test doubles;
    # in that case the query already applied the scheduled filter.
    if run_type is not None and (
        isinstance(run_type, str) or hasattr(run_type, "value")
    ):
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
                if (
                    document.get("run_id") != run_id
                    or document.get("task_id") != task_id
                ):
                    continue
                return _normalize_airflow_problem_reason(document)
    except Exception as exc:
        # Never include the exception text: it may contain a credential or URL.
        LOGGER.warning(
            "Unable to resolve Airflow failure reason from R2: error_type=%s",
            type(exc).__name__,
        )
    return AIRFLOW_FAILURE_REASON_FALLBACK


def _log_airflow_metadata_query_failure(
    exc: Exception,
    *,
    attempt: int,
) -> None:
    """Log a retryable metadata failure without exposing its original detail."""
    try:
        from common.security import refresh_env_secrets, scrub_exception

        refresh_env_secrets()
        scrub_exception(exc)
    except Exception:
        # Do not fall back to the original exception text when the safety layer
        # is unavailable. Retain the query stack with a synthetic exception so
        # operators still get a safe traceback while the caller fails closed.
        safe_exc = RuntimeError("diagnostic redaction unavailable")
        LOGGER.warning(
            "Traffic Airflow metadata query failed: attempt=%s/%s error_type=%s "
            "diagnostic=redaction_unavailable",
            attempt,
            AIRFLOW_METADATA_QUERY_MAX_ATTEMPTS,
            type(exc).__name__,
            exc_info=(RuntimeError, safe_exc, exc.__traceback__),
        )
        return

    LOGGER.warning(
        "Traffic Airflow metadata query failed: attempt=%s/%s error_type=%s",
        attempt,
        AIRFLOW_METADATA_QUERY_MAX_ATTEMPTS,
        type(exc).__name__,
        exc_info=(type(exc), exc, exc.__traceback__),
    )


def collect_airflow_scheduled_run_summary(
    dag_id: str,
    detected_at: datetime,
    lookback_hours: int,
) -> dict[str, Any]:
    """Retry one transient, read-only Airflow metadata query failure."""
    for attempt in range(1, AIRFLOW_METADATA_QUERY_MAX_ATTEMPTS + 1):
        try:
            return _collect_airflow_scheduled_run_summary_once(
                dag_id,
                detected_at,
                lookback_hours,
            )
        except Exception as exc:
            _log_airflow_metadata_query_failure(exc, attempt=attempt)
            if attempt == AIRFLOW_METADATA_QUERY_MAX_ATTEMPTS:
                raise

    raise AssertionError("Airflow metadata retry loop exited unexpectedly")


def _collect_airflow_scheduled_run_summary_once(
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
        query = query.filter(
            logical_date_column >= cutoff, logical_date_column <= query_end
        )
        if logical_date_column is not None:
            query = query.order_by(logical_date_column)
        runs = [
            dag_run
            for dag_run in query.all()
            if _is_scheduled_airflow_run(dag_run)
            and (
                (
                    logical_date := _as_utc_datetime(
                        getattr(dag_run, "logical_date", None)
                    )
                )
                is not None
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
                if _airflow_state_name(getattr(task_instance, "state", None))
                == "failed"
            ]

            def _task_start_key(task_instance: Any) -> datetime:
                return _as_utc_datetime(
                    getattr(task_instance, "start_date", None)
                ) or datetime.max.replace(tzinfo=timezone.utc)

            failed_task = (
                min(
                    failed_tasks,
                    key=_task_start_key,
                )
                if failed_tasks
                else None
            )
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


def _redacted_airflow_metadata_log_url(value: Any) -> str | None:
    """Return an operator-facing log location only when it can be redacted safely."""
    if not value:
        return None
    try:
        from common.security import redact, refresh_env_secrets

        refresh_env_secrets()
        return str(redact(str(value)))
    except Exception:
        return None
