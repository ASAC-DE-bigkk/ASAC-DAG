"""Failure recording and notification support for the Traffic transform DAG."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from airflow.sdk.exceptions import AirflowFailException


TRAFFIC_TRANSFORM_CRON_KST = "12 * * * *"


@dataclass(frozen=True)
class TransformFailurePorts:
    """Runtime side effects supplied by the Airflow entrypoint."""

    fallback_recorder: Callable[[dict], None]
    problem_from_context: Callable[..., Any]
    error_sink_factory: Callable[[], Any]
    recovery_sink_factory: Callable[[], Any]
    first_notice: Callable[[object, object], bool]
    notification_builder: Callable[[dict], tuple[str, str, str]]
    send_notification: Callable[..., None]
    failure_color: int
    logger: Any


def transform_schedule() -> str | None:
    if "ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE"] or None
    return TRAFFIC_TRANSFORM_CRON_KST


def fail_transform_if_upstream_failed() -> None:
    """Leave a failed DAG leaf whenever a transform task fails."""
    raise AirflowFailException("traffic transform upstream task failed")


def record_classified_dbt_problem(
    context: dict,
    *,
    failure_xcom_key: str,
    run_results_record_key: str,
    ports: TransformFailurePorts,
) -> None:
    """Persist and notify one final classified dbt failure without masking it."""

    task_instance = context.get("task_instance") or context.get("ti")
    try:
        record = task_instance.xcom_pull(
            task_ids=getattr(task_instance, "task_id", None),
            key=failure_xcom_key,
        )
    except Exception as exc:
        ports.logger.warning(
            "traffic dbt failure XCom lookup failed: %s",
            type(exc).__name__,
        )
        record = None
    if not isinstance(record, dict):
        ports.fallback_recorder(context)
        return

    try:
        problem = ports.problem_from_context(context, domain="traffic")
        problem.detail = (
            f"{record.get('failure_classification')}; snapshot="
            f"{record.get('traffic_snapshot_dag_run_id')}; artifact="
            f"{record.get('dbt_artifact_path')}"
        )
        problem.extensions = {
            name: record.get(name)
            for name in (
                "traffic_snapshot_dag_run_id",
                "dbt_test_names",
                "dbt_failed_row_count",
                "dbt_artifact_path",
                run_results_record_key,
                "dbt_sources_path",
                "dbt_manifest_path",
                "silver_persisted",
                "failure_classification",
                "recovery_action",
            )
        }
        ports.error_sink_factory().write(problem)
    except Exception as exc:
        ports.logger.warning(
            "traffic Problem record write failed: %s",
            type(exc).__name__,
        )

    try:
        ports.recovery_sink_factory().write(record)
    except Exception as exc:
        ports.logger.warning(
            "traffic recovery record write failed: %s",
            type(exc).__name__,
        )

    try:
        if ports.first_notice(record.get("dag_id"), record.get("run_id")):
            title, description, footer = ports.notification_builder(record)
            ports.send_notification(
                title,
                description,
                color=ports.failure_color,
                footer=footer,
                domain="traffic",
            )
    except Exception as exc:
        ports.logger.warning(
            "traffic dbt failure notification failed: %s",
            type(exc).__name__,
        )


__all__ = [
    "TRAFFIC_TRANSFORM_CRON_KST",
    "TransformFailurePorts",
    "fail_transform_if_upstream_failed",
    "record_classified_dbt_problem",
    "transform_schedule",
]
