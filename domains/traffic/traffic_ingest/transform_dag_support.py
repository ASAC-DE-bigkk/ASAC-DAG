"""Failure recording and notification support for the Traffic transform DAG."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowFailException

from traffic_ingest.assets import (
    TRAFFIC_FLOW_BRONZE_ASSET,
    TRAFFIC_INCIDENT_BRONZE_ASSET,
    TrafficAssetContractError,
    flow_bronze_events,
    incident_bronze_events,
    schedule_asset,
)
from traffic_ingest.common.resources import DbtWorkload, TRINO_HEAVY_POOL
from traffic_ingest.external_snapshot import ExternalSnapshotUnavailableError
from traffic_ingest.transform_specs import DbtPhaseSpec


TRAFFIC_TRANSFORM_CRON_KST = "12 * * * *"


@dataclass(frozen=True)
class SnapshotPair:
    incident_run_id: str
    flow_run_id: str | None = None


def _require_publishable(manifest, run_id: str, *, domain: str) -> str:
    try:
        verified = manifest.require_publishable(run_id)
    except Exception as exc:
        raise AirflowFailException(
            f"{domain} Bronze snapshot is not publishable: {run_id}"
        ) from exc
    if str(verified) != run_id:
        raise AirflowFailException(
            f"{domain} Bronze manifest identity mismatch for snapshot: {run_id}"
        )
    return run_id


def resolve_transform_snapshot_pair(
    *,
    context: dict,
    incident_manifest_factory: Callable[[], Any],
    flow_manifest_factory: Callable[[], Any],
) -> SnapshotPair:
    """Select a non-regressing Incident/Flow pair from triggering Assets."""

    try:
        incident_events = incident_bronze_events(context)
        flow_events = flow_bronze_events(context)
    except TrafficAssetContractError as exc:
        raise AirflowFailException(str(exc)) from exc
    if not incident_events and not flow_events:
        raise AirflowFailException(
            "traffic transform requires at least one triggering Bronze asset event"
        )

    incident_manifest = incident_manifest_factory()
    try:
        latest_incident_run_id = str(
            incident_manifest.latest_publishable_run_id()
        )
    except Exception as exc:
        raise AirflowFailException(
            "No publishable Traffic Incident Bronze snapshot is available"
        ) from exc
    _require_publishable(
        incident_manifest,
        latest_incident_run_id,
        domain="traffic",
    )

    stale_incident_run_ids = [
        str(event["bronze_dag_run_id"])
        for event in incident_events
        if str(event["bronze_dag_run_id"]) != latest_incident_run_id
    ]
    incident_manifest.coalesce_many(
        stale_incident_run_ids,
        replacement_run_id=latest_incident_run_id,
    )

    if flow_events:
        selected_flow = flow_events[-1]
        parent_incident_run_id = str(selected_flow["parent_incident_run_id"])
        if parent_incident_run_id == latest_incident_run_id:
            flow_run_id = str(selected_flow["flow_dag_run_id"])
            _require_publishable(
                flow_manifest_factory(),
                flow_run_id,
                domain="traffic flow",
            )
            return SnapshotPair(
                incident_run_id=latest_incident_run_id,
                flow_run_id=flow_run_id,
            )

    return SnapshotPair(incident_run_id=latest_incident_run_id)


def resolve_traffic_snapshot_run(
    *,
    context: dict,
    snapshot_pair_resolver,
    incident_manifest_factory,
    flow_manifest_factory,
    citydata_snapshot_resolver,
    flow_xcom_key: str,
    citydata_xcom_key: str,
) -> str:
    """Pin a non-regressing Incident/Flow/Citydata snapshot set."""
    pair = snapshot_pair_resolver(
        context=context,
        incident_manifest_factory=incident_manifest_factory,
        flow_manifest_factory=flow_manifest_factory,
    )
    try:
        citydata_crowding_snapshot_id = citydata_snapshot_resolver()
    except ExternalSnapshotUnavailableError as exc:
        raise AirflowFailException(str(exc)) from exc
    task_instance = context.get("ti") or context.get("task_instance")
    if task_instance is not None:
        task_instance.xcom_push(key=flow_xcom_key, value=pair.flow_run_id)
        task_instance.xcom_push(
            key=citydata_xcom_key,
            value=citydata_crowding_snapshot_id,
        )
    return pair.incident_run_id


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


def transform_schedule():
    if "ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE"] or None
    return schedule_asset(TRAFFIC_INCIDENT_BRONZE_ASSET) | schedule_asset(
        TRAFFIC_FLOW_BRONZE_ASSET
    )


def dbt_snapshot_variables(
    task_instance,
    snapshot_task_id: str,
    incident_run_id: str,
    flow_xcom_key: str,
    citydata_crowding_snapshot_xcom_key: str,
) -> dict[str, object]:
    variables: dict[str, object] = {"traffic_snapshot_dag_run_id": incident_run_id}
    try:
        flow_run_id = task_instance.xcom_pull(
            task_ids=snapshot_task_id,
            key=flow_xcom_key,
        )
    except TypeError:
        flow_run_id = None
    if flow_run_id:
        variables["traffic_flow_snapshot_dag_run_id"] = flow_run_id
    try:
        citydata_crowding_snapshot_id = task_instance.xcom_pull(
            task_ids=snapshot_task_id,
            key=citydata_crowding_snapshot_xcom_key,
        )
    except TypeError:
        citydata_crowding_snapshot_id = None
    if (
        isinstance(citydata_crowding_snapshot_id, int)
        and not isinstance(citydata_crowding_snapshot_id, bool)
        and citydata_crowding_snapshot_id > 0
    ):
        variables[citydata_crowding_snapshot_xcom_key] = (
            citydata_crowding_snapshot_id
        )
    return variables


def build_dbt_phase_task(
    spec: DbtPhaseSpec,
    *,
    python_callable,
    snapshot_task_id: str,
    retry_delay,
    pin_critical_priority: int,
    failure_callback,
) -> PythonOperator:
    operator_kwargs = {
        "task_id": spec.task_id,
        "python_callable": python_callable,
        "op_kwargs": {
            "dbt_command": spec.dbt_command,
            "selector": spec.selector,
            "snapshot_task_id": snapshot_task_id,
            "silver_persisted": spec.silver_persisted,
            "fresh_parse": spec.fresh_parse,
            "snapshot_required": spec.snapshot_required,
            "threads": spec.threads,
            "selector_by_test_tier": spec.selector_by_test_tier,
        },
        "retries": 1,
        "retry_delay": retry_delay,
        "priority_weight": pin_critical_priority if spec.pin_critical else 1,
        "weight_rule": "absolute",
        "on_failure_callback": failure_callback,
    }
    if spec.workload is DbtWorkload.TRINO:
        operator_kwargs["pool"] = TRINO_HEAVY_POOL
    return PythonOperator(**operator_kwargs)


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
                "traffic_citydata_crowding_snapshot_id",
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
    "SnapshotPair",
    "TransformFailurePorts",
    "build_dbt_phase_task",
    "dbt_snapshot_variables",
    "resolve_transform_snapshot_pair",
    "record_classified_dbt_problem",
    "resolve_traffic_snapshot_run",
    "transform_schedule",
]
