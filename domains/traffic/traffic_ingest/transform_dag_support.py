"""Failure recording and notification support for the Traffic transform DAG."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from traffic_ingest.assets import (
    TRAFFIC_FLOW_BRONZE_ASSET,
    TRAFFIC_INCIDENT_BRONZE_ASSET,
    TRAFFIC_INCIDENT_SILVER_ASSET,
    TrafficAssetContractError,
    flow_bronze_events,
    flow_silver_events,
    incident_bronze_events,
    schedule_asset,
)
from traffic_ingest.common.resources import DbtWorkload, TRINO_HEAVY_POOL
from traffic_ingest.external_snapshot import ExternalSnapshotUnavailableError
from traffic_ingest.run_manifest import RunNotPublishableError
from traffic_ingest.silver_snapshot_fence import (
    SilverSnapshotEvidence,
    collect_silver_snapshot_evidence,
)
from traffic_ingest.transform_admission import (
    TransformAdmissionError,
    TransformIdentity,
    TransformSuccessMarker,
    admission_decision,
)
from traffic_ingest.transform_specs import DbtPhaseSpec


TRAFFIC_TRANSFORM_CRON_KST = "12 * * * *"
SILVER_SUCCESS_MARKER_KEY = "ask_seoul.traffic.silver_transform.last_success.v1"
GOLD_SUCCESS_MARKER_KEY = "ask_seoul.traffic.gold_transform.last_success.v1"
SILVER_OUTPUT_EVIDENCE_XCOM_KEY = "traffic_silver_output_evidence"
STALE_INCIDENT_RUN_IDS_XCOM_KEY = "traffic_stale_incident_run_ids"
SILVER_ASSET_CONTRACT = "traffic_incident_silver.v1"


@dataclass(frozen=True)
class SnapshotPair:
    incident_run_id: str
    flow_run_id: str | None = None


@dataclass(frozen=True)
class SilverOutputEvidence:
    snapshot_id: int
    compacted_files_fingerprint: str

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "compacted_files_fingerprint": self.compacted_files_fingerprint,
        }


@dataclass(frozen=True)
class GoldSnapshotResolution:
    incident_run_id: str
    flow_run_id: str | None
    citydata_snapshot_id: int
    silver_output_evidence: SilverOutputEvidence


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
        latest_incident_run_id = str(incident_manifest.latest_publishable_run_id())
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


def compacted_files_fingerprint(compacted_files: object) -> str:
    if not isinstance(compacted_files, (list, tuple)) or any(
        not isinstance(path, str) or not path for path in compacted_files
    ):
        raise AirflowFailException("Silver compacted file evidence is invalid")
    canonical = json.dumps(
        sorted(compacted_files), ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def silver_output_evidence_from_dbt_run(
    task_instance,
    *,
    task_id: str = "dbt_run_silver",
) -> SilverOutputEvidence:
    try:
        result = task_instance.xcom_pull(task_ids=task_id)
        raw_evidence = result["silver_snapshot_evidence"]
        evidence = SilverSnapshotEvidence.from_dict(raw_evidence)
    except Exception as exc:
        raise AirflowFailException("Silver output evidence is unavailable") from exc
    return SilverOutputEvidence(
        snapshot_id=evidence.snapshot_id,
        compacted_files_fingerprint=compacted_files_fingerprint(
            evidence.compacted_files
        ),
    )


def current_silver_output_evidence() -> SilverOutputEvidence:
    try:
        evidence = collect_silver_snapshot_evidence()
    except Exception as exc:
        raise AirflowFailException("Silver output evidence is unavailable") from exc
    return SilverOutputEvidence(
        snapshot_id=evidence.snapshot_id,
        compacted_files_fingerprint=compacted_files_fingerprint(
            evidence.compacted_files
        ),
    )


def require_current_silver_output_evidence(
    expected: SilverOutputEvidence,
    *,
    current_evidence_loader: Callable[[], SilverOutputEvidence],
    mismatch_action: str,
) -> SilverOutputEvidence:
    """Fence Gold against a mutable Silver current table without hiding test races."""
    if mismatch_action not in {"skip", "fail"}:
        raise AirflowFailException("Traffic Silver evidence mismatch action is invalid")
    current = current_evidence_loader()
    if current != expected:
        message = "Traffic Silver output changed after Gold snapshot resolution"
        if mismatch_action == "skip":
            raise AirflowSkipException(f"superseded {message}")
        raise AirflowFailException(message)
    return current


def require_publishable_incident_snapshot(
    manifest,
    run_id: str,
    *,
    mismatch_action: str,
) -> str:
    """Distinguish an expected superseded run from manifest I/O failures."""
    if mismatch_action not in {"skip", "fail"}:
        raise AirflowFailException("Traffic manifest mismatch action is invalid")
    try:
        verified = manifest.require_publishable(run_id)
    except RunNotPublishableError as exc:
        message = f"Traffic Incident snapshot was superseded: {run_id}"
        if mismatch_action == "skip":
            raise AirflowSkipException(message) from exc
        raise AirflowFailException(message) from exc
    except Exception as exc:
        raise AirflowFailException(
            f"Traffic Incident manifest verification failed: {run_id}"
        ) from exc
    if str(verified) != run_id:
        raise AirflowFailException(
            f"Traffic Incident manifest identity mismatch: {run_id}"
        )
    return run_id


def require_latest_publishable_incident_snapshot(manifest, run_id: str) -> str:
    """Skip an immutable pin only when a newer publishable Bronze run overtook it."""
    if not isinstance(run_id, str) or not run_id.strip():
        raise AirflowFailException("Traffic pinned snapshot identity is invalid")
    try:
        latest_run_id = manifest.latest_publishable_run_id()
    except Exception as exc:
        raise AirflowFailException(
            f"Traffic latest publishable snapshot verification failed: {run_id}"
        ) from exc
    if not isinstance(latest_run_id, str) or not latest_run_id.strip():
        raise AirflowFailException(
            "Traffic latest publishable snapshot identity is invalid"
        )
    if latest_run_id != run_id:
        raise AirflowSkipException(
            "superseded Traffic Incident snapshot: "
            f"pinned={run_id}, latest={latest_run_id}"
        )
    return run_id


def _silver_output_evidence_from_dict(value: object) -> SilverOutputEvidence:
    if not isinstance(value, dict) or set(value) != {
        "snapshot_id",
        "compacted_files_fingerprint",
    }:
        raise AirflowFailException("Silver output evidence is invalid")
    try:
        marker = TransformSuccessMarker(
            version=1,
            pipeline="silver",
            identity=TransformIdentity.silver("evidence-validation"),
            output_snapshot_id=value["snapshot_id"],
            compacted_files_fingerprint=value["compacted_files_fingerprint"],
        )
    except TransformAdmissionError as exc:
        raise AirflowFailException("Silver output evidence is invalid") from exc
    return SilverOutputEvidence(
        snapshot_id=marker.output_snapshot_id,
        compacted_files_fingerprint=marker.compacted_files_fingerprint,
    )


def load_success_marker(
    variable, *, key: str, pipeline: str
) -> TransformSuccessMarker | None:
    try:
        raw_marker = variable.get(key, default=None)
    except Exception as exc:
        raise AirflowFailException("Traffic transform marker is unavailable") from exc
    if raw_marker is None:
        return None
    try:
        marker = TransformSuccessMarker.from_json(raw_marker)
    except TransformAdmissionError as exc:
        raise AirflowFailException("Traffic transform marker is malformed") from exc
    if marker.pipeline != pipeline:
        raise AirflowFailException("Traffic transform marker pipeline is invalid")
    return marker


def admit_transform(
    *,
    variable,
    marker_key: str,
    identity: TransformIdentity,
    current_evidence_loader: Callable[[], SilverOutputEvidence],
) -> dict[str, object]:
    marker = load_success_marker(variable, key=marker_key, pipeline=identity.pipeline)
    if marker is None or marker.identity != identity:
        return {"action": "RUN", "identity": identity.as_dict()}

    evidence = current_evidence_loader()
    try:
        decision = admission_decision(
            marker,
            identity,
            output_snapshot_id=evidence.snapshot_id,
            compacted_files_fingerprint=evidence.compacted_files_fingerprint,
        )
    except TransformAdmissionError as exc:
        raise AirflowFailException(
            "Traffic transform admission evidence is invalid"
        ) from exc
    if decision.skip:
        raise AirflowSkipException("matching successful Traffic transform marker")
    return {"action": decision.action, "identity": identity.as_dict()}


def write_success_marker(
    *,
    variable,
    marker_key: str,
    identity: TransformIdentity,
    evidence: SilverOutputEvidence,
) -> str:
    marker = TransformSuccessMarker(
        version=1,
        pipeline=identity.pipeline,
        identity=identity,
        output_snapshot_id=evidence.snapshot_id,
        compacted_files_fingerprint=evidence.compacted_files_fingerprint,
    )
    serialized = marker.to_json()
    variable.set(marker_key, serialized)
    return serialized


def resolve_traffic_silver_snapshot_run(
    *,
    context: dict,
    incident_manifest_factory: Callable[[], Any],
) -> str:
    """Pin and coalesce only the latest publishable Incident Bronze run."""
    try:
        events = incident_bronze_events(context)
    except TrafficAssetContractError as exc:
        raise AirflowFailException(str(exc)) from exc
    if not events:
        raise AirflowFailException(
            "traffic Silver requires an Incident Bronze asset event"
        )
    manifest = incident_manifest_factory()
    try:
        latest_run_id = str(manifest.latest_publishable_run_id())
    except Exception as exc:
        raise AirflowFailException(
            "No publishable Traffic Incident Bronze snapshot is available"
        ) from exc
    _require_publishable(manifest, latest_run_id, domain="traffic")
    stale_run_ids = [
        str(event["bronze_dag_run_id"])
        for event in events
        if str(event["bronze_dag_run_id"]) != latest_run_id
    ]
    task_instance = context.get("ti") or context.get("task_instance")
    if task_instance is not None:
        task_instance.xcom_push(
            key=STALE_INCIDENT_RUN_IDS_XCOM_KEY,
            value=stale_run_ids,
        )
    return latest_run_id


def coalesce_deferred_incident_runs(
    task_instance,
    *,
    snapshot_task_id: str,
    incident_manifest_factory: Callable[[], Any],
    stale_xcom_key: str = STALE_INCIDENT_RUN_IDS_XCOM_KEY,
) -> tuple[str, ...]:
    """Invalidate superseded Bronze inputs only after replacement Silver is durable."""
    replacement_run_id = task_instance.xcom_pull(task_ids=snapshot_task_id)
    if not isinstance(replacement_run_id, str) or not replacement_run_id.strip():
        raise AirflowFailException("Traffic replacement Silver snapshot is invalid")
    raw_stale_run_ids = task_instance.xcom_pull(
        task_ids=snapshot_task_id,
        key=stale_xcom_key,
    )
    if raw_stale_run_ids is None:
        return ()
    if not isinstance(raw_stale_run_ids, Sequence) or isinstance(
        raw_stale_run_ids, (str, bytes, bytearray)
    ):
        raise AirflowFailException("Traffic deferred coalescing evidence is invalid")

    normalized: dict[str, None] = {}
    for value in raw_stale_run_ids:
        if not isinstance(value, str) or not value.strip():
            raise AirflowFailException(
                "Traffic deferred coalescing evidence is invalid"
            )
        run_id = value.strip()
        if run_id != replacement_run_id:
            normalized.setdefault(run_id, None)
    stale_run_ids = tuple(normalized)
    if stale_run_ids:
        incident_manifest_factory().coalesce_many(
            stale_run_ids,
            replacement_run_id=replacement_run_id,
        )
    return stale_run_ids


def _events_for_asset(context: Mapping[str, object], asset_uri: str) -> list[object]:
    triggering = context.get("triggering_asset_events") or {}
    if not isinstance(triggering, Mapping):
        raise AirflowFailException("Traffic triggering Asset events are malformed")
    events: list[object] = []
    for asset, values in triggering.items():
        if str(getattr(asset, "uri", asset)) != asset_uri:
            continue
        if isinstance(values, Sequence) and not isinstance(
            values, (str, bytes, bytearray)
        ):
            events.extend(values)
        else:
            events.append(values)
    return events


def _silver_materialization_from_event(
    event: object,
) -> tuple[str, SilverOutputEvidence, datetime]:
    metadata = getattr(event, "extra", None)
    if not isinstance(metadata, dict) or set(metadata) != {
        "source_id",
        "incident_run_id",
        "silver_snapshot_id",
        "compacted_files_fingerprint",
        "event_at",
        "is_publishable",
        "contract",
    }:
        raise AirflowFailException("Traffic Silver Asset metadata is invalid")
    if (
        metadata["source_id"] != "seoul_traffic_incident"
        or metadata["is_publishable"] is not True
        or metadata["contract"] != SILVER_ASSET_CONTRACT
    ):
        raise AirflowFailException("Traffic Silver Asset metadata is invalid")
    try:
        incident_run_id = TransformIdentity.silver(
            metadata["incident_run_id"]
        ).incident_run_id
        evidence = _silver_output_evidence_from_dict(
            {
                "snapshot_id": metadata["silver_snapshot_id"],
                "compacted_files_fingerprint": metadata["compacted_files_fingerprint"],
            }
        )
        event_at = datetime.fromisoformat(
            str(metadata["event_at"]).replace("Z", "+00:00")
        )
    except (TransformAdmissionError, TypeError, ValueError) as exc:
        raise AirflowFailException("Traffic Silver Asset metadata is invalid") from exc
    if event_at.tzinfo is None:
        raise AirflowFailException("Traffic Silver Asset metadata is invalid")
    return incident_run_id, evidence, event_at


def resolve_traffic_flow_silver_snapshot_run(
    *,
    context: dict,
    flow_manifest_factory: Callable[[], Any],
    flow_xcom_key: str,
) -> str:
    """Pin one publishable Flow Bronze run to its materialized Incident parent."""
    silver_events = _events_for_asset(context, TRAFFIC_INCIDENT_SILVER_ASSET)
    if not silver_events:
        raise AirflowFailException(
            "Traffic Flow Silver requires an Incident Silver asset event"
        )
    incident_run_id, _, _ = max(
        (_silver_materialization_from_event(event) for event in silver_events),
        key=lambda item: (item[2], item[0]),
    )
    try:
        flow_events = flow_bronze_events(context)
    except TrafficAssetContractError as exc:
        raise AirflowFailException(str(exc)) from exc
    compatible_flow_events = [
        event
        for event in flow_events
        if str(event["parent_incident_run_id"]) == incident_run_id
    ]
    if not compatible_flow_events:
        raise AirflowFailException(
            "Traffic Flow Silver requires a matching Incident parent"
        )
    flow_run_id = str(compatible_flow_events[-1]["flow_dag_run_id"])
    _require_publishable(
        flow_manifest_factory(),
        flow_run_id,
        domain="traffic flow",
    )
    task_instance = context.get("ti") or context.get("task_instance")
    if task_instance is not None:
        task_instance.xcom_push(key=flow_xcom_key, value=flow_run_id)
    return incident_run_id


def resolve_traffic_gold_snapshot_run(
    *,
    context: dict,
    variable,
    incident_manifest_factory: Callable[[], Any],
    flow_manifest_factory: Callable[[], Any],
    citydata_snapshot_resolver: Callable[[], int],
    current_silver_evidence_loader: Callable[[], SilverOutputEvidence],
    flow_xcom_key: str,
    citydata_xcom_key: str,
    silver_evidence_xcom_key: str = SILVER_OUTPUT_EVIDENCE_XCOM_KEY,
) -> str:
    """Pin a Silver parent, optional compatible Flow, and Citydata scalar."""
    silver_events = _events_for_asset(context, "iceberg://traffic/incident/silver")
    if silver_events:
        incident_run_id, evidence, _ = max(
            (_silver_materialization_from_event(event) for event in silver_events),
            key=lambda item: (item[2], item[0]),
        )
    else:
        marker = load_success_marker(
            variable, key=SILVER_SUCCESS_MARKER_KEY, pipeline="silver"
        )
        if marker is None:
            raise AirflowFailException(
                "No successful Traffic Silver marker is available"
            )
        incident_run_id = marker.identity.incident_run_id
        evidence = SilverOutputEvidence(
            snapshot_id=marker.output_snapshot_id,
            compacted_files_fingerprint=marker.compacted_files_fingerprint,
        )

    require_current_silver_output_evidence(
        evidence,
        current_evidence_loader=current_silver_evidence_loader,
        mismatch_action="skip",
    )
    require_publishable_incident_snapshot(
        incident_manifest_factory(),
        incident_run_id,
        mismatch_action="skip",
    )

    try:
        flow_events = flow_silver_events(context)
    except TrafficAssetContractError as exc:
        raise AirflowFailException(str(exc)) from exc
    compatible_flow_events = [
        event
        for event in flow_events
        if str(event["parent_incident_run_id"]) == incident_run_id
    ]
    flow_run_id = None
    if compatible_flow_events:
        flow_run_id = str(compatible_flow_events[-1]["flow_dag_run_id"])
        _require_publishable(
            flow_manifest_factory(), flow_run_id, domain="traffic flow"
        )

    try:
        citydata_snapshot_id = citydata_snapshot_resolver()
    except ExternalSnapshotUnavailableError as exc:
        raise AirflowFailException(str(exc)) from exc
    if (
        not isinstance(citydata_snapshot_id, int)
        or isinstance(citydata_snapshot_id, bool)
        or citydata_snapshot_id <= 0
    ):
        raise AirflowFailException("Traffic Citydata snapshot is invalid")
    task_instance = context.get("ti") or context.get("task_instance")
    if task_instance is not None:
        task_instance.xcom_push(key=flow_xcom_key, value=flow_run_id)
        task_instance.xcom_push(key=citydata_xcom_key, value=citydata_snapshot_id)
        task_instance.xcom_push(key=silver_evidence_xcom_key, value=evidence.as_dict())
    return incident_run_id


def silver_output_evidence_from_resolver(
    task_instance,
    *,
    snapshot_task_id: str,
    evidence_xcom_key: str = SILVER_OUTPUT_EVIDENCE_XCOM_KEY,
) -> SilverOutputEvidence:
    try:
        value = task_instance.xcom_pull(
            task_ids=snapshot_task_id, key=evidence_xcom_key
        )
    except Exception as exc:
        raise AirflowFailException("Silver output evidence is unavailable") from exc
    return _silver_output_evidence_from_dict(value)


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
        variables[citydata_crowding_snapshot_xcom_key] = citydata_crowding_snapshot_id
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
            "citydata_snapshot_required": spec.citydata_snapshot_required,
            "silver_fence_mode": spec.silver_fence_mode,
            "threads": spec.threads,
            "selector_by_test_tier": spec.selector_by_test_tier,
            "selector_when_flow_missing": spec.selector_when_flow_missing,
            "selector_by_test_tier_when_flow_missing": (
                spec.selector_by_test_tier_when_flow_missing
            ),
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
                "traffic_flow_snapshot_dag_run_id",
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
    "GOLD_SUCCESS_MARKER_KEY",
    "SILVER_ASSET_CONTRACT",
    "SILVER_OUTPUT_EVIDENCE_XCOM_KEY",
    "SILVER_SUCCESS_MARKER_KEY",
    "STALE_INCIDENT_RUN_IDS_XCOM_KEY",
    "TRAFFIC_TRANSFORM_CRON_KST",
    "GoldSnapshotResolution",
    "SilverOutputEvidence",
    "SnapshotPair",
    "TransformFailurePorts",
    "admit_transform",
    "build_dbt_phase_task",
    "coalesce_deferred_incident_runs",
    "compacted_files_fingerprint",
    "current_silver_output_evidence",
    "dbt_snapshot_variables",
    "load_success_marker",
    "resolve_transform_snapshot_pair",
    "record_classified_dbt_problem",
    "require_current_silver_output_evidence",
    "require_latest_publishable_incident_snapshot",
    "require_publishable_incident_snapshot",
    "resolve_traffic_gold_snapshot_run",
    "resolve_traffic_flow_silver_snapshot_run",
    "resolve_traffic_silver_snapshot_run",
    "resolve_traffic_snapshot_run",
    "silver_output_evidence_from_dbt_run",
    "silver_output_evidence_from_resolver",
    "transform_schedule",
    "write_success_marker",
]
