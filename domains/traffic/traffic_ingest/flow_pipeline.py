"""Exact-parent Traffic Flow landing and Bronze lifecycle."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from zoneinfo import ZoneInfo

from traffic_ingest.incident_pipeline import landing_asset_metadata
from traffic_ingest.run_manifest import TrafficRun


LOGGER = logging.getLogger(__name__)
FLOW_MATERIALIZE_TASK_ID = "materialize_verify_publish_traffic_flow"
KST = ZoneInfo("Asia/Seoul")


class Manifest(Protocol):
    def start(self, run: TrafficRun, **metrics): ...

    def publish(self, run: TrafficRun, **metrics): ...

    def fail(self, run: TrafficRun, **metrics): ...


@dataclass(frozen=True)
class FlowMaterializationOutcome:
    row_count: int
    asset_metadata: dict[str, object] | None


def _raw_objects(raw_result: Mapping[str, object]) -> list[Mapping[str, object]]:
    values = raw_result.get("raw_objects")
    if not isinstance(values, list) or not values:
        raise ValueError("Traffic Flow result requires at least one raw object")
    if any(not isinstance(item, Mapping) for item in values):
        raise ValueError("Traffic Flow raw object descriptor is malformed")
    return values


def flow_asset_metadata(
    *,
    raw_result: Mapping[str, object],
    flow_run_id: str,
    parent_incident_run_id: str,
    row_count: int,
) -> dict[str, object]:
    landing_metadata = landing_asset_metadata(flow_run_id, raw_result)
    event_at = str(landing_metadata["event_at"])
    event_datetime = datetime.fromisoformat(event_at.replace("Z", "+00:00"))
    return {
        "source_id": "seoul_traffic_flow",
        "flow_run_id": flow_run_id,
        "flow_dag_run_id": flow_run_id,
        "parent_incident_run_id": parent_incident_run_id,
        "event_at": event_at,
        "load_date": event_datetime.astimezone(KST).date().isoformat(),
        "row_count": int(row_count),
        "payload_hash": landing_metadata["payload_hash"],
        "is_publishable": True,
    }


class TrafficFlowPipeline:
    def __init__(
        self,
        *,
        runtime_guard: Callable[[], None],
        incident_manifest,
        flow_manifest: Manifest,
        resolve_links: Callable[[dict[str, object], str], list[str]],
        landing,
        load: Callable[[dict[str, object], str], dict[str, object]],
        verify: Callable[[dict[str, object], str], int],
    ) -> None:
        self._runtime_guard = runtime_guard
        self._incident_manifest = incident_manifest
        self._flow_manifest = flow_manifest
        self._resolve_links = resolve_links
        self._landing = landing
        self._load = load
        self._verify = verify

    def land(
        self,
        *,
        parent_incident_run_id: str,
        flow_run_id: str,
        conf: dict[str, object],
    ) -> dict[str, object]:
        self._runtime_guard()
        verified_parent = self._incident_manifest.require_publishable(
            parent_incident_run_id
        )
        if str(verified_parent) != parent_incident_run_id:
            raise ValueError("Traffic Flow Incident parent identity mismatch")
        link_ids = self._resolve_links(conf, parent_incident_run_id)
        landing_load_date = conf.get("load_date")
        raw_result = dict(
            self._landing.collect(
                link_ids=link_ids,
                dag_run_id=flow_run_id,
                landing_load_date=(
                    str(landing_load_date) if landing_load_date is not None else None
                ),
            )
        )
        raw_result["parent_incident_run_id"] = parent_incident_run_id
        return raw_result

    def materialize(
        self,
        *,
        raw_result: dict[str, object],
        flow_run_id: str,
    ) -> FlowMaterializationOutcome:
        parent_incident_run_id = str(
            raw_result.get("parent_incident_run_id") or ""
        )
        if not parent_incident_run_id:
            raise ValueError("Traffic Flow raw result is missing parent_incident_run_id")
        raw_object_count = len(_raw_objects(raw_result))
        run = TrafficRun("traffic_flow_bronze", flow_run_id)
        try:
            self._flow_manifest.start(
                run,
                expected_raw_objects=raw_object_count,
            )
            load_result = self._load(raw_result, flow_run_id)
            verified_rows = self._verify(load_result, flow_run_id)
            page_count = int(load_result.get("page_count", raw_object_count))
            is_publishable = bool(load_result.get("is_publishable", True))
            self._flow_manifest.publish(
                run,
                expected_rows=int(
                    load_result.get("expected_rows", load_result.get("inserted", 0))
                ),
                actual_rows=verified_rows,
                expected_raw_objects=page_count,
                actual_raw_objects=len(load_result.get("raw_object_keys") or []),
                is_publishable=is_publishable,
            )
            latest_incident_run_id = (
                self._incident_manifest.latest_publishable_run_id()
            )
            metadata = None
            if is_publishable and latest_incident_run_id == parent_incident_run_id:
                metadata = flow_asset_metadata(
                    raw_result=raw_result,
                    flow_run_id=flow_run_id,
                    parent_incident_run_id=parent_incident_run_id,
                    row_count=verified_rows,
                )
            return FlowMaterializationOutcome(
                row_count=verified_rows,
                asset_metadata=metadata,
            )
        except Exception as error:
            try:
                self._flow_manifest.fail(
                    run,
                    task_id=FLOW_MATERIALIZE_TASK_ID,
                    error=error,
                    expected_raw_objects=raw_object_count,
                )
            except Exception as manifest_error:
                LOGGER.warning(
                    "Traffic Flow FAILED manifest write failed: %s",
                    type(manifest_error).__name__,
                )
            raise


__all__ = [
    "FLOW_MATERIALIZE_TASK_ID",
    "FlowMaterializationOutcome",
    "TrafficFlowPipeline",
    "flow_asset_metadata",
]
