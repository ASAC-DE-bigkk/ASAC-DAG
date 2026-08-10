"""Cache-first lifecycle for TOPIS road link references."""

from __future__ import annotations

from datetime import datetime

from traffic_ingest.errors import (
    TrafficBronzeConfigurationError,
    TrafficCompletenessError,
)
from traffic_ingest.flow_info import normalize_link_ids


def _force_refresh_link_ids(
    conf: dict[str, object], requested: list[str]
) -> list[str]:
    raw = conf.get("force_refresh_link_ids")
    if raw in (None, ""):
        return []
    forced = normalize_link_ids(raw)
    requested_set = set(requested)
    outside_scope = [link_id for link_id in forced if link_id not in requested_set]
    if outside_scope:
        raise TrafficBronzeConfigurationError(
            "force_refresh_link_ids must be a subset of requested links: "
            + ",".join(outside_scope)
        )
    return forced


def _load_date(conf: dict[str, object]) -> str | None:
    raw = conf.get("load_date")
    if raw in (None, ""):
        return None
    try:
        return datetime.strptime(str(raw), "%Y-%m-%d").date().isoformat()
    except (TypeError, ValueError) as exc:
        raise TrafficBronzeConfigurationError(
            "Traffic link reference load_date must be YYYY-MM-DD"
        ) from exc


class TrafficLinkReferencePipeline:
    def __init__(
        self,
        *,
        runtime_guard,
        incident_manifest,
        resolve_links,
        unresolved_link_ids,
        landing,
        load,
        verify,
    ) -> None:
        self._runtime_guard = runtime_guard
        self._incident_manifest = incident_manifest
        self._resolve_links = resolve_links
        self._unresolved_link_ids = unresolved_link_ids
        self._landing = landing
        self._load = load
        self._verify = verify

    def land(
        self,
        *,
        parent_incident_run_id: str,
        link_reference_run_id: str,
        conf: dict[str, object],
    ) -> dict[str, object]:
        self._runtime_guard()
        verified_parent = self._incident_manifest.require_publishable(
            parent_incident_run_id
        )
        if str(verified_parent) != parent_incident_run_id:
            raise TrafficCompletenessError(
                "Traffic link reference parent identity mismatch"
            )
        requested = normalize_link_ids(
            self._resolve_links(conf, parent_incident_run_id)
        )
        forced = _force_refresh_link_ids(conf, requested)
        cache_misses = self._unresolved_link_ids(requested)
        unresolved = list(dict.fromkeys([*cache_misses, *forced]))
        raw_result = dict(
            self._landing.collect(
                link_ids=unresolved,
                dag_run_id=link_reference_run_id,
                landing_load_date=_load_date(conf),
            )
        )
        raw_result.update(
            {
                "parent_incident_run_id": parent_incident_run_id,
                "requested_link_ids": requested,
                "unresolved_link_ids": unresolved,
            }
        )
        return raw_result

    def materialize(
        self,
        *,
        raw_result: dict[str, object],
        link_reference_run_id: str,
    ) -> dict[str, object]:
        requested = normalize_link_ids(raw_result.get("requested_link_ids") or [])
        unresolved = normalize_link_ids(raw_result.get("unresolved_link_ids") or [])
        if unresolved:
            load_result = self._load(
                raw_result=raw_result,
                dag_run_id=link_reference_run_id,
            )
            verification = self._verify(
                dag_run_id=link_reference_run_id,
                load_result=load_result,
            )
        else:
            load_result = {
                "inserted_info": 0,
                "inserted_vertices": 0,
                "audit_rows": 0,
                "raw_object_keys": [],
            }
            verification = {
                "info_rows": 0,
                "vertex_rows": 0,
                "raw_objects": 0,
                "audit_rows": 0,
            }
        remaining = self._unresolved_link_ids(requested)
        if remaining:
            raise TrafficCompletenessError(
                "Traffic link reference remains unresolved after materialization: "
                + ",".join(remaining)
            )
        return {
            **load_result,
            "verification": verification,
            "requested_link_ids": requested,
            "parent_incident_run_id": str(
                raw_result.get("parent_incident_run_id") or ""
            ),
        }


__all__ = ["TrafficLinkReferencePipeline"]
