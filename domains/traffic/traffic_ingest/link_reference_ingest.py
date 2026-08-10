"""Runtime adapters for TOPIS LinkInfo and LinkVerInfo collection."""

from __future__ import annotations

from datetime import datetime, timezone

from common.http import HttpCore, PathKey
from common.runtime_guard import validate_dev_runtime
from traffic_ingest.common.runtime import (
    download_raw_object,
    r2_env,
    trino_cursor,
)
from traffic_ingest.flow_info import resolve_flow_link_ids, traffic_api_key
from traffic_ingest.link_reference_bronze import (
    load_traffic_link_reference_batch,
    unresolved_link_reference_ids,
    verify_seoul_traffic_link_reference_runtime,
)
from traffic_ingest.link_reference_info import build_link_reference_api_url
from traffic_ingest.link_reference_landing import TrafficLinkReferenceLanding
from traffic_ingest.link_reference_pipeline import TrafficLinkReferencePipeline
from traffic_ingest.runtime import (
    R2RawObjectStore,
    _build_s3_client,
    build_traffic_manifest,
)


def _build_link_reference_http() -> HttpCore:
    return HttpCore(
        source="seoul_topis",
        timeout=30.0,
        max_attempts=1,
        rate_limit=None,
    )


def build_traffic_link_reference_landing() -> TrafficLinkReferenceLanding:
    http = _build_link_reference_http()
    api_key = traffic_api_key()

    def fetch_service(service_name: str, link_id: str) -> tuple[int, bytes]:
        response = http.get(
            build_link_reference_api_url(
                service_name,
                link_id,
                api_key="{api_key}",
            ),
            auth=PathKey(api_key, encode=False),
        )
        return int(response.status), response.content

    raw_store = R2RawObjectStore(
        _build_s3_client(),
        bucket=r2_env("R2_BUCKET_NAME"),
    )
    return TrafficLinkReferenceLanding(
        raw_store=raw_store,
        fetch_service=fetch_service,
        clock=lambda: datetime.now(timezone.utc),
    )


def build_traffic_link_reference_pipeline() -> TrafficLinkReferencePipeline:
    def load(*, raw_result: dict[str, object], dag_run_id: str) -> dict[str, object]:
        return load_traffic_link_reference_batch(
            raw_result=raw_result,
            dag_run_id=dag_run_id,
            cursor_factory=trino_cursor,
            download_raw_object=download_raw_object,
        )

    def verify(
        *, dag_run_id: str, load_result: dict[str, object]
    ) -> dict[str, int]:
        return verify_seoul_traffic_link_reference_runtime(
            dag_run_id=dag_run_id,
            expected_info_rows=int(load_result.get("inserted_info", 0)),
            expected_vertex_rows=int(load_result.get("inserted_vertices", 0)),
            expected_raw_objects=int(load_result.get("audit_rows", 0)),
            cursor_factory=trino_cursor,
        )

    return TrafficLinkReferencePipeline(
        runtime_guard=lambda: validate_dev_runtime("traffic"),
        incident_manifest=build_traffic_manifest(),
        resolve_links=lambda conf, parent: resolve_flow_link_ids(
            conf=conf,
            incident_run_id=parent,
        ),
        unresolved_link_ids=unresolved_link_reference_ids,
        landing=build_traffic_link_reference_landing(),
        load=load,
        verify=verify,
    )


__all__ = [
    "build_traffic_link_reference_landing",
    "build_traffic_link_reference_pipeline",
]
