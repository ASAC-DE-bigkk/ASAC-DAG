"""Runtime adapters for Seoul TrafficInfo flow collection."""

from __future__ import annotations

from datetime import datetime, timezone

from common.http import HttpCore, PathKey
from traffic_ingest.flow_info import (
    SOURCE_ID,
    build_api_url,
    traffic_api_key,
)
from traffic_ingest.flow_bronze import (
    load_traffic_flow_batch,
    verify_seoul_traffic_flow_bronze_runtime,
)
from traffic_ingest.flow_landing import TrafficFlowLanding
from traffic_ingest.flow_pipeline import TrafficFlowPipeline
from traffic_ingest.run_manifest import TrafficRunManifest
from traffic_ingest.common.runtime import (
    r2_env,
    trino_cursor,
)
from traffic_ingest.runtime import R2RawObjectStore, _build_s3_client
from traffic_ingest.runtime import build_traffic_manifest
from common.runtime_guard import validate_dev_runtime
from traffic_ingest.common.runtime import download_raw_object


def _build_flow_http():
    return HttpCore(
        source="seoul_topis",
        timeout=30.0,
        max_attempts=1,
        rate_limit=None,
    )


def build_traffic_flow_landing() -> TrafficFlowLanding:
    http = _build_flow_http()
    api_key = traffic_api_key()

    def fetch_page(link_id: str) -> tuple[int, bytes]:
        response = http.get(
            build_api_url(link_id, api_key="{api_key}"),
            auth=PathKey(api_key, encode=False),
        )
        return int(response.status), response.content

    raw_store = R2RawObjectStore(
        _build_s3_client(),
        bucket=r2_env("R2_BUCKET_NAME"),
    )
    return TrafficFlowLanding(
        raw_store=raw_store,
        fetch_page=fetch_page,
        clock=lambda: datetime.now(timezone.utc),
    )


def build_traffic_flow_manifest() -> TrafficRunManifest:
    return TrafficRunManifest(trino_cursor, source_id=SOURCE_ID)


def build_traffic_flow_pipeline() -> TrafficFlowPipeline:
    def load(raw_result: dict[str, object], flow_run_id: str) -> dict[str, object]:
        return load_traffic_flow_batch(
            raw_result=raw_result,
            dag_run_id=flow_run_id,
            cursor_factory=trino_cursor,
            download_raw_object=download_raw_object,
        )

    def verify(load_result: dict[str, object], flow_run_id: str) -> int:
        return verify_seoul_traffic_flow_bronze_runtime(
            dag_run_id=flow_run_id,
            expected_rows=int(
                load_result.get("expected_rows", load_result.get("inserted", 0))
            ),
            expected_raw_objects=int(load_result.get("page_count", 0)),
        )

    return TrafficFlowPipeline(
        runtime_guard=lambda: validate_dev_runtime("traffic"),
        incident_manifest=build_traffic_manifest(),
        flow_manifest=build_traffic_flow_manifest(),
        landing=build_traffic_flow_landing(),
        load=load,
        verify=verify,
    )


__all__ = [
    "build_traffic_flow_landing",
    "build_traffic_flow_manifest",
    "build_traffic_flow_pipeline",
]
