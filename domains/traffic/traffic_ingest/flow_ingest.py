"""Runtime adapters for Seoul TrafficInfo flow collection."""

from __future__ import annotations

from datetime import datetime, timezone

from common.http import HttpCore, PathKey
from traffic_ingest.flow_info import (
    SOURCE_ID,
    build_api_url,
    traffic_api_key,
)
from traffic_ingest.flow_landing import TrafficFlowLanding
from traffic_ingest.run_manifest import TrafficRunManifest
from traffic_ingest.common.runtime import (
    r2_env,
    trino_cursor,
)
from traffic_ingest.runtime import R2RawObjectStore, _build_s3_client


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


__all__ = ["build_traffic_flow_landing", "build_traffic_flow_manifest"]
