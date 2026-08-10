"""Runtime adapters for TOPIS LinkInfo and LinkVerInfo collection."""

from __future__ import annotations

from datetime import datetime, timezone

from common.http import HttpCore, PathKey
from traffic_ingest.common.runtime import (
    r2_env,
)
from traffic_ingest.flow_info import traffic_api_key
from traffic_ingest.link_reference_info import build_link_reference_api_url
from traffic_ingest.link_reference_landing import TrafficLinkReferenceLanding
from traffic_ingest.runtime import (
    R2RawObjectStore,
    _build_s3_client,
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


__all__ = [
    "build_traffic_link_reference_landing",
]
