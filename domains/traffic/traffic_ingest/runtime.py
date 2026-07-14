"""Traffic landing adapters for network, object storage, and runtime wiring."""

from __future__ import annotations

import os
import uuid
import warnings
from datetime import datetime, timezone
from typing import Callable, Protocol

from common.http import HttpCore
from common.http.seoul import SeoulOpenApiClient
from traffic_ingest.common.runtime import raw_prefix, r2_env, trino_cursor
from traffic_ingest.landing import TrafficLanding
from traffic_ingest.errors import TrafficBronzeConfigurationError
from traffic_ingest.run_manifest import TrafficRunManifest


_MISSING_OBJECT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})


class SeoulClient(Protocol):
    def fetch_bytes(
        self,
        service: str,
        start: int,
        end: int,
        *,
        fmt: str,
    ): ...


class S3Client(Protocol):
    def head_object(self, *, Bucket: str, Key: str): ...

    def get_object(self, *, Bucket: str, Key: str): ...

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
    ): ...


class _LazySeoulClient:
    def __init__(self, factory: Callable[[], SeoulClient]) -> None:
        self._factory = factory
        self._client: SeoulClient | None = None

    def fetch_bytes(
        self,
        service: str,
        start: int,
        end: int,
        *,
        fmt: str,
    ):
        if self._client is None:
            self._client = self._factory()
        return self._client.fetch_bytes(service, start, end, fmt=fmt)


class TopisHttpAdapter:
    def __init__(self, client: SeoulClient) -> None:
        self._client = client

    def fetch_page(self, start_index: int, end_index: int) -> tuple[int, bytes]:
        response = self._client.fetch_bytes(
            "AccInfo",
            start_index,
            end_index,
            fmt="xml",
        )
        return int(response.status), response.content


class R2RawObjectStore:
    def __init__(self, client: S3Client, *, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except Exception as exc:
            error = (getattr(exc, "response", {}) or {}).get("Error", {})
            if str(error.get("Code", "")) in _MISSING_OBJECT_CODES:
                return False
            raise
        return True

    def read_bytes(self, key: str) -> bytes:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        return response["Body"].read()

    def write_bytes(self, key: str, payload: bytes, content_type: str) -> None:
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=payload,
            ContentType=content_type,
        )


def traffic_api_key() -> str:
    """Return the canonical credential, with a temporary secret-safe fallback."""
    canonical = os.environ.get("SEOUL_OPEN_API_KEY")
    if canonical:
        return canonical
    legacy = os.environ.get("SEOUL_API_KEY_TRIC")
    if legacy:
        warnings.warn(
            "SEOUL_API_KEY_TRIC is deprecated; configure SEOUL_OPEN_API_KEY instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return legacy
    raise TrafficBronzeConfigurationError(
        "Missing required environment variable: SEOUL_OPEN_API_KEY"
    )


def _build_s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=r2_env("R2_ENDPOINT"),
        aws_access_key_id=r2_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=r2_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )


def _build_seoul_client() -> SeoulOpenApiClient:
    core = HttpCore(
        source="seoul_topis",
        timeout=30.0,
        max_attempts=1,
        rate_limit=None,
    )
    return SeoulOpenApiClient(core, traffic_api_key())


def build_traffic_landing() -> TrafficLanding:
    source = TopisHttpAdapter(_LazySeoulClient(_build_seoul_client))
    raw_store = R2RawObjectStore(
        _build_s3_client(),
        bucket=r2_env("R2_BUCKET_NAME"),
    )
    return TrafficLanding(
        source=source,
        raw_store=raw_store,
        raw_prefix=raw_prefix(),
        clock=lambda: datetime.now(timezone.utc),
        request_id=lambda: str(uuid.uuid4()),
    )


def build_traffic_manifest() -> TrafficRunManifest:
    return TrafficRunManifest(trino_cursor)
