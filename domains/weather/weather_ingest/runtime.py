"""Weather landing adapters for network, object storage, and runtime wiring."""

from __future__ import annotations

import uuid
import time
from datetime import datetime, timezone
from typing import Callable, Protocol

from weather_ingest.common.runtime import fetch_url, raw_prefix, r2_env, trino_cursor
from weather_ingest.kma import build_kma_url
from weather_ingest.landing import KmaLanding
from weather_ingest.run_manifest import WeatherRunManifest


KMA_RETRY_STATUSES = (429, 500, 502, 503, 504)
KMA_429_BACKOFF_SECONDS = (3600.0, 5400.0, 7200.0)
_MISSING_OBJECT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})


FetchUrl = Callable[..., tuple[int, bytes]]


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


class KmaHttpAdapter:
    def __init__(
        self,
        fetch: FetchUrl,
        *,
        delay_seconds: float = 4.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._fetch = fetch
        self._delay_seconds = delay_seconds
        self._sleep = sleep
        self._has_successful_request = False

    def fetch_page(
        self,
        *,
        base_date: str,
        base_time: str,
        nx: int,
        ny: int,
        page_no: int,
        num_of_rows: int,
    ) -> tuple[int, bytes]:
        if self._has_successful_request:
            self._sleep(self._delay_seconds)
        response = self._fetch(
            build_kma_url(
                base_date=base_date,
                base_time=base_time,
                nx=nx,
                ny=ny,
                page_no=page_no,
                num_of_rows=num_of_rows,
            ),
            "ask-seoul-kma-bronze/1.0",
            max_attempts=4,
            retry_statuses=KMA_RETRY_STATUSES,
            retry_base_delay_seconds=30,
            retry_429_backoff_seconds=KMA_429_BACKOFF_SECONDS,
        )
        self._has_successful_request = True
        return response


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


def _build_s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=r2_env("R2_ENDPOINT"),
        aws_access_key_id=r2_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=r2_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )


def build_weather_landing() -> KmaLanding:
    raw_store = R2RawObjectStore(
        _build_s3_client(),
        bucket=r2_env("R2_BUCKET_NAME"),
    )
    return KmaLanding(
        source=KmaHttpAdapter(fetch_url),
        raw_store=raw_store,
        raw_prefix=raw_prefix(),
        clock=lambda: datetime.now(timezone.utc),
        request_id=lambda: str(uuid.uuid4()),
    )


def build_weather_manifest() -> WeatherRunManifest:
    return WeatherRunManifest(trino_cursor)
