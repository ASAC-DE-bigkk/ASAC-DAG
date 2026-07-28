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
from traffic_ingest.incident_pipeline import (
    IncidentLandingLifecycle,
    IncidentMaterializer,
)
from traffic_ingest.errors import TrafficBronzeConfigurationError
from traffic_ingest.run_ledger import TrafficRunLedger
from traffic_ingest.run_manifest import TrafficRunManifest
from traffic_ingest.snapshot_receipt import TrafficSnapshotReceipts


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


def build_traffic_snapshot_receipts() -> TrafficSnapshotReceipts:
    from common.storage import build_storage, r2_env as storage_r2_env

    storage = build_storage(
        "r2",
        bucket=storage_r2_env("R2_BUCKET_NAME"),
        endpoint=storage_r2_env("R2_ENDPOINT"),
        key=storage_r2_env("R2_ACCESS_KEY_ID"),
        secret=storage_r2_env("R2_SECRET_ACCESS_KEY"),
        region="auto",
    )
    return TrafficSnapshotReceipts(storage)


def build_incident_landing_lifecycle() -> IncidentLandingLifecycle:
    from common.runtime_guard import validate_dev_runtime

    return IncidentLandingLifecycle(
        runtime_guard=lambda: validate_dev_runtime("traffic"),
        ledger=TrafficRunLedger(),
        landing=build_traffic_landing(),
        receipts=build_traffic_snapshot_receipts(),
        clock=lambda: datetime.now(timezone.utc),
    )


def build_incident_materializer() -> IncidentMaterializer:
    from traffic_ingest.bronze import (
        create_seoul_traffic_bronze_table,
        find_verified_seoul_traffic_bronze_receipts,
        insert_seoul_traffic_bronze_rows,
        verify_seoul_traffic_bronze_runtime,
    )
    from traffic_ingest.bronze_batch import load_traffic_bronze_batch
    from traffic_ingest.common.runtime import download_raw_object
    from traffic_ingest.landing_contracts import RunIdentity

    def load(raw_result: dict[str, object], snapshot_run_id: str) -> dict[str, object]:
        return load_traffic_bronze_batch(
            raw_result=raw_result,
            dag_run_id=snapshot_run_id,
            cursor_factory=trino_cursor,
            create_table=create_seoul_traffic_bronze_table,
            download_raw_object=download_raw_object,
            insert_rows=insert_seoul_traffic_bronze_rows,
        )

    def verify(load_result: dict[str, object], snapshot_run_id: str) -> int:
        return verify_seoul_traffic_bronze_runtime(
            raw_object_keys=list(load_result.get("raw_object_keys") or []),
            dag_run_id=snapshot_run_id,
            expected_rows=int(load_result.get("inserted") or 0),
            expected_raw_objects=int(load_result.get("page_count") or 0),
        )

    def verified_receipts(receipts) -> dict[str, int]:
        return find_verified_seoul_traffic_bronze_receipts(
            {
                receipt.snapshot_run_id: receipt.raw_result
                for receipt in receipts
            }
        )

    def recover_legacy_raw_result(
        raw_result: dict[str, object], snapshot_run_id: str
    ) -> dict[str, object]:
        raw_object_keys = [
            str(value) for value in raw_result.get("raw_object_keys") or []
        ]
        if not raw_object_keys or len(set(raw_object_keys)) != len(raw_object_keys):
            raise ValueError("legacy Traffic receipt raw_object_keys are invalid")
        replayed = build_traffic_landing().replay(
            raw_object_keys,
            run=RunIdentity("traffic_incident_landing", snapshot_run_id),
        ).to_xcom()
        if replayed.get("raw_object_keys") != raw_object_keys:
            raise ValueError("legacy Traffic replay raw_object_keys do not match receipt")
        if int(replayed["expected_rows"]) != int(raw_result["expected_rows"]):
            raise ValueError("legacy Traffic replay row count does not match receipt")
        receipt_hashes = {
            str(item.get("raw_object_key") or ""): str(
                item.get("raw_hash") or item.get("payload_hash") or ""
            )
            for item in raw_result.get("raw_objects") or []
            if isinstance(item, dict)
        }
        replayed_hashes = {
            str(item.get("raw_object_key") or ""): str(
                item.get("raw_hash") or item.get("payload_hash") or ""
            )
            for item in replayed.get("raw_objects") or []
            if isinstance(item, dict)
        }
        if (
            set(receipt_hashes) != set(raw_object_keys)
            or receipt_hashes != replayed_hashes
            or not all(receipt_hashes.values())
        ):
            raise ValueError("legacy Traffic replay payload hashes do not match receipt")
        manifest_key = str(replayed.get("manifest_key") or "")
        if not manifest_key:
            raise ValueError("legacy Traffic replay did not produce a raw manifest")
        return {**raw_result, "manifest_key": manifest_key}

    return IncidentMaterializer(
        receipts=build_traffic_snapshot_receipts(),
        manifest=build_traffic_manifest(),
        load=load,
        verify=verify,
        verified_receipts=verified_receipts,
        recover_legacy_raw_result=recover_legacy_raw_result,
        clock=lambda: datetime.now(timezone.utc),
    )
