"""Traffic landing adapters for network, object storage, and runtime wiring."""

from __future__ import annotations

import os
import uuid
import warnings
from datetime import datetime, timezone
from typing import Callable, Protocol

from common.collection_slots import (
    ExpectedSlot,
    is_slot_active,
    parse_activation_at,
    require_policy_boundary,
)
from common.http import HttpCore
from common.http.seoul import SeoulOpenApiClient
from common.raw_manifest import validate_raw_manifest
from traffic_ingest.common.runtime import (
    checkpoint_prefix,
    raw_prefix,
    r2_env,
    trino_cursor,
)
from traffic_ingest.collection_slots import floor_to_five_minutes, traffic_incident_slot
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
_COLLECTION_SLOT_ACTIVATION_ENV = "ASK_SEOUL_COLLECTION_SLOT_ACTIVATION_AT"
_TRAFFIC_RAW_RETENTION_BOUNDARY_ENV = "ASK_SEOUL_TRAFFIC_RAW_RETENTION_BOUNDARY_AT"


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
        IfNoneMatch: str | None = None,
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

    def write_bytes_if_absent(
        self,
        key: str,
        payload: bytes,
        content_type: str,
    ) -> bool:
        from botocore.exceptions import ClientError

        for _ in range(3):
            try:
                self._client.put_object(
                    Bucket=self._bucket,
                    Key=key,
                    Body=payload,
                    ContentType=content_type,
                    IfNoneMatch="*",
                )
                return True
            except ClientError as exc:
                response = exc.response or {}
                error = response.get("Error", {})
                code = str(error.get("Code", ""))
                status = int((response.get("ResponseMetadata", {}) or {}).get(
                    "HTTPStatusCode", 0
                ))
                if code == "PreconditionFailed" or status == 412:
                    return False
                if code == "ConditionalRequestConflict" or status == 409:
                    continue
                raise
        raise RuntimeError("R2 conditional write conflicted repeatedly")


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
        checkpoint_prefix=checkpoint_prefix(),
        clock=lambda: datetime.now(timezone.utc),
        request_id=lambda: str(uuid.uuid4()),
    )


def build_traffic_manifest() -> TrafficRunManifest:
    return TrafficRunManifest(trino_cursor)


def _build_traffic_r2_storage():
    from common.storage import build_storage

    return build_storage(
        "r2",
        bucket=r2_env("R2_BUCKET_NAME"),
        endpoint=r2_env("R2_ENDPOINT"),
        key=r2_env("R2_ACCESS_KEY_ID"),
        secret=r2_env("R2_SECRET_ACCESS_KEY"),
        region="auto",
    )


def build_traffic_snapshot_receipts() -> TrafficSnapshotReceipts:
    storage = _build_traffic_r2_storage()
    return TrafficSnapshotReceipts(storage)


def build_traffic_collection_slot_storage():
    return _build_traffic_r2_storage()


def build_traffic_collection_slot_receipts():
    from common.collection_slots.receipts import CollectionSlotReceipts

    return CollectionSlotReceipts(build_traffic_collection_slot_storage())


class _NoOpCollectionSlotReceipts:
    def record_expected(self, _slot: ExpectedSlot) -> str:
        return "collection-slot-receipts/noop"

    def record_outcome(self, _outcome) -> str:
        return "collection-slot-receipts/noop"


def _traffic_collection_slot_for_logical_date(logical_date: datetime | str):
    activation_at = parse_activation_at(
        os.environ.get(_COLLECTION_SLOT_ACTIVATION_ENV)
    )
    slot_at = floor_to_five_minutes(logical_date)
    if not is_slot_active(slot_at, activation_at):
        return None
    recovery_boundary = require_policy_boundary(
        os.environ.get(_TRAFFIC_RAW_RETENTION_BOUNDARY_ENV),
        _TRAFFIC_RAW_RETENTION_BOUNDARY_ENV,
    )
    return traffic_incident_slot(
        logical_date,
        recovery_boundary=recovery_boundary,
    )


def build_traffic_collection_slot_receipt_ports():
    activation_at = parse_activation_at(
        os.environ.get(_COLLECTION_SLOT_ACTIVATION_ENV)
    )
    if activation_at is None:
        return _NoOpCollectionSlotReceipts(), _traffic_collection_slot_for_logical_date
    return (
        build_traffic_collection_slot_receipts(),
        _traffic_collection_slot_for_logical_date,
    )


def traffic_raw_manifest_is_verified(
    raw_result: object,
    *,
    dag_run_id: str,
) -> bool:
    """Return true only for a complete manifest matching this run's raw objects."""
    if not isinstance(raw_result, dict):
        return False
    manifest_key = raw_result.get("manifest_key")
    raw_objects = raw_result.get("raw_objects")
    if not isinstance(manifest_key, str) or not manifest_key:
        return False
    if not isinstance(raw_objects, list) or not raw_objects:
        return False
    object_keys: list[str] = []
    for raw_object in raw_objects:
        if not isinstance(raw_object, dict):
            return False
        raw_object_key = raw_object.get("raw_object_key")
        if not isinstance(raw_object_key, str) or not raw_object_key:
            return False
        object_keys.append(raw_object_key)
    if len(object_keys) != len(set(object_keys)):
        return False
    try:
        manifest = build_traffic_collection_slot_storage().read_json(manifest_key)
        validate_raw_manifest(
            manifest,
            run_id=dag_run_id,
            dataset="seoul_traffic_incident",
            object_keys=object_keys,
        )
    except (FileNotFoundError, TypeError, ValueError):
        return False
    return True


def build_incident_landing_lifecycle() -> IncidentLandingLifecycle:
    from common.runtime_guard import validate_dev_runtime

    slot_receipts, slot_for_logical_date = build_traffic_collection_slot_receipt_ports()
    return IncidentLandingLifecycle(
        runtime_guard=lambda: validate_dev_runtime("traffic"),
        ledger=TrafficRunLedger(),
        landing=build_traffic_landing(),
        receipts=build_traffic_snapshot_receipts(),
        slot_receipts=slot_receipts,
        slot_for_logical_date=slot_for_logical_date,
        raw_manifest_is_verified=traffic_raw_manifest_is_verified,
        clock=lambda: datetime.now(timezone.utc),
    )


def build_incident_materializer() -> IncidentMaterializer:
    from traffic_ingest.bronze import (
        create_seoul_traffic_bronze_table,
        find_verified_seoul_traffic_bronze_receipts,
        insert_seoul_traffic_bronze_rows,
        replace_seoul_traffic_bronze_snapshots,
        verify_seoul_traffic_bronze_runtime,
    )
    from traffic_ingest.bronze_batch import (
        load_traffic_bronze_batch,
        load_traffic_bronze_batches,
    )
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

    def load_many(
        raw_results: dict[str, dict[str, object]],
    ) -> dict[str, dict[str, object]]:
        return load_traffic_bronze_batches(
            raw_results=raw_results,
            cursor_factory=trino_cursor,
            create_table=create_seoul_traffic_bronze_table,
            download_raw_object=download_raw_object,
            replace_snapshots=replace_seoul_traffic_bronze_snapshots,
        )

    def verify_many(load_results, receipts) -> dict[str, int]:
        del load_results
        return find_verified_seoul_traffic_bronze_receipts(
            {
                receipt.snapshot_run_id: receipt.raw_result
                for receipt in receipts
            }
        )

    def verified_receipts(receipts) -> dict[str, int]:
        cursor, catalog, schema = trino_cursor()
        create_seoul_traffic_bronze_table(cursor, catalog, schema)
        return find_verified_seoul_traffic_bronze_receipts(
            {
                receipt.snapshot_run_id: receipt.raw_result
                for receipt in receipts
            },
            cursor_factory=lambda: (cursor, catalog, schema),
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

    slot_receipts, slot_for_logical_date = build_traffic_collection_slot_receipt_ports()
    return IncidentMaterializer(
        receipts=build_traffic_snapshot_receipts(),
        manifest=build_traffic_manifest(),
        load=load,
        verify=verify,
        load_many=load_many,
        verify_many=verify_many,
        verified_receipts=verified_receipts,
        recover_legacy_raw_result=recover_legacy_raw_result,
        clock=lambda: datetime.now(timezone.utc),
        slot_receipts=slot_receipts,
        slot_for_logical_date=slot_for_logical_date,
        raw_manifest_is_verified=traffic_raw_manifest_is_verified,
    )
