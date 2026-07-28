"""Traffic Incident lifecycle services independent from Airflow orchestration."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

from traffic_ingest.landing import RunIdentity, TrafficLandingRequest
from traffic_ingest.run_ledger import STATUS_FAILED, STATUS_STARTED, STATUS_SUCCESS
from traffic_ingest.run_manifest import TrafficRun
from traffic_ingest.snapshot_receipt import LandedSnapshot, MaterializedSnapshot


LOGGER = logging.getLogger(__name__)
LANDING_TASK_ID = "land_traffic_incident_snapshot"
MATERIALIZER_TASK_ID = "materialize_pending_traffic_incident_snapshots"
KST = ZoneInfo("Asia/Seoul")


class Landing(Protocol):
    def collect(self, run: RunIdentity, request: TrafficLandingRequest): ...


class Ledger(Protocol):
    def record(self, **kwargs) -> str: ...


class ReceiptWriter(Protocol):
    def record_landed(self, receipt: LandedSnapshot) -> str: ...


class ReceiptQueue(Protocol):
    def pending(self, *, limit: int) -> list[LandedSnapshot]: ...

    def is_pending(self, snapshot_run_id: str) -> bool: ...

    def record_materialized(self, receipt: MaterializedSnapshot) -> str: ...


class Manifest(Protocol):
    def start(self, run: TrafficRun, **metrics) -> str: ...

    def publish(self, run: TrafficRun, **metrics) -> str: ...

    def fail(self, run: TrafficRun, **metrics) -> str: ...


def _iso_timestamp(value: datetime | str) -> str:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise TypeError("Traffic lifecycle timestamp must be datetime or ISO string")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.isoformat()


def _raw_objects(raw_result: Mapping[str, object]) -> list[Mapping[str, object]]:
    values = raw_result.get("raw_objects")
    if not isinstance(values, list) or not values:
        raise ValueError("Traffic landing result requires at least one raw object")
    if any(not isinstance(item, Mapping) for item in values):
        raise ValueError("Traffic landing raw object descriptor is malformed")
    return values


def landing_asset_metadata(
    snapshot_run_id: str,
    raw_result: Mapping[str, object],
) -> dict[str, object]:
    raw_objects = _raw_objects(raw_result)
    collected = [
        (
            datetime.fromisoformat(str(item["collected_at"]).replace("Z", "+00:00")),
            str(item["collected_at"]),
        )
        for item in raw_objects
    ]
    hashes = sorted(str(item.get("raw_hash") or "") for item in raw_objects)
    if any(not value for value in hashes):
        raise ValueError("Traffic landing raw object requires payload hash")
    payload_hash = (
        hashes[0]
        if len(hashes) == 1
        else hashlib.sha256("|".join(hashes).encode("utf-8")).hexdigest()
    )
    event_at = max(collected, key=lambda item: item[0])[1]
    return {
        "source_id": "seoul_traffic_incident",
        "snapshot_run_id": snapshot_run_id,
        "event_at": event_at,
        "raw_object_count": len(raw_objects),
        "payload_hash": payload_hash,
        "is_publishable": raw_result.get("is_publishable") is True,
    }


@dataclass(frozen=True)
class LandingOutcome:
    raw_result: dict[str, object]
    receipt_key: str
    asset_metadata: dict[str, object]


class IncidentLandingLifecycle:
    """Execute one scheduled snapshot while keeping lifecycle evidence atomic."""

    def __init__(
        self,
        *,
        runtime_guard: Callable[[], None],
        ledger: Ledger,
        landing: Landing,
        receipts: ReceiptWriter,
        clock: Callable[[], datetime],
    ) -> None:
        self._runtime_guard = runtime_guard
        self._ledger = ledger
        self._landing = landing
        self._receipts = receipts
        self._clock = clock

    def run(
        self,
        *,
        run: RunIdentity,
        logical_date: datetime | str,
        request: TrafficLandingRequest,
    ) -> LandingOutcome:
        logical_at = _iso_timestamp(logical_date)
        try:
            self._record_ledger(
                run=run,
                status=STATUS_STARTED,
                logical_date=logical_at,
            )
            self._runtime_guard()
            raw_result = dict(self._landing.collect(run, request).to_xcom())
            asset_metadata = landing_asset_metadata(run.run_id, raw_result)
            receipt = LandedSnapshot(
                source_id="seoul_traffic_incident",
                producer_dag_id=run.dag_id,
                snapshot_run_id=run.run_id,
                logical_date=logical_at,
                snapshot_at=str(asset_metadata["event_at"]),
                raw_result=raw_result,
                event_at=_iso_timestamp(self._clock()),
            )
            receipt_key = self._receipts.record_landed(receipt)
            self._record_ledger(
                run=run,
                status=STATUS_SUCCESS,
                logical_date=logical_at,
            )
            return LandingOutcome(
                raw_result=raw_result,
                receipt_key=receipt_key,
                asset_metadata=asset_metadata,
            )
        except Exception as error:
            try:
                self._record_ledger(
                    run=run,
                    status=STATUS_FAILED,
                    logical_date=logical_at,
                    error=error,
                )
            except Exception as ledger_error:  # failure evidence must not mask cause
                LOGGER.warning(
                    "Traffic landing FAILED ledger write failed: %s",
                    type(ledger_error).__name__,
                )
            raise

    def _record_ledger(
        self,
        *,
        run: RunIdentity,
        status: str,
        logical_date: str,
        error: BaseException | None = None,
    ) -> None:
        self._ledger.record(
            dag_id=run.dag_id,
            run_id=run.run_id,
            status=status,
            logical_date=logical_date,
            task_id=LANDING_TASK_ID,
            error=error,
        )


def bronze_asset_metadata(
    receipt: LandedSnapshot,
    *,
    row_count: int,
) -> dict[str, object]:
    landing_metadata = landing_asset_metadata(
        receipt.snapshot_run_id,
        receipt.raw_result,
    )
    snapshot_at = datetime.fromisoformat(receipt.snapshot_at.replace("Z", "+00:00"))
    return {
        "source_id": receipt.source_id,
        "bronze_run_id": receipt.snapshot_run_id,
        "bronze_dag_run_id": receipt.snapshot_run_id,
        "event_at": receipt.snapshot_at,
        "load_date": snapshot_at.astimezone(KST).date().isoformat(),
        "row_count": int(row_count),
        "payload_hash": landing_metadata["payload_hash"],
        "is_publishable": True,
    }


@dataclass(frozen=True)
class MaterializationBatch:
    snapshot_run_ids: tuple[str, ...]
    asset_metadata: tuple[dict[str, object], ...]

    @property
    def processed_count(self) -> int:
        return len(self.snapshot_run_ids)

    @property
    def latest_asset_metadata(self) -> dict[str, object] | None:
        if not self.asset_metadata:
            return None
        return max(
            self.asset_metadata,
            key=lambda metadata: (
                datetime.fromisoformat(
                    str(metadata["event_at"]).replace("Z", "+00:00")
                ),
                str(metadata["bronze_dag_run_id"]),
            ),
        )


class IncidentMaterializer:
    """Drain landed snapshots into Bronze without losing landing identity."""

    def __init__(
        self,
        *,
        receipts: ReceiptQueue,
        manifest: Manifest,
        load: Callable[[dict[str, object], str], dict[str, object]],
        verify: Callable[[dict[str, object], str], int],
        verified_receipts: (
            Callable[[list[LandedSnapshot]], Mapping[str, int]] | None
        ) = None,
        recover_legacy_raw_result: (
            Callable[[dict[str, object], str], dict[str, object]] | None
        ) = None,
        clock: Callable[[], datetime],
    ) -> None:
        self._receipts = receipts
        self._manifest = manifest
        self._load = load
        self._verify = verify
        self._verified_receipts = verified_receipts
        self._recover_legacy_raw_result = recover_legacy_raw_result
        self._clock = clock

    def run(
        self,
        *,
        materializer_dag_id: str,
        materializer_run_id: str,
        limit: int,
    ) -> MaterializationBatch:
        snapshot_run_ids: list[str] = []
        asset_metadata: list[dict[str, object]] = []
        pending_receipts = self._receipts.pending(limit=limit)
        verified_rows_by_snapshot = self._preflight_verified_receipts(
            pending_receipts,
            materializer_dag_id=materializer_dag_id,
        )
        for receipt in pending_receipts:
            if (
                receipt.snapshot_run_id in verified_rows_by_snapshot
                and not self._receipts.is_pending(receipt.snapshot_run_id)
            ):
                continue
            run = TrafficRun(materializer_dag_id, receipt.snapshot_run_id)
            raw_result = dict(receipt.raw_result)
            raw_object_count = len(_raw_objects(raw_result))
            try:
                self._manifest.start(
                    run,
                    expected_raw_objects=raw_object_count,
                )
                if (
                    not raw_result.get("manifest_key")
                    and self._recover_legacy_raw_result is not None
                ):
                    raw_result = self._recover_legacy_raw_result(
                        raw_result,
                        receipt.snapshot_run_id,
                    )
                if receipt.snapshot_run_id in verified_rows_by_snapshot:
                    (
                        expected_rows,
                        page_count,
                        raw_object_keys,
                        is_publishable,
                    ) = _receipt_materialization_contract(raw_result)
                    verified_rows = verified_rows_by_snapshot[receipt.snapshot_run_id]
                    if verified_rows != expected_rows:
                        raise ValueError(
                            "Traffic materializer preflight row count does not "
                            f"match receipt: expected={expected_rows}, "
                            f"actual={verified_rows}"
                        )
                else:
                    load_result = self._load(
                        raw_result,
                        receipt.snapshot_run_id,
                    )
                    verified_rows = self._verify(
                        load_result,
                        receipt.snapshot_run_id,
                    )
                    expected_rows = int(
                        load_result.get(
                            "expected_rows", load_result.get("inserted", 0)
                        )
                    )
                    page_count = int(load_result.get("page_count", raw_object_count))
                    raw_object_keys = list(load_result.get("raw_object_keys") or [])
                    is_publishable = bool(load_result.get("is_publishable", True))
                self._manifest.publish(
                    run,
                    expected_rows=expected_rows,
                    actual_rows=verified_rows,
                    expected_raw_objects=page_count,
                    actual_raw_objects=len(raw_object_keys),
                    is_publishable=is_publishable,
                )
                self._receipts.record_materialized(
                    MaterializedSnapshot(
                        source_id=receipt.source_id,
                        snapshot_run_id=receipt.snapshot_run_id,
                        snapshot_at=receipt.snapshot_at,
                        materializer_dag_id=materializer_dag_id,
                        materializer_run_id=materializer_run_id,
                        row_count=verified_rows,
                        raw_object_count=page_count,
                        event_at=_iso_timestamp(self._clock()),
                    )
                )
                snapshot_run_ids.append(receipt.snapshot_run_id)
                if is_publishable:
                    asset_metadata.append(
                        bronze_asset_metadata(receipt, row_count=verified_rows)
                    )
            except Exception as error:
                try:
                    self._manifest.fail(
                        run,
                        task_id=MATERIALIZER_TASK_ID,
                        error=error,
                        expected_raw_objects=raw_object_count,
                    )
                except Exception as manifest_error:  # preserve materialization cause
                    LOGGER.warning(
                        "Traffic materialization FAILED manifest write failed: %s",
                        type(manifest_error).__name__,
                    )
                raise
        return MaterializationBatch(
            snapshot_run_ids=tuple(snapshot_run_ids),
            asset_metadata=tuple(asset_metadata),
        )

    def _preflight_verified_receipts(
        self,
        pending_receipts: list[LandedSnapshot],
        *,
        materializer_dag_id: str,
    ) -> dict[str, int]:
        if not pending_receipts or self._verified_receipts is None:
            return {}
        try:
            verified_rows_by_snapshot = dict(
                self._verified_receipts(pending_receipts)
            )
            pending_snapshot_ids = {
                receipt.snapshot_run_id for receipt in pending_receipts
            }
            unexpected_snapshot_ids = set(verified_rows_by_snapshot).difference(
                pending_snapshot_ids
            )
            if unexpected_snapshot_ids:
                raise ValueError(
                    "Traffic materializer preflight returned unknown receipts: "
                    f"{sorted(unexpected_snapshot_ids)}"
                )
            return verified_rows_by_snapshot
        except Exception as error:
            first_receipt = pending_receipts[0]
            run = TrafficRun(materializer_dag_id, first_receipt.snapshot_run_id)
            raw_object_count = len(_raw_objects(first_receipt.raw_result))
            try:
                self._manifest.start(run, expected_raw_objects=raw_object_count)
            except Exception as manifest_error:
                LOGGER.warning(
                    "Traffic preflight manifest START write failed: %s",
                    type(manifest_error).__name__,
                )
            try:
                self._manifest.fail(
                    run,
                    task_id=MATERIALIZER_TASK_ID,
                    error=error,
                    expected_raw_objects=raw_object_count,
                )
            except Exception as manifest_error:
                LOGGER.warning(
                    "Traffic preflight manifest FAILED write failed: %s",
                    type(manifest_error).__name__,
                )
            raise


def _receipt_materialization_contract(
    raw_result: Mapping[str, object],
) -> tuple[int, int, list[str], bool]:
    raw_objects = _raw_objects(raw_result)
    raw_object_keys = [str(item.get("raw_object_key") or "") for item in raw_objects]
    if not all(raw_object_keys) or len(set(raw_object_keys)) != len(raw_object_keys):
        raise ValueError("Traffic materializer receipt raw object keys are invalid")
    try:
        expected_rows = int(raw_result.get("expected_rows"))
        page_count = int(raw_result.get("page_count", len(raw_objects)))
    except (TypeError, ValueError) as exc:
        raise ValueError("Traffic materializer receipt counts are invalid") from exc
    if expected_rows < 0 or page_count != len(raw_objects):
        raise ValueError("Traffic materializer receipt counts are invalid")
    return (
        expected_rows,
        page_count,
        raw_object_keys,
        bool(raw_result.get("is_publishable", True)),
    )


__all__ = [
    "IncidentLandingLifecycle",
    "IncidentMaterializer",
    "LANDING_TASK_ID",
    "LandingOutcome",
    "MATERIALIZER_TASK_ID",
    "MaterializationBatch",
    "bronze_asset_metadata",
    "landing_asset_metadata",
]
