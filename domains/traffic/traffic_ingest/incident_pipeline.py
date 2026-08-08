"""Traffic Incident lifecycle services independent from Airflow orchestration."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

from common.collection_slots.contract import CollectionOutcome, ExpectedSlot
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


class CollectionSlotReceiptWriter(Protocol):
    def record_expected(self, slot: ExpectedSlot) -> str: ...

    def record_outcome(self, outcome: CollectionOutcome) -> str: ...


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
        slot_receipts: CollectionSlotReceiptWriter,
        slot_for_logical_date: Callable[[datetime | str], ExpectedSlot],
        clock: Callable[[], datetime],
    ) -> None:
        self._runtime_guard = runtime_guard
        self._ledger = ledger
        self._landing = landing
        self._receipts = receipts
        self._slot_receipts = slot_receipts
        self._slot_for_logical_date = slot_for_logical_date
        self._clock = clock

    def run(
        self,
        *,
        run: RunIdentity,
        logical_date: datetime | str,
        request: TrafficLandingRequest,
    ) -> LandingOutcome:
        logical_at = _iso_timestamp(logical_date)
        slot: ExpectedSlot | None = None
        expected_recorded = False
        try:
            slot = self._slot_for_logical_date(logical_at)
            self._slot_receipts.record_expected(slot)
            expected_recorded = True
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
            if expected_recorded and slot is not None:
                try:
                    self._slot_receipts.record_outcome(
                        CollectionOutcome.create(
                            expected_slot_id=slot.expected_slot_id,
                            collection_state="collection_failed",
                            recovery_state="pending",
                            recovery_class="none",
                            gap_reason_code="landing_failed",
                            event_at=self._clock(),
                            dag_id=run.dag_id,
                            dag_run_id=run.run_id,
                            task_id=LANDING_TASK_ID,
                        )
                    )
                except Exception as receipt_error:  # preserve landing failure cause
                    LOGGER.warning(
                        "Traffic landing FAILED slot receipt write failed: %s",
                        type(receipt_error).__name__,
                    )
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
    row_count: int

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
        load_many: (
            Callable[
                [Mapping[str, dict[str, object]]],
                Mapping[str, dict[str, object]],
            ]
            | None
        ) = None,
        verify_many: (
            Callable[
                [Mapping[str, dict[str, object]], list[LandedSnapshot]],
                Mapping[str, int],
            ]
            | None
        ) = None,
        verified_receipts: (
            Callable[[list[LandedSnapshot]], Mapping[str, int]] | None
        ) = None,
        recover_legacy_raw_result: (
            Callable[[dict[str, object], str], dict[str, object]] | None
        ) = None,
        clock: Callable[[], datetime],
        slot_receipts: CollectionSlotReceiptWriter | None = None,
        slot_for_logical_date: Callable[[datetime | str], ExpectedSlot] | None = None,
    ) -> None:
        if (slot_receipts is None) != (slot_for_logical_date is None):
            raise ValueError(
                "slot_receipts and slot_for_logical_date must be configured together"
            )
        self._receipts = receipts
        self._manifest = manifest
        self._load = load
        self._verify = verify
        self._load_many = load_many
        self._verify_many = verify_many
        self._verified_receipts = verified_receipts
        self._recover_legacy_raw_result = recover_legacy_raw_result
        self._clock = clock
        self._slot_receipts = slot_receipts
        self._slot_for_logical_date = slot_for_logical_date

    def run(
        self,
        *,
        materializer_dag_id: str,
        materializer_run_id: str,
        limit: int,
    ) -> MaterializationBatch:
        pending_receipts = self._receipts.pending(limit=limit)
        verified_rows_by_snapshot = self._preflight_verified_receipts(
            pending_receipts,
            materializer_dag_id=materializer_dag_id,
        )
        active_receipts = [
            receipt
            for receipt in pending_receipts
            if not (
                receipt.snapshot_run_id in verified_rows_by_snapshot
                and not self._receipts.is_pending(receipt.snapshot_run_id)
            )
        ]
        if not active_receipts:
            return MaterializationBatch(
                snapshot_run_ids=(),
                asset_metadata=(),
                row_count=0,
            )

        runs = {
            receipt.snapshot_run_id: TrafficRun(
                materializer_dag_id,
                receipt.snapshot_run_id,
            )
            for receipt in active_receipts
        }
        raw_object_counts = {
            receipt.snapshot_run_id: len(_raw_objects(receipt.raw_result))
            for receipt in active_receipts
        }
        try:
            self._manifest_start_many(
                [
                    (runs[receipt.snapshot_run_id], raw_object_counts[receipt.snapshot_run_id])
                    for receipt in active_receipts
                ]
            )
            raw_results: dict[str, dict[str, object]] = {}
            for receipt in active_receipts:
                raw_result = dict(receipt.raw_result)
                if (
                    not raw_result.get("manifest_key")
                    and self._recover_legacy_raw_result is not None
                ):
                    raw_result = self._recover_legacy_raw_result(
                        raw_result,
                        receipt.snapshot_run_id,
                    )
                raw_results[receipt.snapshot_run_id] = raw_result

            publish_metrics: dict[str, dict[str, object]] = {}
            receipts_to_load: list[LandedSnapshot] = []
            for receipt in active_receipts:
                snapshot_run_id = receipt.snapshot_run_id
                raw_result = raw_results[snapshot_run_id]
                if snapshot_run_id not in verified_rows_by_snapshot:
                    receipts_to_load.append(receipt)
                    continue
                (
                    expected_rows,
                    page_count,
                    raw_object_keys,
                    is_publishable,
                ) = _receipt_materialization_contract(raw_result)
                verified_rows = verified_rows_by_snapshot[snapshot_run_id]
                if verified_rows != expected_rows:
                    raise ValueError(
                        "Traffic materializer preflight row count does not "
                        f"match receipt: expected={expected_rows}, "
                        f"actual={verified_rows}"
                    )
                publish_metrics[snapshot_run_id] = {
                    "expected_rows": expected_rows,
                    "actual_rows": verified_rows,
                    "expected_raw_objects": page_count,
                    "actual_raw_objects": len(raw_object_keys),
                    "is_publishable": is_publishable,
                }

            if receipts_to_load:
                load_results = self._load_receipts(
                    {
                        receipt.snapshot_run_id: raw_results[receipt.snapshot_run_id]
                        for receipt in receipts_to_load
                    }
                )
                verified_loaded = self._verify_loaded_receipts(
                    load_results,
                    receipts_to_load,
                )
                expected_snapshot_ids = {
                    receipt.snapshot_run_id for receipt in receipts_to_load
                }
                if set(load_results) != expected_snapshot_ids:
                    raise ValueError(
                        "Traffic materializer batch load returned incomplete receipts"
                    )
                if set(verified_loaded) != expected_snapshot_ids:
                    raise ValueError(
                        "Traffic materializer batch verification returned incomplete receipts"
                    )
                for receipt in receipts_to_load:
                    snapshot_run_id = receipt.snapshot_run_id
                    load_result = load_results[snapshot_run_id]
                    publish_metrics[snapshot_run_id] = {
                        "expected_rows": int(
                            load_result.get(
                                "expected_rows",
                                load_result.get("inserted", 0),
                            )
                        ),
                        "actual_rows": int(verified_loaded[snapshot_run_id]),
                        "expected_raw_objects": int(
                            load_result.get(
                                "page_count",
                                raw_object_counts[snapshot_run_id],
                            )
                        ),
                        "actual_raw_objects": len(
                            list(load_result.get("raw_object_keys") or [])
                        ),
                        "is_publishable": bool(
                            load_result.get("is_publishable", True)
                        ),
                    }

            self._manifest_publish_many(
                [
                    (runs[receipt.snapshot_run_id], publish_metrics[receipt.snapshot_run_id])
                    for receipt in active_receipts
                ]
            )
            snapshot_run_ids: list[str] = []
            asset_metadata: list[dict[str, object]] = []
            for receipt in active_receipts:
                metrics = publish_metrics[receipt.snapshot_run_id]
                verified_rows = int(metrics["actual_rows"])
                page_count = int(metrics["expected_raw_objects"])
                self._record_terminal_slot_outcome(
                    receipt,
                    materializer_dag_id=materializer_dag_id,
                    verified_rows=verified_rows,
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
                if bool(metrics["is_publishable"]):
                    asset_metadata.append(
                        bronze_asset_metadata(receipt, row_count=verified_rows)
                    )
            return MaterializationBatch(
                snapshot_run_ids=tuple(snapshot_run_ids),
                asset_metadata=tuple(asset_metadata),
                row_count=sum(
                    int(publish_metrics[receipt.snapshot_run_id]["actual_rows"])
                    for receipt in active_receipts
                ),
            )
        except Exception as error:
            try:
                self._manifest_fail_many(
                    [
                        (
                            runs[receipt.snapshot_run_id],
                            raw_object_counts[receipt.snapshot_run_id],
                        )
                        for receipt in active_receipts
                    ],
                    error=error,
                )
            except Exception as manifest_error:  # preserve materialization cause
                LOGGER.warning(
                    "Traffic materialization FAILED manifest write failed: %s",
                    type(manifest_error).__name__,
                )
            raise

    def _record_terminal_slot_outcome(
        self,
        receipt: LandedSnapshot,
        *,
        materializer_dag_id: str,
        verified_rows: int,
    ) -> None:
        if self._slot_receipts is None:
            return
        assert self._slot_for_logical_date is not None
        result_code = str(receipt.raw_result.get("result_code") or "")
        if result_code != "INFO-000":
            raise ValueError(
                "Traffic slot outcome requires verified INFO-000 source evidence"
            )
        expected_rows = int(receipt.raw_result.get("expected_rows") or 0)
        collection_state = (
            "source_empty_valid"
            if expected_rows == 0 and verified_rows == 0
            else "observed"
        )
        slot = self._slot_for_logical_date(receipt.logical_date)
        self._slot_receipts.record_outcome(
            CollectionOutcome.create(
                expected_slot_id=slot.expected_slot_id,
                collection_state=collection_state,
                recovery_state="not_required",
                recovery_class="none",
                event_at=receipt.snapshot_at,
                dag_id=materializer_dag_id,
                dag_run_id=receipt.snapshot_run_id,
                task_id=MATERIALIZER_TASK_ID,
                raw_manifest_key=(
                    str(receipt.raw_result["manifest_key"])
                    if receipt.raw_result.get("manifest_key")
                    else None
                ),
                raw_object_count=len(_raw_objects(receipt.raw_result)),
                row_count=verified_rows,
                source_result_code=result_code,
            )
        )

    def _load_receipts(
        self,
        raw_results: Mapping[str, dict[str, object]],
    ) -> dict[str, dict[str, object]]:
        if self._load_many is not None:
            return dict(self._load_many(raw_results))
        return {
            snapshot_run_id: self._load(raw_result, snapshot_run_id)
            for snapshot_run_id, raw_result in raw_results.items()
        }

    def _verify_loaded_receipts(
        self,
        load_results: Mapping[str, dict[str, object]],
        receipts: list[LandedSnapshot],
    ) -> dict[str, int]:
        if self._verify_many is not None:
            return {
                key: int(value)
                for key, value in self._verify_many(load_results, receipts).items()
            }
        return {
            snapshot_run_id: self._verify(load_result, snapshot_run_id)
            for snapshot_run_id, load_result in load_results.items()
        }

    def _manifest_start_many(
        self,
        entries: list[tuple[TrafficRun, int]],
    ) -> None:
        method = getattr(self._manifest, "start_many", None)
        if callable(method):
            method(entries)
            return
        for run, expected_raw_objects in entries:
            self._manifest.start(
                run,
                expected_raw_objects=expected_raw_objects,
            )

    def _manifest_publish_many(
        self,
        entries: list[tuple[TrafficRun, dict[str, object]]],
    ) -> None:
        method = getattr(self._manifest, "publish_many", None)
        if callable(method):
            method(entries)
            return
        for run, metrics in entries:
            self._manifest.publish(run, **metrics)

    def _manifest_fail_many(
        self,
        entries: list[tuple[TrafficRun, int]],
        *,
        error: BaseException,
    ) -> None:
        method = getattr(self._manifest, "fail_many", None)
        if callable(method):
            method(entries, task_id=MATERIALIZER_TASK_ID, error=error)
            return
        for run, expected_raw_objects in entries:
            self._manifest.fail(
                run,
                task_id=MATERIALIZER_TASK_ID,
                error=error,
                expected_raw_objects=expected_raw_objects,
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
