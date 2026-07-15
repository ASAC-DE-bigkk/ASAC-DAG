"""Durable R2 receipts between Traffic raw landing and Bronze materialization."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from urllib.parse import quote
from zoneinfo import ZoneInfo


RECEIPT_VERSION = 1
RECEIPT_PREFIX = "traffic-snapshot-receipts"
INCIDENT_SOURCE_ID = "seoul_traffic_incident"
KST = ZoneInfo("Asia/Seoul")


class SnapshotReceiptContractError(ValueError):
    """A receipt document is malformed."""


class SnapshotReceiptConflict(SnapshotReceiptContractError):
    """A retry attempted to overwrite a snapshot with different evidence."""


class JsonStorage(Protocol):
    def write_json(self, key: str, value: object) -> None: ...

    def read_json(self, key: str) -> object: ...

    def exists(self, key: str) -> bool: ...

    def list_keys(self, prefix: str) -> list[str]: ...

    def delete(self, key: str) -> None: ...


def _safe_segment(value: object) -> str:
    return quote(str(value or "unknown"), safe="-._~")


def _timestamp(value: object, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise SnapshotReceiptContractError(f"receipt {field} is malformed") from exc
    if parsed.tzinfo is None:
        raise SnapshotReceiptContractError(f"receipt {field} must include timezone")
    return parsed


def _text(document: Mapping[str, object], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise SnapshotReceiptContractError(
            f"receipt {field} must be a non-empty string"
        )
    return value


@dataclass(frozen=True)
class LandedSnapshot:
    source_id: str
    producer_dag_id: str
    snapshot_run_id: str
    logical_date: str
    snapshot_at: str
    raw_result: dict[str, object]
    event_at: str

    def __post_init__(self) -> None:
        for field in (
            "source_id",
            "producer_dag_id",
            "snapshot_run_id",
            "logical_date",
            "snapshot_at",
            "event_at",
        ):
            if not str(getattr(self, field) or ""):
                raise SnapshotReceiptContractError(f"receipt {field} is required")
        _timestamp(self.logical_date, field="logical_date")
        _timestamp(self.snapshot_at, field="snapshot_at")
        _timestamp(self.event_at, field="event_at")
        if not isinstance(self.raw_result, dict):
            raise SnapshotReceiptContractError("receipt raw_result must be an object")
        raw_objects = self.raw_result.get("raw_objects")
        if not isinstance(raw_objects, list) or not raw_objects:
            raise SnapshotReceiptContractError(
                "receipt raw_result requires at least one raw object"
            )
        if self.raw_result.get("is_publishable") is not True:
            raise SnapshotReceiptContractError(
                "scheduled Traffic receipt must be publishable"
            )

    def to_document(self) -> dict[str, object]:
        return {
            "version": RECEIPT_VERSION,
            "status": "LANDED",
            "source_id": self.source_id,
            "producer_dag_id": self.producer_dag_id,
            "snapshot_run_id": self.snapshot_run_id,
            "logical_date": self.logical_date,
            "snapshot_at": self.snapshot_at,
            "raw_result": self.raw_result,
            "event_at": self.event_at,
        }

    @classmethod
    def from_document(cls, value: object) -> "LandedSnapshot":
        if not isinstance(value, Mapping):
            raise SnapshotReceiptContractError("LANDED receipt root must be an object")
        if value.get("version") != RECEIPT_VERSION or value.get("status") != "LANDED":
            raise SnapshotReceiptContractError("unsupported LANDED receipt version")
        raw_result = value.get("raw_result")
        if not isinstance(raw_result, Mapping):
            raise SnapshotReceiptContractError("receipt raw_result must be an object")
        return cls(
            source_id=_text(value, "source_id"),
            producer_dag_id=_text(value, "producer_dag_id"),
            snapshot_run_id=_text(value, "snapshot_run_id"),
            logical_date=_text(value, "logical_date"),
            snapshot_at=_text(value, "snapshot_at"),
            raw_result=dict(raw_result),
            event_at=_text(value, "event_at"),
        )


@dataclass(frozen=True)
class MaterializedSnapshot:
    source_id: str
    snapshot_run_id: str
    snapshot_at: str
    materializer_dag_id: str
    materializer_run_id: str
    row_count: int
    raw_object_count: int
    event_at: str

    def __post_init__(self) -> None:
        for field in (
            "source_id",
            "snapshot_run_id",
            "snapshot_at",
            "materializer_dag_id",
            "materializer_run_id",
            "event_at",
        ):
            if not str(getattr(self, field) or ""):
                raise SnapshotReceiptContractError(f"receipt {field} is required")
        _timestamp(self.snapshot_at, field="snapshot_at")
        _timestamp(self.event_at, field="event_at")
        if self.row_count < 0 or self.raw_object_count < 1:
            raise SnapshotReceiptContractError("materialized receipt counts are invalid")

    def to_document(self) -> dict[str, object]:
        return {
            "version": RECEIPT_VERSION,
            "status": "MATERIALIZED",
            "source_id": self.source_id,
            "snapshot_run_id": self.snapshot_run_id,
            "snapshot_at": self.snapshot_at,
            "materializer_dag_id": self.materializer_dag_id,
            "materializer_run_id": self.materializer_run_id,
            "row_count": self.row_count,
            "raw_object_count": self.raw_object_count,
            "event_at": self.event_at,
        }

    @classmethod
    def from_document(cls, value: object) -> "MaterializedSnapshot":
        if not isinstance(value, Mapping):
            raise SnapshotReceiptContractError(
                "MATERIALIZED receipt root must be an object"
            )
        if (
            value.get("version") != RECEIPT_VERSION
            or value.get("status") != "MATERIALIZED"
        ):
            raise SnapshotReceiptContractError(
                "unsupported MATERIALIZED receipt version"
            )
        try:
            row_count = int(value["row_count"])
            raw_object_count = int(value["raw_object_count"])
        except (KeyError, TypeError, ValueError) as exc:
            raise SnapshotReceiptContractError(
                "materialized receipt counts are malformed"
            ) from exc
        return cls(
            source_id=_text(value, "source_id"),
            snapshot_run_id=_text(value, "snapshot_run_id"),
            snapshot_at=_text(value, "snapshot_at"),
            materializer_dag_id=_text(value, "materializer_dag_id"),
            materializer_run_id=_text(value, "materializer_run_id"),
            row_count=row_count,
            raw_object_count=raw_object_count,
            event_at=_text(value, "event_at"),
        )


class TrafficSnapshotReceipts:
    """Own terminal receipts and a compact pending index for one Traffic source."""

    def __init__(
        self,
        storage: JsonStorage,
        *,
        source_id: str = INCIDENT_SOURCE_ID,
    ) -> None:
        self._storage = storage
        self._source_id = source_id
        self._source_prefix = (
            f"{RECEIPT_PREFIX}/source_id={_safe_segment(source_id)}"
        )

    def pending_key(self, snapshot_run_id: str) -> str:
        return f"{self._source_prefix}/pending/{_safe_segment(snapshot_run_id)}.json"

    def landed_key(self, receipt: LandedSnapshot) -> str:
        return self._terminal_key(
            snapshot_at=receipt.snapshot_at,
            snapshot_run_id=receipt.snapshot_run_id,
            status="LANDED",
        )

    def materialized_key(self, receipt: MaterializedSnapshot | LandedSnapshot) -> str:
        return self._terminal_key(
            snapshot_at=receipt.snapshot_at,
            snapshot_run_id=receipt.snapshot_run_id,
            status="MATERIALIZED",
        )

    def _terminal_key(
        self,
        *,
        snapshot_at: str,
        snapshot_run_id: str,
        status: str,
    ) -> str:
        snapshot_date = _timestamp(snapshot_at, field="snapshot_at").astimezone(KST).date()
        return (
            f"{self._source_prefix}/snapshot_date={snapshot_date.isoformat()}/"
            f"run_id={_safe_segment(snapshot_run_id)}/{status}.json"
        )

    @staticmethod
    def _same_landed(left: LandedSnapshot, right: LandedSnapshot) -> bool:
        return (
            left.source_id,
            left.producer_dag_id,
            left.snapshot_run_id,
            left.logical_date,
            left.snapshot_at,
            left.raw_result,
        ) == (
            right.source_id,
            right.producer_dag_id,
            right.snapshot_run_id,
            right.logical_date,
            right.snapshot_at,
            right.raw_result,
        )

    @staticmethod
    def _same_materialized(
        left: MaterializedSnapshot,
        right: MaterializedSnapshot,
    ) -> bool:
        return (
            left.source_id,
            left.snapshot_run_id,
            left.snapshot_at,
            left.materializer_dag_id,
            left.row_count,
            left.raw_object_count,
        ) == (
            right.source_id,
            right.snapshot_run_id,
            right.snapshot_at,
            right.materializer_dag_id,
            right.row_count,
            right.raw_object_count,
        )

    def record_landed(self, receipt: LandedSnapshot) -> str:
        self._require_source(receipt.source_id)
        key = self.landed_key(receipt)
        if self._storage.exists(key):
            existing = LandedSnapshot.from_document(self._storage.read_json(key))
            if not self._same_landed(existing, receipt):
                raise SnapshotReceiptConflict(
                    f"divergent LANDED receipt: {receipt.snapshot_run_id}"
                )
        else:
            self._storage.write_json(key, receipt.to_document())

        if not self._storage.exists(self.materialized_key(receipt)):
            self._storage.write_json(
                self.pending_key(receipt.snapshot_run_id),
                receipt.to_document(),
            )
        return key

    def record_materialized(self, receipt: MaterializedSnapshot) -> str:
        self._require_source(receipt.source_id)
        key = self.materialized_key(receipt)
        if self._storage.exists(key):
            existing = MaterializedSnapshot.from_document(self._storage.read_json(key))
            if not self._same_materialized(existing, receipt):
                raise SnapshotReceiptConflict(
                    f"divergent MATERIALIZED receipt: {receipt.snapshot_run_id}"
                )
        else:
            self._storage.write_json(key, receipt.to_document())
        return key

    def acknowledge_materialized(self, snapshot_run_id: str) -> bool:
        """Delete pending only after Airflow accepted the success Asset event."""

        pending_key = self.pending_key(snapshot_run_id)
        if not self._storage.exists(pending_key):
            return False
        landed = LandedSnapshot.from_document(self._storage.read_json(pending_key))
        self._require_source(landed.source_id)
        materialized_key = self.materialized_key(landed)
        if not self._storage.exists(materialized_key):
            raise SnapshotReceiptContractError(
                f"cannot acknowledge unmaterialized snapshot: {snapshot_run_id}"
            )
        materialized = MaterializedSnapshot.from_document(
            self._storage.read_json(materialized_key)
        )
        if (
            materialized.source_id != landed.source_id
            or materialized.snapshot_run_id != landed.snapshot_run_id
            or materialized.snapshot_at != landed.snapshot_at
        ):
            raise SnapshotReceiptConflict(
                f"materialized acknowledgement identity mismatch: {snapshot_run_id}"
            )
        self._storage.delete(pending_key)
        return True

    def pending(self, *, limit: int | None = None) -> list[LandedSnapshot]:
        prefix = f"{self._source_prefix}/pending/"
        receipts: list[LandedSnapshot] = []
        for key in self._storage.list_keys(prefix):
            if not key.endswith(".json"):
                continue
            receipt = LandedSnapshot.from_document(self._storage.read_json(key))
            self._require_source(receipt.source_id)
            receipts.append(receipt)
        receipts.sort(
            key=lambda item: (
                _timestamp(item.snapshot_at, field="snapshot_at"),
                item.snapshot_run_id,
            )
        )
        if limit is None:
            return receipts
        if limit < 1:
            raise ValueError("pending receipt limit must be positive")
        return receipts[:limit]

    def pending_summary(self, detected_at: datetime) -> dict[str, object]:
        pending = self.pending()
        if not pending:
            return {
                "count": 0,
                "oldest_snapshot_at": None,
                "oldest_age_minutes": None,
            }
        detected = detected_at
        if detected.tzinfo is None:
            detected = detected.replace(tzinfo=timezone.utc)
        oldest = _timestamp(pending[0].snapshot_at, field="snapshot_at")
        age_minutes = max(
            0,
            int((detected.astimezone(timezone.utc) - oldest.astimezone(timezone.utc)).total_seconds() // 60),
        )
        return {
            "count": len(pending),
            "oldest_snapshot_at": pending[0].snapshot_at,
            "oldest_age_minutes": age_minutes,
        }

    def _require_source(self, source_id: str) -> None:
        if source_id != self._source_id:
            raise SnapshotReceiptContractError(
                f"receipt source mismatch: expected={self._source_id}, actual={source_id}"
            )


__all__ = [
    "INCIDENT_SOURCE_ID",
    "LandedSnapshot",
    "MaterializedSnapshot",
    "RECEIPT_PREFIX",
    "SnapshotReceiptConflict",
    "SnapshotReceiptContractError",
    "TrafficSnapshotReceipts",
]
