"""Repair deterministic legacy expected receipts before materialization.

The collection-slot contract makes an expected receipt the source of truth. A
rollout can nevertheless encounter terminal event receipts written before the
expected-receipt path was deployed. This module only repairs such rows when a
domain adapter can reconstruct the exact ``ExpectedSlot`` and therefore the
same ``expected_slot_id``. Unknown or ambiguous events fail closed.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

from common.collection_slots.contract import ExpectedSlot
from common.collection_slots.materializer import (
    EVENT_RECEIPTS_PREFIX,
    EXPECTED_RECEIPTS_PREFIX,
    MaterializationError,
    _document,
    _event,
)
from common.collection_slots.receipts import CollectionSlotReceipts


class ReconciliationStorage(Protocol):
    def list_keys(self, prefix: str) -> list[str]: ...

    def read_json(self, key: str) -> object: ...

    def write_json_if_absent(self, key: str, value: object) -> bool: ...


class ReconciliationError(ValueError):
    """A legacy event cannot be mapped to one immutable expected slot."""


ExpectedSlotResolver = Callable[[Mapping[str, object]], ExpectedSlot | None]


@dataclass(frozen=True)
class ReconciliationResult:
    scanned_events: int
    existing_expected: int
    repaired_expected: int

    def as_dict(self) -> dict[str, int]:
        return {
            "scanned_events": self.scanned_events,
            "existing_expected": self.existing_expected,
            "repaired_expected": self.repaired_expected,
        }


def _receipt_identity(key: str) -> str:
    filename = key.rsplit("/", 1)[-1]
    if not filename.endswith(".json"):
        raise ReconciliationError(f"receipt key must end with .json: {key}")
    identity = filename[:-5]
    if not identity:
        raise ReconciliationError(f"receipt key has no identity: {key}")
    return identity


class CollectionSlotReceiptReconciler:
    """Repair only resolvable event→expected receipt gaps in R2."""

    def __init__(
        self,
        storage: ReconciliationStorage,
        resolver: ExpectedSlotResolver,
    ) -> None:
        self._storage = storage
        self._resolver = resolver
        self._receipts = CollectionSlotReceipts(storage)

    def run(self) -> ReconciliationResult:
        expected_ids = {
            _receipt_identity(key)
            for key in self._storage.list_keys(EXPECTED_RECEIPTS_PREFIX)
            if key.endswith(".json")
        }
        scanned_events = 0
        existing_expected = 0
        repaired_expected = 0

        for key in self._storage.list_keys(EVENT_RECEIPTS_PREFIX):
            if not key.endswith(".json"):
                continue
            scanned_events += 1
            expected_slot_id = self._event_expected_slot_id(key)
            if expected_slot_id in expected_ids:
                existing_expected += 1
                continue

            try:
                event = _event(_document(self._storage.read_json(key), key=key), key=key)
            except MaterializationError as exc:
                raise ReconciliationError(str(exc)) from exc
            if str(event["expected_slot_id"]) != expected_slot_id:
                raise ReconciliationError(f"event key identity mismatch: {key}")

            slot = self._resolver(event)
            if slot is None:
                raise ReconciliationError(
                    "unresolvable legacy event expected_slot_id: "
                    + expected_slot_id
                )
            if slot.expected_slot_id != expected_slot_id:
                raise ReconciliationError(
                    "legacy resolver identity mismatch: "
                    f"{expected_slot_id} != {slot.expected_slot_id}"
                )
            self._receipts.record_expected(slot)
            expected_ids.add(expected_slot_id)
            repaired_expected += 1

        return ReconciliationResult(
            scanned_events=scanned_events,
            existing_expected=existing_expected,
            repaired_expected=repaired_expected,
        )

    @staticmethod
    def _event_expected_slot_id(key: str) -> str:
        parts = key.split("/")
        try:
            partition = next(
                part for part in parts if part.startswith("expected_slot_id=")
            )
        except StopIteration as exc:
            raise ReconciliationError(
                f"event receipt key has no expected_slot_id partition: {key}"
            ) from exc
        expected_slot_id = partition.split("=", 1)[1]
        if not expected_slot_id:
            raise ReconciliationError(f"event receipt key has empty slot id: {key}")
        return expected_slot_id


__all__ = [
    "CollectionSlotReceiptReconciler",
    "ExpectedSlotResolver",
    "ReconciliationError",
    "ReconciliationResult",
]
