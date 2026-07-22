"""Run-grain evidence contract for the seven-day delivery reliability pilot."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, Mapping


class DeliveryState(str, Enum):
    DELIVERED = "DELIVERED"
    DELIVERED_ZERO_ROW = "DELIVERED_ZERO_ROW"
    SLA_MISSED = "SLA_MISSED"
    PARTIAL = "PARTIAL"
    API_FAILURE = "API_FAILURE"
    SOURCE_FAILURE = "SOURCE_FAILURE"
    BRONZE_FAILURE = "BRONZE_FAILURE"
    NOT_PUBLISHABLE = "NOT_PUBLISHABLE"
    CONTRACT_FAILURE = "CONTRACT_FAILURE"
    TRANSFORM_FAILURE = "TRANSFORM_FAILURE"
    GOLD_QUERY_FAILURE = "GOLD_QUERY_FAILURE"
    NOT_AVAILABLE = "NOT_AVAILABLE"


ALLOWED_STATUSES = {
    "bronze_status": frozenset({"STARTED", "SUCCESS", "FAILED"}),
    "completeness_status": frozenset({"COMPLETE", "PARTIAL", "UNKNOWN"}),
    "source_status": frozenset(
        {"SUCCESS", "ZERO_ROW", "API_FAILURE", "SOURCE_FAILURE"}
    ),
    "transform_status": frozenset({"SUCCESS", "FAILED", "CONTRACT_FAILURE"}),
    "gold_query_status": frozenset({"SUCCESS", "FAILED"}),
}


def _required_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _status(value: Any, field: str) -> str | None:
    status = _optional_text(value)
    if status is not None and status not in ALLOWED_STATUSES[field]:
        allowed = ", ".join(sorted(ALLOWED_STATUSES[field]))
        raise ValueError(f"{field} must be one of {allowed} or null")
    return status


def _utc_datetime(value: Any, field: str, *, required: bool) -> datetime | None:
    if value is None:
        if required:
            raise ValueError(f"{field} is required")
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be an ISO-8601 timestamp")
    if value.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return value.astimezone(timezone.utc)


def _optional_non_negative_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer or null")
    return value


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


@dataclass(frozen=True)
class DeliveryEvidence:
    """One scheduled Weather/Traffic run and its end-to-end delivery evidence."""

    domain: str
    scheduled_run_id: str
    scheduled_at: datetime
    bronze_status: str | None
    is_publishable: bool | None
    completeness_status: str | None
    source_status: str | None
    transform_status: str | None
    gold_query_status: str | None
    gold_available_at: datetime | None
    gold_row_count: int | None
    sla_minutes: int
    detected_at: datetime | None = None
    api_failure_type: str | None = None
    retry_count: int | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "DeliveryEvidence":
        domain = _required_text(value.get("domain"), "domain").lower()
        if domain not in {"weather", "traffic"}:
            raise ValueError("domain must be weather or traffic")
        is_publishable = value.get("is_publishable")
        if is_publishable is not None and not isinstance(is_publishable, bool):
            raise ValueError("is_publishable must be boolean or null")
        row = cls(
            domain=domain,
            scheduled_run_id=_required_text(
                value.get("scheduled_run_id"), "scheduled_run_id"
            ),
            scheduled_at=_utc_datetime(
                value.get("scheduled_at"), "scheduled_at", required=True
            ),
            bronze_status=_status(value.get("bronze_status"), "bronze_status"),
            is_publishable=is_publishable,
            completeness_status=_status(
                value.get("completeness_status"), "completeness_status"
            ),
            source_status=_status(value.get("source_status"), "source_status"),
            transform_status=_status(
                value.get("transform_status"), "transform_status"
            ),
            gold_query_status=_status(
                value.get("gold_query_status"), "gold_query_status"
            ),
            gold_available_at=_utc_datetime(
                value.get("gold_available_at"), "gold_available_at", required=False
            ),
            gold_row_count=_optional_non_negative_int(
                value.get("gold_row_count"), "gold_row_count"
            ),
            sla_minutes=_positive_int(value.get("sla_minutes"), "sla_minutes"),
            detected_at=_utc_datetime(
                value.get("detected_at"), "detected_at", required=False
            ),
            api_failure_type=_optional_text(value.get("api_failure_type")),
            retry_count=_optional_non_negative_int(
                value.get("retry_count"), "retry_count"
            ),
        )
        if (
            row.source_status == "ZERO_ROW"
            and row.gold_row_count is not None
            and row.gold_row_count != 0
        ):
            raise ValueError("ZERO_ROW requires gold_row_count=0")
        if row.is_publishable is True and (
            row.bronze_status != "SUCCESS"
            or row.completeness_status != "COMPLETE"
            or row.source_status in {"API_FAILURE", "SOURCE_FAILURE"}
        ):
            raise ValueError("partial or failed source evidence cannot be publishable")
        if row.detected_at is not None and row.detected_at < row.scheduled_at:
            raise ValueError("detected_at cannot precede scheduled_at")
        if row.gold_available_at is not None and row.gold_available_at < row.scheduled_at:
            raise ValueError("gold_available_at cannot precede scheduled_at")
        return row

    @property
    def grain(self) -> tuple[str, str]:
        return self.domain, self.scheduled_run_id

    @property
    def bronze_supplied(self) -> bool | None:
        if self.bronze_status is None or self.is_publishable is None:
            return None
        return self.bronze_status == "SUCCESS" and self.is_publishable

    @property
    def delivery_latency_minutes(self) -> float | None:
        if self.gold_available_at is None:
            return None
        return (self.gold_available_at - self.scheduled_at).total_seconds() / 60

    @property
    def gold_delivered(self) -> bool | None:
        if (
            self.source_status in {"API_FAILURE", "SOURCE_FAILURE"}
            or self.completeness_status == "PARTIAL"
            or self.bronze_status == "FAILED"
            or (self.bronze_status == "SUCCESS" and self.is_publishable is False)
            or self.transform_status in {"FAILED", "CONTRACT_FAILURE"}
            or self.gold_query_status == "FAILED"
        ):
            return False
        required = (
            self.bronze_status,
            self.is_publishable,
            self.completeness_status,
            self.source_status,
            self.transform_status,
            self.gold_query_status,
            self.gold_available_at,
            self.gold_row_count,
        )
        if any(item is None for item in required):
            return None
        return (
            self.bronze_supplied is True
            and self.completeness_status == "COMPLETE"
            and self.source_status in {"SUCCESS", "ZERO_ROW"}
            and self.transform_status == "SUCCESS"
            and self.gold_query_status == "SUCCESS"
        )

    @property
    def sla_met(self) -> bool | None:
        if self.gold_delivered is not True:
            return None
        latency = self.delivery_latency_minutes
        return latency is not None and latency <= self.sla_minutes

    @property
    def state(self) -> DeliveryState:
        if self.source_status == "API_FAILURE":
            return DeliveryState.API_FAILURE
        if self.source_status == "SOURCE_FAILURE":
            return DeliveryState.SOURCE_FAILURE
        if self.completeness_status == "PARTIAL":
            return DeliveryState.PARTIAL
        if self.bronze_status == "FAILED":
            return DeliveryState.BRONZE_FAILURE
        if self.bronze_status == "SUCCESS" and self.is_publishable is False:
            return DeliveryState.NOT_PUBLISHABLE
        if self.transform_status == "CONTRACT_FAILURE":
            return DeliveryState.CONTRACT_FAILURE
        if self.transform_status == "FAILED":
            return DeliveryState.TRANSFORM_FAILURE
        if self.gold_query_status == "FAILED":
            return DeliveryState.GOLD_QUERY_FAILURE
        if self.gold_delivered is None:
            return DeliveryState.NOT_AVAILABLE
        if self.gold_delivered is False:
            return DeliveryState.NOT_AVAILABLE
        if self.sla_met is False:
            return DeliveryState.SLA_MISSED
        if self.source_status == "ZERO_ROW":
            return DeliveryState.DELIVERED_ZERO_ROW
        return DeliveryState.DELIVERED


def ensure_unique_grain(
    rows: Iterable[DeliveryEvidence],
) -> tuple[DeliveryEvidence, ...]:
    ordered = tuple(
        sorted(
            rows,
            key=lambda row: (row.domain, row.scheduled_at, row.scheduled_run_id),
        )
    )
    seen: set[tuple[str, str]] = set()
    for row in ordered:
        if row.grain in seen:
            raise ValueError(
                "duplicate delivery evidence grain: " + "/".join(row.grain)
            )
        seen.add(row.grain)
    return ordered
