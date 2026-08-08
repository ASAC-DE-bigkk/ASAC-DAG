"""Pure expected-slot plan for Traffic Incident collection."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from common.collection_slots.contract import ExpectedSlot


INCIDENT_SOURCE_ID = "seoul_traffic_incident"
INCIDENT_COLLECTION_CONTRACT_ID = "traffic.incident.v1"
INCIDENT_SCHEDULE_VERSION = "traffic.incident.5m.v1"
INCIDENT_RECOVERY_BOUNDARY = "raw_retention_not_declared"


def _as_utc_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("logical_date must be an ISO timestamp") from exc
    else:
        raise ValueError("logical_date must be a datetime or ISO timestamp")
    if parsed.tzinfo is None:
        raise ValueError("logical_date must include timezone")
    return parsed.astimezone(timezone.utc)


def floor_to_five_minutes(value: datetime | str) -> datetime:
    """Return the UTC five-minute collection boundary containing ``value``."""
    utc_value = _as_utc_datetime(value)
    return utc_value.replace(
        minute=utc_value.minute - (utc_value.minute % 5),
        second=0,
        microsecond=0,
    )


def traffic_incident_slot(logical_date: datetime | str) -> ExpectedSlot:
    """Build one scheduled Traffic Incident expected slot without runtime I/O."""
    slot_at = floor_to_five_minutes(logical_date)
    return ExpectedSlot.create(
        contract_version="v1",
        domain="traffic",
        collection_contract_id=INCIDENT_COLLECTION_CONTRACT_ID,
        source_id=INCIDENT_SOURCE_ID,
        collection_slot_at=slot_at,
        scheduled_at=slot_at,
        deadline_at=slot_at + timedelta(minutes=15),
        grain={"source_id": INCIDENT_SOURCE_ID},
        schedule_version=INCIDENT_SCHEDULE_VERSION,
        is_scheduled=True,
        recovery_boundary_type="raw_retention",
        recovery_boundary=INCIDENT_RECOVERY_BOUNDARY,
        declared_at=slot_at,
        declared_by="traffic_incident_landing",
    )


__all__ = [
    "INCIDENT_COLLECTION_CONTRACT_ID",
    "INCIDENT_RECOVERY_BOUNDARY",
    "INCIDENT_SCHEDULE_VERSION",
    "INCIDENT_SOURCE_ID",
    "floor_to_five_minutes",
    "traffic_incident_slot",
]
