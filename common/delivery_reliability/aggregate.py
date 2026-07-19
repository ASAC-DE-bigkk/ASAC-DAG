"""Deterministic aggregation for a seven-day delivery reliability pilot."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any, Iterable

from .contract import DeliveryEvidence, DeliveryState, ensure_unique_grain


FAILURE_STATES = frozenset(
    {
        DeliveryState.PARTIAL,
        DeliveryState.API_FAILURE,
        DeliveryState.SOURCE_FAILURE,
        DeliveryState.BRONZE_FAILURE,
        DeliveryState.NOT_PUBLISHABLE,
        DeliveryState.CONTRACT_FAILURE,
        DeliveryState.TRANSFORM_FAILURE,
        DeliveryState.GOLD_QUERY_FAILURE,
    }
)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _freshness(rows: tuple[DeliveryEvidence, ...]) -> dict[str, Any]:
    samples = sorted(
        row.delivery_latency_minutes
        for row in rows
        if row.gold_delivered is True and row.delivery_latency_minutes is not None
    )
    return {
        "sample_count": len(samples),
        "p50": float(median(samples)) if samples else None,
        "max": float(max(samples)) if samples else None,
    }


def _recovery(rows: tuple[DeliveryEvidence, ...]) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    unrecovered = 0
    grouped: dict[str, list[DeliveryEvidence]] = {}
    for row in rows:
        grouped.setdefault(row.domain, []).append(row)
    for domain_rows in grouped.values():
        ordered = tuple(
            sorted(
                domain_rows,
                key=lambda row: (row.scheduled_at, row.scheduled_run_id),
            )
        )
        for index, failed in enumerate(ordered):
            if failed.state not in FAILURE_STATES:
                continue
            recovery = next(
                (
                    candidate
                    for candidate in ordered[index + 1 :]
                    if candidate.gold_delivered is True
                    and candidate.gold_available_at is not None
                ),
                None,
            )
            if recovery is None:
                unrecovered += 1
                continue
            detected_at = failed.detected_at or failed.scheduled_at
            minutes = (
                recovery.gold_available_at - detected_at
            ).total_seconds() / 60
            if minutes < 0:
                raise ValueError(
                    "recovery cannot precede failure detection: "
                    f"{failed.domain}/{failed.scheduled_run_id}"
                )
            events.append(
                {
                    "failed_run_id": failed.scheduled_run_id,
                    "recovered_by_run_id": recovery.scheduled_run_id,
                    "minutes": float(minutes),
                }
            )
    return {
        "recovered_failures": len(events),
        "unrecovered_failures": unrecovered,
        "mttr_minutes": (
            float(sum(event["minutes"] for event in events) / len(events))
            if events
            else None
        ),
        "events": events,
    }


def _summary(rows: tuple[DeliveryEvidence, ...]) -> dict[str, Any]:
    supply_evaluable = [row for row in rows if row.bronze_supplied is not None]
    delivery_evaluable = [row for row in rows if row.gold_delivered is not None]
    sla_evaluable = [row for row in rows if row.gold_delivered is True]
    completeness_evaluable = [
        row for row in rows if row.completeness_status is not None
    ]
    supplied = sum(row.bronze_supplied is True for row in supply_evaluable)
    delivered = sum(row.gold_delivered is True for row in delivery_evaluable)
    within_sla = sum(row.sla_met is True for row in sla_evaluable)
    complete = sum(
        row.completeness_status == "COMPLETE" for row in completeness_evaluable
    )
    state_counts = Counter(row.state.value for row in rows)
    api_failure_profile = Counter(
        row.api_failure_type or "UNCLASSIFIED"
        for row in rows
        if row.state is DeliveryState.API_FAILURE
    )
    retries = [row.retry_count for row in rows if row.retry_count is not None]
    return {
        "scheduled_runs": len(rows),
        "supply": {
            "successful_runs": supplied,
            "evaluable_runs": len(supply_evaluable),
            "rate": _rate(supplied, len(supply_evaluable)),
        },
        "gold_delivery": {
            "delivered_runs": delivered,
            "evaluable_runs": len(delivery_evaluable),
            "rate": _rate(delivered, len(delivery_evaluable)),
        },
        "sla_delivery": {
            "within_sla_runs": within_sla,
            "evaluable_runs": len(sla_evaluable),
            "rate": _rate(within_sla, len(sla_evaluable)),
        },
        "completeness": {
            "complete_runs": complete,
            "evaluable_runs": len(completeness_evaluable),
            "rate": _rate(complete, len(completeness_evaluable)),
        },
        "not_available_runs": state_counts.get(DeliveryState.NOT_AVAILABLE.value, 0),
        "state_counts": dict(sorted(state_counts.items())),
        "api_failure_profile": dict(sorted(api_failure_profile.items())),
        "retry_count": sum(retries) if retries else None,
        "freshness_minutes": _freshness(rows),
        "recovery": _recovery(rows),
    }


def _run_record(row: DeliveryEvidence) -> dict[str, Any]:
    return {
        "domain": row.domain,
        "scheduled_run_id": row.scheduled_run_id,
        "scheduled_at": _iso(row.scheduled_at),
        "detected_at": _iso(row.detected_at),
        "state": row.state.value,
        "bronze_supplied": row.bronze_supplied,
        "gold_delivered": row.gold_delivered,
        "sla_met": row.sla_met,
        "delivery_latency_minutes": row.delivery_latency_minutes,
        "completeness_status": row.completeness_status,
        "source_status": row.source_status,
        "transform_status": row.transform_status,
        "gold_query_status": row.gold_query_status,
        "gold_available_at": _iso(row.gold_available_at),
        "gold_row_count": row.gold_row_count,
        "api_failure_type": row.api_failure_type,
        "retry_count": row.retry_count,
    }


def build_pilot_report(
    rows: Iterable[DeliveryEvidence],
    *,
    observed_at: datetime,
    lookback_days: int = 7,
) -> dict[str, Any]:
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must include a timezone")
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    observed_utc = observed_at.astimezone(timezone.utc)
    window_start = observed_utc - timedelta(days=lookback_days)
    unique = ensure_unique_grain(rows)
    included = tuple(
        row for row in unique if window_start <= row.scheduled_at <= observed_utc
    )
    grouped: dict[str, list[DeliveryEvidence]] = {}
    for row in included:
        grouped.setdefault(row.domain, []).append(row)
    domains = {
        domain: _summary(tuple(domain_rows))
        for domain, domain_rows in sorted(grouped.items())
    }
    return {
        "contract_version": "delivery-reliability-pilot-v1",
        "observed_at": _iso(observed_utc),
        "window_start": _iso(window_start),
        "lookback_days": lookback_days,
        "excluded_outside_window": len(unique) - len(included),
        "domains": domains,
        "overall": _summary(included),
        "runs": [_run_record(row) for row in included],
    }
