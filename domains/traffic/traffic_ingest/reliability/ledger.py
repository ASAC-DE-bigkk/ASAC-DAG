"""Reliability adapter for the Traffic-owned R2 run ledger."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ..run_ledger import TrafficRunLedger
from .config import TrafficReportConfig


def collect_scheduled_run_summary(
    dag_id: str,
    detected_at: datetime,
    config: TrafficReportConfig,
) -> dict[str, Any]:
    return TrafficRunLedger().collect_scheduled_run_summary(
        dag_id=dag_id,
        detected_at=detected_at,
        lookback_hours=config.lookback_hours,
        schedule_interval_minutes=config.scheduled_run_interval_minutes,
        stale_after_minutes=config.scheduled_run_stale_minutes,
    )
