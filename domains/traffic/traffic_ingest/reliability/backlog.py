"""Reliability adapter for pending Traffic Incident materialization receipts."""

from __future__ import annotations

from datetime import datetime

from ..runtime import build_traffic_snapshot_receipts
from .config import TrafficReportConfig


def collect_materialization_backlog(
    detected_at: datetime,
    config: TrafficReportConfig,
) -> dict[str, object]:
    summary = build_traffic_snapshot_receipts().pending_summary(detected_at)
    age = summary.get("oldest_age_minutes")
    if age is None:
        status = "PASS"
    elif int(age) >= config.materialization_backlog_error_minutes:
        status = "FAIL"
    elif int(age) >= config.materialization_backlog_warn_minutes:
        status = "WARN"
    else:
        status = "PASS"
    return {**summary, "status": status}


__all__ = ["collect_materialization_backlog"]
