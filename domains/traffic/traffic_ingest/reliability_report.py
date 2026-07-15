"""Compatibility facade for the traffic reliability report API."""

from __future__ import annotations

from .reliability.config import (
    DISCORD_GREEN,
    DISCORD_RED,
    DISCORD_YELLOW,
    GLOBAL_SCHEDULE_ENV,
    IDENTIFIER_PATTERN,
    KST,
    LOGGER,
    MANIFEST_TABLE,
    SCHEDULE_ENV,
    TRAFFIC_AUDIT_TABLE,
    TRAFFIC_BRONZE_DAG_ID,
    TRAFFIC_RUN_STALE_MINUTES,
    TRAFFIC_SCHEDULE_INTERVAL_MINUTES,
    TRAFFIC_TABLE,
    WEBHOOK_ENVS,
    TrafficReportConfig,
    ask_seoul_schema,
    discord_webhook_url,
    is_dev_target,
    report_config,
    report_dag_schedule,
    sql_identifier,
    trino_catalog,
)
from .reliability.discord import (
    _scheduled_failure_time,
    _scheduled_failure_window,
    _discord_payload,
    _format_bool,
    _format_minutes,
    _icon,
    _status_icon,
    _status_label,
    format_traffic_discord_message,
    send_discord_message,
)
from .reliability.ledger import collect_scheduled_run_summary
from .reliability.report import build_traffic_reliability_report
from .reliability.trino_repository import (
    _age_minutes,
    _fetch_one,
    _qualified,
    _sql_string,
    _sql_timestamp_utc,
    _traffic_cutoffs,
    collect_dag_run_summary,
    collect_traffic_summary,
    freshness_status,
    trino_cursor,
)


__all__ = [name for name in globals() if not name.startswith("__")]
