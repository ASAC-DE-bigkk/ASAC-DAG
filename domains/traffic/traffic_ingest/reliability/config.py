from __future__ import annotations

import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from ..run_manifest import MANIFEST_TABLE as MANIFEST_TABLE
from .lineage import StagePolicy


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KST = ZoneInfo("Asia/Seoul")
LOGGER = logging.getLogger("traffic_ingest.reliability_report")

TRAFFIC_BRONZE_DAG_ID = "traffic_incident_bronze"
TRAFFIC_FLOW_BRONZE_DAG_ID = "traffic_flow_bronze"
TRAFFIC_LANDING_DAG_ID = "traffic_incident_landing"
# ``traffic_incident_bronze`` runs on a five-minute cron in the dev smoke flow.
# The interval is added to the first-to-last failed slot so the reported window
# includes the final slot's collection period.
TRAFFIC_SCHEDULE_INTERVAL_MINUTES = 5
TRAFFIC_RUN_STALE_MINUTES = 15
TRAFFIC_TABLE = "bronze_seoul_traffic_incident"
TRAFFIC_AUDIT_TABLE = "bronze_seoul_traffic_incident_request_audit"
TRAFFIC_FLOW_TABLE = "bronze_seoul_traffic_flow"
TRAFFIC_FLOW_AUDIT_TABLE = "bronze_seoul_traffic_flow_request_audit"
WEBHOOK_ENVS = ("ASK_SEOUL_DISCORD_WEBHOOK_URL", "TRAFFIC_DISCORD_WEBHOOK_URL")
DAILY_REPORT_SCHEDULE = "0 9 * * *"
# Retained for compatibility only. Pipeline Reliability v2 ignores legacy
# cadence overrides so an old */15 setting cannot reactivate frequent reports.
SCHEDULE_ENV = "ASK_SEOUL_TRAFFIC_REPORT_DAG_SCHEDULE"
GLOBAL_SCHEDULE_ENV = "ASK_SEOUL_REPORT_DAG_SCHEDULE"
DISCORD_GREEN = 3066993
DISCORD_YELLOW = 16776960
DISCORD_RED = 15158332
SCHEDULED_FAILURE_REASON_FALLBACK = "원인 미확인"
MARQUEZ_BASE_URL = "http://marquez-api:5000/api/v1"
MARQUEZ_NAMESPACE = "ask-seoul-dev-airflow"
TRAFFIC_PIPELINE_STAGE_POLICIES = (
    StagePolicy("landing", "Raw landing", "traffic_incident_landing", 20),
    StagePolicy("incident_bronze", "Incident Bronze", "traffic_incident_bronze", 60),
    StagePolicy("flow_bronze", "Flow Bronze", "traffic_flow_bronze", 30),
    StagePolicy(
        "incident_source_freshness",
        "Incident source freshness",
        "traffic_incident_transform.dbt_source_freshness",
        120,
    ),
    StagePolicy(
        "incident_silver", "Incident Silver", "traffic_incident_transform", 120
    ),
    StagePolicy("flow_silver", "Flow Silver", "traffic_flow_transform", 120),
    StagePolicy("gold", "Traffic Gold", "traffic_gold_transform", 240),
    StagePolicy(
        "maintenance",
        "Iceberg maintenance",
        "ask_seoul_iceberg_maintenance",
        1_560,
        required=False,
    ),
)


@dataclass(frozen=True)
class TrafficReportConfig:
    catalog: str
    schema: str
    lookback_hours: int
    freshness_warn_minutes: int
    freshness_error_minutes: int
    scheduled_run_interval_minutes: int
    scheduled_run_stale_minutes: int
    materialization_backlog_warn_minutes: int
    materialization_backlog_error_minutes: int


def is_dev_target(env: Mapping[str, str] = os.environ) -> bool:
    return env.get("ASK_SEOUL_TARGET", env.get("DBT_TARGET", "prod")) == "dev"


def discord_webhook_url(env: Mapping[str, str] = os.environ) -> str | None:
    for key in WEBHOOK_ENVS:
        value = (env.get(key) or "").strip()
        if value:
            return value
    return None


def report_dag_schedule(env: Mapping[str, str] = os.environ) -> str | None:
    if not discord_webhook_url(env):
        return None
    return DAILY_REPORT_SCHEDULE


def sql_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def trino_catalog(env: Mapping[str, str] = os.environ) -> str:
    if is_dev_target(env):
        return env.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    return env.get("TRINO_ICEBERG_CATALOG", "iceberg")


def ask_seoul_schema(env: Mapping[str, str] = os.environ) -> str:
    return env.get("ASK_SEOUL_SCHEMA", "ask_seoul")


def report_config(env: Mapping[str, str] = os.environ) -> TrafficReportConfig:
    return TrafficReportConfig(
        catalog=sql_identifier(trino_catalog(env)),
        schema=sql_identifier(ask_seoul_schema(env)),
        lookback_hours=int(env.get("ASK_SEOUL_REPORT_LOOKBACK_HOURS", "24")),
        freshness_warn_minutes=int(
            env.get("ASK_SEOUL_REPORT_TRAFFIC_FRESHNESS_WARN_MINUTES", "15")
        ),
        freshness_error_minutes=int(
            env.get("ASK_SEOUL_REPORT_TRAFFIC_FRESHNESS_ERROR_MINUTES", "30")
        ),
        scheduled_run_interval_minutes=int(
            env.get(
                "ASK_SEOUL_REPORT_TRAFFIC_SCHEDULE_INTERVAL_MINUTES",
                str(TRAFFIC_SCHEDULE_INTERVAL_MINUTES),
            )
        ),
        scheduled_run_stale_minutes=int(
            env.get(
                "ASK_SEOUL_REPORT_TRAFFIC_RUN_STALE_MINUTES",
                str(TRAFFIC_RUN_STALE_MINUTES),
            )
        ),
        materialization_backlog_warn_minutes=int(
            env.get("ASK_SEOUL_TRAFFIC_BACKLOG_WARN_MINUTES", "15")
        ),
        materialization_backlog_error_minutes=int(
            env.get("ASK_SEOUL_TRAFFIC_BACKLOG_ERROR_MINUTES", "30")
        ),
    )
