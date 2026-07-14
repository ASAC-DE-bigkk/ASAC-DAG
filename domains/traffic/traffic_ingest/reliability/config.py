from __future__ import annotations

import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from ..run_manifest import MANIFEST_TABLE as MANIFEST_TABLE


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KST = ZoneInfo("Asia/Seoul")
LOGGER = logging.getLogger("traffic_ingest.reliability_report")

TRAFFIC_BRONZE_DAG_ID = "traffic_incident_bronze"
# ``traffic_incident_bronze`` runs on a five-minute cron in the dev smoke flow.
# The interval is added to the first-to-last failed slot so the reported window
# includes the final slot's collection period.
TRAFFIC_SCHEDULE_INTERVAL_MINUTES = 5
TRAFFIC_TABLE = "bronze_seoul_traffic_incident"
TRAFFIC_AUDIT_TABLE = "bronze_seoul_traffic_incident_request_audit"
WEBHOOK_ENVS = ("ASK_SEOUL_DISCORD_WEBHOOK_URL", "TRAFFIC_DISCORD_WEBHOOK_URL")
SCHEDULE_ENV = "ASK_SEOUL_TRAFFIC_REPORT_DAG_SCHEDULE"
GLOBAL_SCHEDULE_ENV = "ASK_SEOUL_REPORT_DAG_SCHEDULE"
DISCORD_GREEN = 3066993
DISCORD_YELLOW = 16776960
DISCORD_RED = 15158332
AIRFLOW_METADATA_QUERY_MAX_ATTEMPTS = 2
AIRFLOW_FAILURE_REASON_FALLBACK = "원인 미확인"
AIRFLOW_FAILURE_REASON_MAX_LENGTH = 240
PROBLEM_DOCUMENT_PREFIX = "errors"


@dataclass(frozen=True)
class TrafficReportConfig:
    catalog: str
    schema: str
    lookback_hours: int
    freshness_warn_minutes: int
    freshness_error_minutes: int


def is_dev_target(env: Mapping[str, str] = os.environ) -> bool:
    return env.get("ASK_SEOUL_TARGET", env.get("DBT_TARGET", "prod")) == "dev"


def discord_webhook_url(env: Mapping[str, str] = os.environ) -> str | None:
    for key in WEBHOOK_ENVS:
        value = (env.get(key) or "").strip()
        if value:
            return value
    return None


def report_dag_schedule(env: Mapping[str, str] = os.environ) -> str | None:
    if not is_dev_target(env):
        return None
    if SCHEDULE_ENV in env:
        return env[SCHEDULE_ENV] or None
    if GLOBAL_SCHEDULE_ENV in env:
        return env[GLOBAL_SCHEDULE_ENV] or None
    if not discord_webhook_url(env):
        return None
    return "*/15 * * * *"


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
    )
