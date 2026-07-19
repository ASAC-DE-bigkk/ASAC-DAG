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
LOGGER = logging.getLogger("weather_ingest.reliability_report")

WEATHER_BRONZE_DAG_ID = "weather_vilage_fcst_bronze"
WEATHER_TABLE = "bronze_kma_vilage_fcst"
WEBHOOK_ENVS = ("ASK_SEOUL_DISCORD_WEBHOOK_URL", "WEATHER_DISCORD_WEBHOOK_URL")
DAILY_REPORT_SCHEDULE = "0 9 * * *"
# Retained for compatibility only. Pipeline Reliability v2 ignores legacy
# cadence overrides so an old */15 setting cannot reactivate frequent reports.
SCHEDULE_ENV = "ASK_SEOUL_WEATHER_REPORT_DAG_SCHEDULE"
GLOBAL_SCHEDULE_ENV = "ASK_SEOUL_REPORT_DAG_SCHEDULE"
DISCORD_GREEN = 3066993
DISCORD_YELLOW = 16776960
DISCORD_RED = 15158332
KMA_BASE_INTERVAL_HOURS = 3
MARQUEZ_BASE_URL = "http://marquez-api:5000/api/v1"
MARQUEZ_NAMESPACE = "ask-seoul-dev-airflow"
WEATHER_PIPELINE_STAGE_POLICIES = (
    StagePolicy("bronze", "Weather Bronze", "weather_vilage_fcst_bronze", 360),
    StagePolicy(
        "source_freshness",
        "Weather source freshness",
        "weather_vilage_fcst_transform.dbt_source_freshness",
        360,
    ),
    StagePolicy(
        "transform", "Weather Silver/Gold", "weather_vilage_fcst_transform", 360
    ),
    StagePolicy(
        "maintenance",
        "Iceberg maintenance",
        "ask_seoul_iceberg_maintenance",
        1_560,
        required=False,
    ),
)


@dataclass(frozen=True)
class WeatherReportConfig:
    catalog: str
    schema: str
    lookback_hours: int
    expected_kma_grids: int
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


def report_config(env: Mapping[str, str] = os.environ) -> WeatherReportConfig:
    return WeatherReportConfig(
        catalog=sql_identifier(trino_catalog(env)),
        schema=sql_identifier(ask_seoul_schema(env)),
        lookback_hours=int(env.get("ASK_SEOUL_REPORT_LOOKBACK_HOURS", "24")),
        expected_kma_grids=int(env.get("ASK_SEOUL_REPORT_EXPECTED_KMA_GRIDS", "80")),
        freshness_warn_minutes=int(
            env.get("ASK_SEOUL_REPORT_WEATHER_FRESHNESS_WARN_MINUTES", "240")
        ),
        freshness_error_minutes=int(
            env.get("ASK_SEOUL_REPORT_WEATHER_FRESHNESS_ERROR_MINUTES", "360")
        ),
    )
