"""Stable execution contract for the Weather/Traffic cost proxy."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from weather_ingest.run_manifest import MANIFEST_TABLE


DEFAULT_DBT_PROJECT = "/opt/airflow/dbt"
DBT_PROJECT_ENV = "ASK_SEOUL_DBT_PROJECT_DIR"
DBT_BIN = os.environ.get("DBT_BIN", "/home/airflow/dbt-venv/bin/dbt")


def _dbt_project_dir(env: Mapping[str, str] = os.environ) -> str:
    return (env.get(DBT_PROJECT_ENV) or DEFAULT_DBT_PROJECT).strip()


DBT_PROJECT = _dbt_project_dir()
MIN_REPEAT = 3
EXECUTION_FINGERPRINT_KEY = "execution_fingerprint"
WEATHER_SOURCE_TABLES = ("bronze_kma_vilage_fcst", MANIFEST_TABLE)
TRAFFIC_SOURCE_TABLES = (
    "bronze_seoul_traffic_incident",
    "bronze_seoul_traffic_incident_request_audit",
    MANIFEST_TABLE,
)


@dataclass(frozen=True, slots=True)
class BenchmarkContract:
    """Single immutable owner for the fixed cost-proxy model registry."""

    cases: Mapping[str, Mapping[str, Any]]


def _immutable_cases(
    cases: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Mapping[str, Any]]:
    return MappingProxyType(
        {name: MappingProxyType(dict(case)) for name, case in cases.items()}
    )


BENCHMARK_CONTRACT = BenchmarkContract(
    cases=_immutable_cases(
        {
            "weather_silver": {
                "domain": "weather",
                "model": "silver_kma_vilage_fcst",
                "source_tables": WEATHER_SOURCE_TABLES,
            },
            "traffic_silver": {
                "domain": "traffic",
                "model": "silver_seoul_traffic_incident",
                "snapshot_source_id": "seoul_traffic_incident",
                "snapshot_var": "traffic_snapshot_dag_run_id",
                "source_tables": TRAFFIC_SOURCE_TABLES,
            },
            "weather_gold": {
                "domain": "weather",
                "model": "gold_weather_forecast_by_place",
                "source_tables": WEATHER_SOURCE_TABLES,
            },
            "traffic_gold": {
                "domain": "traffic",
                "model": "gold_traffic_incident_current_by_admin_dong_hourly",
                "snapshot_source_id": "seoul_traffic_incident",
                "snapshot_var": "traffic_snapshot_dag_run_id",
                "source_tables": TRAFFIC_SOURCE_TABLES,
            },
        }
    )
)
# Backward-compatible alias; it is the exact registry owned by BENCHMARK_CONTRACT.
CASES = BENCHMARK_CONTRACT.cases
COMPARISON_METRICS = (
    "physical_input_bytes",
    "cpu_time_ms",
    "physical_written_bytes",
    "spilled_bytes",
    "peak_user_memory_bytes",
    "wall_time_ms",
)
_MAX_METRICS = {"peak_user_memory_bytes"}


class CompilePaths:
    def __init__(self, *, target_path: str, log_path: str, manifest_path: str):
        self.target_path = target_path
        self.log_path = log_path
        self.manifest_path = manifest_path


def median_or_none(values: list[int | float | None]) -> int | float | None:
    """Return a median only if every repeat exposed a real value."""
    usable = sorted(value for value in values if value is not None)
    if len(usable) != len(values) or not usable:
        return None
    middle = len(usable) // 2
    return (
        usable[middle] if len(usable) % 2 else (usable[middle - 1] + usable[middle]) / 2
    )


def _target(env: Mapping[str, str] = os.environ) -> str:
    return env.get("ASK_SEOUL_TARGET", env.get("DBT_TARGET", "prod"))


def _catalog(env: Mapping[str, str] = os.environ) -> str:
    if _target(env) == "dev":
        return env.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    return env.get("TRINO_ICEBERG_CATALOG", "iceberg")


def _schema(env: Mapping[str, str] = os.environ) -> str:
    return env.get("ASK_SEOUL_SCHEMA", "ask_seoul")


def _ensure_dev_target(env: Mapping[str, str] = os.environ) -> None:
    if _target(env) != "dev":
        raise RuntimeError("cost-proxy collect is limited to ASK_SEOUL_TARGET=dev")


def _qualified(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"
