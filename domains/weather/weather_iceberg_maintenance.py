"""Airflow DAG: weekly Iceberg maintenance for weather/traffic pipeline tables."""

from __future__ import annotations

from datetime import timedelta
import os
import sys

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from weather_ingest.iceberg_maintenance import (  # noqa: E402
    _normalize_tables,
    run_maintenance,
)
from weather_ingest.run_manifest import MANIFEST_TABLE  # noqa: E402
from weather_lineage import enable_lineage_if_configured  # noqa: E402

KST = "Asia/Seoul"

DEFAULT_PARAMS = {
    "target": "dev",
    "retention": "7d",
    "tables": (
        "bronze_kma_vilage_fcst",
        "bronze_seoul_traffic_incident",
        "bronze_seoul_traffic_incident_request_audit",
        MANIFEST_TABLE,
        "silver_kma_vilage_fcst",
        "gold_weather_forecast_summary",
        "dim_weather_place",
        "gold_weather_forecast_by_place",
        "silver_seoul_traffic_incident",
        "gold_traffic_incident_summary",
    ),
}


def _maintain(**context) -> None:
    params = context["params"]
    target = str(params.get("target", "dev"))
    retention = str(params.get("retention", DEFAULT_PARAMS["retention"]))
    tables = _normalize_tables(params.get("tables", DEFAULT_PARAMS["tables"]))
    results = run_maintenance(target=target, retention=retention, tables=tables)
    for table, status in results.items():
        print(f"[iceberg maintenance] {table}: {status}")
    failed = [
        table
        for table, status in results.items()
        if status != "ok" and not status.startswith("skipped")
    ]
    if failed:
        raise RuntimeError(f"Iceberg maintenance failed for: {', '.join(failed)}")


def _default_schedule() -> str | None:
    if "ASK_SEOUL_ICEBERG_MAINTENANCE_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_ICEBERG_MAINTENANCE_SCHEDULE"] or None
    return "0 4 * * 0"


with DAG(
    dag_id="ask_seoul_iceberg_maintenance",
    description="Weekly metadata cleanup for weather/traffic Iceberg bronze/silver/gold tables.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule=_default_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    params=DEFAULT_PARAMS,
    tags=[
        "maintenance",
        "ask_seoul",
        "iceberg",
        "weather",
        "traffic",
        "bronze",
        "silver",
        "gold",
    ],
) as dag:
    PythonOperator(
        task_id="maintain",
        python_callable=_maintain,
    )


enable_lineage_if_configured(dag)
