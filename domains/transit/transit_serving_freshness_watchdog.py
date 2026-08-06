"""Independently monitor Transit fast-tier D1 freshness (Serving Contract v1 §7.4)."""

from __future__ import annotations

import os

from common.errors.airflow import problem_failure_callback
from common.serving.dag_factory import build_serving_freshness_watchdog_dag


# Kept intentionally narrow: #719 concerns the current-time fast tier. Daily and
# hourly products have different declared cadences and should opt in separately.
FAST_PRODUCTS = ["transit_dong_now", "transit_parking_full_risk"]
_TARGET = os.environ.get("DBT_TARGET", "prod")
_KST_WALL_CLOCK_PRODUCTS = {
    # dbt transit freshness macros define these source observation timestamps as
    # timezone-naive Asia/Seoul wall-clock values; never apply this fallback to
    # products without that source-contract evidence.
    "transit_dong_now": "Asia/Seoul",
    "transit_parking_full_risk": "Asia/Seoul",
}

record_transit_serving_watchdog_problem = problem_failure_callback(
    domain="transit",
    source_system="serving_freshness_watchdog",
)

dag = build_serving_freshness_watchdog_dag(
    domain="transit",
    product_ids=FAST_PRODUCTS,
    schedule="7,22,37,52 * * * *",
    dag_id="transit_serving_freshness_watchdog",
    target=_TARGET,
    naive_freshness_timezones=_KST_WALL_CLOCK_PRODUCTS,
    # Export starts at :10/:25/:40/:55. A 10-minute execution allowance prevents
    # race alerts while a scheduled publication is still finishing; missing runs
    # still breach after one cadence plus the allowance.
    publication_grace_minutes=10,
    failure_callback=record_transit_serving_watchdog_problem,
)
