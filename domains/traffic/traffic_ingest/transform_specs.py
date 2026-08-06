"""Named dbt phase contracts for the Traffic transform DAG."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from traffic_ingest.common.resources import DbtWorkload
from traffic_ingest.test_cadence import TrafficTestTier


@dataclass(frozen=True)
class DbtPhaseSpec:
    task_id: str
    dbt_command: str
    selector: str | None = None
    silver_persisted: bool = False
    fresh_parse: bool = False
    snapshot_required: bool = False
    pin_critical: bool = False
    workload: DbtWorkload = DbtWorkload.TRINO
    heavy_pool: bool = True
    threads: int | None = 2
    selector_by_test_tier: dict[TrafficTestTier, str | None] | None = None
    selector_when_flow_missing: str | None = None
    selector_by_test_tier_when_flow_missing: (
        dict[TrafficTestTier, str | None] | None
    ) = None
    citydata_snapshot_required: bool = False
    admin_dong_crosswalk_pin_required: bool = False
    silver_fence_mode: Literal["write", "verify"] | None = None


INCIDENT_SILVER_SELECTOR = "ask_seoul_traffic_transform_incident_silver"


SILVER_DBT_PHASE_SPECS = (
    DbtPhaseSpec(
        "dbt_deps",
        "deps",
        workload=DbtWorkload.LOCAL,
        threads=None,
    ),
    DbtPhaseSpec(
        "dbt_run_silver",
        "build",
        "ask_seoul_traffic_transform_incident_hot_build",
        silver_persisted=True,
        fresh_parse=True,
        snapshot_required=True,
        pin_critical=True,
        silver_fence_mode="write",
    ),
)

FLOW_SILVER_DBT_PHASE_SPECS = (
    DbtPhaseSpec(
        "dbt_deps_flow_silver",
        "deps",
        workload=DbtWorkload.LOCAL,
        threads=None,
    ),
    DbtPhaseSpec(
        "dbt_run_flow_silver",
        "build",
        "ask_seoul_traffic_transform_flow_hot_build",
        silver_persisted=True,
        fresh_parse=True,
        snapshot_required=True,
        pin_critical=True,
    ),
)

GOLD_DBT_PHASE_SPECS = (
    DbtPhaseSpec(
        "dbt_deps_gold",
        "deps",
        workload=DbtWorkload.LOCAL,
        threads=None,
    ),
    DbtPhaseSpec(
        "dbt_run_gold",
        "build",
        "ask_seoul_traffic_transform_core_gold_hot_build",
        silver_persisted=True,
        fresh_parse=True,
        snapshot_required=True,
        pin_critical=True,
        admin_dong_crosswalk_pin_required=True,
        selector_when_flow_missing=(
            "ask_seoul_traffic_transform_core_gold_incident_hot_build"
        ),
    ),
)

DBT_PHASE_SPECS = (
    *SILVER_DBT_PHASE_SPECS,
    *GOLD_DBT_PHASE_SPECS,
)
DBT_PHASE_TASK_IDS = tuple(spec.task_id for spec in DBT_PHASE_SPECS)


__all__ = [
    "DBT_PHASE_SPECS",
    "DBT_PHASE_TASK_IDS",
    "FLOW_SILVER_DBT_PHASE_SPECS",
    "GOLD_DBT_PHASE_SPECS",
    "INCIDENT_SILVER_SELECTOR",
    "SILVER_DBT_PHASE_SPECS",
    "DbtPhaseSpec",
]
