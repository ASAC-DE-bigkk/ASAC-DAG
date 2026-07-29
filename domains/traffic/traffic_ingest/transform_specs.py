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
        "dbt_source_freshness",
        "source freshness",
        "ask_seoul_traffic_transform_source",
        heavy_pool=False,
    ),
    DbtPhaseSpec(
        "dbt_test_traffic_bronze_source_contract",
        "test",
        "ask_seoul_traffic_transform_incident_preflight_contracts",
        heavy_pool=False,
    ),
    DbtPhaseSpec(
        "dbt_run_silver",
        "build",
        INCIDENT_SILVER_SELECTOR,
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
        "run",
        "ask_seoul_traffic_transform_flow_silver_model",
        fresh_parse=True,
        snapshot_required=True,
        pin_critical=True,
    ),
    DbtPhaseSpec(
        "dbt_test_flow_silver",
        "test",
        "ask_seoul_traffic_transform_flow_silver_tests",
        silver_persisted=True,
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
        "dbt_seed_asac_axes",
        "seed",
        "ask_seoul_traffic_transform_asac_axes",
    ),
    DbtPhaseSpec(
        "dbt_run_common_admin_dong_dimension",
        "run",
        "ask_seoul_traffic_transform_common_admin",
    ),
    DbtPhaseSpec(
        "dbt_test_common_admin_dong_dimension",
        "test",
        "ask_seoul_traffic_transform_common_admin",
        selector_by_test_tier={
            TrafficTestTier.GATE: None,
            TrafficTestTier.HOURLY: None,
            TrafficTestTier.FULL: "ask_seoul_traffic_transform_common_admin",
        },
    ),
    DbtPhaseSpec(
        "dbt_test_asac_axes_seed_contract",
        "test",
        "ask_seoul_traffic_transform_asac_axes_contract",
        selector_by_test_tier={
            TrafficTestTier.GATE: None,
            TrafficTestTier.HOURLY: None,
            TrafficTestTier.FULL: "ask_seoul_traffic_transform_asac_axes_contract",
        },
    ),
    DbtPhaseSpec(
        "dbt_run_gold",
        "run",
        "ask_seoul_traffic_transform_gold_models_without_commerce",
        silver_persisted=True,
        snapshot_required=True,
        citydata_snapshot_required=True,
        admin_dong_crosswalk_pin_required=True,
        selector_when_flow_missing=(
            "ask_seoul_traffic_transform_gold_incident_models_without_commerce"
        ),
    ),
    DbtPhaseSpec(
        "dbt_test_gold",
        "test",
        "ask_seoul_traffic_transform_gold_full_tests_without_commerce",
        silver_persisted=True,
        fresh_parse=True,
        snapshot_required=True,
        pin_critical=True,
        citydata_snapshot_required=True,
        admin_dong_crosswalk_pin_required=True,
        selector_by_test_tier={
            TrafficTestTier.GATE: (
                "ask_seoul_traffic_transform_gold_gate_tests_without_commerce"
            ),
            TrafficTestTier.HOURLY: (
                "ask_seoul_traffic_transform_gold_hourly_tests_without_commerce"
            ),
            TrafficTestTier.FULL: (
                "ask_seoul_traffic_transform_gold_full_tests_without_commerce"
            ),
        },
        selector_by_test_tier_when_flow_missing={
            TrafficTestTier.GATE: (
                "ask_seoul_traffic_transform_gold_incident_gate_tests_without_commerce"
            ),
            TrafficTestTier.HOURLY: (
                "ask_seoul_traffic_transform_gold_incident_hourly_tests_without_commerce"
            ),
            TrafficTestTier.FULL: (
                "ask_seoul_traffic_transform_gold_incident_full_tests_without_commerce"
            ),
        },
    ),
)

# Transitional view for the unsplit DAG. Keep its original phase order and do not
# add the Gold-owned deps task until the two DAGs are wired independently.
DBT_PHASE_SPECS = (
    *SILVER_DBT_PHASE_SPECS[:-1],
    *GOLD_DBT_PHASE_SPECS[1:-2],
    *SILVER_DBT_PHASE_SPECS[-1:],
    *GOLD_DBT_PHASE_SPECS[-2:],
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
