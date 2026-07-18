"""Named dbt phase contracts for the Traffic transform DAG."""

from __future__ import annotations

from dataclasses import dataclass

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
    threads: int | None = 2
    selector_by_test_tier: dict[TrafficTestTier, str | None] | None = None


DBT_PHASE_SPECS = (
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
    ),
    DbtPhaseSpec(
        "dbt_test_traffic_incident_availability",
        "test",
        "ask_seoul_traffic_transform_availability",
    ),
    DbtPhaseSpec(
        "dbt_test_traffic_bronze_source_contract",
        "test",
        "traffic_transform_contract_gate",
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
        "dbt_run_silver",
        "run",
        "ask_seoul_traffic_transform_silver",
        fresh_parse=True,
        snapshot_required=True,
        pin_critical=True,
    ),
    DbtPhaseSpec(
        "dbt_test_silver",
        "test",
        "ask_seoul_traffic_transform_silver",
        silver_persisted=True,
        snapshot_required=True,
        pin_critical=True,
    ),
    DbtPhaseSpec(
        "dbt_run_gold",
        "run",
        "ask_seoul_traffic_transform_gold",
        silver_persisted=True,
        snapshot_required=True,
    ),
    DbtPhaseSpec(
        "dbt_test_gold",
        "test",
        "ask_seoul_traffic_transform_gold_full_tests",
        silver_persisted=True,
        fresh_parse=True,
        snapshot_required=True,
        pin_critical=True,
        selector_by_test_tier={
            TrafficTestTier.GATE: "ask_seoul_traffic_transform_gold_gate_tests",
            TrafficTestTier.HOURLY: "ask_seoul_traffic_transform_gold_hourly_tests",
            TrafficTestTier.FULL: "ask_seoul_traffic_transform_gold_full_tests",
        },
    ),
)
DBT_PHASE_TASK_IDS = tuple(spec.task_id for spec in DBT_PHASE_SPECS)


__all__ = ["DBT_PHASE_SPECS", "DBT_PHASE_TASK_IDS", "DbtPhaseSpec"]
