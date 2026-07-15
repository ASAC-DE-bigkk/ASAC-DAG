from __future__ import annotations

import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_flow_bronze as flow_module  # noqa: E402
import traffic_incident_bronze as incident_module  # noqa: E402
from traffic_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402


@pytest.mark.parametrize(
    ("dag", "task_ids"),
    (
        (
            incident_module.dag,
            (
                incident_module.LOAD_TRAFFIC_BRONZE_TASK_ID,
                "verify_seoul_traffic_bronze_runtime",
            ),
        ),
        (
            incident_module.recollect_dag,
            (
                incident_module.LOAD_TRAFFIC_BRONZE_TASK_ID,
                "verify_seoul_traffic_bronze_runtime",
            ),
        ),
        (
            incident_module.backfill_dag,
            (
                incident_module.LOAD_TRAFFIC_BRONZE_TASK_ID,
                "verify_seoul_traffic_bronze_runtime",
            ),
        ),
        (
            flow_module.dag,
            (
                flow_module.LOAD_TASK_ID,
                "verify_seoul_traffic_flow_bronze",
            ),
        ),
    ),
)
def test_traffic_bronze_trino_tasks_use_heavy_pool(dag, task_ids):
    for task_id in task_ids:
        assert dag.get_task(task_id).pool == TRINO_HEAVY_POOL
