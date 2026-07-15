from __future__ import annotations

import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_flow_bronze as flow_module  # noqa: E402
import traffic_incident_bronze as incident_module  # noqa: E402
import traffic_incident_manual as manual_module  # noqa: E402
import traffic_ingest.manual_incident as manual_tasks  # noqa: E402
from traffic_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402


@pytest.mark.parametrize(
    ("dag", "task_ids"),
    (
        (
            incident_module.dag,
            (
                incident_module.MATERIALIZER_TASK_ID,
            ),
        ),
        (
            manual_module.recollect_dag,
            (
                manual_tasks.LOAD_TRAFFIC_BRONZE_TASK_ID,
                "verify_seoul_traffic_bronze_runtime",
            ),
        ),
        (
            manual_module.backfill_dag,
            (
                manual_tasks.LOAD_TRAFFIC_BRONZE_TASK_ID,
                "verify_seoul_traffic_bronze_runtime",
            ),
        ),
        (
            flow_module.dag,
            (
                flow_module.FLOW_MATERIALIZE_TASK_ID,
            ),
        ),
    ),
)
def test_traffic_bronze_trino_tasks_use_heavy_pool(dag, task_ids):
    for task_id in task_ids:
        assert dag.get_task(task_id).pool == TRINO_HEAVY_POOL
