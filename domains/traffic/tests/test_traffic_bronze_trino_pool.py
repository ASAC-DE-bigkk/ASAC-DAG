from __future__ import annotations

import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_flow_bronze as flow_module  # noqa: E402
import traffic_incident_bronze as incident_module  # noqa: E402
import traffic_incident_manual as manual_module  # noqa: E402
import traffic_ingest.manual_incident as manual_tasks  # noqa: E402
from traffic_ingest.common.resources import (  # noqa: E402
    TRINO_HEAVY_POOL,
    TRINO_INGEST_POOL,
)


@pytest.mark.parametrize(
    ("dag", "task_ids", "expected_pool"),
    (
        (
            incident_module.dag,
            (
                incident_module.MATERIALIZER_TASK_ID,
            ),
            TRINO_INGEST_POOL,
        ),
        (
            manual_module.recollect_dag,
            (
                manual_tasks.LOAD_TRAFFIC_BRONZE_TASK_ID,
                "verify_seoul_traffic_bronze_runtime",
            ),
            TRINO_HEAVY_POOL,
        ),
        (
            manual_module.backfill_dag,
            (
                manual_tasks.LOAD_TRAFFIC_BRONZE_TASK_ID,
                "verify_seoul_traffic_bronze_runtime",
            ),
            TRINO_HEAVY_POOL,
        ),
        (
            flow_module.dag,
            (
                flow_module.FLOW_MATERIALIZE_TASK_ID,
            ),
            TRINO_INGEST_POOL,
        ),
    ),
)
def test_traffic_bronze_trino_tasks_use_expected_pool(dag, task_ids, expected_pool):
    for task_id in task_ids:
        assert dag.get_task(task_id).pool == expected_pool
