from __future__ import annotations

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_flow_bronze as dag_module  # noqa: E402
from traffic_ingest.run_manifest import TrafficRun  # noqa: E402


class Dag:
    dag_id = "traffic_flow_bronze"


def test_flow_verification_publishes_success_manifest(monkeypatch):
    calls: list[tuple[TrafficRun, dict]] = []
    verified: list[dict] = []

    class Manifest:
        def publish(self, run, **metrics):
            calls.append((run, metrics))
            return "manifest-table"

    class TI:
        def xcom_pull(self, *, task_ids):
            assert task_ids == dag_module.LOAD_TASK_ID
            return {
                "raw_object_keys": ["raw/flow/one.xml", "raw/flow/two.xml"],
                "inserted": 6,
                "expected_rows": 6,
                "page_count": 2,
                "is_publishable": True,
            }

    monkeypatch.setattr(dag_module, "build_traffic_flow_manifest", lambda: Manifest())
    monkeypatch.setattr(
        dag_module,
        "verify_seoul_traffic_flow_bronze_runtime",
        lambda **kwargs: verified.append(kwargs) or 6,
    )

    result = dag_module.verify_seoul_traffic_flow_bronze(
        dag=Dag(),
        run_id="scheduled__flow",
        ti=TI(),
    )

    assert result == 6
    assert verified == [
        {
            "dag_run_id": "scheduled__flow",
            "expected_rows": 6,
            "expected_raw_objects": 2,
        }
    ]
    assert calls == [
        (
            TrafficRun(Dag.dag_id, "scheduled__flow"),
            {
                "expected_rows": 6,
                "actual_rows": 6,
                "expected_raw_objects": 2,
                "actual_raw_objects": 2,
                "is_publishable": True,
            },
        )
    ]
