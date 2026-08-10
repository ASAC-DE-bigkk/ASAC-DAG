from __future__ import annotations

from pathlib import Path
import sys
import types


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_link_reference_backfill as dag_module


def test_backfill_dag_is_manual_serial_paused_and_has_no_assets():
    dag = dag_module.dag

    assert dag.dag_id == "traffic_link_reference_backfill"
    assert getattr(dag, "schedule", None) is None
    assert type(dag.timetable).__name__ == "NullTimetable"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is True
    assert dag.task_ids == [
        "validate_dev_runtime",
        "land_link_reference_backfill",
        "materialize_link_reference_backfill",
    ]
    validate = dag.get_task("validate_dev_runtime")
    land = dag.get_task("land_link_reference_backfill")
    materialize = dag.get_task("materialize_link_reference_backfill")
    assert validate.downstream_task_ids == {land.task_id}
    assert land.downstream_task_ids == {materialize.task_id}
    assert materialize.pool == dag_module.TRINO_INGEST_POOL
    assert not materialize.outlets
    assert validate.op_kwargs == {"domain": "traffic"}


def test_backfill_dag_wrappers_pass_conf_run_and_landing_xcom(monkeypatch):
    calls = []
    raw_result = {"requested_link_ids": ["1220003800"]}

    monkeypatch.setattr(
        dag_module,
        "land_backfill_batch",
        lambda **kwargs: calls.append(("land", kwargs)) or raw_result,
    )
    monkeypatch.setattr(
        dag_module,
        "materialize_backfill_batch",
        lambda **kwargs: calls.append(("materialize", kwargs)) or {"inserted_info": 1},
    )

    landed = dag_module.land_link_reference_backfill(
        run_id="manual__backfill-1",
        dag_run=types.SimpleNamespace(conf={"batch_size": 25}),
    )

    class TI:
        def xcom_pull(self, *, task_ids):
            assert task_ids == dag_module.LAND_TASK_ID
            return raw_result

    materialized = dag_module.materialize_link_reference_backfill(
        run_id="manual__backfill-1",
        ti=TI(),
    )

    assert landed is raw_result
    assert materialized == {"inserted_info": 1}
    assert calls == [
        (
            "land",
            {
                "conf": {"batch_size": 25},
                "dag_run_id": "manual__backfill-1",
            },
        ),
        (
            "materialize",
            {
                "raw_result": raw_result,
                "dag_run_id": "manual__backfill-1",
            },
        ),
    ]
