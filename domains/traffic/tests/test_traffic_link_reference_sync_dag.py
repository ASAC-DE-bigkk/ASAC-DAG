from __future__ import annotations

from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_link_reference_sync as dag_module


def test_sync_dag_is_daily_serial_initially_paused_and_pool_bounded():
    dag = dag_module.dag

    assert dag.dag_id == "traffic_link_reference_sync"
    assert dag_module.SYNC_SCHEDULE == "37 3 * * *"
    assert type(dag.timetable).__name__ != "NullTimetable"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is True
    assert dag.task_ids == [
        "validate_dev_runtime",
        "resolve_link_reference_sync_candidates",
        "land_link_reference_sync",
        "materialize_link_reference_sync",
    ]

    validate = dag.get_task("validate_dev_runtime")
    resolve = dag.get_task("resolve_link_reference_sync_candidates")
    land = dag.get_task("land_link_reference_sync")
    materialize = dag.get_task("materialize_link_reference_sync")

    assert validate.downstream_task_ids == {resolve.task_id}
    assert resolve.downstream_task_ids == {land.task_id}
    assert land.downstream_task_ids == {materialize.task_id}
    assert resolve.pool == dag_module.TRINO_INGEST_POOL
    assert land.pool != dag_module.TRINO_INGEST_POOL
    assert materialize.pool == dag_module.TRINO_INGEST_POOL
    assert not materialize.outlets


def test_sync_wrappers_pass_only_candidate_and_raw_xcom(monkeypatch):
    calls = []
    candidates = ["1220003900", "1220004000"]
    raw_result = {
        "requested_link_ids": candidates,
        "unresolved_link_ids": candidates,
    }

    monkeypatch.setattr(
        dag_module,
        "resolve_incremental_sync_link_ids",
        lambda **kwargs: calls.append(("resolve", kwargs)) or candidates,
    )
    monkeypatch.setattr(
        dag_module,
        "land_incremental_sync_batch",
        lambda **kwargs: calls.append(("land", kwargs)) or raw_result,
    )
    monkeypatch.setattr(
        dag_module,
        "materialize_incremental_sync_batch",
        lambda **kwargs: calls.append(("materialize", kwargs))
        or {"inserted_info": 2},
    )

    resolved = dag_module.resolve_link_reference_sync_candidates(
        params={"batch_size": 25, "stale_after_days": 45},
    )

    class TI:
        def xcom_pull(self, *, task_ids):
            if task_ids == dag_module.RESOLVE_TASK_ID:
                return candidates
            if task_ids == dag_module.LAND_TASK_ID:
                return raw_result
            raise AssertionError(f"unexpected task_id: {task_ids}")

    landed = dag_module.land_link_reference_sync(
        run_id="scheduled__2026-08-10T03:37:00+09:00",
        ti=TI(),
    )
    materialized = dag_module.materialize_link_reference_sync(
        run_id="scheduled__2026-08-10T03:37:00+09:00",
        ti=TI(),
    )

    assert resolved == candidates
    assert landed is raw_result
    assert materialized == {"inserted_info": 2}
    assert calls == [
        (
            "resolve",
            {"batch_size": 25, "stale_after_days": 45},
        ),
        (
            "land",
            {
                "link_ids": candidates,
                "dag_run_id": "scheduled__2026-08-10T03:37:00+09:00",
            },
        ),
        (
            "materialize",
            {
                "raw_result": raw_result,
                "dag_run_id": "scheduled__2026-08-10T03:37:00+09:00",
            },
        ),
    ]
