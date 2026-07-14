from pathlib import Path

import pytest

from traffic_snapshot_recovery_test_support import (
    FakeAirflowFailException,
    load_recovery_module,
)
from traffic_snapshot_recovery_test_support import (
    restore_airflow_modules_after_dag_import,  # noqa: F401
)


def test_recovery_dag_is_manual_and_uses_only_recovery_selectors():
    module = load_recovery_module()
    dag = module.dag

    assert dag.dag_id == "traffic_snapshot_recovery"
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["params"]["target"].schema["enum"] == ["dev"]
    assert dag.kwargs["params"]["snapshot_dag_run_id"].value == ""

    expected_task_order = [
        "validate_dev_runtime",
        "validate_publishable_snapshot",
        "dbt_deps",
        "dbt_run_recovery_silver",
        "dbt_run_recovery_metadata",
        "dbt_test_recovery_silver",
        "dbt_run_recovery_gold",
        "dbt_test_recovery_gold",
        "record_recovery_completion",
    ]
    assert set(expected_task_order) == set(dag.task_ids)
    for upstream_task_id, downstream_task_id in zip(
        expected_task_order, expected_task_order[1:]
    ):
        assert dag.task_dict[upstream_task_id].downstream_task_ids == {
            downstream_task_id
        }

    phase_contracts = {
        task_id: (
            dag.task_dict[task_id].kwargs["op_kwargs"]["dbt_command"],
            dag.task_dict[task_id].kwargs["op_kwargs"]["selection"],
        )
        for task_id in expected_task_order
        if task_id.startswith("dbt_")
    }
    assert phase_contracts == {
        "dbt_deps": ("deps", None),
        "dbt_run_recovery_silver": (
            "run",
            "tag:ask_seoul_traffic_recovery_silver",
        ),
        "dbt_run_recovery_metadata": (
            "run",
            "tag:ask_seoul_traffic_recovery_metadata",
        ),
        "dbt_test_recovery_silver": (
            "test",
            "tag:ask_seoul_traffic_recovery_silver",
        ),
        "dbt_run_recovery_gold": (
            "run",
            "tag:ask_seoul_traffic_recovery_gold",
        ),
        "dbt_test_recovery_gold": (
            "test",
            "tag:ask_seoul_traffic_recovery_gold",
        ),
    }
    assert module.RECOVERY_DBT_TASK_IDS == tuple(phase_contracts)
    assert tuple(spec.task_id for spec in module.RECOVERY_DBT_PHASE_SPECS) == (
        module.RECOVERY_DBT_TASK_IDS
    )
    assert {
        spec.task_id: (spec.dbt_command, spec.selection)
        for spec in module.RECOVERY_DBT_PHASE_SPECS
    } == phase_contracts
    assert list(module.recovery_dbt_tasks) == list(phase_contracts)
    assert {
        spec.task_id
        for spec in module.RECOVERY_DBT_PHASE_SPECS
        if spec.recovery_silver_persisted
    } == {
        "dbt_run_recovery_metadata",
        "dbt_test_recovery_silver",
        "dbt_run_recovery_gold",
        "dbt_test_recovery_gold",
    }
    with pytest.raises(AttributeError):
        module.RECOVERY_DBT_PHASE_SPECS[0].task_id = "mutated"
    for task_id in phase_contracts:
        assert (
            dag.task_dict[task_id].kwargs["on_failure_callback"]
            is module.record_recovery_dbt_problem
        )
        assert dag.task_dict[task_id].kwargs["pool"] == module.TRINO_HEAVY_POOL
        assert "dbt_args" not in dag.task_dict[task_id].kwargs["op_kwargs"]

    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "RECOVERY_RELATIONS" not in source
    assert "RECOVERY_SILVER_MODEL_ID" not in source
    assert "run --select recovery_" not in source
    assert "test --select recovery_" not in source


def test_validate_publishable_snapshot_rejects_blank_and_nonpublishable_inputs(
    monkeypatch,
):
    module = load_recovery_module()

    with pytest.raises(
        FakeAirflowFailException, match="snapshot_dag_run_id is required"
    ):
        module.validate_publishable_snapshot(params={"snapshot_dag_run_id": "  "})

    class MissingManifest:
        def require_publishable(self, run_id):
            raise module.RunNotPublishableError(
                f"Traffic snapshot is not publishable: {run_id}"
            )

    monkeypatch.setattr(
        module,
        "build_traffic_manifest",
        lambda: MissingManifest(),
    )
    with pytest.raises(FakeAirflowFailException, match="not publishable"):
        module.validate_publishable_snapshot(
            params={"snapshot_dag_run_id": "historical-run"}
        )

    calls = []

    class PublishableManifest:
        def require_publishable(self, run_id):
            calls.append(run_id)
            return run_id

    monkeypatch.setattr(
        module,
        "build_traffic_manifest",
        lambda: PublishableManifest(),
    )

    assert (
        module.validate_publishable_snapshot(
            params={"snapshot_dag_run_id": "historical-run"}
        )
        == "historical-run"
    )
    assert calls == ["historical-run"]
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "bronze_collection_run_manifest" not in source
    assert "TRAFFIC_SOURCE_ID" not in source


def test_recovery_silver_persistence_uses_node_name_not_project_namespace():
    module = load_recovery_module()

    assert (
        module.recovery_silver_persisted_from_results(
            [
                {
                    "unique_id": "model.asac_seoul.recovery_silver_seoul_traffic_incident",
                    "status": "success",
                }
            ],
            selected_unique_ids=(
                "model.asac_seoul.recovery_silver_seoul_traffic_incident",
            ),
            default=False,
        )
        is True
    )
    assert (
        module.recovery_silver_persisted_from_results(
            [
                {
                    "unique_id": "model.asac_seoul.unrelated_recovery_model",
                    "status": "success",
                }
            ],
            selected_unique_ids=(
                "model.asac_seoul.recovery_silver_seoul_traffic_incident",
            ),
            default=False,
        )
        is False
    )
