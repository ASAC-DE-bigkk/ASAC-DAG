import json
import types
from pathlib import Path

import pytest

from traffic_transform_test_support import (
    FakeAirflowFailException,
    load_transform_module,
    write_materialization_artifacts,
)
from traffic_transform_test_support import restore_airflow_modules_after_dag_import  # noqa: F401


def test_snapshot_resolver_delegates_to_the_traffic_manifest(monkeypatch):
    module = load_transform_module()
    calls = []

    class Manifest:
        def latest_publishable_run_id(self):
            calls.append("latest")
            return "traffic-run-42"

    monkeypatch.setattr(module, "build_traffic_manifest", lambda: Manifest())

    assert module.resolve_traffic_snapshot_run() == "traffic-run-42"
    assert calls == ["latest"]
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "bronze_collection_run_manifest" not in source
    assert "seoul_traffic_incident" not in source


def test_traffic_contract_gates_delegate_membership_to_dbt_selectors():
    module = load_transform_module()
    source_task = module.dag.task_dict["dbt_test_traffic_bronze_source_contract"]
    seed_task = module.dag.task_dict["dbt_test_asac_axes_seed_contract"]
    assert source_task.kwargs["op_kwargs"]["dbt_command"] == "test"
    assert (
        source_task.kwargs["op_kwargs"]["selection"]
        == "traffic_transform_contract_gate"
    )
    assert seed_task.kwargs["op_kwargs"]["dbt_command"] == "test"
    assert seed_task.kwargs["op_kwargs"]["selection"] == (
        "tag:ask_seoul_traffic_transform_asac_axes_contract"
    )
    assert not hasattr(module, "TRAFFIC_BRONZE_SOURCE_CONTRACT_TESTS")
    assert not hasattr(module, "ASAC_AXES_SEED_CONTRACT_TESTS")
    assert not hasattr(module, "normalize_dbt_test_tuples")
    assert not hasattr(module, "assert_exact_dbt_test_set")


def test_contract_gate_runs_non_empty_dbt_ls_before_dbt_test(tmp_path, monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "dbt"))
    commands = []
    ti = types.SimpleNamespace(
        task_id="dbt_test_traffic_bronze_source_contract",
        try_number=1,
        xcom_pull=lambda task_ids: 'snapshot-"\\a',
    )

    def fake_run(command, **_kwargs):
        commands.append(command)
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"test.traffic.contract","resource_type":"test"}\n',
                stderr="",
            )
        write_materialization_artifacts(command)
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    result = module.run_dbt_phase(
        dbt_command="test",
        selection="traffic_transform_contract_gate",
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=False,
        ti=ti,
        run_id="manual__a",
        params={"target": "dev"},
    )

    assert result["status"] == "success"
    assert result["manifest_path"].endswith("/manifest.json")
    assert commands[0][1:4] == ["ls", "--resource-type", "test"]
    assert "--output" in commands[0]
    assert (
        commands[0][commands[0].index("--selector") + 1]
        == "traffic_transform_contract_gate"
    )
    assert commands[1][1:3] == ["test", "--selector"]
    assert commands[1][3] == "traffic_transform_contract_gate"
    for command in commands:
        assert "--indirect-selection=buildable" in command
        assert "--target-path" in command
        assert "--log-path" in command
        variables = command[command.index("--vars") + 1]
        assert json.loads(variables) == {"traffic_snapshot_dag_run_id": 'snapshot-"\\a'}


def test_contract_gate_does_not_run_dbt_test_after_empty_selection(monkeypatch):
    module = load_transform_module()
    commands = []
    pushed = {}
    ti = types.SimpleNamespace(
        dag_id="traffic_incident_transform",
        task_id="dbt_test_traffic_bronze_source_contract",
        try_number=1,
        xcom_pull=lambda task_ids: "snapshot-a",
        xcom_push=lambda key, value: pushed.update(key=key, value=value),
    )

    def fake_run(command, **_kwargs):
        commands.append(command)
        return types.SimpleNamespace(
            returncode=0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    with pytest.raises(FakeAirflowFailException, match="model-execution-failed"):
        module.run_dbt_phase(
            dbt_command="test",
            selection="traffic_transform_contract_gate",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=False,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    assert len(commands) == 1
    assert commands[0][1] == "ls"
    assert pushed["value"]["failure_classification"] == "model-execution-failed"


def test_traffic_transform_bootstraps_asac_axes_before_silver():
    module = load_transform_module()
    dag = module.dag

    expected_task_order = [
        "resolve_traffic_snapshot_run",
        "dbt_deps",
        "dbt_source_freshness",
        "dbt_test_traffic_incident_availability",
        "dbt_test_traffic_bronze_source_contract",
        "dbt_seed_asac_axes",
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
        "dbt_test_asac_axes_seed_contract",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
    ]

    assert set(expected_task_order) <= set(dag.task_ids)
    for upstream_task_id, downstream_task_id in zip(
        expected_task_order, expected_task_order[1:]
    ):
        assert dag.task_dict[upstream_task_id].downstream_task_ids == {
            downstream_task_id,
            "fail_transform_if_upstream_failed",
        }

    expected_phase_contracts = {
        "dbt_deps": ("deps", None),
        "dbt_source_freshness": (
            "source freshness",
            "tag:ask_seoul_traffic_transform_source",
        ),
        "dbt_test_traffic_incident_availability": (
            "test",
            "tag:ask_seoul_traffic_transform_availability",
        ),
        "dbt_test_traffic_bronze_source_contract": (
            "test",
            "traffic_transform_contract_gate",
        ),
        "dbt_seed_asac_axes": (
            "seed",
            "tag:ask_seoul_traffic_transform_asac_axes",
        ),
        "dbt_run_common_admin_dong_dimension": (
            "run",
            "tag:ask_seoul_traffic_transform_common_admin",
        ),
        "dbt_test_common_admin_dong_dimension": (
            "test",
            "tag:ask_seoul_traffic_transform_common_admin",
        ),
        "dbt_test_asac_axes_seed_contract": (
            "test",
            "tag:ask_seoul_traffic_transform_asac_axes_contract",
        ),
        "dbt_run_silver": ("run", "tag:ask_seoul_traffic_transform_silver"),
        "dbt_test_silver": ("test", "tag:ask_seoul_traffic_transform_silver"),
        "dbt_run_gold": ("run", "tag:ask_seoul_traffic_transform_gold"),
        "dbt_test_gold": ("test", "tag:ask_seoul_traffic_transform_gold"),
    }
    assert module.DBT_PHASE_TASK_IDS == tuple(expected_phase_contracts)
    assert tuple(spec.task_id for spec in module.DBT_PHASE_SPECS) == (
        module.DBT_PHASE_TASK_IDS
    )
    assert {
        spec.task_id: (spec.dbt_command, spec.selection)
        for spec in module.DBT_PHASE_SPECS
    } == expected_phase_contracts
    assert list(module.dbt_phase_tasks) == list(expected_phase_contracts)
    with pytest.raises(AttributeError):
        module.DBT_PHASE_SPECS[0].task_id = "mutated"
    for task_id, (dbt_command, selection) in expected_phase_contracts.items():
        op_kwargs = dag.task_dict[task_id].kwargs["op_kwargs"]
        assert op_kwargs["dbt_command"] == dbt_command
        assert op_kwargs["selection"] == selection
        assert "dbt_args" not in op_kwargs
    for task_id in (
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
    ):
        assert (
            dag.task_dict[task_id].kwargs["op_kwargs"]["snapshot_task_id"]
            == "resolve_traffic_snapshot_run"
        )
        assert dag.task_dict[task_id].kwargs["op_kwargs"]["silver_persisted"] is False
    assert dag.task_dict["resolve_traffic_snapshot_run"].downstream_task_ids == {
        "dbt_deps",
        "fail_transform_if_upstream_failed",
    }
    assert (
        dag.task_dict["dbt_run_silver"].kwargs["op_kwargs"]["snapshot_task_id"]
        == "resolve_traffic_snapshot_run"
    )
    assert dag.task_dict["dbt_run_silver"].kwargs["op_kwargs"]["fresh_parse"] is True
    assert (
        dag.task_dict["dbt_test_silver"].kwargs["op_kwargs"]["snapshot_task_id"]
        == "resolve_traffic_snapshot_run"
    )
    assert dag.task_dict["dbt_test_gold"].kwargs["op_kwargs"]["fresh_parse"] is True
    assert {
        spec.task_id: (spec.silver_persisted, spec.fresh_parse)
        for spec in module.DBT_PHASE_SPECS
        if spec.silver_persisted or spec.fresh_parse
    } == {
        "dbt_run_silver": (False, True),
        "dbt_test_silver": (True, False),
        "dbt_run_gold": (True, False),
        "dbt_test_gold": (True, True),
    }


def test_contract_gates_are_the_only_path_into_persisted_silver():
    module = load_transform_module()
    dag = module.dag

    assert dag.task_dict[
        "dbt_test_traffic_bronze_source_contract"
    ].downstream_task_ids == {
        "dbt_seed_asac_axes",
        "fail_transform_if_upstream_failed",
    }
    assert dag.task_dict[
        "dbt_test_common_admin_dong_dimension"
    ].downstream_task_ids == {
        "dbt_test_asac_axes_seed_contract",
        "fail_transform_if_upstream_failed",
    }
    assert dag.task_dict["dbt_test_asac_axes_seed_contract"].downstream_task_ids == {
        "dbt_run_silver",
        "fail_transform_if_upstream_failed",
    }
    assert dag.task_dict["dbt_run_silver"].upstream_task_ids == {
        "dbt_test_asac_axes_seed_contract",
    }


def test_traffic_dag_contains_no_model_or_test_membership_literals():
    module = load_transform_module()
    source = Path(module.__file__).read_text(encoding="utf-8")

    assert "silver_seoul_traffic_incident" not in source
    assert "gold_traffic_incident_summary" not in source
    assert "assert_silver_traffic_" not in source
    assert "assert_gold_traffic_" not in source


def test_gold_contract_test_fresh_parses_in_same_task_artifact(tmp_path, monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "dbt"))
    commands = []
    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=2,
        xcom_pull=lambda task_ids: "snapshot-a",
    )

    def run(command, **_kwargs):
        commands.append(command)
        write_materialization_artifacts(command)
        return types.SimpleNamespace(
            returncode=0,
            stdout=(
                '{"unique_id":"test.traffic.gold","resource_type":"test"}\n'
                if command[1] == "ls"
                else ""
            ),
            stderr="",
        )

    monkeypatch.setattr(module.subprocess, "run", run)

    result = module.run_dbt_phase(
        dbt_command="test",
        selection="tag:ask_seoul_traffic_transform_gold",
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=True,
        fresh_parse=True,
        ti=ti,
        run_id="manual__a",
        params={"target": "dev"},
    )

    assert [command[1] for command in commands] == ["parse", "ls", "test"]
    assert "--no-partial-parse" in commands[0]
    assert commands[0][commands[0].index("--target") + 1] == "dev"
    assert commands[2][commands[2].index("--target") + 1] == "dev"
    assert commands[0][commands[0].index("--vars") + 1] == (
        '{"traffic_snapshot_dag_run_id": "snapshot-a"}'
    )
    assert commands[2][commands[2].index("--vars") + 1] == (
        '{"traffic_snapshot_dag_run_id": "snapshot-a"}'
    )
    parse_target = Path(commands[0][commands[0].index("--target-path") + 1])
    test_target = Path(commands[2][commands[2].index("--target-path") + 1])
    assert parse_target.name == "preflight"
    assert test_target.name == "execution"
    assert parse_target.parent == test_target.parent
    assert test_target == Path(result["run_results_path"]).parent
    assert commands[2][commands[2].index("--log-path") + 1].endswith(
        "traffic-transform/manual__a/dbt_test_gold/try2/execution"
    )
    assert Path(result["run_results_path"]).parts[-5:] == (
        "manual__a",
        "dbt_test_gold",
        "try2",
        "execution",
        "run_results.json",
    )


def test_gold_contract_parse_failure_stops_before_test_and_records_task_artifact(
    monkeypatch,
):
    module = load_transform_module()
    commands = []
    loaded_paths = []
    pushed = {}
    ti = types.SimpleNamespace(
        dag_id="traffic_incident_transform",
        task_id="dbt_test_gold",
        try_number=2,
        xcom_pull=lambda task_ids: "snapshot-a",
        xcom_push=lambda key, value: pushed.update(key=key, value=value),
    )

    def run(command, **_kwargs):
        commands.append(command)
        if command[1] == "parse":
            return types.SimpleNamespace(
                returncode=1,
                stdout="",
                stderr="Compilation Error",
            )
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", run)
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda path: loaded_paths.append(Path(path)) or [],
    )

    with pytest.raises(FakeAirflowFailException, match="model-execution-failed"):
        module.run_dbt_phase(
            dbt_command="test",
            selection="tag:ask_seoul_traffic_transform_gold",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            fresh_parse=True,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    assert [command[1] for command in commands] == ["parse"]
    assert pushed["key"] == module.DBT_FAILURE_XCOM_KEY
    record = pushed["value"]
    assert record["dbt_artifact_path"] is None
    assert record["dbt_run_results_path"] is None
    assert record["dbt_manifest_path"] is None
    assert loaded_paths == []
    assert record["failure_classification"] == "model-execution-failed"
    assert record["traffic_snapshot_dag_run_id"] == "snapshot-a"
    assert record["run_id"] == "manual__a"
    assert record["task_id"] == "dbt_test_gold"
    assert record["try_number"] == 2
