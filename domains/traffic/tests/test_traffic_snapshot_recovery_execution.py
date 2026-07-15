import types
from pathlib import Path

import pytest

from traffic_snapshot_recovery_test_support import (
    FakeAirflowFailException,
    load_recovery_module,
    write_materialization_artifacts,
)
from traffic_snapshot_recovery_test_support import (
    restore_airflow_modules_after_dag_import,  # noqa: F401
)


def test_recovery_dbt_phase_scopes_its_artifact_and_pins_the_validated_snapshot(
    tmp_path, monkeypatch
):
    module = load_recovery_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "dbt"))

    class TaskInstance:
        task_id = "dbt_run_recovery_silver"
        try_number = 2

        def xcom_pull(self, *, task_ids):
            assert task_ids == "validate_publishable_snapshot"
            return "historical-run"

    observed = []

    class Completed:
        returncode = 0
        stdout = "dbt succeeded\n"
        stderr = ""

    def fake_run(command, **kwargs):
        observed.append((command, kwargs))
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"model.asac_seoul.recovery_silver","resource_type":"model"}\n',
                stderr="",
            )
        write_materialization_artifacts(command)
        return Completed()

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    result = module.run_recovery_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_recovery_silver",
        snapshot_task_id="validate_publishable_snapshot",
        recovery_silver_persisted=False,
        ti=TaskInstance(),
        run_id="manual__recovery",
        params={"target": "dev"},
    )

    expected_paths = module.traffic_dbt.attempt_paths(
        project_dir=module.DBT_PROJECT,
        pipeline="traffic-snapshot-recovery",
        run_id="manual__recovery",
        task_id="dbt_run_recovery_silver",
        try_number=2,
        invocation_id="dbt_run_recovery_silver",
        dbt_command="run",
    )
    assert result == {
        "status": "success",
        "run_results_path": expected_paths.run_results_path,
        "sources_path": None,
        "manifest_path": expected_paths.manifest_path,
        "selected_unique_ids": ["model.asac_seoul.recovery_silver"],
    }
    ls_command, run_command = [entry[0] for entry in observed]
    assert ls_command[:5] == [
        "/home/airflow/dbt-venv/bin/dbt",
        "ls",
        "--resource-type",
        "model",
        "--selector",
    ]
    assert "ask_seoul_traffic_recovery_silver" in ls_command
    assert run_command[:4] == [
        "/home/airflow/dbt-venv/bin/dbt",
        "run",
        "--selector",
        "ask_seoul_traffic_recovery_silver",
    ]
    assert run_command[4:] == [
        "--target",
        "dev",
        "--no-use-colors",
        "--vars",
        '{"traffic_snapshot_dag_run_id": "historical-run"}',
        "--target-path",
        (
            f"{module.DBT_PROJECT.replace(chr(92), '/')}/target/"
            "traffic-snapshot-recovery/manual__recovery/"
            "dbt_run_recovery_silver/try2/dbt_run_recovery_silver/execution"
        ),
        "--log-path",
        (
            f"{module.DBT_PROJECT.replace(chr(92), '/')}/logs/"
            "traffic-snapshot-recovery/manual__recovery/"
            "dbt_run_recovery_silver/try2/dbt_run_recovery_silver/execution"
        ),
    ]
    assert observed[-1][1]["cwd"] == module.DBT_PROJECT
    assert observed[-1][1]["check"] is False
    ls_target = Path(ls_command[ls_command.index("--target-path") + 1])
    run_target = Path(run_command[run_command.index("--target-path") + 1])
    assert ls_target.name == "preflight"
    assert run_target.name == "execution"
    assert ls_target.parent == run_target.parent


def test_recovery_dbt_contract_failure_records_the_recovery_snapshot(
    tmp_path, monkeypatch
):
    module = load_recovery_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "dbt"))

    class TaskInstance:
        dag_id = "traffic_snapshot_recovery"
        task_id = "dbt_test_recovery_silver"
        try_number = 1

        def __init__(self):
            self.pushed = {}

        def xcom_pull(self, *, task_ids):
            assert task_ids == "validate_publishable_snapshot"
            return "historical-run"

        def xcom_push(self, *, key, value):
            self.pushed[key] = value

    class Completed:
        returncode = 1
        stdout = "dbt failed\n"
        stderr = ""

    ti = TaskInstance()

    def fail_contract_after_preflight(command, **_kwargs):
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"test.asac_seoul.recovery_silver","resource_type":"test"}\n',
                stderr="",
            )
        write_materialization_artifacts(command)
        return Completed()

    monkeypatch.setattr(module.subprocess, "run", fail_contract_after_preflight)
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda _path: [
            {
                "unique_id": "test.traffic.assert_recovery_silver_traffic_snapshot_matches_bronze",
                "status": "fail",
                "failures": 1,
            }
        ],
    )

    with pytest.raises(FakeAirflowFailException, match="data-contract-violation"):
        module.run_recovery_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_recovery_silver",
            snapshot_task_id="validate_publishable_snapshot",
            recovery_silver_persisted=True,
            ti=ti,
            run_id="manual__recovery",
            params={"target": "dev"},
        )

    record = ti.pushed[module.DBT_FAILURE_XCOM_KEY]
    assert record["dag_id"] == "traffic_snapshot_recovery"
    assert record["traffic_snapshot_dag_run_id"] == "historical-run"
    assert record["failure_classification"] == "data-contract-violation"
    assert record["silver_persisted"] is True
    assert "traffic-snapshot-recovery" in record["dbt_artifact_path"]
    assert record["dbt_run_results_path"] == record["dbt_artifact_path"]
    assert record["dbt_manifest_path"].endswith("/manifest.json")
