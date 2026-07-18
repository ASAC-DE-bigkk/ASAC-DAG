import os
import sys
import types
from pathlib import Path

import pytest

from traffic_dbt_execution_test_support import (
    RAW_DBT,
    completed,
    load_execution_module,
    option,
    write_actual_artifacts,
)


def test_execution_loader_restores_existing_synthetic_module():
    name = "traffic_dbt_execution_under_test"
    sentinel = types.ModuleType("preexisting_traffic_execution")
    previous = sys.modules.get(name)
    sys.modules[name] = sentinel
    try:
        loaded = load_execution_module()

        assert loaded is not sentinel
        assert sys.modules[name] is sentinel
    finally:
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


def test_deps_actual_omits_unsupported_target_path_but_keeps_runtime_context(tmp_path):
    module = load_execution_module()
    observed = {}

    def runner(command, **kwargs):
        observed.update(command=command, kwargs=kwargs)
        return completed(command)

    execution = module.execute_dbt_phase(
        dbt_command="deps",
        selector=None,
        invocation_id="deps",
        pipeline="traffic-transform",
        run_id="manual__deps",
        task_id="dbt_deps",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    command = observed["command"]
    assert command[:2] == [RAW_DBT, "deps"]
    assert "--target-path" not in command
    assert option(command, "--log-path") == execution.paths.execution_log_path
    assert option(command, "--target") == "dev"
    assert observed["kwargs"]["env"]["DBT_PACKAGES_INSTALL_PATH"] == (
        execution.paths.packages_path
    )


def test_deps_phase_does_not_self_heal_recursively(tmp_path):
    module = load_execution_module()
    (tmp_path / "packages.yml").write_text(
        "packages:\n  - package: asac_axes\n", encoding="utf-8"
    )
    observed = []

    module.execute_dbt_phase(
        dbt_command="deps",
        selector=None,
        invocation_id="deps-no-recursion",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_deps",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=lambda command, **_kwargs: observed.append(command)
        or completed(command),
        environ={},
    )

    assert [command[1] for command in observed] == ["deps"]


def test_non_deps_phase_keeps_current_behavior_without_packages_yml(tmp_path):
    module = load_execution_module()
    observed = []

    def runner(command, **_kwargs):
        observed.append(command)
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"model.asac.silver","resource_type":"model"}\n',
            )
        write_actual_artifacts(command)
        return completed(command)

    module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        invocation_id="no-packages-yml",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[1] for command in observed] == ["ls", "run"]


def test_non_deps_phase_does_not_self_heal_when_package_sentinel_exists(tmp_path):
    module = load_execution_module()
    paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        invocation_id="sentinel-present",
        dbt_command="run",
    )
    (tmp_path / "packages.yml").write_text(
        "packages:\n  - package: asac_axes\n", encoding="utf-8"
    )
    sentinel = Path(paths.packages_path) / "asac_axes" / "dbt_project.yml"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_text("name: asac_axes\n", encoding="utf-8")
    observed = []

    def runner(command, **_kwargs):
        observed.append(command)
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"model.asac.silver","resource_type":"model"}\n',
            )
        write_actual_artifacts(command)
        return completed(command)

    module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        invocation_id="sentinel-present",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[1] for command in observed] == ["ls", "run"]


def test_non_deps_phase_self_heals_missing_packages_before_ls(tmp_path):
    module = load_execution_module()
    (tmp_path / "packages.yml").write_text(
        "packages:\n  - package: asac_axes\n", encoding="utf-8"
    )
    observed = []

    def runner(command, **kwargs):
        observed.append((command, kwargs))
        if command[1] == "deps":
            packages_path = Path(kwargs["env"]["DBT_PACKAGES_INSTALL_PATH"])
            sentinel = packages_path / "asac_axes" / "dbt_project.yml"
            sentinel.parent.mkdir(parents=True)
            sentinel.write_text("name: asac_axes\n", encoding="utf-8")
            return completed(command)
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"model.asac.silver","resource_type":"model"}\n',
            )
        write_actual_artifacts(command)
        return completed(command)

    execution = module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        invocation_id="self-heal",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[0][1] for command in observed] == ["deps", "ls", "run"]
    deps_command, deps_kwargs = observed[0]
    assert "--target-path" not in deps_command
    assert deps_kwargs["env"]["DBT_PACKAGES_INSTALL_PATH"] == execution.paths.packages_path


def test_non_deps_phase_stops_when_self_heal_deps_fails(tmp_path):
    module = load_execution_module()
    (tmp_path / "packages.yml").write_text(
        "packages:\n  - package: asac_axes\n", encoding="utf-8"
    )
    observed = []

    def runner(command, **_kwargs):
        observed.append(command)
        return completed(command, returncode=1, stderr="deps failed")

    execution = module.execute_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_traffic_transform_gold",
        invocation_id="self-heal-fails",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_test_gold",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[1] for command in observed] == ["deps"]
    assert execution.completed.returncode == 1
    assert execution.actual_attempted is False


def test_non_deps_phase_fails_closed_when_sentinel_still_missing_after_deps(tmp_path):
    module = load_execution_module()
    (tmp_path / "packages.yml").write_text(
        "packages:\n  - package: asac_axes\n", encoding="utf-8"
    )
    observed = []

    execution = module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        invocation_id="self-heal-missing-sentinel",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=lambda command, **_kwargs: observed.append(command)
        or completed(command),
        environ={},
    )

    assert [command[1] for command in observed] == ["deps"]
    assert execution.completed.returncode == 2
    assert "package sentinel missing" in execution.completed.stderr
    assert execution.actual_attempted is False


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        (
            {
                "ASK_SEOUL_DBT_OPENLINEAGE_ENABLED": "true",
                "ASK_SEOUL_DBT_OPENLINEAGE_NAMESPACE": "ask-seoul-dbt",
            },
            "ASK_SEOUL_DBT_OPENLINEAGE_URL",
        ),
        (
            {
                "ASK_SEOUL_DBT_OPENLINEAGE_ENABLED": "true",
                "ASK_SEOUL_DBT_OPENLINEAGE_URL": "http://marquez:5000",
            },
            "ASK_SEOUL_DBT_OPENLINEAGE_NAMESPACE",
        ),
    ],
)
def test_openlineage_configuration_fails_closed_before_actual(
    environment, message, tmp_path, monkeypatch
):
    module = load_execution_module()
    observed = []
    monkeypatch.setattr(module._environment, "executable_available", lambda _path: True)

    def runner(command, **_kwargs):
        observed.append(command)
        return completed(
            command,
            stdout='{"unique_id":"model.asac_seoul.silver","resource_type":"model"}\n',
        )

    with pytest.raises(RuntimeError, match=message):
        module.execute_dbt_phase(
            dbt_command="run",
            selector="selected",
            invocation_id="openlineage-config",
            pipeline="traffic-transform",
            run_id="manual__1",
            task_id="dbt_run_silver",
            try_number=1,
            target="dev",
            variables=None,
            project_dir=str(tmp_path),
            executable=RAW_DBT,
            runner=runner,
            environ=environment,
        )

    assert [command[1] for command in observed] == ["ls"]


def test_missing_dbt_ol_fails_closed_before_actual(tmp_path, monkeypatch):
    module = load_execution_module()
    observed = []
    monkeypatch.setattr(
        module._environment, "executable_available", lambda _path: False
    )

    def runner(command, **_kwargs):
        observed.append(command)
        return completed(
            command,
            stdout='{"unique_id":"model.asac_seoul.silver","resource_type":"model"}\n',
        )

    with pytest.raises(RuntimeError, match="dbt-ol"):
        module.execute_dbt_phase(
            dbt_command="run",
            selector="selected",
            invocation_id="missing-dbt-ol",
            pipeline="traffic-transform",
            run_id="manual__1",
            task_id="dbt_run_silver",
            try_number=1,
            target="dev",
            variables=None,
            project_dir=str(tmp_path),
            executable=RAW_DBT,
            runner=runner,
            environ={
                "ASK_SEOUL_DBT_OPENLINEAGE_ENABLED": "true",
                "ASK_SEOUL_DBT_OPENLINEAGE_URL": "http://marquez:5000",
                "ASK_SEOUL_DBT_OPENLINEAGE_NAMESPACE": "ask-seoul-dbt",
            },
        )

    assert [command[1] for command in observed] == ["ls"]


def test_actual_reset_removes_only_current_execution_and_reports_fresh_failure_artifact(
    tmp_path,
):
    module = load_execution_module()
    paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_test_gold",
        try_number=2,
        invocation_id="actual-reset",
        dbt_command="test",
    )
    stale = Path(paths.run_results_path)
    stale.parent.mkdir(parents=True)
    stale.write_text('{"stale":true}', encoding="utf-8")
    sibling = stale.parent.parent / "keep" / "sentinel.txt"
    sibling.parent.mkdir()
    sibling.write_text("keep", encoding="utf-8")

    def runner(command, **_kwargs):
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"test.asac_seoul.gold","resource_type":"test"}\n',
            )
        assert not stale.exists()
        write_actual_artifacts(command)
        return completed(command, returncode=1, stderr="contract failed")

    execution = module.execute_dbt_phase(
        dbt_command="test",
        selector="gold",
        invocation_id="actual-reset",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_test_gold",
        try_number=2,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert execution.completed.returncode == 1
    assert execution.actual_attempted is True
    assert execution.existing_run_results_path == paths.run_results_path
    assert execution.existing_manifest_path == paths.manifest_path
    assert sibling.read_text(encoding="utf-8") == "keep"


def test_preflight_failure_never_reports_stale_execution_artifacts(tmp_path):
    module = load_execution_module()
    paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_test_gold",
        try_number=2,
        invocation_id="preflight-failure",
        dbt_command="test",
    )
    stale = Path(paths.run_results_path)
    stale.parent.mkdir(parents=True)
    stale.write_text('{"stale":true}', encoding="utf-8")

    execution = module.execute_dbt_phase(
        dbt_command="test",
        selector="gold",
        invocation_id="preflight-failure",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_test_gold",
        try_number=2,
        target="dev",
        variables=None,
        fresh_parse=True,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=lambda command, **_kwargs: completed(
            command, returncode=1, stderr="Compilation Error"
        ),
        environ={},
    )

    assert execution.actual_attempted is False
    assert execution.existing_run_results_path is None
    assert execution.existing_manifest_path is None
    assert stale.exists()


def test_deps_success_prunes_only_same_pipeline_run_directories(tmp_path):
    module = load_execution_module()
    for root_name in ("target", "logs"):
        pipeline_root = tmp_path / root_name / "traffic-transform"
        for index, run_name in enumerate(("run-a", "run-b", "run-c"), start=1):
            run_dir = pipeline_root / run_name
            run_dir.mkdir(parents=True)
            (run_dir / "sentinel").write_text(run_name, encoding="utf-8")
            os.utime(run_dir, (index, index))
        other = tmp_path / root_name / "weather-transform" / "other-run"
        other.mkdir(parents=True)
        (other / "sentinel").write_text("other", encoding="utf-8")

    module.execute_dbt_phase(
        dbt_command="deps",
        selector=None,
        invocation_id="retention",
        pipeline="traffic-transform",
        run_id="current-run",
        task_id="dbt_deps",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=lambda command, **_kwargs: completed(command),
        environ={"ASK_SEOUL_DBT_ARTIFACT_RETENTION_RUNS": "2"},
    )

    for root_name in ("target", "logs"):
        pipeline_root = tmp_path / root_name / "traffic-transform"
        assert {path.name for path in pipeline_root.iterdir() if path.is_dir()} == {
            "current-run",
            "run-c",
        }
        assert (
            tmp_path / root_name / "weather-transform" / "other-run" / "sentinel"
        ).is_file()


@pytest.mark.parametrize("value", ["0", "-1", "not-an-int", ""])
def test_invalid_retention_configuration_fails_before_deps(value, tmp_path):
    module = load_execution_module()
    calls = []

    with pytest.raises(RuntimeError, match="ASK_SEOUL_DBT_ARTIFACT_RETENTION_RUNS"):
        module.execute_dbt_phase(
            dbt_command="deps",
            selector=None,
            invocation_id="invalid-retention",
            pipeline="traffic-transform",
            run_id="current-run",
            task_id="dbt_deps",
            try_number=1,
            target="dev",
            variables=None,
            project_dir=str(tmp_path),
            executable=RAW_DBT,
            runner=lambda command, **_kwargs: calls.append(command),
            environ={"ASK_SEOUL_DBT_ARTIFACT_RETENTION_RUNS": value},
        )

    assert calls == []
