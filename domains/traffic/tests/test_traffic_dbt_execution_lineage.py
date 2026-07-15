import json
import os
from pathlib import Path

import pytest

from traffic_dbt_execution_test_support import (
    DBT_OL,
    RAW_DBT,
    completed,
    load_execution_module,
    option,
    write_actual_artifacts,
)


def test_attempt_paths_separate_preflight_execution_and_command_artifacts(tmp_path):
    module = load_execution_module()
    run_paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id="manual/run",
        task_id="dbt_run_silver",
        try_number=1,
        invocation_id="run",
        dbt_command="run",
    )
    source_paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id="manual/run",
        task_id="dbt_source_freshness",
        try_number=1,
        invocation_id="source",
        dbt_command="source freshness",
    )
    deps_paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id="manual/run",
        task_id="dbt_deps",
        try_number=1,
        invocation_id="deps",
        dbt_command="deps",
    )

    assert run_paths.preflight_target_path != run_paths.execution_target_path
    assert run_paths.preflight_log_path != run_paths.execution_log_path
    assert run_paths.preflight_target_path.endswith("try1/run/preflight")
    assert run_paths.execution_target_path.endswith("try1/run/execution")
    package_parts = Path(run_paths.packages_path).relative_to(tmp_path).parts
    assert package_parts[:2] == ("target", "traffic-transform")
    assert package_parts[-1] == "dbt_packages"
    assert run_paths.run_results_path.endswith("execution/run_results.json")
    assert run_paths.sources_path is None
    assert run_paths.manifest_path.endswith("execution/manifest.json")
    assert source_paths.run_results_path is None
    assert source_paths.sources_path.endswith("execution/sources.json")
    assert source_paths.manifest_path.endswith("execution/manifest.json")
    assert deps_paths.run_results_path is None
    assert deps_paths.sources_path is None
    assert deps_paths.manifest_path is None


def test_attempt_paths_falls_back_to_unknown_for_blank_segments(tmp_path):
    module = load_execution_module()
    paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id="",
        task_id=None,
        try_number=None,
        invocation_id="unknown-segments",
        dbt_command="run",
    )

    assert (
        "/unknown/unknown/tryunknown/unknown-segments/"
        in paths.execution_target_path.replace("\\", "/")
    )


def test_attempt_paths_do_not_collapse_distinct_run_ids(tmp_path):
    module = load_execution_module()

    slash = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id="manual/run",
        task_id="dbt_run",
        try_number=1,
        invocation_id="slash",
        dbt_command="run",
    )
    dash = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id="manual-run",
        task_id="dbt_run",
        try_number=1,
        invocation_id="dash",
        dbt_command="run",
    )

    assert slash.execution_target_path != dash.execution_target_path
    assert slash.packages_path != dash.packages_path


@pytest.mark.parametrize("reserved", [".", ".."])
def test_attempt_paths_reject_reserved_segments(tmp_path, reserved):
    module = load_execution_module()

    with pytest.raises(ValueError, match="reserved dbt path segment"):
        module.attempt_paths(
            project_dir=str(tmp_path),
            pipeline="traffic-transform",
            run_id=reserved,
            task_id="dbt_run",
            try_number=1,
            invocation_id="reserved",
            dbt_command="run",
        )


def test_attempt_paths_bound_unicode_and_long_segments_without_collisions(tmp_path):
    module = load_execution_module()
    prefix = "서울-" + ("x" * 300)

    first = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id=prefix + "-first",
        task_id="dbt_run",
        try_number=1,
        invocation_id="first",
        dbt_command="run",
    )
    second = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id=prefix + "-second",
        task_id="dbt_run",
        try_number=1,
        invocation_id="second",
        dbt_command="run",
    )

    assert first.execution_target_path != second.execution_target_path
    relative_parts = Path(first.execution_target_path).relative_to(tmp_path).parts
    assert max(map(len, relative_parts)) <= 96


def test_attempt_paths_remain_inside_project_artifact_roots(tmp_path):
    module = load_execution_module()
    paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic/transform",
        run_id="manual/run",
        task_id="dbt/run",
        try_number=1,
        invocation_id="inside-roots",
        dbt_command="run",
    )

    target_root = (tmp_path / "target").resolve()
    log_root = (tmp_path / "logs").resolve()
    for value in (
        paths.preflight_target_path,
        paths.execution_target_path,
        paths.packages_path,
        paths.run_results_path,
        paths.manifest_path,
    ):
        assert Path(value).resolve().is_relative_to(target_root)
    for value in (paths.preflight_log_path, paths.execution_log_path):
        assert Path(value).resolve().is_relative_to(log_root)


def test_openlineage_enabled_routes_only_materialization_to_dbt_ol(
    tmp_path, monkeypatch
):
    module = load_execution_module()
    observed = []
    monkeypatch.setattr(
        module._environment, "executable_available", lambda path: path == DBT_OL
    )

    def runner(command, **kwargs):
        observed.append((command, kwargs))
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"model.asac_seoul.silver","resource_type":"model"}\n',
            )
        write_actual_artifacts(command)
        return completed(command)

    execution = module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        invocation_id="silver",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables='{"snapshot":"one"}',
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={
            "PATH": "/usr/bin",
            "ASK_SEOUL_DBT_OPENLINEAGE_ENABLED": "true",
            "ASK_SEOUL_DBT_OPENLINEAGE_URL": "http://marquez:5000",
            "ASK_SEOUL_DBT_OPENLINEAGE_ENDPOINT": "api/v1/lineage",
            "ASK_SEOUL_DBT_OPENLINEAGE_NAMESPACE": "ask-seoul-dbt",
            "OPENLINEAGE_PARENT_ID": "parent-run-id",
            "OPENLINEAGE_URL": "must-not-leak-to-raw",
        },
    )

    assert [command[0] for command, _kwargs in observed] == [RAW_DBT, DBT_OL]
    preflight_command, preflight_kwargs = observed[0]
    actual_command, actual_kwargs = observed[1]
    assert preflight_command[1] == "ls"
    assert actual_command[1] == "run"
    assert actual_command[1:] == [
        "run",
        "--selector",
        "ask_seoul_traffic_transform_silver",
        "--target",
        "dev",
        "--no-use-colors",
        "--vars",
        '{"snapshot":"one"}',
        "--target-path",
        execution.paths.execution_target_path,
        "--log-path",
        execution.paths.execution_log_path,
    ]
    assert option(preflight_command, "--target-path") == (
        execution.paths.preflight_target_path
    )
    assert "OPENLINEAGE_URL" not in preflight_kwargs["env"]
    assert "OPENLINEAGE_NAMESPACE" not in preflight_kwargs["env"]
    actual_env = actual_kwargs["env"]
    assert actual_env["OPENLINEAGE_URL"] == "http://marquez:5000"
    assert actual_env["OPENLINEAGE_ENDPOINT"] == "api/v1/lineage"
    assert actual_env["OPENLINEAGE_NAMESPACE"] == "ask-seoul-dbt"
    assert actual_env["OPENLINEAGE_DBT_JOB_NAME"] == (
        "traffic-transform.dbt_run_silver"
    )
    assert actual_env["OPENLINEAGE_PARENT_ID"] == "parent-run-id"
    assert actual_env["OPENLINEAGE__FACETS__SOURCE_CODE_LOCATION__DISABLED"] == "true"
    assert actual_env["PATH"].split(os.pathsep)[0] == "/home/airflow/dbt-venv/bin"
    assert execution.existing_run_results_path == execution.paths.run_results_path
    assert execution.existing_manifest_path == execution.paths.manifest_path
    assert execution.missing_expected_artifacts == ()


@pytest.mark.parametrize("dbt_command", ["seed", "run", "test", "build", "snapshot"])
def test_materialization_commands_use_dbt_ol_when_enabled(
    dbt_command, tmp_path, monkeypatch
):
    module = load_execution_module()
    observed = []
    monkeypatch.setattr(module._environment, "executable_available", lambda _path: True)

    def runner(command, **_kwargs):
        observed.append(command)
        if command[1] == "ls":
            resource_type = "snapshot" if dbt_command == "snapshot" else "model"
            return completed(
                command,
                stdout=json.dumps(
                    {
                        "unique_id": f"{resource_type}.asac_seoul.selected",
                        "resource_type": resource_type,
                    }
                )
                + "\n",
            )
        return completed(command)

    module.execute_dbt_phase(
        dbt_command=dbt_command,
        selector="selected",
        invocation_id=f"{dbt_command}-materialization",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id=f"dbt_{dbt_command}",
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

    assert observed[-1][0] == DBT_OL
    assert observed[-1][1] == dbt_command


@pytest.mark.parametrize("dbt_command", ["deps", "source freshness"])
def test_non_materialization_commands_stay_on_raw_dbt(
    dbt_command, tmp_path, monkeypatch
):
    module = load_execution_module()
    observed = []

    def unavailable_must_not_be_checked(_path):
        raise AssertionError("dbt-ol availability is irrelevant to raw dbt phases")

    monkeypatch.setattr(
        module._environment,
        "executable_available",
        unavailable_must_not_be_checked,
    )

    def runner(command, **_kwargs):
        observed.append(command)
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"source.asac_seoul.raw","resource_type":"source"}\n',
            )
        if dbt_command == "source freshness":
            write_actual_artifacts(command, source_freshness=True)
        return completed(command)

    execution = module.execute_dbt_phase(
        dbt_command=dbt_command,
        selector=("source" if dbt_command == "source freshness" else None),
        invocation_id=f"{dbt_command}-raw",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_raw_phase",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={"ASK_SEOUL_DBT_OPENLINEAGE_ENABLED": "true"},
    )

    assert all(command[0] == RAW_DBT for command in observed)
    if dbt_command == "source freshness":
        assert execution.existing_sources_path == execution.paths.sources_path
        assert execution.existing_run_results_path is None
    else:
        assert execution.paths.manifest_path is None


def test_named_selector_is_used_for_preflight_and_actual_without_indirect_selection(
    tmp_path,
):
    module = load_execution_module()
    observed = []

    def runner(command, **_kwargs):
        observed.append(command)
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"model.asac_seoul.silver","resource_type":"model"}\n',
            )
        write_actual_artifacts(command)
        return completed(command)

    module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        invocation_id="silver-run",
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

    assert [option(command, "--selector") for command in observed] == [
        "ask_seoul_traffic_transform_silver",
        "ask_seoul_traffic_transform_silver",
    ]
    assert all("--select" not in command for command in observed)
    assert all("--indirect-selection=buildable" not in command for command in observed)


@pytest.mark.parametrize("selector", ["tag:selected", "models/traffic", " "])
def test_invalid_selector_fails_before_runner(selector, tmp_path):
    module = load_execution_module()
    observed = []

    with pytest.raises(ValueError, match="named dbt selector"):
        module.execute_dbt_phase(
            dbt_command="run",
            selector=selector,
            invocation_id="invalid-selector",
            pipeline="traffic-transform",
            run_id="manual__1",
            task_id="dbt_run_silver",
            try_number=1,
            target="dev",
            variables=None,
            project_dir=str(tmp_path),
            executable=RAW_DBT,
            runner=lambda command, **_kwargs: observed.append(command),
            environ={},
        )

    assert observed == []


def test_threads_apply_only_to_actual_materialization(tmp_path):
    module = load_execution_module()
    observed = []

    def runner(command, **_kwargs):
        observed.append(command)
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"model.asac_seoul.silver","resource_type":"model"}\n',
            )
        write_actual_artifacts(command)
        return completed(command)

    module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        invocation_id="threaded-run",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        threads=3,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert "--threads" not in observed[0]
    assert option(observed[1], "--threads") == "3"


def test_invocation_identity_separates_artifact_paths(tmp_path):
    module = load_execution_module()
    common = {
        "project_dir": str(tmp_path),
        "pipeline": "traffic-transform",
        "run_id": "manual__1",
        "task_id": "dbt_run_silver",
        "try_number": 1,
        "dbt_command": "run",
    }

    first = module.attempt_paths(invocation_id="first", **common)
    second = module.attempt_paths(invocation_id="second", **common)

    assert first.execution_target_path != second.execution_target_path
    assert first.execution_log_path != second.execution_log_path
