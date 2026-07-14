import os
import sys
import types
from pathlib import Path

import pytest

from weather_dbt_execution_test_support import (
    RAW_DBT,
    completed,
    load_execution_module,
    write_artifacts,
)


def test_execution_loader_restores_existing_synthetic_module():
    name = "weather_dbt_execution_under_test"
    sentinel = types.ModuleType("preexisting_weather_execution")
    missing = object()
    previous = sys.modules.get(name, missing)
    sys.modules[name] = sentinel
    try:
        loaded = load_execution_module()

        assert loaded is not sentinel
        assert sys.modules[name] is sentinel
    finally:
        if previous is missing:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


def test_weather_preflight_failure_ignores_stale_execution_artifacts(tmp_path):
    module = load_execution_module()
    paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_test_gold",
        try_number=2,
        dbt_command="test",
    )
    stale = Path(paths.run_results_path)
    stale.parent.mkdir(parents=True)
    stale.write_text('{"stale":true}', encoding="utf-8")

    execution = module.execute_dbt_phase(
        dbt_command="test",
        selection="tag:gold",
        pipeline="weather-transform",
        run_id="scheduled__1",
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


def test_weather_actual_resets_only_current_execution_directory(tmp_path):
    module = load_execution_module()
    paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_test_gold",
        try_number=2,
        dbt_command="test",
    )
    stale = Path(paths.run_results_path)
    stale.parent.mkdir(parents=True)
    stale.write_text('{"stale":true}', encoding="utf-8")
    sibling = stale.parent.parent / "keep" / "sentinel"
    sibling.parent.mkdir()
    sibling.write_text("keep", encoding="utf-8")

    def runner(command, **_kwargs):
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"test.asac_seoul.gold","resource_type":"test"}\n',
            )
        assert not stale.exists()
        write_artifacts(command)
        return completed(command, returncode=1)

    execution = module.execute_dbt_phase(
        dbt_command="test",
        selection="tag:gold",
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_test_gold",
        try_number=2,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert execution.existing_run_results_path == paths.run_results_path
    assert execution.existing_manifest_path == paths.manifest_path
    assert sibling.is_file()


def test_weather_deps_retention_preserves_current_and_other_pipeline(tmp_path):
    module = load_execution_module()
    for root_name in ("target", "logs"):
        pipeline_root = tmp_path / root_name / "weather-transform"
        for index, run_name in enumerate(("run-a", "run-b", "run-c"), start=1):
            run_dir = pipeline_root / run_name
            run_dir.mkdir(parents=True)
            os.utime(run_dir, (index, index))
        other = tmp_path / root_name / "traffic-transform" / "other-run"
        other.mkdir(parents=True)

    module.execute_dbt_phase(
        dbt_command="deps",
        selection=None,
        pipeline="weather-transform",
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
        assert {
            path.name
            for path in (tmp_path / root_name / "weather-transform").iterdir()
            if path.is_dir()
        } == {"current-run", "run-c"}
        assert (tmp_path / root_name / "traffic-transform" / "other-run").is_dir()


@pytest.mark.parametrize("value", ["0", "bad"])
def test_weather_invalid_retention_fails_fast(value, tmp_path):
    module = load_execution_module()
    calls = []

    with pytest.raises(RuntimeError, match="ASK_SEOUL_DBT_ARTIFACT_RETENTION_RUNS"):
        module.execute_dbt_phase(
            dbt_command="deps",
            selection=None,
            pipeline="weather-transform",
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
