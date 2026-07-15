from __future__ import annotations

import importlib
from pathlib import Path
import subprocess

import pytest

from traffic_dbt_execution_test_support import load_execution_module


def _paths_module():
    load_execution_module()
    return importlib.import_module("traffic_ingest._dbt_execution.paths")


def _directory_symlink(link: Path, target: Path) -> None:
    link.parent.mkdir(parents=True, exist_ok=True)
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError as symlink_error:  # pragma: no cover - Windows fallback
        completed = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target)],
            capture_output=True,
            check=False,
            text=True,
        )
        if completed.returncode != 0:
            pytest.skip(
                "directory links are unavailable: "
                f"symlink={symlink_error}; junction={completed.stderr}"
            )


def test_traffic_cleanup_rejects_candidate_outside_trusted_artifact_root(tmp_path):
    module = _paths_module()
    trusted_root = tmp_path / "target"
    allowed_parent = tmp_path / "outside"
    candidate = allowed_parent / "victim"
    candidate.mkdir(parents=True)

    with pytest.raises(RuntimeError, match="trusted artifact root"):
        module._safe_remove_tree(
            candidate,
            allowed_parent=allowed_parent,
            trusted_root=trusted_root,
        )

    assert candidate.is_dir()


def test_traffic_cleanup_rejects_dotdot_lexical_disguise(tmp_path):
    module = _paths_module()
    trusted_root = tmp_path / "target"
    allowed_parent = trusted_root / "traffic-transform"
    candidate = allowed_parent / "victim"
    candidate.mkdir(parents=True)
    disguised = allowed_parent / "nested" / ".." / "victim"

    with pytest.raises(RuntimeError, match="lexical"):
        module._safe_remove_tree(
            disguised,
            allowed_parent=allowed_parent,
            trusted_root=trusted_root,
        )

    assert candidate.is_dir()


def test_traffic_reset_rejects_ancestor_symlink_escape(tmp_path):
    execution = load_execution_module()
    module = _paths_module()
    paths = execution.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id="scheduled__1",
        task_id="dbt_run_silver",
        try_number=1,
        invocation_id="ancestor-symlink",
        dbt_command="run",
    )
    outside_pipeline = tmp_path / "outside-target"
    escaped_execution = (
        outside_pipeline
        / "scheduled__1"
        / "dbt_run_silver"
        / "try1"
        / "ancestor-symlink"
        / "execution"
    )
    escaped_execution.mkdir(parents=True)
    sentinel = escaped_execution / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")
    _directory_symlink(tmp_path / "target" / "traffic-transform", outside_pipeline)

    with pytest.raises(RuntimeError, match="symlink|resolved outside"):
        module.reset_execution_directories(paths)

    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_traffic_retention_rejects_pipeline_ancestor_symlink_escape(tmp_path):
    module = _paths_module()
    outside_pipeline = tmp_path / "outside-retention"
    sentinels = []
    for run_name in ("run-a", "run-b"):
        sentinel = outside_pipeline / run_name / "keep.txt"
        sentinel.parent.mkdir(parents=True)
        sentinel.write_text("keep", encoding="utf-8")
        sentinels.append(sentinel)
    _directory_symlink(tmp_path / "target" / "traffic-transform", outside_pipeline)

    with pytest.raises(RuntimeError, match="symlink|resolved outside"):
        module.prune_pipeline_runs(
            project_dir=str(tmp_path),
            pipeline="traffic-transform",
            run_id="current-run",
            retention_runs=1,
        )

    assert all(path.read_text(encoding="utf-8") == "keep" for path in sentinels)


def test_traffic_reset_rejects_trusted_artifact_root_symlink(tmp_path):
    execution = load_execution_module()
    module = _paths_module()
    paths = execution.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id="scheduled__root",
        task_id="dbt_run_silver",
        try_number=1,
        invocation_id="root-symlink",
        dbt_command="run",
    )
    outside_target = tmp_path / "outside-target-root"
    escaped_execution = (
        outside_target
        / "traffic-transform"
        / "scheduled__root"
        / "dbt_run_silver"
        / "try1"
        / "root-symlink"
        / "execution"
    )
    escaped_execution.mkdir(parents=True)
    sentinel = escaped_execution / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")
    _directory_symlink(tmp_path / "target", outside_target)

    with pytest.raises(RuntimeError, match="symlink|reparse"):
        module.reset_execution_directories(paths)

    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_traffic_retention_rejects_trusted_artifact_root_symlink(tmp_path):
    module = _paths_module()
    outside_target = tmp_path / "outside-retention-root"
    sentinels = []
    for run_name in ("run-a", "run-b"):
        sentinel = outside_target / "traffic-transform" / run_name / "keep.txt"
        sentinel.parent.mkdir(parents=True)
        sentinel.write_text("keep", encoding="utf-8")
        sentinels.append(sentinel)
    _directory_symlink(tmp_path / "target", outside_target)

    with pytest.raises(RuntimeError, match="symlink|reparse"):
        module.prune_pipeline_runs(
            project_dir=str(tmp_path),
            pipeline="traffic-transform",
            run_id="current-run",
            retention_runs=1,
        )

    assert all(path.read_text(encoding="utf-8") == "keep" for path in sentinels)
