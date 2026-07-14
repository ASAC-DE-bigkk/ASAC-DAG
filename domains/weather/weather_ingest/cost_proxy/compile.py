"""Snapshot pinning, isolated dbt compilation, and metric aggregation."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping
from pathlib import Path
import subprocess
from typing import Any
import uuid

from weather_ingest.cost_proxy.config import (
    COMPARISON_METRICS,
    DBT_BIN,
    DBT_PROJECT,
    MANIFEST_TABLE,
    CompilePaths,
    _MAX_METRICS,
    _qualified,
)
from weather_ingest.trino_query_metrics import collect_iceberg_fingerprint, sql_string


def _source_tables(case: Mapping[str, Any]) -> list[str]:
    return list(case["source_tables"])


def _fingerprint_sources(
    cursor: Any, *, catalog: str, schema: str, tables: Iterable[str]
) -> dict[str, Any]:
    return {
        table: collect_iceberg_fingerprint(cursor, _qualified(catalog, schema, table))
        for table in tables
    }


def _resolve_publishable_snapshot(
    cursor: Any,
    *,
    catalog: str,
    schema: str,
    source_id: str,
) -> str:
    """Resolve the same latest publishable Bronze run that Traffic transform pins."""
    cursor.execute(
        f"""
        SELECT CAST(dag_run_id AS varchar)
        FROM {_qualified(catalog, schema, MANIFEST_TABLE)}
        WHERE source_id = {sql_string(source_id)}
          AND status = 'SUCCESS'
          AND is_publishable
        ORDER BY CAST(event_at AS timestamp(6)) DESC, CAST(dag_run_id AS varchar) DESC
        LIMIT 1
        """
    )
    row = cursor.fetchone()
    if not row or not row[0]:
        raise RuntimeError(
            f"No publishable Bronze run is available for source: {source_id}"
        )
    return str(row[0])


def _dbt_vars_for_case(
    case: Mapping[str, Any],
    cursor: Any,
    *,
    catalog: str,
    schema: str,
) -> dict[str, str]:
    """Build only the pinned dbt vars required by a benchmark case."""
    source_id = case.get("snapshot_source_id")
    variable_name = case.get("snapshot_var")
    if source_id is None or variable_name is None:
        return {}
    return {
        str(variable_name): _resolve_publishable_snapshot(
            cursor,
            catalog=catalog,
            schema=schema,
            source_id=str(source_id),
        )
    }


def _compile_command(
    case: Mapping[str, Any],
    dbt_vars: Mapping[str, str],
    *,
    paths: CompilePaths | None = None,
) -> list[str]:
    command = [
        DBT_BIN,
        "compile",
        "--select",
        str(case["model"]),
        "--target",
        "dev",
        "--no-use-colors",
    ]
    if paths is not None:
        command.extend(
            ["--target-path", paths.target_path, "--log-path", paths.log_path]
        )
    if dbt_vars:
        command.extend(["--vars", json.dumps(dict(dbt_vars), sort_keys=True)])
    return command


def _execution_fingerprint(
    name: str,
    case: Mapping[str, Any],
    dbt_vars: Mapping[str, str],
    *,
    catalog: str,
    schema: str,
    project_dir: str | None = None,
) -> dict[str, Any]:
    """Describe the stable dev-only execution contract for one benchmark suite."""
    fingerprint: dict[str, Any] = {
        "name": name,
        "domain": case["domain"],
        "source_tables": _source_tables(case),
        "target": "dev",
        "catalog": catalog,
        "schema": schema,
        "dbt_bin": DBT_BIN,
    }
    fingerprint["project"] = project_dir or DBT_PROJECT
    fingerprint["compile_command"] = _compile_command(case, dbt_vars)
    return fingerprint


def _safe_compile_segment(value: str) -> str:
    safe = "".join(
        char if char.isascii() and (char.isalnum() or char in "._=-") else "-"
        for char in value
    )
    return safe or "unknown"


def _compile_paths(
    suite_name: str,
    *,
    invocation_id: str | None = None,
    project_dir: str | None = None,
) -> CompilePaths:
    project = Path(project_dir or DBT_PROJECT)
    suite = _safe_compile_segment(suite_name)
    invocation = _safe_compile_segment(invocation_id or uuid.uuid4().hex)
    target_path = project / "target" / "cost-proxy" / suite / invocation / "target"
    log_path = project / "logs" / "cost-proxy" / suite / invocation / "logs"
    return CompilePaths(
        target_path=str(target_path),
        log_path=str(log_path),
        manifest_path=str(target_path / "manifest.json"),
    )


def _compiled_model_code(manifest: Mapping[str, Any], configured_model: str) -> str:
    """Return SQL only when one compiled manifest node matches the contract."""
    nodes = manifest.get("nodes")
    if not isinstance(nodes, Mapping):
        raise RuntimeError(
            f"configured benchmark model missing from dbt manifest: {configured_model}"
        )
    matches = [
        node
        for node in nodes.values()
        if isinstance(node, Mapping)
        and node.get("resource_type") == "model"
        and node.get("name") == configured_model
    ]
    if not matches:
        raise RuntimeError(
            f"configured benchmark model missing from dbt manifest: {configured_model}"
        )
    if len(matches) > 1:
        raise RuntimeError(
            f"ambiguous configured benchmark model in dbt manifest: {configured_model}"
        )
    compiled_code = matches[0].get("compiled_code")
    if not isinstance(compiled_code, str) or not compiled_code.strip():
        raise RuntimeError(
            f"configured benchmark model has no compiled SQL: {configured_model}"
        )
    return compiled_code


def _compile_model(
    case: Mapping[str, Any],
    *,
    dbt_vars: Mapping[str, str] | None = None,
    suite_name: str | None = None,
    invocation_id: str | None = None,
    runner=None,
) -> str:
    project = DBT_PROJECT
    paths = _compile_paths(
        suite_name or str(case["model"]),
        invocation_id=invocation_id,
        project_dir=project,
    )
    Path(paths.target_path).mkdir(parents=True, exist_ok=True)
    Path(paths.log_path).mkdir(parents=True, exist_ok=True)
    Path(paths.manifest_path).unlink(missing_ok=True)
    env = {
        **os.environ,
        "DBT_PROJECT_DIR": project,
        "DBT_PROFILES_DIR": project,
    }
    (runner or subprocess.run)(
        _compile_command(case, dbt_vars or {}, paths=paths),
        cwd=project,
        env=env,
        check=True,
        text=True,
    )
    manifest_path = Path(paths.manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, Mapping):
        raise RuntimeError("dbt manifest must be a JSON object")
    return _compiled_model_code(manifest, str(case["model"]))


def _aggregate_query_metrics(
    records: list[dict[str, Any]],
) -> dict[str, int | float | None]:
    """Aggregate all query records in one repeat without filling absent data with zero."""
    result: dict[str, int | float | None] = {}
    for metric in COMPARISON_METRICS:
        values = [record.get(metric) for record in records]
        if not values or any(value is None for value in values):
            result[metric] = None
            continue
        numeric_values = [value for value in values if isinstance(value, (int, float))]
        if len(numeric_values) != len(values):
            result[metric] = None
        elif metric in _MAX_METRICS:
            result[metric] = max(numeric_values)
        else:
            result[metric] = sum(numeric_values)
    return result
