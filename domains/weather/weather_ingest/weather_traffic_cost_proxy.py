"""Read-only Weather/Traffic Trino cost-proxy benchmark.

The benchmark deliberately measures Trino/Iceberg/Airflow-adjacent proxy
metrics, not Cloudflare billing.  It compiles dbt without running models and
executes only ``EXPLAIN ANALYZE`` or the existing read-only watchdog queries.
Every suite records Iceberg metadata fingerprints before and after its repeats;
comparisons are rejected when input snapshots do not match.
"""
from __future__ import annotations

import argparse
import contextlib
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Iterable, Mapping


DAGS_ROOT = Path(__file__).resolve().parents[3]
for import_path in (
    DAGS_ROOT,
    DAGS_ROOT / "domains" / "weather",
    DAGS_ROOT / "domains" / "traffic",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from weather_ingest.trino_query_metrics import (  # noqa: E402
    TelemetryCursor,
    collect_iceberg_fingerprint,
    sql_string,
)


DBT_BIN = os.environ.get("DBT_BIN", "/home/airflow/dbt-venv/bin/dbt")
MIN_REPEAT = 3
CASES: dict[str, dict[str, Any]] = {
    "weather_silver": {
        "domain": "weather",
        "project": "/opt/airflow/dbt/domains/weather",
        "model": "silver_kma_vilage_fcst",
        "source_tables": [
            "bronze_kma_vilage_fcst",
            "bronze_collection_run_manifest",
        ],
    },
    "traffic_silver": {
        "domain": "traffic",
        "project": "/opt/airflow/dbt/domains/traffic",
        "model": "silver_seoul_traffic_incident",
        "snapshot_source_id": "seoul_traffic_incident",
        "snapshot_var": "traffic_snapshot_dag_run_id",
        "source_tables": [
            "bronze_seoul_traffic_incident",
            "bronze_seoul_traffic_incident_request_audit",
            "bronze_collection_run_manifest",
        ],
    },
    "weather_gold": {
        "domain": "weather",
        "project": "/opt/airflow/dbt/domains/weather",
        "model": "gold_weather_forecast_by_place",
        "source_tables": [
            "bronze_kma_vilage_fcst",
            "bronze_collection_run_manifest",
        ],
    },
    "traffic_gold": {
        "domain": "traffic",
        "project": "/opt/airflow/dbt/domains/traffic",
        "model": "gold_traffic_incident_current_by_admin_dong_hourly",
        "snapshot_source_id": "seoul_traffic_incident",
        "snapshot_var": "traffic_snapshot_dag_run_id",
        "source_tables": [
            "bronze_seoul_traffic_incident",
            "bronze_seoul_traffic_incident_request_audit",
            "bronze_collection_run_manifest",
        ],
    },
    "weather_watchdog": {"domain": "weather", "report": "weather"},
    "traffic_watchdog": {"domain": "traffic", "report": "traffic"},
}
WATCHDOG_SOURCE_TABLES = {
    "weather": ["bronze_kma_vilage_fcst", "bronze_collection_run_manifest"],
    "traffic": [
        "bronze_seoul_traffic_incident",
        "bronze_seoul_traffic_incident_request_audit",
        "bronze_collection_run_manifest",
    ],
}
COMPARISON_METRICS = (
    "physical_input_bytes",
    "cpu_time_ms",
    "physical_written_bytes",
    "spilled_bytes",
    "peak_user_memory_bytes",
    "wall_time_ms",
)
_MAX_METRICS = {"peak_user_memory_bytes"}


def median_or_none(values: list[int | float | None]) -> int | float | None:
    """Return a median only if every repeat exposed a real value."""
    usable = sorted(value for value in values if value is not None)
    if len(usable) != len(values) or not usable:
        return None
    middle = len(usable) // 2
    return usable[middle] if len(usable) % 2 else (usable[middle - 1] + usable[middle]) / 2


def _target(env: Mapping[str, str] = os.environ) -> str:
    return env.get("ASK_SEOUL_TARGET", env.get("DBT_TARGET", "prod"))


def _catalog(env: Mapping[str, str] = os.environ) -> str:
    if _target(env) == "dev":
        return env.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    return env.get("TRINO_ICEBERG_CATALOG", "iceberg")


def _schema(env: Mapping[str, str] = os.environ) -> str:
    return env.get("ASK_SEOUL_SCHEMA", "ask_seoul")


def _ensure_dev_target(env: Mapping[str, str] = os.environ) -> None:
    if _target(env) != "dev":
        raise RuntimeError("cost-proxy collect is limited to ASK_SEOUL_TARGET=dev")


def _qualified(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _source_tables(case: Mapping[str, Any]) -> list[str]:
    if "source_tables" in case:
        return list(case["source_tables"])
    return list(WATCHDOG_SOURCE_TABLES[case["report"]])


def _fingerprint_sources(cursor: Any, *, catalog: str, schema: str, tables: Iterable[str]) -> dict[str, Any]:
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
        FROM {_qualified(catalog, schema, 'bronze_collection_run_manifest')}
        WHERE source_id = {sql_string(source_id)}
          AND status = 'SUCCESS'
          AND is_publishable
        ORDER BY CAST(event_at AS timestamp(6)) DESC, CAST(dag_run_id AS varchar) DESC
        LIMIT 1
        """
    )
    row = cursor.fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"No publishable Bronze run is available for source: {source_id}")
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


def _compile_command(case: Mapping[str, Any], dbt_vars: Mapping[str, str]) -> list[str]:
    command = [
        DBT_BIN,
        "compile",
        "--select",
        str(case["model"]),
        "--target",
        "dev",
        "--no-use-colors",
    ]
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
    if "model" in case:
        fingerprint["compile_command"] = _compile_command(case, dbt_vars)
    else:
        fingerprint["report"] = str(case["report"])
    return fingerprint


def _compile_model(case: Mapping[str, Any], *, dbt_vars: Mapping[str, str] | None = None) -> str:
    project = str(case["project"])
    env = {
        **os.environ,
        "DBT_PROJECT_DIR": project,
        "DBT_PROFILES_DIR": project,
    }
    subprocess.run(
        _compile_command(case, dbt_vars or {}),
        cwd=project,
        env=env,
        check=True,
        text=True,
    )
    manifest_path = Path(project) / "target" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for node in (manifest.get("nodes") or {}).values():
        if node.get("resource_type") == "model" and node.get("name") == case["model"]:
            compiled_code = node.get("compiled_code")
            if compiled_code:
                return str(compiled_code)
    raise RuntimeError(f"compiled dbt model not found: {case['model']}")


def _aggregate_query_metrics(records: list[dict[str, Any]]) -> dict[str, int | float | None]:
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


def _execute_dbt_case(telemetry: TelemetryCursor, compiled_code: str) -> list[dict[str, Any]]:
    start = len(telemetry.records)
    telemetry.execute("EXPLAIN ANALYZE " + compiled_code)
    telemetry.fetchall()
    return telemetry.records[start:]


def _execute_watchdog_case(telemetry: TelemetryCursor, report_name: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    start = len(telemetry.records)
    if report_name == "weather":
        from weather_ingest.reliability_report import build_weather_reliability_report

        report = build_weather_reliability_report(cursor=telemetry)
    elif report_name == "traffic":
        from traffic_ingest.reliability_report import build_traffic_reliability_report

        report = build_traffic_reliability_report(cursor=telemetry)
    else:  # pragma: no cover - guarded by CASES
        raise ValueError(f"unsupported watchdog report: {report_name}")
    return report, telemetry.records[start:]


def _collect_suite(
    name: str,
    case: Mapping[str, Any],
    telemetry: TelemetryCursor,
    metadata_cursor: Any,
    *,
    catalog: str,
    schema: str,
    repeat: int,
) -> dict[str, Any]:
    tables = _source_tables(case)
    before_fingerprint = _fingerprint_sources(metadata_cursor, catalog=catalog, schema=schema, tables=tables)
    dbt_vars = _dbt_vars_for_case(
        case,
        metadata_cursor,
        catalog=catalog,
        schema=schema,
    ) if "model" in case else {}
    execution_fingerprint = _execution_fingerprint(
        name,
        case,
        dbt_vars,
        catalog=catalog,
        schema=schema,
    )
    compiled_code = _compile_model(case, dbt_vars=dbt_vars) if "model" in case else None
    runs: list[dict[str, Any]] = []
    for iteration in range(1, repeat + 1):
        if compiled_code is not None:
            records = _execute_dbt_case(telemetry, compiled_code)
            report_status = None
        else:
            report, records = _execute_watchdog_case(telemetry, str(case["report"]))
            report_status = report.get("status")
        runs.append(
            {
                "iteration": iteration,
                "metrics": _aggregate_query_metrics(records),
                "queries": records,
                "report_status": report_status,
            }
        )
    after_fingerprint = _fingerprint_sources(metadata_cursor, catalog=catalog, schema=schema, tables=tables)
    return {
        "name": name,
        "domain": case["domain"],
        "source_tables": tables,
        "dbt_vars": dbt_vars,
        "execution_fingerprint": execution_fingerprint,
        "fingerprints": {"before": before_fingerprint, "after": after_fingerprint},
        "comparable_within_run": before_fingerprint == after_fingerprint,
        "runs": runs,
    }


def _connect_cursors(*, catalog: str, schema: str) -> tuple[Any, Any, Any]:
    import trino.dbapi

    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog,
        schema=schema,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return connection, connection.cursor(), connection.cursor()


def collect_bundle(label: str, repeat: int) -> dict[str, Any]:
    """Collect one dev-only benchmark bundle.  This never runs dbt models."""
    _ensure_dev_target()
    if repeat < MIN_REPEAT:
        raise ValueError(f"repeat must be at least {MIN_REPEAT}")
    catalog = _catalog()
    schema = _schema()
    connection, workload_cursor, metadata_cursor = _connect_cursors(catalog=catalog, schema=schema)
    try:
        telemetry = TelemetryCursor(workload_cursor, metadata_cursor)
        suites = [
            _collect_suite(
                name,
                case,
                telemetry,
                metadata_cursor,
                catalog=catalog,
                schema=schema,
                repeat=repeat,
            )
            for name, case in CASES.items()
        ]
        return {
            "label": label,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "target": "dev",
            "catalog": catalog,
            "schema": schema,
            "repeat": repeat,
            "fingerprint": {suite["name"]: suite["fingerprints"]["after"] for suite in suites},
            "execution_fingerprint": {
                suite["name"]: suite["execution_fingerprint"] for suite in suites
            },
            "suites": suites,
            "proxy_notice": "실제 Cloudflare 청구액이 아닌 Trino·Iceberg 비용 대리 지표입니다.",
        }
    finally:
        for resource in (metadata_cursor, workload_cursor, connection):
            with contextlib.suppress(Exception):
                resource.close()


def _runs_for_suite(suite: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return list(suite.get("runs") or [])


def _metrics_for_run(run: Mapping[str, Any]) -> Mapping[str, Any]:
    metrics = run.get("metrics")
    return metrics if isinstance(metrics, Mapping) else run


def _suite_mapping(bundle: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    if "runs" in bundle:
        return {"default": bundle}
    return {
        str(suite.get("name", index)): suite
        for index, suite in enumerate(bundle.get("suites") or [])
        if isinstance(suite, Mapping)
    }


def _compare_metric(before_runs: list[Mapping[str, Any]], after_runs: list[Mapping[str, Any]], metric: str) -> dict[str, Any]:
    before_values = [_metrics_for_run(run).get(metric) for run in before_runs]
    after_values = [_metrics_for_run(run).get(metric) for run in after_runs]
    before_median = median_or_none(before_values)
    after_median = median_or_none(after_values)
    if before_median is None or after_median is None:
        return {
            "status": "unavailable",
            "before_min": None,
            "before_median": None,
            "before_max": None,
            "after_min": None,
            "after_median": None,
            "after_max": None,
            "change_percent": None,
        }
    change_percent = None if before_median == 0 else ((after_median - before_median) / before_median) * 100
    return {
        "status": "available",
        "before_min": min(before_values),
        "before_median": before_median,
        "before_max": max(before_values),
        "after_min": min(after_values),
        "after_median": after_median,
        "after_max": max(after_values),
        "change_percent": change_percent,
    }


def compare_bundles(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    """Compare matching benchmark bundles while refusing changed input snapshots."""
    if before.get("execution_fingerprint") != after.get("execution_fingerprint"):
        return {"comparable": False, "reason": "execution_fingerprint_mismatch", "metrics": []}
    if before.get("fingerprint") != after.get("fingerprint"):
        return {"comparable": False, "reason": "fingerprint_mismatch", "metrics": []}
    for bundle in (before, after):
        if any(not suite.get("comparable_within_run", True) for suite in bundle.get("suites") or []):
            return {"comparable": False, "reason": "fingerprint_changed_during_run", "metrics": []}

    before_suites = _suite_mapping(before)
    after_suites = _suite_mapping(after)
    if set(before_suites) != set(after_suites):
        return {"comparable": False, "reason": "suite_set_mismatch", "metrics": []}

    suite_comparisons: dict[str, dict[str, Any]] = {}
    for name in before_suites:
        before_runs = _runs_for_suite(before_suites[name])
        after_runs = _runs_for_suite(after_suites[name])
        if len(before_runs) != len(after_runs) or not before_runs:
            return {"comparable": False, "reason": "repeat_count_mismatch", "metrics": []}
        suite_comparisons[name] = {
            "metrics": {
                metric: _compare_metric(before_runs, after_runs, metric)
                for metric in COMPARISON_METRICS
            }
        }

    default_metrics: dict[str, Any] | dict[Any, Any]
    if set(suite_comparisons) == {"default"}:
        default_metrics = suite_comparisons["default"]["metrics"]
    else:
        default_metrics = {}
    return {
        "comparable": True,
        "reason": None,
        "metrics": default_metrics,
        "suites": suite_comparisons,
    }


def _format_range(metric: Mapping[str, Any], prefix: str) -> str:
    if metric.get("status") != "available":
        return "측정 불가"
    return f"{metric[prefix + '_median']} ({metric[prefix + '_min']}–{metric[prefix + '_max']})"


def render_comparison_markdown(comparison: Mapping[str, Any]) -> str:
    """Render an explicit Korean before/after record for a retrospective."""
    lines = ["# Weather·Traffic 비용 대리 지표 비교", "", "- 실제 Cloudflare 청구액이 아니라 Trino·Iceberg 비용 대리 지표입니다.", ""]
    if not comparison.get("comparable"):
        lines.extend(["## 비교 불가", "", f"사유: `{comparison.get('reason', 'unknown')}`", ""])
        return "\n".join(lines)

    suites = comparison.get("suites")
    if not suites:
        suites = {"default": {"metrics": comparison.get("metrics", {})}}
    for suite_name, suite in suites.items():
        lines.extend(
            [
                f"## {suite_name}",
                "",
                "| 지표 | Before median (min–max) | After median (min–max) | 변화율 | 상태 |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for metric_name in COMPARISON_METRICS:
            metric = suite["metrics"][metric_name]
            change = metric.get("change_percent")
            change_text = "측정 불가" if change is None else f"{change:.2f}%"
            lines.append(
                "| "
                + metric_name
                + " | "
                + _format_range(metric, "before")
                + " | "
                + _format_range(metric, "after")
                + " | "
                + change_text
                + " | "
                + ("비교 가능" if metric.get("status") == "available" else "측정 불가")
                + " |"
            )
        lines.append("")
    return "\n".join(lines)


def _write_output(path: str, content: str) -> None:
    if path == "-":
        print(content)
        return
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8")


def _load_json(path: str) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"benchmark bundle must be a JSON object: {path}")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Weather/Traffic read-only cost-proxy benchmark")
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect = subparsers.add_parser("collect", help="collect a dev-only read-only benchmark bundle")
    collect.add_argument("--label", choices=("before", "after"), required=True)
    collect.add_argument("--repeat", type=int, required=True)
    collect.add_argument("--output", required=True, help="JSON path, or - for stdout")
    compare = subparsers.add_parser("compare", help="compare two matching benchmark bundles")
    compare.add_argument("--before", required=True)
    compare.add_argument("--after", required=True)
    compare.add_argument("--output", required=True, help="Markdown path, or - for stdout")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "collect":
        bundle = collect_bundle(args.label, args.repeat)
        _write_output(args.output, json.dumps(bundle, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    comparison = compare_bundles(_load_json(args.before), _load_json(args.after))
    _write_output(args.output, render_comparison_markdown(comparison))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through CLI
    raise SystemExit(main())
