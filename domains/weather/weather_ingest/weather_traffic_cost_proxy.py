"""Compatibility facade and CLI for the Weather/Traffic cost proxy."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


DAGS_ROOT = Path(__file__).resolve().parents[3]
for import_path in (
    DAGS_ROOT,
    DAGS_ROOT / "domains" / "weather",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from weather_ingest.cost_proxy.collection import (  # noqa: E402
    _collect_suite,
    _connect_cursors,
    _execute_dbt_case,
    collect_bundle,
)
from weather_ingest.cost_proxy.comparison import (  # noqa: E402
    _compare_metric,
    _format_range,
    _metrics_for_run,
    _runs_for_suite,
    _suite_mapping,
    compare_bundles,
    render_comparison_markdown,
)
from weather_ingest.cost_proxy.compile import (  # noqa: E402
    _aggregate_query_metrics,
    _compile_command,
    _compile_model,
    _compile_paths,
    _compiled_model_code,
    _dbt_vars_for_case,
    _execution_fingerprint,
    _fingerprint_sources,
    _resolve_publishable_snapshot,
    _safe_compile_segment,
    _source_tables,
)
from weather_ingest.cost_proxy.config import (  # noqa: E402
    BENCHMARK_CONTRACT,
    CASES,
    COMPARISON_METRICS,
    DBT_BIN,
    DBT_PROJECT,
    DBT_PROJECT_ENV,
    DEFAULT_DBT_PROJECT,
    EXECUTION_FINGERPRINT_KEY,
    MANIFEST_TABLE,
    MIN_REPEAT,
    TRAFFIC_SOURCE_TABLES,
    WEATHER_SOURCE_TABLES,
    BenchmarkContract,
    CompilePaths,
    _catalog,
    _dbt_project_dir,
    _ensure_dev_target,
    _qualified,
    _schema,
    _target,
    median_or_none,
)

__all__ = [
    "BENCHMARK_CONTRACT",
    "CASES",
    "COMPARISON_METRICS",
    "DBT_BIN",
    "DBT_PROJECT",
    "DBT_PROJECT_ENV",
    "DEFAULT_DBT_PROJECT",
    "EXECUTION_FINGERPRINT_KEY",
    "MANIFEST_TABLE",
    "MIN_REPEAT",
    "TRAFFIC_SOURCE_TABLES",
    "WEATHER_SOURCE_TABLES",
    "BenchmarkContract",
    "CompilePaths",
    "_aggregate_query_metrics",
    "_catalog",
    "_collect_suite",
    "_compare_metric",
    "_compile_command",
    "_compile_model",
    "_compile_paths",
    "_compiled_model_code",
    "_connect_cursors",
    "_dbt_project_dir",
    "_dbt_vars_for_case",
    "_ensure_dev_target",
    "_execute_dbt_case",
    "_execution_fingerprint",
    "_fingerprint_sources",
    "_format_range",
    "_metrics_for_run",
    "_qualified",
    "_resolve_publishable_snapshot",
    "_runs_for_suite",
    "_safe_compile_segment",
    "_schema",
    "_source_tables",
    "_suite_mapping",
    "_target",
    "build_parser",
    "collect_bundle",
    "compare_bundles",
    "main",
    "median_or_none",
    "render_comparison_markdown",
]


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
    parser = argparse.ArgumentParser(
        description="Weather/Traffic read-only cost-proxy benchmark"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect = subparsers.add_parser(
        "collect", help="collect a dev-only read-only benchmark bundle"
    )
    collect.add_argument("--label", choices=("before", "after"), required=True)
    collect.add_argument("--repeat", type=int, required=True)
    collect.add_argument("--output", required=True, help="JSON path, or - for stdout")
    compare = subparsers.add_parser(
        "compare", help="compare two matching benchmark bundles"
    )
    compare.add_argument("--before", required=True)
    compare.add_argument("--after", required=True)
    compare.add_argument(
        "--output", required=True, help="Markdown path, or - for stdout"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "collect":
        bundle = collect_bundle(args.label, args.repeat)
        _write_output(
            args.output,
            json.dumps(bundle, ensure_ascii=False, indent=2, sort_keys=True),
        )
        return 0
    comparison = compare_bundles(_load_json(args.before), _load_json(args.after))
    _write_output(args.output, render_comparison_markdown(comparison))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through CLI
    raise SystemExit(main())
