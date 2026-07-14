"""Comparison and Markdown rendering for cost-proxy bundles."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from weather_ingest.cost_proxy.config import (
    COMPARISON_METRICS,
    EXECUTION_FINGERPRINT_KEY,
    median_or_none,
)


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


def _compare_metric(
    before_runs: list[Mapping[str, Any]],
    after_runs: list[Mapping[str, Any]],
    metric: str,
) -> dict[str, Any]:
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
    change_percent = (
        None
        if before_median == 0
        else ((after_median - before_median) / before_median) * 100
    )
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


def compare_bundles(
    before: Mapping[str, Any], after: Mapping[str, Any]
) -> dict[str, Any]:
    """Compare matching benchmark bundles while refusing changed input snapshots."""
    if before.get(EXECUTION_FINGERPRINT_KEY) != after.get(EXECUTION_FINGERPRINT_KEY):
        return {
            "comparable": False,
            "reason": "execution_fingerprint_mismatch",
            "metrics": [],
        }
    if before.get("fingerprint") != after.get("fingerprint"):
        return {"comparable": False, "reason": "fingerprint_mismatch", "metrics": []}
    for bundle in (before, after):
        if any(
            not suite.get("comparable_within_run", True)
            for suite in bundle.get("suites") or []
        ):
            return {
                "comparable": False,
                "reason": "fingerprint_changed_during_run",
                "metrics": [],
            }

    before_suites = _suite_mapping(before)
    after_suites = _suite_mapping(after)
    if set(before_suites) != set(after_suites):
        return {"comparable": False, "reason": "suite_set_mismatch", "metrics": []}

    suite_comparisons: dict[str, dict[str, Any]] = {}
    for name in before_suites:
        before_runs = _runs_for_suite(before_suites[name])
        after_runs = _runs_for_suite(after_suites[name])
        if len(before_runs) != len(after_runs) or not before_runs:
            return {
                "comparable": False,
                "reason": "repeat_count_mismatch",
                "metrics": [],
            }
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
    lines = [
        "# Weather·Traffic 비용 대리 지표 비교",
        "",
        "- 실제 Cloudflare 청구액이 아니라 Trino·Iceberg 비용 대리 지표입니다.",
        "",
    ]
    if not comparison.get("comparable"):
        lines.extend(
            ["## 비교 불가", "", f"사유: `{comparison.get('reason', 'unknown')}`", ""]
        )
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
