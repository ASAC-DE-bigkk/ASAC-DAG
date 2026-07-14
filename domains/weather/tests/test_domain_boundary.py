"""Regression tests for the domain-artifact placement boundary."""

from __future__ import annotations

from pathlib import Path
from subprocess import CompletedProcess
import sys


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "domains" / "weather"))

import domain_boundary  # noqa: E402


changed_paths = domain_boundary.changed_paths
validate_paths = domain_boundary.validate_paths


def _write(root: Path, relative_path: str, content: str) -> None:
    destination = root / relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8")


def test_validate_paths_rejects_product_artifacts_outside_a_domain(tmp_path: Path):
    paths = [
        "scripts/new_pipeline.py",
        "common/weather_traffic_cost.py",
        "docs/cross-domain/weather-traffic/cost.md",
    ]
    for path in paths:
        _write(tmp_path, path, "content\n")

    violations = validate_paths(tmp_path, paths)

    assert violations == [
        f"{path}: product artifacts must live under domains/<domain>/" for path in paths
    ]


def test_validate_paths_accepts_weather_and_traffic_domain_artifacts(tmp_path: Path):
    _write(tmp_path, "domains/weather/weather_cost.py", "print('domain local')\n")
    _write(tmp_path, "domains/traffic/docs/cost.md", "# Cost\n")

    violations = validate_paths(
        tmp_path,
        [
            "domains/weather/weather_cost.py",
            "domains/traffic/docs/cost.md",
        ],
    )

    assert violations == []


def test_validate_paths_allows_only_control_plane_files_at_repository_root(
    tmp_path: Path,
):
    _write(tmp_path, "AGENTS.md", "# Repository instructions\n")
    _write(tmp_path, ".github/workflows/test.yml", "name: test\n")
    _write(tmp_path, "docs/agent/workflows/review.md", "# Review workflow\n")

    violations = validate_paths(
        tmp_path,
        ["AGENTS.md", ".github/workflows/test.yml", "docs/agent/workflows/review.md"],
    )

    assert violations == []


def test_weather_traffic_artifacts_are_domain_local_and_leave_no_shared_product_files():
    domain_artifacts = [
        ROOT / "domains/weather/weather_ingest/weather_traffic_cost_proxy.py",
        ROOT / "domains/weather/weather_ingest/trino_query_metrics.py",
        ROOT / "domains/weather/tests/test_weather_traffic_cost_proxy.py",
        ROOT / "domains/weather/tests/test_trino_query_metrics.py",
        ROOT / "domains/weather/domain_boundary.py",
        ROOT / "domains/weather/tests/test_domain_boundary.py",
        ROOT
        / "domains/weather/docs/weather-traffic/2026-07-13-weather-traffic-cost-proxy-comparison.md",
        ROOT
        / "domains/weather/docs/weather-traffic/2026-07-13-weather-traffic-cost-proxy-retrospective.md",
        ROOT
        / "domains/weather/docs/superpowers/plans/2026-07-13-domain-boundary-harness-plan.md",
        ROOT
        / "domains/weather/docs/superpowers/plans/2026-07-13-weather-traffic-cost-observability-watchdog.md",
        ROOT
        / "domains/weather/docs/superpowers/plans/2026-07-13-common-admin-axis-materialization.md",
        ROOT
        / "domains/weather/docs/superpowers/specs/2026-07-13-weather-traffic-cost-slo-design.md",
        ROOT
        / "domains/weather/docs/superpowers/specs/2026-07-13-weather-reliability-pagination-clarity-design.md",
        ROOT
        / "domains/weather/docs/superpowers/specs/2026-07-13-common-admin-axis-materialization-design.md",
        ROOT
        / "domains/traffic/docs/superpowers/plans/2026-07-13-traffic-dbt-deps-target-path.md",
        ROOT
        / "domains/traffic/docs/superpowers/plans/2026-07-13-traffic-reliability-airflow-failure-visibility.md",
        ROOT
        / "domains/traffic/docs/superpowers/specs/2026-07-13-traffic-dbt-deps-target-path-design.md",
        ROOT
        / "domains/traffic/docs/superpowers/specs/2026-07-13-traffic-reliability-airflow-failure-design.md",
    ]
    legacy_artifacts = [
        ROOT / "scripts/benchmark_weather_traffic_cost_proxy.py",
        ROOT / "scripts/tests/test_benchmark_weather_traffic_cost_proxy.py",
        ROOT / "common/benchmarks/weather_traffic_cost_proxy.py",
        ROOT / "common/trino_query_metrics.py",
        ROOT / "common/domain_boundary.py",
        ROOT / "common/tests/test_weather_traffic_cost_proxy.py",
        ROOT / "common/tests/test_trino_query_metrics.py",
        ROOT / "common/tests/test_domain_boundary.py",
        ROOT / "docs/2026-07-13-weather-traffic-cost-proxy-comparison.md",
        ROOT / "docs/2026-07-13-weather-traffic-cost-proxy-retrospective.md",
        ROOT
        / "docs/cross-domain/weather-traffic/2026-07-13-weather-traffic-cost-proxy-comparison.md",
        ROOT
        / "docs/cross-domain/weather-traffic/2026-07-13-weather-traffic-cost-proxy-retrospective.md",
        ROOT
        / "docs/cross-domain/domain-boundary/2026-07-13-domain-boundary-harness-plan.md",
        ROOT / "docs/superpowers/plans/2026-07-13-common-admin-axis-materialization.md",
        ROOT
        / "docs/superpowers/specs/2026-07-13-common-admin-axis-materialization-design.md",
        ROOT
        / "docs/superpowers/specs/2026-07-13-weather-reliability-pagination-clarity-design.md",
        ROOT / "docs/superpowers/plans/2026-07-13-traffic-dbt-deps-target-path.md",
        ROOT
        / "docs/superpowers/plans/2026-07-13-traffic-reliability-airflow-failure-visibility.md",
        ROOT
        / "docs/superpowers/specs/2026-07-13-traffic-dbt-deps-target-path-design.md",
        ROOT
        / "docs/superpowers/specs/2026-07-13-traffic-reliability-airflow-failure-design.md",
    ]

    assert all(artifact.is_file() for artifact in domain_artifacts)
    assert all(not artifact.exists() for artifact in legacy_artifacts)
    assert (
        validate_paths(
            ROOT, [str(artifact.relative_to(ROOT)) for artifact in domain_artifacts]
        )
        == []
    )


def test_changed_paths_combines_base_ref_worktree_and_untracked_without_duplicates(
    tmp_path: Path, monkeypatch
):
    outputs = iter(
        [
            "domains/weather/domain_boundary.py\ndomains/weather/docs/policy.md\ndomains/weather/weather_cost.py\n",
            "domains/weather/domain_boundary.py\ndomains/weather/tests/test_domain_boundary.py\n",
        ]
    )
    calls = []

    def fake_run(arguments, **_kwargs):
        calls.append(arguments)
        return CompletedProcess(arguments, 0, next(outputs), "")

    monkeypatch.setattr(domain_boundary.subprocess, "run", fake_run)

    paths = changed_paths(tmp_path, "origin/dev")

    assert paths == [
        "domains/weather/domain_boundary.py",
        "domains/weather/docs/policy.md",
        "domains/weather/weather_cost.py",
        "domains/weather/tests/test_domain_boundary.py",
    ]
    assert calls == [
        [
            "git",
            "-C",
            str(tmp_path),
            "diff",
            "--name-only",
            "--diff-filter=AMR",
            "origin/dev",
        ],
        [
            "git",
            "-C",
            str(tmp_path),
            "ls-files",
            "--others",
            "--exclude-standard",
        ],
    ]


def test_main_resolves_the_repository_root_from_the_weather_domain_module(monkeypatch):
    observed = {}

    def fake_changed_paths(root: Path, base_ref: str) -> list[str]:
        observed["root"] = root
        observed["base_ref"] = base_ref
        return []

    monkeypatch.setattr(domain_boundary, "changed_paths", fake_changed_paths)
    monkeypatch.setattr(sys, "argv", ["domain_boundary.py", "--base-ref", "origin/dev"])

    assert domain_boundary.main() == 0
    assert observed == {"root": ROOT, "base_ref": "origin/dev"}
