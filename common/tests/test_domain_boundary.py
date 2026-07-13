# shared-domains: all
"""Regression tests for the domain-artifact placement boundary."""
from __future__ import annotations

from pathlib import Path
from subprocess import CompletedProcess
import sys


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from common.domain_boundary import changed_paths, validate_paths  # noqa: E402


def _write(root: Path, relative_path: str, content: str) -> None:
    destination = root / relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8")


def test_validate_paths_rejects_root_product_script(tmp_path: Path):
    _write(tmp_path, "scripts/new_pipeline.py", "print('not allowed')\n")

    violations = validate_paths(tmp_path, ["scripts/new_pipeline.py"])

    assert violations == [
        "scripts/new_pipeline.py: product artifacts must live under domains/<domain>/, "
        "common/, or docs/cross-domain/<scope>/"
    ]


def test_validate_paths_requires_multiple_owners_for_common_artifact(tmp_path: Path):
    _write(tmp_path, "common/weather_only.py", "# shared-domains: weather\n")

    violations = validate_paths(tmp_path, ["common/weather_only.py"])

    assert violations == [
        "common/weather_only.py: shared-domains must be 'all' or name at least two domains"
    ]


def test_validate_paths_accepts_domain_and_explicit_shared_artifacts(tmp_path: Path):
    _write(tmp_path, "domains/weather/weather_cost.py", "print('domain local')\n")
    _write(tmp_path, "common/weather_traffic_cost.py", "# shared-domains: weather,traffic\n")
    _write(
        tmp_path,
        "docs/cross-domain/weather-traffic/cost.md",
        "<!-- shared-domains: weather,traffic -->\n# Cost\n",
    )

    violations = validate_paths(
        tmp_path,
        [
            "domains/weather/weather_cost.py",
            "common/weather_traffic_cost.py",
            "docs/cross-domain/weather-traffic/cost.md",
        ],
    )

    assert violations == []


def test_validate_paths_allows_only_control_plane_files_at_repository_root(tmp_path: Path):
    _write(tmp_path, "AGENTS.md", "# Repository instructions\n")
    _write(tmp_path, ".github/workflows/test.yml", "name: test\n")
    _write(tmp_path, "docs/agent/workflows/review.md", "# Review workflow\n")

    violations = validate_paths(
        tmp_path,
        ["AGENTS.md", ".github/workflows/test.yml", "docs/agent/workflows/review.md"],
    )

    assert violations == []


def test_weather_traffic_artifacts_are_marked_shared_and_leave_no_root_product_files():
    shared_artifacts = [
        ROOT / "common/benchmarks/weather_traffic_cost_proxy.py",
        ROOT / "common/tests/test_weather_traffic_cost_proxy.py",
        ROOT / "common/trino_query_metrics.py",
        ROOT / "common/tests/test_trino_query_metrics.py",
        ROOT / "docs/cross-domain/weather-traffic/2026-07-13-weather-traffic-cost-proxy-comparison.md",
        ROOT / "docs/cross-domain/weather-traffic/2026-07-13-weather-traffic-cost-proxy-retrospective.md",
    ]
    legacy_artifacts = [
        ROOT / "scripts/benchmark_weather_traffic_cost_proxy.py",
        ROOT / "scripts/tests/test_benchmark_weather_traffic_cost_proxy.py",
        ROOT / "docs/2026-07-13-weather-traffic-cost-proxy-comparison.md",
        ROOT / "docs/2026-07-13-weather-traffic-cost-proxy-retrospective.md",
    ]

    assert all(artifact.is_file() for artifact in shared_artifacts)
    assert all(not artifact.exists() for artifact in legacy_artifacts)
    assert validate_paths(ROOT, [str(artifact.relative_to(ROOT)) for artifact in shared_artifacts]) == []


def test_changed_paths_combines_branch_index_and_worktree_without_duplicates(
    tmp_path: Path, monkeypatch
):
    outputs = iter(
        [
            "common/domain_boundary.py\ndocs/cross-domain/domain-boundary/policy.md\n",
            "common/domain_boundary.py\n",
            "domains/weather/weather_cost.py\n",
            "common/tests/test_domain_boundary.py\n",
        ]
    )

    def fake_run(arguments, **_kwargs):
        return CompletedProcess(arguments, 0, next(outputs), "")

    monkeypatch.setattr("common.domain_boundary.subprocess.run", fake_run)

    paths = changed_paths(tmp_path, "origin/dev")

    assert paths == [
        "common/domain_boundary.py",
        "docs/cross-domain/domain-boundary/policy.md",
        "domains/weather/weather_cost.py",
        "common/tests/test_domain_boundary.py",
    ]


def test_current_branch_changes_obey_the_domain_boundary():
    violations = validate_paths(ROOT, changed_paths(ROOT, "origin/dev"))

    assert violations == []
