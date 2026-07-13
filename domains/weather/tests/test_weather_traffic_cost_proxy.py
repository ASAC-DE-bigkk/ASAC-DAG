"""Pure-function tests for the Weather/Traffic cost-proxy benchmark CLI."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "domains" / "weather"))
SCRIPT_PATH = ROOT / "domains" / "weather" / "weather_ingest" / "weather_traffic_cost_proxy.py"
SPEC = importlib.util.spec_from_file_location("cost_proxy_benchmark", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def bundle_with_runs(wall_times, *, physical_input):
    return {
        "fingerprint": {"weather": {"snapshot_id": 1}},
        "runs": [
            {"wall_time_ms": wall_time, "physical_input_bytes": input_bytes}
            for wall_time, input_bytes in zip(wall_times, physical_input, strict=True)
        ],
    }


def test_compare_bundles_rejects_different_fingerprints():
    before = {"fingerprint": {"weather": {"snapshot_id": 1}}, "suites": []}
    after = {"fingerprint": {"weather": {"snapshot_id": 2}}, "suites": []}

    comparison = benchmark.compare_bundles(before, after)

    assert comparison["comparable"] is False
    assert comparison["reason"] == "fingerprint_mismatch"
    assert comparison["metrics"] == []


def test_compare_bundles_uses_median_and_never_coerces_missing_to_zero():
    before = bundle_with_runs([100, 110, 120], physical_input=[1000, None, 1200])
    after = bundle_with_runs([80, 90, 100], physical_input=[800, None, 900])

    comparison = benchmark.compare_bundles(before, after)

    assert comparison["comparable"] is True
    assert comparison["metrics"]["wall_time_ms"]["before_median"] == 110
    assert comparison["metrics"]["wall_time_ms"]["after_median"] == 90
    assert comparison["metrics"]["physical_input_bytes"]["status"] == "unavailable"


def test_render_comparison_markdown_discloses_proxy_and_non_comparable_state():
    rendered = benchmark.render_comparison_markdown(
        {"comparable": False, "reason": "fingerprint_mismatch", "metrics": {}}
    )

    assert "비용 대리 지표" in rendered
    assert "비교 불가" in rendered


def test_median_or_none_requires_every_repeat_to_expose_the_metric():
    assert benchmark.median_or_none([10, 20, 30]) == 20
    assert benchmark.median_or_none([10, None, 30]) is None


class FakeCursor:
    def __init__(self, row):
        self.row = row
        self.sql = None

    def execute(self, sql):
        self.sql = sql

    def fetchone(self):
        return self.row


def test_traffic_case_resolves_the_same_publishable_snapshot_contract_as_transform():
    cursor = FakeCursor(("traffic-run-42",))

    dbt_vars = benchmark._dbt_vars_for_case(
        benchmark.CASES["traffic_silver"],
        cursor,
        catalog="iceberg_dev",
        schema="weather_traffic_bronze",
    )

    assert dbt_vars == {"traffic_snapshot_dag_run_id": "traffic-run-42"}
    assert "source_id = 'seoul_traffic_incident'" in cursor.sql
    assert "status = 'SUCCESS'" in cursor.sql
    assert "AND is_publishable" in cursor.sql
    assert "ORDER BY CAST(event_at AS timestamp(6)) DESC" in cursor.sql


def test_compile_command_encodes_a_pinned_snapshot_as_dbt_vars():
    command = benchmark._compile_command(
        benchmark.CASES["traffic_silver"],
        {"traffic_snapshot_dag_run_id": "traffic-run-42"},
    )

    assert command[:4] == [benchmark.DBT_BIN, "compile", "--select", "silver_seoul_traffic_incident"]
    assert command[-2] == "--vars"
    assert json.loads(command[-1]) == {"traffic_snapshot_dag_run_id": "traffic-run-42"}
