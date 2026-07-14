"""Pure-function tests for the Weather/Traffic cost-proxy benchmark CLI."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "domains" / "weather"))
SCRIPT_PATH = (
    ROOT / "domains" / "weather" / "weather_ingest" / "weather_traffic_cost_proxy.py"
)
SPEC = importlib.util.spec_from_file_location("cost_proxy_benchmark", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)

from weather_ingest.cost_proxy import collection as cost_proxy_collection  # noqa: E402
from weather_ingest.cost_proxy import compile as cost_proxy_compile  # noqa: E402


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


def test_compare_bundles_rejects_different_execution_fingerprints():
    before = {
        "fingerprint": {"weather": {"snapshot_id": 1}},
        "execution_fingerprint": {"weather_silver": {"target": "dev"}},
        "suites": [],
    }
    after = {
        "fingerprint": {"weather": {"snapshot_id": 1}},
        "execution_fingerprint": {
            "weather_silver": {"target": "dev", "schema": "other"}
        },
        "suites": [],
    }

    comparison = benchmark.compare_bundles(before, after)

    assert comparison == {
        "comparable": False,
        "reason": "execution_fingerprint_mismatch",
        "metrics": [],
    }


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


def test_collect_bundle_rejects_fewer_than_three_repeats(monkeypatch):
    monkeypatch.setattr(cost_proxy_collection, "_ensure_dev_target", lambda: None)

    with pytest.raises(ValueError, match="repeat must be at least 3"):
        benchmark.collect_bundle("before", 2)


def test_cost_proxy_contains_only_the_primary_weather_and_traffic_models():
    assert set(benchmark.CASES) == {
        "weather_silver",
        "traffic_silver",
        "weather_gold",
        "traffic_gold",
    }
    assert benchmark.CASES["weather_gold"]["model"] == "gold_weather_forecast_by_place"
    assert (
        benchmark.CASES["traffic_gold"]["model"]
        == "gold_traffic_incident_current_by_admin_dong_hourly"
    )
    assert (
        benchmark.CASES["traffic_gold"]["snapshot_var"] == "traffic_snapshot_dag_run_id"
    )
    assert benchmark.DBT_PROJECT == "/opt/airflow/dbt"
    assert all("model" in case for case in benchmark.CASES.values())
    assert all("report" not in case for case in benchmark.CASES.values())
    assert all("project" not in case for case in benchmark.CASES.values())
    assert (
        benchmark._dbt_project_dir({"ASK_SEOUL_DBT_PROJECT_DIR": "/tmp/root-dbt"})
        == "/tmp/root-dbt"
    )


def test_benchmark_case_registry_has_one_deeply_immutable_owner():
    assert benchmark.CASES is benchmark.BENCHMARK_CONTRACT.cases

    with pytest.raises(TypeError):
        benchmark.CASES["weather_silver"] = benchmark.CASES["weather_silver"]
    with pytest.raises(TypeError):
        benchmark.CASES["weather_silver"]["model"] = "other_model"


def test_benchmark_contract_keys_and_manifest_table_have_single_owners():
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    config_source = (SCRIPT_PATH.parent / "cost_proxy" / "config.py").read_text(
        encoding="utf-8"
    )
    from weather_ingest.run_manifest import MANIFEST_TABLE

    assert benchmark.MANIFEST_TABLE == MANIFEST_TABLE
    assert benchmark.EXECUTION_FINGERPRINT_KEY == "execution_fingerprint"
    assert source.count('"bronze_collection_run_manifest"') == 0
    assert source.count('"execution_fingerprint"') == 0
    assert config_source.count('"execution_fingerprint"') == 1


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
    paths = benchmark.CompilePaths(
        target_path="/tmp/compile-target",
        log_path="/tmp/compile-log",
        manifest_path="/tmp/compile-target/manifest.json",
    )
    command = benchmark._compile_command(
        benchmark.CASES["traffic_silver"],
        {"traffic_snapshot_dag_run_id": "traffic-run-42"},
        paths=paths,
    )

    assert command[:4] == [
        benchmark.DBT_BIN,
        "compile",
        "--select",
        "silver_seoul_traffic_incident",
    ]
    assert command[-2] == "--vars"
    assert json.loads(command[-1]) == {"traffic_snapshot_dag_run_id": "traffic-run-42"}
    assert command[command.index("--target-path") + 1] == paths.target_path
    assert command[command.index("--log-path") + 1] == paths.log_path


def test_compile_model_reads_only_the_current_isolated_manifest(tmp_path, monkeypatch):
    project = tmp_path / "root-dbt"
    stale_manifest = project / "target" / "manifest.json"
    stale_manifest.parent.mkdir(parents=True)
    stale_manifest.write_text(
        json.dumps(
            {
                "nodes": {
                    "model.stale": {
                        "resource_type": "model",
                        "name": "silver_kma_vilage_fcst",
                        "compiled_code": "select 'stale'",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(cost_proxy_compile, "DBT_PROJECT", str(project))
    observed = {}

    def runner(command, **kwargs):
        observed.update(command=command, kwargs=kwargs)
        target_path = Path(command[command.index("--target-path") + 1])
        target_path.mkdir(parents=True, exist_ok=True)
        (target_path / "manifest.json").write_text(
            json.dumps(
                {
                    "nodes": {
                        "model.current": {
                            "resource_type": "model",
                            "name": "silver_kma_vilage_fcst",
                            "compiled_code": "select 'current'",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return None

    compiled = benchmark._compile_model(
        benchmark.CASES["weather_silver"],
        dbt_vars={},
        suite_name="weather_silver",
        invocation_id="attempt-a",
        runner=runner,
    )

    assert compiled == "select 'current'"
    command = observed["command"]
    target_path = Path(command[command.index("--target-path") + 1])
    log_path = Path(command[command.index("--log-path") + 1])
    assert target_path != stale_manifest.parent
    assert target_path.parts[-3:] == ("weather_silver", "attempt-a", "target")
    assert log_path.parts[-3:] == ("weather_silver", "attempt-a", "logs")
    assert observed["kwargs"]["cwd"] == str(project)
    assert observed["kwargs"]["env"]["DBT_PROJECT_DIR"] == str(project)
    assert observed["kwargs"]["env"]["DBT_PROFILES_DIR"] == str(project)
    assert command[0] == benchmark.DBT_BIN


def test_compile_model_rejects_ambiguous_configured_model_in_manifest(
    tmp_path, monkeypatch
):
    project = tmp_path / "root-dbt"
    monkeypatch.setattr(cost_proxy_compile, "DBT_PROJECT", str(project))

    def runner(command, **_kwargs):
        target_path = Path(command[command.index("--target-path") + 1])
        target_path.mkdir(parents=True, exist_ok=True)
        (target_path / "manifest.json").write_text(
            json.dumps(
                {
                    "nodes": {
                        "model.package_a.silver_kma_vilage_fcst": {
                            "resource_type": "model",
                            "name": "silver_kma_vilage_fcst",
                            "compiled_code": "select 'package-a'",
                        },
                        "model.package_b.silver_kma_vilage_fcst": {
                            "resource_type": "model",
                            "name": "silver_kma_vilage_fcst",
                            "compiled_code": "select 'package-b'",
                        },
                    }
                }
            ),
            encoding="utf-8",
        )

    with pytest.raises(
        RuntimeError,
        match="ambiguous configured benchmark model.*silver_kma_vilage_fcst",
    ):
        benchmark._compile_model(
            benchmark.CASES["weather_silver"],
            dbt_vars={},
            suite_name="weather_silver",
            invocation_id="attempt-ambiguous",
            runner=runner,
        )


def test_execution_fingerprint_records_the_read_only_execution_contract():
    model_fingerprint = benchmark._execution_fingerprint(
        "traffic_silver",
        benchmark.CASES["traffic_silver"],
        {"traffic_snapshot_dag_run_id": "traffic-run-42"},
        catalog="iceberg_dev",
        schema="weather_traffic_bronze",
    )
    assert model_fingerprint["name"] == "traffic_silver"
    assert model_fingerprint["domain"] == "traffic"
    assert model_fingerprint["source_tables"] == list(
        benchmark.CASES["traffic_silver"]["source_tables"]
    )
    assert model_fingerprint["target"] == "dev"
    assert model_fingerprint["catalog"] == "iceberg_dev"
    assert model_fingerprint["schema"] == "weather_traffic_bronze"
    assert model_fingerprint["dbt_bin"] == benchmark.DBT_BIN
    assert model_fingerprint["compile_command"][-2] == "--vars"
    assert model_fingerprint["project"] == benchmark.DBT_PROJECT
    assert "--target-path" not in model_fingerprint["compile_command"]
    assert "report" not in model_fingerprint


def test_compare_bundles_rejects_execution_fingerprints_from_different_projects():
    before_case = benchmark.CASES["weather_silver"]
    before = {
        "fingerprint": {"weather": {"snapshot_id": 1}},
        "execution_fingerprint": {
            "weather_silver": benchmark._execution_fingerprint(
                "weather_silver",
                before_case,
                {},
                catalog="iceberg_dev",
                schema="weather_traffic_bronze",
                project_dir="/opt/airflow/dbt",
            )
        },
        "suites": [],
    }
    after = {
        "fingerprint": {"weather": {"snapshot_id": 1}},
        "execution_fingerprint": {
            "weather_silver": benchmark._execution_fingerprint(
                "weather_silver",
                before_case,
                {},
                catalog="iceberg_dev",
                schema="weather_traffic_bronze",
                project_dir="/tmp/other-weather-project",
            )
        },
        "suites": [],
    }

    comparison = benchmark.compare_bundles(before, after)

    assert comparison == {
        "comparable": False,
        "reason": "execution_fingerprint_mismatch",
        "metrics": [],
    }
