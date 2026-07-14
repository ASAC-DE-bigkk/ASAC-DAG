import importlib.util
import sys
import types
from pathlib import Path

import pytest


_AIRFLOW_MODULE_NAMES = (
    "airflow",
    "airflow.models",
    "airflow.models.param",
    "airflow.providers",
    "airflow.providers.standard",
    "airflow.providers.standard.operators",
    "airflow.providers.standard.operators.bash",
    "airflow.providers.standard.operators.python",
    "airflow.sdk",
)


@pytest.fixture(autouse=True)
def restore_airflow_modules_after_transform_import():
    originals = {name: sys.modules.get(name) for name in _AIRFLOW_MODULE_NAMES}
    yield
    for name, module in originals.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


class FakeDAG:
    _stack = []

    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.task_dict = {}

    def __enter__(self):
        self._stack.append(self)
        return self

    def __exit__(self, *_exc_info):
        self._stack.pop()

    @property
    def task_ids(self):
        return list(self.task_dict)

    def add_task(self, task):
        self.task_dict[task.task_id] = task


class FakeBashOperator:
    def __init__(self, task_id, bash_command, **kwargs):
        self.task_id = task_id
        self.bash_command = bash_command
        self.kwargs = kwargs
        self.downstream_task_ids = set()
        self.upstream_task_ids = set()
        FakeDAG._stack[-1].add_task(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)
        return other


class FakePythonOperator:
    def __init__(self, task_id, python_callable, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.kwargs = kwargs
        self.downstream_task_ids = set()
        self.upstream_task_ids = set()
        FakeDAG._stack[-1].add_task(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)
        return other


class FakeParam:
    def __init__(self, default=None, **schema):
        self.value = default
        self.schema = schema


class FakeAsset:
    def __init__(self, uri):
        self.uri = uri

    def __eq__(self, other):
        return isinstance(other, FakeAsset) and self.uri == other.uri


def install_airflow_fakes():
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG

    airflow_models = types.ModuleType("airflow.models")
    airflow_models_param = types.ModuleType("airflow.models.param")
    airflow_models_param.Param = FakeParam

    airflow_providers = types.ModuleType("airflow.providers")
    airflow_standard = types.ModuleType("airflow.providers.standard")
    airflow_operators = types.ModuleType("airflow.providers.standard.operators")
    airflow_bash = types.ModuleType("airflow.providers.standard.operators.bash")
    airflow_bash.BashOperator = FakeBashOperator
    airflow_python = types.ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = FakePythonOperator
    airflow_sdk = types.ModuleType("airflow.sdk")
    airflow_sdk.Asset = FakeAsset

    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.models": airflow_models,
            "airflow.models.param": airflow_models_param,
            "airflow.providers": airflow_providers,
            "airflow.providers.standard": airflow_standard,
            "airflow.providers.standard.operators": airflow_operators,
            "airflow.providers.standard.operators.bash": airflow_bash,
            "airflow.providers.standard.operators.python": airflow_python,
            "airflow.sdk": airflow_sdk,
        }
    )


def load_transform_module():
    install_airflow_fakes()
    module_path = Path(__file__).resolve().parents[1] / "weather_vilage_fcst_transform.py"
    spec = importlib.util.spec_from_file_location("weather_vilage_fcst_transform_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_weather_transform_runs_place_mapping_seed_and_mart():
    module = load_transform_module()
    dag = module.dag

    expected_task_order = [
        "dbt_deps",
        "dbt_source_freshness",
        "dbt_seed_asac_axes",
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
        "dbt_seed_place_mapping",
        "dbt_test_place_mapping_seed",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
        "dbt_run_place_mart",
        "dbt_test_place_mart",
    ]

    assert set(expected_task_order) <= set(dag.task_ids)
    for upstream_task_id, downstream_task_id in zip(expected_task_order, expected_task_order[1:]):
        assert dag.task_dict[upstream_task_id].downstream_task_ids == {downstream_task_id}

    task_commands = {
        task_id: dag.task_dict[task_id].bash_command
        for task_id in expected_task_order
    }

    assert "deps" in task_commands["dbt_deps"]
    assert "source freshness" in task_commands["dbt_source_freshness"]
    assert "seed --select asac_axes" in task_commands["dbt_seed_asac_axes"]
    assert (
        "run --select asac_axes.dim_admin_dong"
        in task_commands["dbt_run_common_admin_dong_dimension"]
    )
    assert (
        "test --select asac_axes.dim_admin_dong"
        in task_commands["dbt_test_common_admin_dong_dimension"]
    )
    for task_id in (
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
    ):
        assert dag.task_dict[task_id].kwargs["on_failure_callback"] == [
            module.notify_weather_transform_failure,
            module.record_weather_problem,
        ]
    assert "seed --select weather_place_grid_mapping" in task_commands["dbt_seed_place_mapping"]
    assert "--target '{{ params.target }}'" in task_commands["dbt_deps"]
    assert "weather_place_grid_mapping" in task_commands["dbt_test_place_mapping_seed"]
    assert "assert_weather_place_grid_mapping_major_aliases" in task_commands["dbt_test_place_mapping_seed"]
    assert (
        "assert_weather_place_grid_mapping_within_collected_grid_scope"
        in task_commands["dbt_test_place_mapping_seed"]
    )
    assert (
        "assert_weather_place_grid_mapping_alias_unique_except_allowed"
        in task_commands["dbt_test_place_mapping_seed"]
    )
    assert "assert_silver_kma_event_at_matches_forecast_at" in task_commands["dbt_test_silver"]
    assert "--exclude assert_gold_weather_counts_match_silver" in task_commands["dbt_test_silver"]
    assert (
        "assert_gold_weather_forecast_by_place_latest_silver_record"
        not in task_commands["dbt_test_silver"]
    )
    assert (
        "run --select dim_weather_place silver_weather_forecast_by_admin_dong gold_weather_forecast_by_place"
        in task_commands["dbt_run_place_mart"]
    )
    assert "dim_weather_place" in task_commands["dbt_test_place_mart"]
    assert "silver_weather_forecast_by_admin_dong" in task_commands["dbt_test_place_mart"]
    assert "gold_weather_forecast_by_place" in task_commands["dbt_test_place_mart"]
    assert "assert_silver_weather_admin_dong_grain_unique" in task_commands["dbt_test_place_mart"]
    assert "assert_silver_weather_admin_axis_consistent" in task_commands["dbt_test_place_mart"]
    assert (
        "assert_silver_weather_admin_event_at_matches_forecast_at"
        in task_commands["dbt_test_place_mart"]
    )
    assert (
        "assert_gold_weather_forecast_by_place_admin_axis_consistent"
        in task_commands["dbt_test_place_mart"]
    )
    assert "assert_gold_weather_forecast_by_place_grain_unique" in task_commands["dbt_test_place_mart"]
    assert "assert_gold_weather_forecast_by_place_major_coverage" in task_commands["dbt_test_place_mart"]
    assert "assert_dim_weather_place_admin_axis_consistent" in task_commands["dbt_test_place_mart"]
    assert (
        "assert_gold_weather_forecast_by_place_event_at_matches_forecast_at"
        in task_commands["dbt_test_place_mart"]
    )
    assert (
        "assert_gold_weather_forecast_by_place_latest_silver_record"
        in task_commands["dbt_test_place_mart"]
    )


def test_weather_transform_passes_w2_canonical_revision_to_model_commands():
    module = load_transform_module()
    dag = module.dag
    expected_vars = "--vars '{\"weather_w2_canonical_revision_date\":\"2025-04-01\"}'"
    model_parsing_task_ids = (
        "dbt_source_freshness",
        "dbt_seed_asac_axes",
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
        "dbt_seed_place_mapping",
        "dbt_test_place_mapping_seed",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
        "dbt_run_place_mart",
        "dbt_test_place_mart",
    )

    for task_id in model_parsing_task_ids:
        assert expected_vars in dag.task_dict[task_id].bash_command
    assert "--vars" not in dag.task_dict["dbt_deps"].bash_command


def test_weather_transform_subscribes_to_bronze_asset_by_default():
    module = load_transform_module()

    assert module.dag.kwargs["schedule"] == [FakeAsset(module.WEATHER_BRONZE_ASSET)]


def test_weather_transform_names_common_admin_dong_failure_stage():
    module = load_transform_module()

    for task_id in (
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
    ):
        assert module.transform_stage_name(task_id) == "공용 행정동 차원 실행/검증"


def test_weather_transform_validates_dev_runtime_before_dbt():
    module = load_transform_module()
    guard = module.dag.task_dict["validate_dev_runtime"]

    assert guard.kwargs["op_kwargs"] == {
        "domain": "weather",
        "requested_target": "{{ params.target }}",
    }
    assert guard.downstream_task_ids == {"dbt_deps"}


def test_weather_transform_limits_target_param_to_dev_or_prod():
    module = load_transform_module()

    target_param = module.DEFAULT_PARAMS["target"]

    assert target_param.value == "dev"
    assert target_param.schema["enum"] == ["dev"]


def test_weather_transform_publishes_dbt_run_metrics_from_dag_success_callback():
    module = load_transform_module()
    dag = module.dag

    assert dag.kwargs["on_success_callback"] is module.publish_weather_transform_success_metrics
    assert "publish_dbt_run_metrics" not in dag.task_ids
    assert "fail_transform_if_upstream_failed" not in dag.task_ids
    assert dag.task_dict["dbt_test_place_mart"].downstream_task_ids == set()

def test_weather_success_callback_forwards_airflow_context(monkeypatch):
    module = load_transform_module()
    captured = {}

    def fake_publish(**context):
        captured.update(context)
        return {"rows": 3, "skipped": False}

    monkeypatch.setattr(module, "publish_dbt_run_metrics", fake_publish)

    assert module.publish_weather_transform_success_metrics({"params": {"target": "dev"}}) == {
        "rows": 3,
        "skipped": False,
    }
    assert captured == {"params": {"target": "dev"}}


def test_weather_publish_dbt_run_metrics_forwards_domain_and_target(tmp_path, monkeypatch):
    module = load_transform_module()
    run_results = tmp_path / "run_results.json"
    run_results.write_text("{}", encoding="utf-8")
    captured = {}

    def fake_dump(path, *, domain, target):
        captured.update(path=path, domain=domain, target=target)
        return [{}, {}]

    monkeypatch.setattr(module, "dump_dbt_run_results", fake_dump)

    assert module.publish_dbt_run_metrics(
        run_results_path=str(run_results), params={"target": "dev"}
    ) == {"rows": 2, "skipped": False}
    assert captured == {"path": str(run_results), "domain": "weather", "target": "dev"}
