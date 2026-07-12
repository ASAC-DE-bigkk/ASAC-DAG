import importlib.util
import sys
import types
from pathlib import Path


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
        FakeDAG._stack[-1].add_task(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        return other


class FakePythonOperator:
    def __init__(self, task_id, python_callable, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.kwargs = kwargs
        self.downstream_task_ids = set()
        FakeDAG._stack[-1].add_task(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
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
    module_path = Path(__file__).resolve().parents[1] / "traffic_incident_transform.py"
    spec = importlib.util.spec_from_file_location("traffic_incident_transform_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_traffic_transform_bootstraps_asac_axes_before_silver():
    module = load_transform_module()
    dag = module.dag

    expected_task_order = [
        "resolve_traffic_snapshot_run",
        "dbt_deps",
        "dbt_source_freshness",
        "dbt_test_traffic_incident_availability",
        "dbt_seed_asac_axes",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
    ]

    assert set(expected_task_order) <= set(dag.task_ids)
    for upstream_task_id, downstream_task_id in zip(expected_task_order, expected_task_order[1:]):
        assert dag.task_dict[upstream_task_id].downstream_task_ids == {downstream_task_id}

    bash_task_ids = [task_id for task_id in expected_task_order if task_id != "resolve_traffic_snapshot_run"]
    task_commands = {
        task_id: dag.task_dict[task_id].bash_command
        for task_id in bash_task_ids
    }

    assert all("traffic_snapshot_dag_run_id" in command for command in task_commands.values())
    assert "deps" in task_commands["dbt_deps"]
    assert "source freshness" in task_commands["dbt_source_freshness"]
    assert (
        "test --select assert_traffic_incident_row_availability"
        in task_commands["dbt_test_traffic_incident_availability"]
    )
    assert "seed --select asac_axes" in task_commands["dbt_seed_asac_axes"]
    assert dag.task_dict["resolve_traffic_snapshot_run"].downstream_task_ids == {"dbt_deps"}
    assert (
        "run --select silver_seoul_traffic_incident silver_seoul_traffic_incident_current"
        in task_commands["dbt_run_silver"]
    )
    assert "traffic_snapshot_dag_run_id" in task_commands["dbt_run_silver"]
    assert "traffic_snapshot_dag_run_id" in task_commands["dbt_test_silver"]
    assert "--target '{{ params.target }}'" in task_commands["dbt_deps"]
    assert "assert_silver_traffic_event_at_matches_occurred_at" in task_commands["dbt_test_silver"]
    assert (
        "assert_silver_traffic_wgs84_required_when_source_coordinate_available"
        in task_commands["dbt_test_silver"]
    )
    assert "assert_silver_traffic_admin_axis_consistent" in task_commands["dbt_test_silver"]
    assert "assert_silver_traffic_admin_axis_coverage" in task_commands["dbt_test_silver"]
    assert "assert_silver_traffic_latest_publishable_record" in task_commands["dbt_test_silver"]
    assert "silver_seoul_traffic_incident_current" in task_commands["dbt_test_silver"]
    assert "assert_traffic_current_pinned_publishable_run" in task_commands["dbt_test_silver"]


def test_traffic_transform_defaults_to_hourly_cron_after_bronze_completion_window(monkeypatch):
    monkeypatch.delenv("ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE", raising=False)
    module = load_transform_module()

    assert module.TRAFFIC_TRANSFORM_CRON_KST == "12 * * * *"
    assert module.dag.kwargs["schedule"] == module.TRAFFIC_TRANSFORM_CRON_KST


def test_traffic_transform_validates_dev_runtime_before_dbt():
    module = load_transform_module()
    guard = module.dag.task_dict["validate_dev_runtime"]

    assert guard.kwargs["op_kwargs"] == {
        "domain": "traffic",
        "requested_target": "{{ params.target }}",
    }
    assert guard.downstream_task_ids == {"resolve_traffic_snapshot_run"}


def test_traffic_transform_limits_target_param_to_dev_or_prod():
    module = load_transform_module()

    target_param = module.DEFAULT_PARAMS["target"]

    assert target_param.value == "dev"
    assert target_param.schema["enum"] == ["dev"]
