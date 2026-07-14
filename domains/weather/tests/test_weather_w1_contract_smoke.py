import importlib.util
import json
import re
import subprocess
import sys
import types
from pathlib import Path

import pytest


_MODULE_NAMES = (
    "airflow",
    "airflow.exceptions",
    "airflow.models",
    "airflow.models.param",
    "airflow.providers",
    "airflow.providers.standard",
    "airflow.providers.standard.operators",
    "airflow.providers.standard.operators.bash",
    "airflow.providers.standard.operators.python",
    "airflow.utils",
    "airflow.utils.trigger_rule",
    "weather_ingest",
    "weather_ingest.common",
    "weather_ingest.common.runtime",
    "weather_ingest.common.resources",
)


@pytest.fixture(autouse=True)
def restore_modules_after_dag_import():
    originals = {name: sys.modules.get(name) for name in _MODULE_NAMES}
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

    def add_task(self, task):
        self.task_dict[task.task_id] = task


class FakeTask:
    def __init__(self, task_id, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs
        self.downstream_task_ids = set()
        FakeDAG._stack[-1].add_task(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        return other


class FakeBashOperator(FakeTask):
    def __init__(self, task_id, bash_command, **kwargs):
        super().__init__(task_id, **kwargs)
        self.bash_command = bash_command


class FakePythonOperator(FakeTask):
    def __init__(self, task_id, python_callable, **kwargs):
        super().__init__(task_id, **kwargs)
        self.python_callable = python_callable


class FakeParam:
    def __init__(self, default=None, **schema):
        self.value = default
        self.schema = schema


class TriggerRule:
    ALL_DONE = "all_done"


class FakeAirflowException(Exception):
    pass


def load_module():
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG
    airflow_exceptions = types.ModuleType("airflow.exceptions")
    airflow_exceptions.AirflowException = FakeAirflowException
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
    airflow_utils = types.ModuleType("airflow.utils")
    airflow_trigger = types.ModuleType("airflow.utils.trigger_rule")
    airflow_trigger.TriggerRule = TriggerRule
    weather_ingest = types.ModuleType("weather_ingest")
    weather_ingest_common = types.ModuleType("weather_ingest.common")
    weather_ingest_runtime = types.ModuleType("weather_ingest.common.runtime")
    weather_ingest_runtime.trino_cursor = lambda: (None, "iceberg_dev", "weather")
    weather_ingest_resources = types.ModuleType("weather_ingest.common.resources")
    weather_ingest_resources.TRINO_HEAVY_POOL = "trino_heavy"
    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.exceptions": airflow_exceptions,
            "airflow.models": airflow_models,
            "airflow.models.param": airflow_models_param,
            "airflow.providers": airflow_providers,
            "airflow.providers.standard": airflow_standard,
            "airflow.providers.standard.operators": airflow_operators,
            "airflow.providers.standard.operators.bash": airflow_bash,
            "airflow.providers.standard.operators.python": airflow_python,
            "airflow.utils": airflow_utils,
            "airflow.utils.trigger_rule": airflow_trigger,
            "weather_ingest": weather_ingest,
            "weather_ingest.common": weather_ingest_common,
            "weather_ingest.common.runtime": weather_ingest_runtime,
            "weather_ingest.common.resources": weather_ingest_resources,
        }
    )
    path = Path(__file__).resolve().parents[1] / "weather_w1_contract_smoke.py"
    spec = importlib.util.spec_from_file_location(
        "weather_w1_contract_smoke_under_test", path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_smoke_schema_name_is_deterministic_and_strictly_scoped():
    module = load_module()

    schema = module.smoke_schema_name("manual__2026-07-13T00:00:00+00:00")

    assert schema == module.smoke_schema_name("manual__2026-07-13T00:00:00+00:00")
    assert re.fullmatch(r"dev_weather_w1_weather_contract_test_[0-9a-f]{24}", schema)


def test_cleanup_rejects_untrusted_schema_without_executing_sql(monkeypatch):
    module = load_module()
    statements = []
    cursor = types.SimpleNamespace(execute=statements.append)
    monkeypatch.setattr(
        module, "trino_cursor", lambda: (cursor, "iceberg_dev", "weather")
    )
    ti = types.SimpleNamespace(xcom_pull=lambda **_kwargs: "weather")

    try:
        module.cleanup_weather_w1_smoke_schema(ti=ti)
    except ValueError as exc:
        assert "Unsafe smoke schema" in str(exc)
    else:
        raise AssertionError("unsafe schema must fail")

    assert statements == []


def test_cleanup_drops_only_valid_isolated_schema(monkeypatch):
    module = load_module()
    statements = []
    cursor = types.SimpleNamespace(execute=statements.append)
    monkeypatch.setattr(
        module, "trino_cursor", lambda: (cursor, "iceberg_dev", "weather")
    )
    schema = module.smoke_schema_name("manual__cleanup")
    ti = types.SimpleNamespace(xcom_pull=lambda **_kwargs: schema)

    module.cleanup_weather_w1_smoke_schema(ti=ti)

    assert statements == [f"DROP SCHEMA IF EXISTS iceberg_dev.{schema} CASCADE"]


def test_cleanup_recomputes_the_isolated_schema_when_xcom_is_missing(monkeypatch):
    module = load_module()
    statements = []
    cursor = types.SimpleNamespace(execute=statements.append)
    monkeypatch.setattr(
        module, "trino_cursor", lambda: (cursor, "iceberg_dev", "weather")
    )
    run_id = "manual__cleanup-without-xcom"
    ti = types.SimpleNamespace(xcom_pull=lambda **_kwargs: None)

    module.cleanup_weather_w1_smoke_schema(ti=ti, run_id=run_id)

    assert statements == [
        f"DROP SCHEMA IF EXISTS iceberg_dev.{module.smoke_schema_name(run_id)} CASCADE"
    ]


def test_smoke_dag_runs_bridge_contract_then_always_cleans_up():
    module = load_module()
    dag = module.dag

    assert dag.kwargs["schedule"] is None
    assert (
        dag.task_dict["cleanup_isolated_schema"].kwargs["trigger_rule"]
        == TriggerRule.ALL_DONE
    )
    assert dag.task_dict["validate_dev_runtime"].downstream_task_ids == {
        "create_isolated_schema"
    }
    assert dag.task_dict["create_isolated_schema"].downstream_task_ids == {"dbt_deps"}
    assert dag.task_dict["dbt_deps"].downstream_task_ids == {"dbt_seed_bridge_inputs"}
    assert dag.task_dict["dbt_seed_bridge_inputs"].downstream_task_ids == {
        "dbt_run_common_admin_dong_dimension"
    }
    assert dag.task_dict["dbt_run_common_admin_dong_dimension"].downstream_task_ids == {
        "dbt_run_bridge"
    }
    assert dag.task_dict["dbt_run_bridge"].downstream_task_ids == {
        "dbt_test_bridge_contract"
    }
    assert dag.task_dict["dbt_test_bridge_contract"].downstream_task_ids == {
        "cleanup_isolated_schema"
    }

    expected_phases = {
        "dbt_deps": ("deps", None),
        "dbt_seed_bridge_inputs": ("seed", "tag:ask_seoul_weather_w1_inputs"),
        "dbt_run_common_admin_dong_dimension": (
            "run",
            "tag:ask_seoul_weather_transform_common_admin",
        ),
        "dbt_run_bridge": ("run", "tag:ask_seoul_weather_w1_bridge"),
        "dbt_test_bridge_contract": ("test", "tag:ask_seoul_weather_w1_bridge"),
    }
    for task_id, (dbt_command, selection) in expected_phases.items():
        task = dag.task_dict[task_id]
        assert isinstance(task, FakePythonOperator)
        assert task.python_callable is module.run_dbt_smoke_phase
        assert task.kwargs["op_kwargs"] == {
            "dbt_command": dbt_command,
            "selection": selection,
        }
        assert task.kwargs["pool"] == module.TRINO_HEAVY_POOL
        assert task.kwargs["on_failure_callback"] is module.record_weather_problem

    assert {
        selection for _command, selection in expected_phases.values() if selection
    } == {
        "tag:ask_seoul_weather_w1_inputs",
        "tag:ask_seoul_weather_transform_common_admin",
        "tag:ask_seoul_weather_w1_bridge",
    }

    assert module.DBT_PROJECT == "/opt/airflow/dbt"
    assert not hasattr(module, "dbt_smoke_command")


def test_run_dbt_smoke_phase_preserves_schema_vars_and_attempt_identity(monkeypatch):
    module = load_module()
    monkeypatch.setenv("OPENLINEAGE_PARENT_ID", "parent-42")
    schema = module.smoke_schema_name("manual__w1")
    captured = {}
    completed = subprocess.CompletedProcess(
        args=["dbt"], returncode=0, stdout="ok\n", stderr=""
    )
    execution = types.SimpleNamespace(
        attempts=(completed,),
        completed=completed,
        missing_expected_artifacts=(),
        existing_run_results_path="/tmp/current/run_results.json",
        existing_sources_path=None,
        existing_manifest_path="/tmp/current/manifest.json",
        selected_unique_ids=("model.asac_seoul.bridge_weather_admin_dong_grid",),
    )

    def execute_dbt_phase(**kwargs):
        captured.update(kwargs)
        return execution

    monkeypatch.setattr(module.weather_dbt, "execute_dbt_phase", execute_dbt_phase)
    ti = types.SimpleNamespace(
        task_id="dbt_run_bridge",
        try_number=2,
        xcom_pull=lambda *, task_ids: schema,
    )

    result = module.run_dbt_smoke_phase(
        dbt_command="run",
        selection="tag:ask_seoul_weather_w1_bridge",
        ti=ti,
        run_id="manual__w1",
        params={"target": "dev"},
    )

    assert captured["pipeline"] == "weather-w1-contract-smoke"
    assert captured["run_id"] == "manual__w1"
    assert captured["task_id"] == "dbt_run_bridge"
    assert captured["try_number"] == 2
    assert captured["project_dir"] == module.DBT_PROJECT
    assert captured["executable"] == module.DBT_BIN
    assert captured["target"] == "dev"
    assert json.loads(captured["variables"]) == {
        "weather_w1_initial_build_mode": "bounded_isolated_smoke"
    }
    assert captured["environ"]["WEATHER_SCHEMA"] == schema
    assert captured["environ"]["ASK_SEOUL_SCHEMA"] == schema
    assert captured["environ"]["ASAC_AXES_SCHEMA"] == schema
    assert captured["environ"]["OPENLINEAGE_PARENT_ID"] == "parent-42"
    assert result == {
        "status": "success",
        "run_results_path": "/tmp/current/run_results.json",
        "sources_path": None,
        "manifest_path": "/tmp/current/manifest.json",
        "selected_unique_ids": ["model.asac_seoul.bridge_weather_admin_dong_grid"],
    }


def test_run_dbt_smoke_phase_fails_when_success_artifacts_are_missing(monkeypatch):
    module = load_module()
    schema = module.smoke_schema_name("manual__missing")
    completed = subprocess.CompletedProcess(
        args=["dbt"], returncode=0, stdout="", stderr=""
    )
    monkeypatch.setattr(
        module.weather_dbt,
        "execute_dbt_phase",
        lambda **_kwargs: types.SimpleNamespace(
            attempts=(completed,),
            completed=completed,
            missing_expected_artifacts=("/tmp/current/run_results.json",),
            existing_run_results_path=None,
            existing_sources_path=None,
            existing_manifest_path=None,
            selected_unique_ids=(),
        ),
    )
    ti = types.SimpleNamespace(
        task_id="dbt_run_bridge",
        try_number=1,
        xcom_pull=lambda *, task_ids: schema,
    )

    with pytest.raises(FakeAirflowException, match="missing expected dbt artifacts"):
        module.run_dbt_smoke_phase(
            dbt_command="run",
            selection="tag:ask_seoul_weather_w1_bridge",
            ti=ti,
            run_id="manual__missing",
            params={"target": "dev"},
        )
