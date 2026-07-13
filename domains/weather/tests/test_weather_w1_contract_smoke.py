import importlib.util
import re
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


def load_module():
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
    airflow_utils = types.ModuleType("airflow.utils")
    airflow_trigger = types.ModuleType("airflow.utils.trigger_rule")
    airflow_trigger.TriggerRule = TriggerRule
    weather_ingest = types.ModuleType("weather_ingest")
    weather_ingest_common = types.ModuleType("weather_ingest.common")
    weather_ingest_runtime = types.ModuleType("weather_ingest.common.runtime")
    weather_ingest_runtime.trino_cursor = lambda: (None, "iceberg_dev", "weather")
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
            "airflow.utils": airflow_utils,
            "airflow.utils.trigger_rule": airflow_trigger,
            "weather_ingest": weather_ingest,
            "weather_ingest.common": weather_ingest_common,
            "weather_ingest.common.runtime": weather_ingest_runtime,
        }
    )
    path = Path(__file__).resolve().parents[1] / "weather_w1_contract_smoke.py"
    spec = importlib.util.spec_from_file_location("weather_w1_contract_smoke_under_test", path)
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
    monkeypatch.setattr(module, "trino_cursor", lambda: (cursor, "iceberg_dev", "weather"))
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
    monkeypatch.setattr(module, "trino_cursor", lambda: (cursor, "iceberg_dev", "weather"))
    schema = module.smoke_schema_name("manual__cleanup")
    ti = types.SimpleNamespace(xcom_pull=lambda **_kwargs: schema)

    module.cleanup_weather_w1_smoke_schema(ti=ti)

    assert statements == [f"DROP SCHEMA IF EXISTS iceberg_dev.{schema} CASCADE"]


def test_cleanup_recomputes_the_isolated_schema_when_xcom_is_missing(monkeypatch):
    module = load_module()
    statements = []
    cursor = types.SimpleNamespace(execute=statements.append)
    monkeypatch.setattr(module, "trino_cursor", lambda: (cursor, "iceberg_dev", "weather"))
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
    assert dag.task_dict["cleanup_isolated_schema"].kwargs["trigger_rule"] == TriggerRule.ALL_DONE
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
    assert dag.task_dict["dbt_run_bridge"].downstream_task_ids == {"dbt_test_bridge_contract"}
    assert dag.task_dict["dbt_test_bridge_contract"].downstream_task_ids == {"cleanup_isolated_schema"}

    for task_id in (
        "dbt_deps",
        "dbt_seed_bridge_inputs",
        "dbt_run_common_admin_dong_dimension",
        "dbt_run_bridge",
        "dbt_test_bridge_contract",
    ):
        command = dag.task_dict[task_id].bash_command
        assert "WEATHER_SCHEMA='{{ ti.xcom_pull(task_ids='create_isolated_schema') }}'" in command
        assert "weather_w1_initial_build_mode" in command

    test_command = dag.task_dict["dbt_test_bridge_contract"].bash_command
    assert set(re.findall(r"assert_weather_bridge_[a-z_]+", test_command)) == {
        "assert_weather_bridge_candidate_grain_unique",
        "assert_weather_bridge_canonical_stamp_exact",
        "assert_weather_bridge_legacy_mapping_reconciles",
        "assert_weather_bridge_temporal_evidence",
        "assert_weather_bridge_validity_non_overlapping",
    }
    assert "assert_weather_bridge_fanout_reconciles" not in test_command
    assert "run --select asac_axes.dim_admin_dong" in dag.task_dict[
        "dbt_run_common_admin_dong_dimension"
    ].bash_command
