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

    def add_task(self, task):
        self.task_dict[task.task_id] = task


class FakePythonOperator:
    def __init__(self, task_id, python_callable, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.kwargs = kwargs
        FakeDAG._stack[-1].add_task(self)


def install_airflow_fakes():
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG

    airflow_providers = types.ModuleType("airflow.providers")
    airflow_standard = types.ModuleType("airflow.providers.standard")
    airflow_operators = types.ModuleType("airflow.providers.standard.operators")
    airflow_python = types.ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = FakePythonOperator

    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.providers": airflow_providers,
            "airflow.providers.standard": airflow_standard,
            "airflow.providers.standard.operators": airflow_operators,
            "airflow.providers.standard.operators.python": airflow_python,
        }
    )


def load_maintenance_module():
    install_airflow_fakes()
    module_path = Path(__file__).resolve().parents[1] / "iceberg_maintenance_dag.py"
    spec = importlib.util.spec_from_file_location("iceberg_maintenance_dag_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_default_maintenance_tables_include_incremental_models():
    module = load_maintenance_module()

    tables = module.DEFAULT_PARAMS["tables"]

    assert "silver_seoul_traffic_incident" in tables
    assert "gold_weather_forecast_by_place" in tables
    assert "gold_traffic_incident_summary" in tables


def test_maintenance_allows_missing_tables_to_be_skipped():
    module = load_maintenance_module()
    seen = {}

    def fake_run_maintenance(target, retention, tables):
        seen["target"] = target
        seen["retention"] = retention
        seen["tables"] = tables
        return {
            "silver_seoul_traffic_incident": "skipped (missing)",
            "gold_weather_forecast_by_place": "ok",
        }

    module.run_maintenance = fake_run_maintenance

    module._maintain(
        params={
            "target": "dev",
            "retention": "7d",
            "tables": "silver_seoul_traffic_incident,gold_weather_forecast_by_place",
        }
    )

    assert seen == {
        "target": "dev",
        "retention": "7d",
        "tables": ("silver_seoul_traffic_incident", "gold_weather_forecast_by_place"),
    }
