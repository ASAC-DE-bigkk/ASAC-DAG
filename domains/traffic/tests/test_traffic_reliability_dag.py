"""Traffic watchdog DAG import tests without a live Airflow installation."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types

import pytest


_FAKE_MODULE_NAMES = (
    "airflow",
    "airflow.models",
    "airflow.sdk",
    "airflow.providers",
    "airflow.providers.standard",
    "airflow.providers.standard.operators",
    "airflow.providers.standard.operators.python",
    "common.errors.airflow",
    "common.runmetrics",
)


@pytest.fixture(autouse=True)
def restore_fake_modules_after_dag_import():
    originals = {name: sys.modules.get(name) for name in _FAKE_MODULE_NAMES}
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


class FakePythonOperator:
    def __init__(self, task_id, python_callable, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.kwargs = kwargs
        FakeDAG._stack[-1].add_task(self)


class FakeVariable:
    @staticmethod
    def get(_key, default_var=None):
        return default_var

    @staticmethod
    def set(_key, _value):
        return None


def _track(**_kwargs):
    return lambda function: function


def load_module():
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG
    airflow_models = types.ModuleType("airflow.models")
    airflow_sdk = types.ModuleType("airflow.sdk")
    airflow_sdk.Variable = FakeVariable
    airflow_providers = types.ModuleType("airflow.providers")
    airflow_standard = types.ModuleType("airflow.providers.standard")
    airflow_operators = types.ModuleType("airflow.providers.standard.operators")
    airflow_python = types.ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = FakePythonOperator
    errors_airflow = types.ModuleType("common.errors.airflow")
    errors_airflow.problem_failure_callback = lambda **_kwargs: lambda *_args, **_kwargs: None
    runmetrics = types.ModuleType("common.runmetrics")
    runmetrics.track = _track
    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.models": airflow_models,
            "airflow.sdk": airflow_sdk,
            "airflow.providers": airflow_providers,
            "airflow.providers.standard": airflow_standard,
            "airflow.providers.standard.operators": airflow_operators,
            "airflow.providers.standard.operators.python": airflow_python,
            "common.errors.airflow": errors_airflow,
            "common.runmetrics": runmetrics,
        }
    )
    module_path = Path(__file__).resolve().parents[1] / "traffic_reliability_report.py"
    spec = importlib.util.spec_from_file_location("traffic_reliability_report_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_traffic_notifies_only_on_status_transition():
    module = load_module()
    state = {"value": "WARN"}

    assert module.should_notify_status_change(
        "WARN",
        get=lambda *_args, **_kwargs: state["value"],
        set=lambda _key, value: state.update(value=value),
    ) is False
    assert module.should_notify_status_change(
        "FAIL",
        get=lambda *_args, **_kwargs: state["value"],
        set=lambda _key, value: state.update(value=value),
    ) is True
    assert state["value"] == "FAIL"


def test_traffic_notifies_fail_open_when_variable_access_fails():
    module = load_module()

    assert module.should_notify_status_change(
        "FAIL",
        get=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("state unavailable")),
        set=lambda *_args, **_kwargs: None,
    ) is True
