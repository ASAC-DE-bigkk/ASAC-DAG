import importlib.util
import sys
import types
from pathlib import Path

import pytest


_AIRFLOW_MODULE_NAMES = (
    "airflow",
    "airflow.exceptions",
    "airflow.models",
    "airflow.models.param",
    "airflow.providers",
    "airflow.providers.standard",
    "airflow.providers.standard.operators",
    "airflow.providers.standard.operators.python",
    "airflow.sdk",
    "airflow.sdk.exceptions",
)


@pytest.fixture(autouse=True)
def restore_airflow_modules_after_dag_import():
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


class FakeAirflowException(Exception):
    pass


class FakeAirflowFailException(Exception):
    pass


def install_airflow_fakes():
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
    airflow_python = types.ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = FakePythonOperator
    airflow_sdk = types.ModuleType("airflow.sdk")
    airflow_sdk.Param = FakeParam
    airflow_sdk_exceptions = types.ModuleType("airflow.sdk.exceptions")
    airflow_sdk_exceptions.AirflowFailException = FakeAirflowFailException

    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.exceptions": airflow_exceptions,
            "airflow.models": airflow_models,
            "airflow.models.param": airflow_models_param,
            "airflow.providers": airflow_providers,
            "airflow.providers.standard": airflow_standard,
            "airflow.providers.standard.operators": airflow_operators,
            "airflow.providers.standard.operators.python": airflow_python,
            "airflow.sdk": airflow_sdk,
            "airflow.sdk.exceptions": airflow_sdk_exceptions,
        }
    )


def load_recovery_module():
    install_airflow_fakes()
    module_path = Path(__file__).resolve().parents[1] / "traffic_snapshot_recovery.py"
    spec = importlib.util.spec_from_file_location(
        "traffic_snapshot_recovery_under_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


def write_materialization_artifacts(command) -> None:
    if "--target-path" not in command or command[1] not in {
        "seed",
        "run",
        "test",
        "build",
        "snapshot",
    }:
        return
    target_path = Path(command[command.index("--target-path") + 1])
    target_path.mkdir(parents=True, exist_ok=True)
    (target_path / "run_results.json").write_text("{}", encoding="utf-8")
    (target_path / "manifest.json").write_text("{}", encoding="utf-8")
