import importlib.util
import sys
import types
from pathlib import Path

# 가드 자체는 스텁으로 무력화하지만, 순수 함수인 타깃 해석은 실물을 그대로 쓴다.
# 여기서 값을 베껴두면 실물과 조용히 어긋나 DAG 파싱 회귀를 놓친다.
from common.runtime_guard import TARGET_CHOICES, default_target


MODULE_NAMES = (
    "airflow",
    "airflow.exceptions",
    "airflow.models",
    "airflow.models.param",
    "airflow.providers",
    "airflow.providers.standard",
    "airflow.providers.standard.operators",
    "airflow.providers.standard.operators.python",
    "airflow.sdk",
    "common.errors.airflow",
    "common.runtime_guard",
    "weather_ingest.common.resources",
    "weather_ingest.common.runtime",
)


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
        self.downstream_task_ids = set()
        FakeDAG._stack[-1].add_task(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        return other


class FakeParam:
    def __init__(self, default=None, **schema):
        self.value = default
        self.schema = schema


class FakeVariable:
    @staticmethod
    def get(*_args, **_kwargs):
        return None

    @staticmethod
    def set(*_args, **_kwargs):
        return None


class FakeAirflowException(Exception):
    pass


class FakeAirflowFailException(FakeAirflowException):
    pass


def install_airflow_fakes():
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG
    airflow_exceptions = types.ModuleType("airflow.exceptions")
    airflow_exceptions.AirflowException = FakeAirflowException
    airflow_exceptions.AirflowFailException = FakeAirflowFailException
    airflow_models = types.ModuleType("airflow.models")
    airflow_models_param = types.ModuleType("airflow.models.param")
    airflow_models_param.Param = FakeParam
    airflow_providers = types.ModuleType("airflow.providers")
    airflow_standard = types.ModuleType("airflow.providers.standard")
    airflow_operators = types.ModuleType("airflow.providers.standard.operators")
    airflow_python = types.ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = FakePythonOperator
    airflow_sdk = types.ModuleType("airflow.sdk")
    airflow_sdk.Variable = FakeVariable

    errors_airflow = types.ModuleType("common.errors.airflow")
    errors_airflow.problem_failure_callback = lambda *, domain: ("problem", domain)
    runtime_guard = types.ModuleType("common.runtime_guard")
    runtime_guard.validate_dev_runtime = lambda **_kwargs: None
    runtime_guard.TARGET_CHOICES = TARGET_CHOICES
    runtime_guard.default_target = default_target
    resources = types.ModuleType("weather_ingest.common.resources")
    resources.TRINO_HEAVY_POOL = "trino_weather_heavy"
    runtime = types.ModuleType("weather_ingest.common.runtime")
    runtime.trino_cursor = lambda: (None, "iceberg_dev", "weather")
    runtime.sql_identifier = lambda value: str(value)

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
            "common.errors.airflow": errors_airflow,
            "common.runtime_guard": runtime_guard,
            "weather_ingest.common.resources": resources,
            "weather_ingest.common.runtime": runtime,
        }
    )


def load_recovery_module():
    install_airflow_fakes()
    module_path = (
        Path(__file__).resolve().parents[1] / "weather_w2_observation_recovery.py"
    )
    spec = importlib.util.spec_from_file_location(
        "weather_w2_observation_recovery_under_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module
