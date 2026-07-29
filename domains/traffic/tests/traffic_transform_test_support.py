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
    "airflow.providers.standard.operators.bash",
    "airflow.providers.standard.operators.python",
    "airflow.sdk",
    "airflow.sdk.exceptions",
    "airflow.utils",
    "airflow.utils.trigger_rule",
    "traffic_ingest.assets",
    "traffic_ingest.transform_dag_support",
    "traffic_ingest.transform_admission",
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
        self.dag = FakeDAG._stack[-1]
        self.dag.add_task(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)
        return other

    def get_flat_relatives(self, upstream=False):
        pending_task_ids = set(
            self.upstream_task_ids if upstream else self.downstream_task_ids
        )
        relatives = []
        seen_task_ids = set()
        while pending_task_ids:
            task_id = pending_task_ids.pop()
            if task_id in seen_task_ids:
                continue
            seen_task_ids.add(task_id)
            task = self.dag.task_dict[task_id]
            relatives.append(task)
            pending_task_ids.update(
                task.upstream_task_ids if upstream else task.downstream_task_ids
            )
        return relatives

    def as_teardown(self, setups=None, on_failure_fail_dagrun=False):
        self.is_teardown = True
        self.on_failure_fail_dagrun = on_failure_fail_dagrun
        self.kwargs["trigger_rule"] = "all_done_setup_success"
        return self


class FakeParam:
    def __init__(self, default=None, **schema):
        self.value = default
        self.schema = schema


class FakeVariable:
    values: dict[str, str] = {}
    fail_get = False
    set_calls: list[tuple[str, str]] = []

    @classmethod
    def reset(cls):
        cls.values = {}
        cls.fail_get = False
        cls.set_calls = []

    @classmethod
    def get(cls, key, default=None, **_kwargs):
        if cls.fail_get:
            raise RuntimeError("metadata unavailable")
        return cls.values.get(key, default)

    @classmethod
    def set(cls, key, value, **_kwargs):
        cls.set_calls.append((key, value))
        cls.values[key] = value


class FakeAsset:
    def __init__(self, uri):
        self.uri = uri

    def __eq__(self, other):
        return isinstance(other, FakeAsset) and self.uri == other.uri

    def __or__(self, other):
        return FakeAssetExpression(self, other)

    def __and__(self, other):
        return FakeAssetExpression(self, other)


class FakeAssetAlias:
    def __init__(self, name):
        self.name = name


class FakeAssetExpression:
    def __init__(self, *assets):
        self.assets = assets

    def __or__(self, other):
        return FakeAssetExpression(*self.assets, other)

    def __and__(self, other):
        return FakeAssetExpression(*self.assets, other)


class FakeAirflowException(Exception):
    pass


class FakeAirflowFailException(Exception):
    pass


class FakeAirflowSkipException(Exception):
    pass


class FakeTriggerRule:
    ALL_DONE = "all_done"
    ONE_FAILED = "one_failed"


def install_airflow_fakes():
    airflow = types.ModuleType("airflow")
    airflow.__version__ = "3.2.2"
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
    airflow_sdk = types.ModuleType("airflow.sdk")
    airflow_sdk.Asset = FakeAsset
    airflow_sdk.AssetAlias = FakeAssetAlias
    airflow_sdk.Param = FakeParam
    FakeVariable.reset()
    airflow_sdk.Variable = FakeVariable
    airflow_sdk_exceptions = types.ModuleType("airflow.sdk.exceptions")
    airflow_sdk_exceptions.AirflowFailException = FakeAirflowFailException
    airflow_sdk_exceptions.AirflowSkipException = FakeAirflowSkipException
    airflow_utils = types.ModuleType("airflow.utils")
    airflow_trigger_rule = types.ModuleType("airflow.utils.trigger_rule")
    airflow_trigger_rule.TriggerRule = FakeTriggerRule

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
            "airflow.sdk": airflow_sdk,
            "airflow.sdk.exceptions": airflow_sdk_exceptions,
            "airflow.utils": airflow_utils,
            "airflow.utils.trigger_rule": airflow_trigger_rule,
        }
    )


def load_transform_module():
    sys.modules.pop("traffic_ingest.assets", None)
    sys.modules.pop("traffic_ingest.transform_dag_support", None)
    install_airflow_fakes()
    module_path = Path(__file__).resolve().parents[1] / "traffic_incident_transform.py"
    spec = importlib.util.spec_from_file_location(
        "traffic_incident_transform_under_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


def load_gold_transform_module():
    sys.modules.pop("traffic_ingest.assets", None)
    sys.modules.pop("traffic_ingest.transform_dag_support", None)
    install_airflow_fakes()
    module_path = Path(__file__).resolve().parents[1] / "traffic_gold_transform.py"
    spec = importlib.util.spec_from_file_location(
        "traffic_gold_transform_under_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


def load_flow_transform_module():
    sys.modules.pop("traffic_ingest.assets", None)
    sys.modules.pop("traffic_ingest.transform_dag_support", None)
    install_airflow_fakes()
    module_path = Path(__file__).resolve().parents[1] / "traffic_flow_transform.py"
    spec = importlib.util.spec_from_file_location(
        "traffic_flow_transform_under_test", module_path
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
    """Simulate the current dbt invocation owning its execution artifacts."""
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
