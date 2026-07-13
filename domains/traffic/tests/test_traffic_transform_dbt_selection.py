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
    airflow_bash = types.ModuleType("airflow.providers.standard.operators.bash")
    airflow_bash.BashOperator = FakeBashOperator
    airflow_python = types.ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = FakePythonOperator
    airflow_sdk = types.ModuleType("airflow.sdk")
    airflow_sdk.Asset = FakeAsset
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
            "airflow.providers.standard.operators.bash": airflow_bash,
            "airflow.providers.standard.operators.python": airflow_python,
            "airflow.sdk": airflow_sdk,
            "airflow.sdk.exceptions": airflow_sdk_exceptions,
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

    dbt_task_ids = [task_id for task_id in expected_task_order if task_id != "resolve_traffic_snapshot_run"]
    task_commands = {
        task_id: dag.task_dict[task_id].kwargs["op_kwargs"]["dbt_args"]
        for task_id in dbt_task_ids
    }

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
    assert dag.task_dict["dbt_run_silver"].kwargs["op_kwargs"]["snapshot_task_id"] == "resolve_traffic_snapshot_run"
    assert dag.task_dict["dbt_test_silver"].kwargs["op_kwargs"]["snapshot_task_id"] == "resolve_traffic_snapshot_run"
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


def test_traffic_dbt_tasks_classify_failures_before_airflow_retries():
    module = load_transform_module()
    dag = module.dag
    classified_task_ids = [
        "dbt_deps",
        "dbt_source_freshness",
        "dbt_test_traffic_incident_availability",
        "dbt_seed_asac_axes",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
    ]

    for task_id in classified_task_ids:
        task = dag.task_dict[task_id]
        assert isinstance(task, FakePythonOperator)
        assert task.python_callable is module.run_dbt_phase
        assert task.kwargs["retries"] == 1
        assert task.kwargs["retry_delay"] == module.DBT_RETRY_DELAY
        assert task.kwargs["on_failure_callback"] is module.record_traffic_dbt_problem
        assert "dbt_args" in task.kwargs["op_kwargs"]

    assert "test --select" in dag.task_dict["dbt_test_silver"].kwargs["op_kwargs"]["dbt_args"]


def test_dbt_contract_failure_skips_airflow_retry_and_records_pinned_snapshot(monkeypatch):
    module = load_transform_module()
    pushed = {}
    ti = types.SimpleNamespace(
        task_id="dbt_test_silver",
        try_number=1,
        xcom_pull=lambda task_ids: "snapshot-a" if task_ids == module.SNAPSHOT_TASK_ID else None,
        xcom_push=lambda key, value: pushed.update(key=key, value=value),
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *_args, **_kwargs: types.SimpleNamespace(returncode=1, stdout="", stderr=""),
    )
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda _path: [{
            "unique_id": "test.ask_seoul.assert_silver_traffic_location_contract",
            "status": "fail",
            "failures": 2,
        }],
    )

    with pytest.raises(FakeAirflowFailException):
        module.run_dbt_phase(
            dbt_args="test --select silver_seoul_traffic_incident",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    assert pushed["key"] == module.DBT_FAILURE_XCOM_KEY
    assert pushed["value"]["traffic_snapshot_dag_run_id"] == "snapshot-a"
    assert pushed["value"]["failure_classification"] == "data-contract-violation"
    assert pushed["value"]["silver_persisted"] is True


def test_failed_silver_run_reports_the_model_that_already_persisted(monkeypatch):
    module = load_transform_module()
    pushed = {}
    ti = types.SimpleNamespace(
        task_id="dbt_run_silver",
        try_number=1,
        xcom_pull=lambda task_ids: "snapshot-a" if task_ids == module.SNAPSHOT_TASK_ID else None,
        xcom_push=lambda key, value: pushed.update(key=key, value=value),
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *_args, **_kwargs: types.SimpleNamespace(returncode=1, stdout="", stderr=""),
    )
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda _path: [
            {
                "unique_id": "model.ask_seoul.silver_seoul_traffic_incident",
                "status": "success",
            },
            {
                "unique_id": "model.ask_seoul.silver_seoul_traffic_incident_current",
                "status": "error",
                "message": "Compilation Error",
            },
        ],
    )

    with pytest.raises(FakeAirflowFailException):
        module.run_dbt_phase(
            dbt_args="run --select silver_seoul_traffic_incident silver_seoul_traffic_incident_current",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=False,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    assert pushed["value"]["silver_persisted"] is True


def test_trino_dns_failure_retries_with_the_same_pinned_snapshot(monkeypatch):
    module = load_transform_module()
    pushed = {}
    ti = types.SimpleNamespace(
        task_id="dbt_test_silver",
        try_number=1,
        xcom_pull=lambda task_ids: "snapshot-a" if task_ids == module.SNAPSHOT_TASK_ID else None,
        xcom_push=lambda key, value: pushed.update(key=key, value=value),
    )
    commands = []
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, **_kwargs: (commands.append(command) or types.SimpleNamespace(
            returncode=2, stdout="", stderr=""
        )),
    )
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda _path: [{
            "unique_id": "test.ask_seoul.assert_silver_traffic_location_contract",
            "status": "error",
            "message": "TrinoConnectionError: getaddrinfo temporary failure in name resolution",
        }],
    )

    with pytest.raises(FakeAirflowException):
        module.run_dbt_phase(
            dbt_args="test --select silver_seoul_traffic_incident",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    assert pushed["value"]["failure_classification"] == "retryable-infrastructure-error"
    assert any("snapshot-a" in argument for argument in commands[0])


def test_model_execution_failure_skips_airflow_retry(monkeypatch):
    module = load_transform_module()
    ti = types.SimpleNamespace(
        task_id="dbt_run_silver",
        try_number=1,
        xcom_pull=lambda task_ids: "snapshot-a" if task_ids == module.SNAPSHOT_TASK_ID else None,
        xcom_push=lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *_args, **_kwargs: types.SimpleNamespace(returncode=1, stdout="", stderr=""),
    )
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda _path: [{
            "unique_id": "model.ask_seoul.silver_seoul_traffic_incident",
            "status": "error",
            "message": "Compilation Error",
        }],
    )

    with pytest.raises(FakeAirflowFailException):
        module.run_dbt_phase(
            dbt_args="run --select silver_seoul_traffic_incident",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=False,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )


def test_final_dbt_failure_callback_persists_recovery_record_and_notifies(monkeypatch):
    module = load_transform_module()
    record = {
        "dag_id": "traffic_incident_transform",
        "task_id": "dbt_test_silver",
        "run_id": "manual__a",
        "failure_classification": "data-contract-violation",
        "traffic_snapshot_dag_run_id": "snapshot-a",
        "dbt_artifact_path": "/tmp/run_results.json",
        "dbt_test_names": ["assert_silver_traffic_location_contract"],
        "dbt_failed_row_count": 2,
        "silver_persisted": True,
        "recovery_action": "manual-approval-required",
    }
    ti = types.SimpleNamespace(
        task_id="dbt_test_silver",
        dag_id="traffic_incident_transform",
        try_number=1,
        xcom_pull=lambda **_kwargs: record,
    )
    written = {}
    notified = {}
    monkeypatch.setattr(
        module,
        "R2ErrorSink",
        lambda: types.SimpleNamespace(write=lambda problem: written.update(problem=problem)),
    )
    monkeypatch.setattr(
        module,
        "R2RecoveryRecordSink",
        lambda: types.SimpleNamespace(write=lambda value: written.update(recovery=value)),
    )
    monkeypatch.setattr(module, "first_notice_for_run", lambda *_args: True)
    monkeypatch.setattr(
        module,
        "send_embed",
        lambda title, description, **kwargs: notified.update(
            title=title, description=description, kwargs=kwargs
        ),
    )

    module.record_traffic_dbt_problem({
        "ti": ti,
        "dag": types.SimpleNamespace(dag_id="traffic_incident_transform"),
        "run_id": "manual__a",
        "exception": FakeAirflowFailException("contract"),
    })

    assert written["recovery"] == record
    problem_document = written["problem"].to_dict()
    assert problem_document["traffic_snapshot_dag_run_id"] == "snapshot-a"
    assert problem_document["dbt_test_names"] == ["assert_silver_traffic_location_contract"]
    assert problem_document["dbt_failed_row_count"] == 2
    assert problem_document["dbt_artifact_path"] == "/tmp/run_results.json"
    assert problem_document["silver_persisted"] is True
    assert problem_document["failure_classification"] == "data-contract-violation"
    assert "assert_silver_traffic_location_contract" in notified["description"]
    assert "/tmp/run_results.json" in notified["description"]


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
