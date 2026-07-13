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
    spec = importlib.util.spec_from_file_location("traffic_snapshot_recovery_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_recovery_dag_is_manual_and_uses_only_recovery_selectors():
    module = load_recovery_module()
    dag = module.dag

    assert dag.dag_id == "traffic_snapshot_recovery"
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["params"]["target"].schema["enum"] == ["dev"]
    assert dag.kwargs["params"]["snapshot_dag_run_id"].value == ""

    expected_task_order = [
        "validate_dev_runtime",
        "validate_publishable_snapshot",
        "dbt_deps",
        "dbt_run_recovery_silver",
        "dbt_run_recovery_metadata",
        "dbt_test_recovery_silver",
        "dbt_run_recovery_gold",
        "dbt_test_recovery_gold",
        "record_recovery_completion",
    ]
    assert set(expected_task_order) == set(dag.task_ids)
    for upstream_task_id, downstream_task_id in zip(expected_task_order, expected_task_order[1:]):
        assert dag.task_dict[upstream_task_id].downstream_task_ids == {downstream_task_id}

    dbt_args = {
        task_id: dag.task_dict[task_id].kwargs["op_kwargs"]["dbt_args"]
        for task_id in expected_task_order
        if task_id.startswith("dbt_")
    }
    assert dbt_args == {
        "dbt_deps": "deps",
        "dbt_run_recovery_silver": "run --select recovery_silver_seoul_traffic_incident",
        "dbt_run_recovery_metadata": "run --select recovery_traffic_snapshot_metadata",
        "dbt_test_recovery_silver": (
            "test --select recovery_traffic_snapshot_metadata "
            "recovery_silver_seoul_traffic_incident "
            "assert_recovery_silver_traffic_snapshot_matches_bronze"
        ),
        "dbt_run_recovery_gold": "run --select recovery_gold_traffic_incident_summary",
        "dbt_test_recovery_gold": (
            "test --select recovery_traffic_snapshot_metadata "
            "recovery_silver_seoul_traffic_incident "
            "recovery_gold_traffic_incident_summary "
            "assert_recovery_gold_traffic_counts_match_silver"
        ),
    }
    for task_id in dbt_args:
        assert dag.task_dict[task_id].kwargs["on_failure_callback"] is module.record_recovery_dbt_problem


def test_validate_publishable_snapshot_rejects_blank_and_nonpublishable_inputs(monkeypatch):
    module = load_recovery_module()

    with pytest.raises(FakeAirflowFailException, match="snapshot_dag_run_id is required"):
        module.validate_publishable_snapshot(params={"snapshot_dag_run_id": "  "})

    class Cursor:
        def __init__(self, row):
            self.row = row
            self.query = ""

        def execute(self, query):
            self.query = query

        def fetchone(self):
            return self.row

    missing_cursor = Cursor(None)
    monkeypatch.setattr(module, "trino_cursor", lambda: (missing_cursor, "iceberg_dev", "ask_seoul"))
    with pytest.raises(FakeAirflowFailException, match="not publishable"):
        module.validate_publishable_snapshot(params={"snapshot_dag_run_id": "historical-run"})

    publishable_cursor = Cursor(("historical-run",))
    monkeypatch.setattr(module, "trino_cursor", lambda: (publishable_cursor, "iceberg_dev", "ask_seoul"))

    assert module.validate_publishable_snapshot(params={"snapshot_dag_run_id": "historical-run"}) == "historical-run"
    assert "source_id = 'seoul_traffic_incident'" in publishable_cursor.query
    assert "status = 'SUCCESS'" in publishable_cursor.query
    assert "AND is_publishable" in publishable_cursor.query


def test_recovery_dbt_phase_scopes_its_artifact_and_pins_the_validated_snapshot(monkeypatch):
    module = load_recovery_module()

    class TaskInstance:
        task_id = "dbt_run_recovery_silver"
        try_number = 2

        def xcom_pull(self, *, task_ids):
            assert task_ids == "validate_publishable_snapshot"
            return "historical-run"

    observed = {}

    class Completed:
        returncode = 0
        stdout = "dbt succeeded\n"
        stderr = ""

    def fake_run(command, **kwargs):
        observed["command"] = command
        observed["kwargs"] = kwargs
        return Completed()

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    result = module.run_recovery_dbt_phase(
        dbt_args="run --select recovery_silver_seoul_traffic_incident",
        snapshot_task_id="validate_publishable_snapshot",
        recovery_silver_persisted=False,
        ti=TaskInstance(),
        run_id="manual__recovery",
        params={"target": "dev"},
    )

    assert result == {
        "status": "success",
        "artifact_path": (
            "/opt/airflow/dbt/domains/traffic/target/traffic-snapshot-recovery/"
            "manual__recovery/dbt_run_recovery_silver/try2/run_results.json"
        ),
    }
    assert observed["command"] == [
        "/home/airflow/dbt-venv/bin/dbt",
        "run",
        "--select",
        "recovery_silver_seoul_traffic_incident",
        "--target",
        "dev",
        "--no-use-colors",
        "--vars",
        '{"traffic_snapshot_dag_run_id": "historical-run"}',
        "--target-path",
        (
            "/opt/airflow/dbt/domains/traffic/target/traffic-snapshot-recovery/"
            "manual__recovery/dbt_run_recovery_silver/try2"
        ),
    ]
    assert observed["kwargs"]["cwd"] == "/opt/airflow/dbt/domains/traffic"
    assert observed["kwargs"]["check"] is False


def test_recovery_dbt_contract_failure_records_the_recovery_snapshot(monkeypatch):
    module = load_recovery_module()

    class TaskInstance:
        dag_id = "traffic_snapshot_recovery"
        task_id = "dbt_test_recovery_silver"
        try_number = 1

        def __init__(self):
            self.pushed = {}

        def xcom_pull(self, *, task_ids):
            assert task_ids == "validate_publishable_snapshot"
            return "historical-run"

        def xcom_push(self, *, key, value):
            self.pushed[key] = value

    class Completed:
        returncode = 1
        stdout = "dbt failed\n"
        stderr = ""

    ti = TaskInstance()
    monkeypatch.setattr(module.subprocess, "run", lambda *_args, **_kwargs: Completed())
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda _path: [
            {
                "unique_id": "test.traffic.assert_recovery_silver_traffic_snapshot_matches_bronze",
                "status": "fail",
                "failures": 1,
            }
        ],
    )

    with pytest.raises(FakeAirflowFailException, match="data-contract-violation"):
        module.run_recovery_dbt_phase(
            dbt_args="test --select assert_recovery_silver_traffic_snapshot_matches_bronze",
            snapshot_task_id="validate_publishable_snapshot",
            recovery_silver_persisted=True,
            ti=ti,
            run_id="manual__recovery",
            params={"target": "dev"},
        )

    record = ti.pushed[module.DBT_FAILURE_XCOM_KEY]
    assert record["dag_id"] == "traffic_snapshot_recovery"
    assert record["traffic_snapshot_dag_run_id"] == "historical-run"
    assert record["failure_classification"] == "data-contract-violation"
    assert record["silver_persisted"] is True
    assert "traffic-snapshot-recovery" in record["dbt_artifact_path"]


def test_recovery_completion_writes_and_notifies_snapshot_evidence(monkeypatch):
    module = load_recovery_module()

    artifacts = {
        task_id: {
            "status": "success",
            "artifact_path": f"/artifacts/{task_id}/run_results.json",
        }
        for task_id in module.RECOVERY_DBT_TASK_IDS
    }

    class TaskInstance:
        dag_id = "traffic_snapshot_recovery"
        task_id = "record_recovery_completion"
        try_number = 1

        def xcom_pull(self, *, task_ids, key=None):
            assert key is None
            if task_ids == "validate_publishable_snapshot":
                return "historical-run"
            return artifacts[task_ids]

    written = {}
    notified = {}
    monkeypatch.setattr(
        module,
        "R2RecoveryRecordSink",
        lambda: types.SimpleNamespace(write=lambda record: written.update(record=record) or "recovery/key.json"),
    )
    monkeypatch.setattr(module, "first_notice_for_run", lambda *_args: True)
    monkeypatch.setattr(
        module,
        "send_embed",
        lambda title, description, **kwargs: notified.update(
            title=title, description=description, kwargs=kwargs
        ),
    )

    record = module.record_recovery_completion(
        ti=TaskInstance(),
        run_id="manual__recovery",
        dag=types.SimpleNamespace(dag_id="traffic_snapshot_recovery"),
    )

    assert written["record"] == record
    assert record["recovery_purpose"] == "historical-snapshot-validation"
    assert record["recovery_status"] == "success"
    assert record["traffic_snapshot_dag_run_id"] == "historical-run"
    assert record["recovery_relations"] == list(module.RECOVERY_RELATIONS)
    assert record["dbt_task_statuses"] == {
        task_id: "success" for task_id in module.RECOVERY_DBT_TASK_IDS
    }
    assert record["dbt_artifacts"] == {
        task_id: artifacts[task_id]["artifact_path"]
        for task_id in module.RECOVERY_ARTIFACT_TASK_IDS
    }
    assert "historical-run" in notified["description"]
    assert "recovery_gold_traffic_incident_summary" in notified["description"]


def test_recovery_dbt_failure_callback_writes_and_notifies_the_classified_record(monkeypatch):
    module = load_recovery_module()
    failure_record = {
        "traffic_snapshot_dag_run_id": "historical-run",
        "failure_classification": "data-contract-violation",
        "dbt_test_names": ["assert_recovery_silver_traffic_snapshot_matches_bronze"],
        "dbt_failed_row_count": 1,
        "dbt_artifact_path": "/artifacts/test/run_results.json",
        "silver_persisted": True,
        "recovery_action": "manual-approval-required",
        "dag_id": "traffic_snapshot_recovery",
        "run_id": "manual__recovery",
        "task_id": "dbt_test_recovery_silver",
    }

    class TaskInstance:
        task_id = "dbt_test_recovery_silver"

        def xcom_pull(self, *, task_ids, key):
            assert task_ids == self.task_id
            assert key == module.DBT_FAILURE_XCOM_KEY
            return failure_record

    written = {}
    notified = {}
    monkeypatch.setattr(
        module,
        "R2RecoveryRecordSink",
        lambda: types.SimpleNamespace(write=lambda record: written.update(record=record)),
    )
    monkeypatch.setattr(module, "first_notice_for_run", lambda *_args: True)
    monkeypatch.setattr(
        module,
        "send_embed",
        lambda title, description, **kwargs: notified.update(
            title=title, description=description, kwargs=kwargs
        ),
    )

    module.record_recovery_dbt_problem({"ti": TaskInstance()})

    assert written["record"] == failure_record
    assert "historical-run" in notified["description"]
    assert notified["kwargs"]["domain"] == "traffic"


def test_readme_documents_the_manual_recovery_boundary():
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text(
        encoding="utf-8"
    )

    assert "traffic_snapshot_recovery" in readme
    assert "snapshot_dag_run_id" in readme
    assert "recovery_silver_seoul_traffic_incident" in readme
    assert "canonical" in readme


def test_recovery_completion_keeps_deps_status_without_a_run_results_artifact(monkeypatch):
    module = load_recovery_module()
    results = {
        task_id: {"status": "success", "artifact_path": f"/artifacts/{task_id}/run_results.json"}
        for task_id in module.RECOVERY_DBT_TASK_IDS
    }
    results["dbt_deps"] = {"status": "success", "artifact_path": None}

    class TaskInstance:
        dag_id = "traffic_snapshot_recovery"
        task_id = "record_recovery_completion"
        try_number = 1

        def xcom_pull(self, *, task_ids, key=None):
            if task_ids == module.SNAPSHOT_TASK_ID:
                return "historical-run"
            return results[task_ids]

    monkeypatch.setattr(
        module, "R2RecoveryRecordSink", lambda: types.SimpleNamespace(write=lambda _record: None)
    )
    monkeypatch.setattr(module, "first_notice_for_run", lambda *_args: False)

    record = module.record_recovery_completion(
        ti=TaskInstance(), run_id="manual__recovery"
    )

    assert record["dbt_task_statuses"]["dbt_deps"] == "success"
    assert "dbt_deps" not in record["dbt_artifacts"]
