import importlib.util
import shlex
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
        FakeDAG._stack[-1].add_task(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)
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


class FakeTriggerRule:
    ALL_DONE = "all_done"
    ONE_FAILED = "one_failed"


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
    install_airflow_fakes()
    module_path = Path(__file__).resolve().parents[1] / "traffic_incident_transform.py"
    spec = importlib.util.spec_from_file_location("traffic_incident_transform_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def dbt_option_tokens(dbt_args: str, option: str) -> set[str]:
    tokens = shlex.split(dbt_args)
    option_start = tokens.index(option) + 1
    option_end = next(
        (
            index
            for index, token in enumerate(tokens[option_start:], start=option_start)
            if token.startswith("--")
        ),
        len(tokens),
    )
    return set(tokens[option_start:option_end])


def test_traffic_transform_bootstraps_asac_axes_before_silver():
    module = load_transform_module()
    dag = module.dag

    expected_task_order = [
        "resolve_traffic_snapshot_run",
        "dbt_deps",
        "dbt_source_freshness",
        "dbt_test_traffic_incident_availability",
        "dbt_seed_asac_axes",
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
    ]

    assert set(expected_task_order) <= set(dag.task_ids)
    for upstream_task_id, downstream_task_id in zip(expected_task_order, expected_task_order[1:]):
        assert dag.task_dict[upstream_task_id].downstream_task_ids == {
            downstream_task_id,
            "fail_transform_if_upstream_failed",
        }

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
    assert (
        task_commands["dbt_run_common_admin_dong_dimension"]
        == "run --select asac_axes.dim_admin_dong"
    )
    assert (
        task_commands["dbt_test_common_admin_dong_dimension"]
        == "test --select asac_axes.dim_admin_dong"
    )
    for task_id in (
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
    ):
        assert (
            dag.task_dict[task_id].kwargs["op_kwargs"]["snapshot_task_id"]
            == "resolve_traffic_snapshot_run"
        )
        assert dag.task_dict[task_id].kwargs["op_kwargs"]["silver_persisted"] is False
    assert dag.task_dict["resolve_traffic_snapshot_run"].downstream_task_ids == {
        "dbt_deps",
        "fail_transform_if_upstream_failed",
    }
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
    canonical_gold_model = "gold_traffic_incident_current_by_admin_dong_hourly"
    assert canonical_gold_model in dbt_option_tokens(task_commands["dbt_run_gold"], "--select")
    assert canonical_gold_model in dbt_option_tokens(task_commands["dbt_test_gold"], "--select")
    assert dag.task_dict["dbt_test_gold"].kwargs["op_kwargs"]["fresh_parse"] is True


def test_silver_excludes_eager_gold_contracts_until_gold_rebuild():
    module = load_transform_module()
    silver_test_args = module.dag.task_dict["dbt_test_silver"].kwargs["op_kwargs"]["dbt_args"]
    excluded_tokens = dbt_option_tokens(silver_test_args, "--exclude")

    assert {
        "assert_gold_traffic_counts_match_silver",
        "assert_gold_traffic_current_by_admin_dong_hourly_fanout_reconciles",
        "assert_gold_traffic_current_by_admin_dong_hourly_snapshot_reconciles",
    } <= excluded_tokens


def test_gold_phase_selects_all_canonical_hourly_contracts():
    module = load_transform_module()
    gold_test_args = module.dag.task_dict["dbt_test_gold"].kwargs["op_kwargs"]["dbt_args"]
    selected_tokens = dbt_option_tokens(gold_test_args, "--select")
    expected_test_names = {
        "assert_gold_traffic_current_by_admin_dong_hourly_admin_join_reconciles",
        "assert_gold_traffic_current_by_admin_dong_hourly_admin_stamp_exact",
        "assert_gold_traffic_current_by_admin_dong_hourly_fanout_reconciles",
        "assert_gold_traffic_current_by_admin_dong_hourly_grain_unique",
        "assert_gold_traffic_current_by_admin_dong_hourly_hourly_completeness",
        "assert_gold_traffic_current_by_admin_dong_hourly_product_row_id_reproducible",
        "assert_gold_traffic_current_by_admin_dong_hourly_snapshot_reconciles",
        "assert_gold_traffic_current_by_admin_dong_hourly_zero_requires_complete",
    }

    assert expected_test_names <= selected_tokens


def test_gold_contract_test_fresh_parses_in_same_task_artifact(monkeypatch):
    module = load_transform_module()
    commands = []
    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=2,
        xcom_pull=lambda task_ids: "snapshot-a",
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, **_kwargs: (
            commands.append(command)
            or types.SimpleNamespace(returncode=0, stdout="", stderr="")
        ),
    )

    result = module.run_dbt_phase(
        dbt_args="test --select gold_traffic_incident_current_by_admin_dong_hourly",
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=True,
        fresh_parse=True,
        ti=ti,
        run_id="manual__a",
        params={"target": "dev"},
    )

    assert [command[1] for command in commands] == ["parse", "test"]
    assert "--no-partial-parse" in commands[0]
    assert commands[0][commands[0].index("--target") + 1] == "dev"
    assert commands[1][commands[1].index("--target") + 1] == "dev"
    assert commands[0][commands[0].index("--vars") + 1] == (
        '{"traffic_snapshot_dag_run_id": "snapshot-a"}'
    )
    assert commands[1][commands[1].index("--vars") + 1] == (
        '{"traffic_snapshot_dag_run_id": "snapshot-a"}'
    )
    parse_target = Path(commands[0][commands[0].index("--target-path") + 1])
    test_target = Path(commands[1][commands[1].index("--target-path") + 1])
    assert parse_target == test_target == Path(result["artifact_path"]).parent
    assert Path(result["artifact_path"]).parts[-4:] == (
        "manual__a",
        "dbt_test_gold",
        "try2",
        "run_results.json",
    )


def test_gold_contract_parse_failure_stops_before_test_and_records_task_artifact(monkeypatch):
    module = load_transform_module()
    commands = []
    loaded_paths = []
    pushed = {}
    ti = types.SimpleNamespace(
        dag_id="traffic_incident_transform",
        task_id="dbt_test_gold",
        try_number=2,
        xcom_pull=lambda task_ids: "snapshot-a",
        xcom_push=lambda key, value: pushed.update(key=key, value=value),
    )

    def run(command, **_kwargs):
        commands.append(command)
        if command[1] == "parse":
            return types.SimpleNamespace(
                returncode=1,
                stdout="",
                stderr="Compilation Error",
            )
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", run)
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda path: loaded_paths.append(Path(path)) or [],
    )

    with pytest.raises(FakeAirflowFailException, match="model-execution-failed"):
        module.run_dbt_phase(
            dbt_args="test --select gold_traffic_incident_current_by_admin_dong_hourly",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            fresh_parse=True,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    assert [command[1] for command in commands] == ["parse"]
    assert pushed["key"] == module.DBT_FAILURE_XCOM_KEY
    record = pushed["value"]
    artifact_path = Path(record["dbt_artifact_path"])
    assert loaded_paths == [artifact_path]
    assert record["failure_classification"] == "model-execution-failed"
    assert record["traffic_snapshot_dag_run_id"] == "snapshot-a"
    assert record["run_id"] == "manual__a"
    assert record["task_id"] == "dbt_test_gold"
    assert record["try_number"] == 2
    assert artifact_path.parts[-5:] == (
        "traffic-transform",
        "manual__a",
        "dbt_test_gold",
        "try2",
        "run_results.json",
    )


def test_traffic_dbt_tasks_classify_failures_before_airflow_retries():
    module = load_transform_module()
    dag = module.dag
    classified_task_ids = [
        "dbt_deps",
        "dbt_source_freshness",
        "dbt_test_traffic_incident_availability",
        "dbt_seed_asac_axes",
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
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


def test_dbt_deps_omits_target_path_but_model_phases_keep_isolated_artifacts(monkeypatch):
    module = load_transform_module()
    commands = []
    ti = types.SimpleNamespace(
        task_id="dbt_run_silver",
        try_number=1,
        xcom_pull=lambda task_ids: "snapshot-a" if task_ids == module.SNAPSHOT_TASK_ID else None,
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, **_kwargs: (commands.append(command) or types.SimpleNamespace(
            returncode=0, stdout="", stderr=""
        )),
    )

    for dbt_args in ("deps", "run --select silver_seoul_traffic_incident"):
        module.run_dbt_phase(
            dbt_args=dbt_args,
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=False,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    deps_command, run_command = commands
    assert "--target-path" not in deps_command
    assert "--target-path" in run_command
    assert deps_command[deps_command.index("--vars") + 1] == '{"traffic_snapshot_dag_run_id": "snapshot-a"}'
    assert run_command[run_command.index("--vars") + 1] == '{"traffic_snapshot_dag_run_id": "snapshot-a"}'


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
    assert guard.downstream_task_ids == {
        "resolve_traffic_snapshot_run",
        "fail_transform_if_upstream_failed",
    }


def test_traffic_transform_limits_target_param_to_dev_or_prod():
    module = load_transform_module()

    target_param = module.DEFAULT_PARAMS["target"]

    assert target_param.value == "dev"
    assert target_param.schema["enum"] == ["dev"]


def test_traffic_transform_publishes_dbt_run_metrics_after_terminal_test():
    module = load_transform_module()

    task = module.dag.task_dict["publish_dbt_run_metrics"]

    assert task.kwargs["trigger_rule"] == "all_done"
    assert module.dag.task_dict["dbt_test_gold"].downstream_task_ids == {
        "publish_dbt_run_metrics",
        "fail_transform_if_upstream_failed",
    }


def test_traffic_transform_has_independent_failure_propagating_leaf():
    module = load_transform_module()
    dag = module.dag
    transform_task_ids = {
        "validate_dev_runtime",
        "resolve_traffic_snapshot_run",
        "dbt_deps",
        "dbt_source_freshness",
        "dbt_test_traffic_incident_availability",
        "dbt_seed_asac_axes",
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
    }

    metrics = dag.task_dict["publish_dbt_run_metrics"]
    watcher = dag.task_dict["fail_transform_if_upstream_failed"]

    assert metrics.kwargs["trigger_rule"] == FakeTriggerRule.ALL_DONE
    assert watcher.kwargs["trigger_rule"] == FakeTriggerRule.ONE_FAILED
    assert watcher.kwargs["retries"] == 0
    assert metrics.downstream_task_ids == set()
    assert watcher.downstream_task_ids == set()
    assert watcher.upstream_task_ids == transform_task_ids


def test_traffic_failure_propagation_callable_always_fails():
    module = load_transform_module()

    with pytest.raises(FakeAirflowFailException, match="traffic transform upstream task failed"):
        module.fail_transform_if_upstream_failed()


def test_traffic_metrics_use_latest_current_run_dbt_artifact_path():
    module = load_transform_module()
    terminal_path = "/tmp/traffic-terminal/run_results.json"
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            {"artifact_path": terminal_path} if task_ids == "dbt_test_gold" and key is None else None
        )
    )

    assert module._current_run_results_path(ti=ti) == terminal_path


def test_traffic_metrics_use_earlier_success_artifact_when_later_phases_have_none():
    module = load_transform_module()
    earlier_path = "/tmp/traffic-silver/run_results.json"
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            {"artifact_path": earlier_path}
            if task_ids == "dbt_run_silver" and key is None
            else None
        )
    )

    assert module._current_run_results_path(ti=ti) == earlier_path


def test_traffic_metrics_use_earlier_failure_artifact():
    module = load_transform_module()
    failure_path = "/tmp/traffic-freshness-failure/run_results.json"
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            {"dbt_artifact_path": failure_path}
            if task_ids == "dbt_source_freshness" and key == module.DBT_FAILURE_XCOM_KEY
            else None
        )
    )

    assert module._current_run_results_path(ti=ti) == failure_path


def test_traffic_metrics_prefer_most_advanced_current_run_artifact():
    module = load_transform_module()
    artifacts = {
        ("dbt_deps", None): {"artifact_path": "/tmp/deps/run_results.json"},
        ("dbt_run_silver", module.DBT_FAILURE_XCOM_KEY): {
            "dbt_artifact_path": "/tmp/silver-failure/run_results.json"
        },
        ("dbt_run_gold", None): {"artifact_path": "/tmp/gold/run_results.json"},
    }
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: artifacts.get((task_ids, key))
    )

    assert module._current_run_results_path(ti=ti) == "/tmp/gold/run_results.json"


def test_traffic_metrics_resolver_never_falls_back_to_shared_target(tmp_path, monkeypatch):
    module = load_transform_module()
    stale_shared_result = tmp_path / "target" / "run_results.json"
    stale_shared_result.parent.mkdir()
    stale_shared_result.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(module, "RUN_RESULTS_PATH", str(stale_shared_result))
    ti = types.SimpleNamespace(xcom_pull=lambda **_kwargs: None)

    assert module._current_run_results_path(ti=ti) is None


def test_traffic_metrics_skip_stale_shared_target_without_current_run_artifact(
    tmp_path, monkeypatch
):
    module = load_transform_module()
    stale_shared_result = tmp_path / "target" / "run_results.json"
    stale_shared_result.parent.mkdir()
    stale_shared_result.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(module, "RUN_RESULTS_PATH", str(stale_shared_result))
    published_paths = []
    monkeypatch.setattr(
        module,
        "dump_dbt_run_results",
        lambda path, **_kwargs: published_paths.append(path) or [{}],
    )
    ti = types.SimpleNamespace(xcom_pull=lambda **_kwargs: None)

    assert module.publish_dbt_run_metrics(ti=ti, params={"target": "dev"}) == {
        "rows": 0,
        "skipped": True,
    }
    assert published_paths == []


def test_traffic_publish_dbt_run_metrics_forwards_domain_and_target(tmp_path, monkeypatch):
    module = load_transform_module()
    run_results = tmp_path / "run_results.json"
    run_results.write_text("{}", encoding="utf-8")
    captured = {}

    def fake_dump(path, *, domain, target):
        captured.update(path=path, domain=domain, target=target)
        return [{}]

    monkeypatch.setattr(module, "dump_dbt_run_results", fake_dump)

    assert module.publish_dbt_run_metrics(
        run_results_path=str(run_results), params={"target": "dev"}
    ) == {"rows": 1, "skipped": False}
    assert captured == {"path": str(run_results), "domain": "traffic", "target": "dev"}
