import importlib.util
import sys
import types
from pathlib import Path

import pytest


_FAKE_MODULE_NAMES = (
    "airflow",
    "airflow.exceptions",
    "airflow.models",
    "airflow.models.param",
    "airflow.providers",
    "airflow.providers.standard",
    "airflow.providers.standard.operators",
    "airflow.providers.standard.operators.python",
    "airflow.utils",
    "airflow.utils.trigger_rule",
)


@pytest.fixture(autouse=True)
def restore_fake_modules_after_maintenance_import():
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
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()
        FakeDAG._stack[-1].add_task(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)
        return other


class FakeParam:
    def __init__(self, default, **schema):
        self.default = default
        self.schema = schema


class FakeTriggerRule:
    ALL_DONE = "all_done"


class FakeAirflowSkipException(Exception):
    pass


class FakeTaskInstance:
    def __init__(self, values=None):
        self.values = dict(values or {})
        self.current_task_id = None

    def xcom_pull(self, task_ids, key="return_value"):
        return self.values.get((task_ids, key))

    def xcom_push(self, key, value):
        self.values[(self.current_task_id, key)] = value


class FakeDagRun:
    def __init__(self, run_id="manual__test", states=None):
        self.run_id = run_id
        self.states = dict(states or {})

    def get_task_instance(self, task_id):
        return types.SimpleNamespace(state=self.states.get(task_id))


def install_airflow_fakes():
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG
    airflow_exceptions = types.ModuleType("airflow.exceptions")
    airflow_exceptions.AirflowSkipException = FakeAirflowSkipException
    airflow_models = types.ModuleType("airflow.models")
    airflow_param = types.ModuleType("airflow.models.param")
    airflow_param.Param = FakeParam
    airflow_providers = types.ModuleType("airflow.providers")
    airflow_standard = types.ModuleType("airflow.providers.standard")
    airflow_operators = types.ModuleType("airflow.providers.standard.operators")
    airflow_python = types.ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = FakePythonOperator
    airflow_utils = types.ModuleType("airflow.utils")
    airflow_trigger_rule = types.ModuleType("airflow.utils.trigger_rule")
    airflow_trigger_rule.TriggerRule = FakeTriggerRule

    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.exceptions": airflow_exceptions,
            "airflow.models": airflow_models,
            "airflow.models.param": airflow_param,
            "airflow.providers": airflow_providers,
            "airflow.providers.standard": airflow_standard,
            "airflow.providers.standard.operators": airflow_operators,
            "airflow.providers.standard.operators.python": airflow_python,
            "airflow.utils": airflow_utils,
            "airflow.utils.trigger_rule": airflow_trigger_rule,
        }
    )


def load_maintenance_module():
    install_airflow_fakes()
    module_path = Path(__file__).resolve().parents[1] / "weather_iceberg_maintenance.py"
    spec = importlib.util.spec_from_file_location(
        "weather_iceberg_maintenance_under_test",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def immutable_plan_payload(module, tables):
    return {
        "plan_id": "manual__test",
        "plan_hash": "a" * 64,
        "tables": list(tables),
    }


def action_result(module, *, table, operation, status="SUCCEEDED", **extra):
    return {
        "plan_id": "manual__test",
        "plan_hash": "a" * 64,
        "table": table,
        "operation": operation,
        "status": status,
        "circuit_breaker": False,
        **extra,
    }


def confirmed_table_failure(module, *, table, operation):
    return action_result(
        module,
        table=table,
        operation=operation,
        status="FAILED",
        category="TABLE_OPERATION",
        phase="SUBMITTED",
        query_id="query_123",
        query_state="FAILED",
        structured_error_type="USER_ERROR",
        structured_error_name="TABLE_NOT_FOUND",
    )


def gate_control(table, status, circuit_open, **extra):
    return {"table": table, "status": status, "circuit_open": circuit_open, **extra}


def test_maintenance_dag_is_paused_dev_only_and_serial():
    module = load_maintenance_module()

    assert module.dag.dag_id == "ask_seoul_iceberg_maintenance"
    assert module.dag.kwargs["max_active_runs"] == 1
    assert module.dag.kwargs["is_paused_upon_creation"] is True
    assert module.DEFAULT_PARAM_VALUES == {
        "target": "dev",
        "retention": "7d",
        "tables": module.CANONICAL_TABLES,
    }


def test_maintenance_builds_static_canonical_action_chain():
    module = load_maintenance_module()

    expected_actions = [
        module.action_task_id(index, table, operation)
        for index, table in enumerate(module.CANONICAL_TABLES, start=1)
        for operation in module.OPERATIONS
    ]
    assert len(expected_actions) == 30
    assert all(task_id in module.dag.task_dict for task_id in expected_actions)

    previous = module.PLAN_TASK_ID
    for index, table in enumerate(module.CANONICAL_TABLES, start=1):
        for operation in module.OPERATIONS:
            task_id = module.action_task_id(index, table, operation)
            task = module.dag.task_dict[task_id]
            assert task.upstream_task_ids == {previous}
            assert task.kwargs["pool"] == module.TRINO_HEAVY_POOL
            assert task.kwargs["pool_slots"] == 1
            assert task.kwargs["weight_rule"] == "absolute"
            assert task.kwargs["retries"] == 0
            previous = task_id
        gate_id = module.gate_task_id(index, table)
        gate = module.dag.task_dict[gate_id]
        assert gate.upstream_task_ids == {previous}
        assert gate.kwargs["trigger_rule"] == "all_done"
        previous = gate_id

    assert module.dag.task_dict["final_report"].upstream_task_ids == {previous}
    assert "run_maintenance" not in Path(module.__file__).read_text(encoding="utf-8")

    for index, table in enumerate(module.CANONICAL_TABLES, start=1):
        expected_previous_gate = (
            None
            if index == 1
            else module.gate_task_id(index - 1, module.CANONICAL_TABLES[index - 2])
        )
        for operation in module.OPERATIONS:
            action = module.dag.task_dict[module.action_task_id(index, table, operation)]
            assert action.kwargs["op_kwargs"]["previous_gate_task_id"] == expected_previous_gate


def test_mutation_tasks_keep_weather_failure_callback():
    module = load_maintenance_module()

    action_tasks = [
        task
        for task_id, task in module.dag.task_dict.items()
        if task_id.startswith("maint_")
    ]
    assert len(action_tasks) == 30
    assert all(
        task.kwargs["on_failure_callback"] is module.record_weather_problem
        for task in action_tasks
    )


def test_preflight_resolves_and_inventories_the_canonical_allowlist(monkeypatch):
    module = load_maintenance_module()
    plan = types.SimpleNamespace(plan_id="manual__test", plan_hash="a" * 64)
    calls = {}

    monkeypatch.setattr(module, "validate_dev_runtime", lambda *args, **kwargs: None)

    def resolve(**kwargs):
        calls["resolve"] = kwargs
        return plan

    def payload(value):
        assert value is plan
        return {"plan_id": plan.plan_id, "plan_hash": plan.plan_hash, "tables": []}

    def inventory(value, *, allowed_tables):
        assert value is plan
        calls["inventory"] = allowed_tables
        return {module.CANONICAL_TABLES[0]: {"status": "EXISTS"}}

    monkeypatch.setattr(module, "resolve_maintenance_plan", resolve)
    monkeypatch.setattr(module, "maintenance_plan_payload", payload)
    monkeypatch.setattr(module, "collect_maintenance_inventory", inventory)

    result = module._preflight(
        params=module.DEFAULT_PARAM_VALUES,
        dag_run=FakeDagRun(),
    )

    assert calls["resolve"]["allowed_tables"] == module.CANONICAL_TABLES
    assert calls["resolve"]["dag_run_id"] == "manual__test"
    assert calls["inventory"] == module.CANONICAL_TABLES
    assert result["inventory"][module.CANONICAL_TABLES[0]]["status"] == "EXISTS"


def test_first_action_skips_before_executor_when_previous_gate_circuit_is_open(monkeypatch):
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[1]
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(
                module, [module.CANONICAL_TABLES[0], table]
            ),
            (module.gate_task_id(1, module.CANONICAL_TABLES[0]), "return_value"): {
                "table": module.CANONICAL_TABLES[0],
                "status": "CIRCUIT_OPEN",
                "circuit_open": True
                ,"source_table": module.CANONICAL_TABLES[0],
                "reason": "UNRECORDED_TASK_FAILURE",
            },
        }
    )

    def executor(*args, **kwargs):
        raise AssertionError("executor must not be reached")

    monkeypatch.setattr(module, "execute_maintenance_action", executor)

    with pytest.raises(FakeAirflowSkipException):
        module._run_action(
            table=table,
            operation="optimize",
            previous_task_id=module.PLAN_TASK_ID,
            previous_gate_task_id=module.gate_task_id(1, module.CANONICAL_TABLES[0]),
            ti=ti,
        )


def test_non_selected_gate_preserves_an_existing_circuit():
    module = load_maintenance_module()
    first_table, unselected_table = module.CANONICAL_TABLES[:2]
    previous_gate = module.gate_task_id(1, first_table)
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(
                module, [first_table]
            ),
            (previous_gate, "return_value"): {
                "table": first_table,
                "status": "CIRCUIT_OPEN",
                "circuit_open": True,
                "source_table": first_table,
                "reason": "UNRECORDED_TASK_FAILURE",
            },
        }
    )

    result = module._table_gate(
        table=unselected_table,
        action_task_ids=["first", "second", "third"],
        previous_gate_task_id=previous_gate,
        ti=ti,
        dag_run=FakeDagRun(),
    )

    assert result == {
        "table": unselected_table,
        "status": "CIRCUIT_OPEN",
        "circuit_open": True,
        "source_table": first_table,
        "reason": "UNRECORDED_TASK_FAILURE",
    }


def test_table_gate_opens_circuit_for_stale_action_identity():
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    task_ids = ["optimize", "expire", "orphan"]
    stale = action_result(module, table=table, operation="optimize")
    stale["plan_hash"] = "b" * 64
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            ("optimize", module.RESULT_XCOM_KEY): stale,
            ("expire", module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation="expire_snapshots"
            ),
            ("orphan", module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation="remove_orphan_files"
            ),
        }
    )

    result = module._table_gate(
        table=table,
        action_task_ids=task_ids,
        ti=ti,
        dag_run=FakeDagRun(states={task_id: "success" for task_id in task_ids}),
    )

    assert result["status"] == "CIRCUIT_OPEN"
    assert result["reason"] == "ACTION_PLAN_HASH_MISMATCH"


def test_table_gate_opens_circuit_when_success_xcom_has_failed_task_instance():
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    task_ids = ["optimize", "expire", "orphan"]
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            ("optimize", module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation="optimize"
            ),
            ("expire", module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation="expire_snapshots"
            ),
            ("orphan", module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation="remove_orphan_files"
            ),
        }
    )

    result = module._table_gate(
        table=table,
        action_task_ids=task_ids,
        ti=ti,
        dag_run=FakeDagRun(
            states={"optimize": "success", "expire": "failed", "orphan": "success"}
        ),
    )

    assert result["status"] == "CIRCUIT_OPEN"
    assert result["reason"] == "ACTION_STATE_MISMATCH"


def test_table_gate_opens_circuit_for_hard_kill_after_last_action_xcom():
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    task_ids = ["optimize", "expire", "orphan"]
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            ("optimize", module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation="optimize"
            ),
            ("expire", module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation="expire_snapshots"
            ),
            ("orphan", module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation="remove_orphan_files"
            ),
        }
    )

    result = module._table_gate(
        table=table,
        action_task_ids=task_ids,
        ti=ti,
        dag_run=FakeDagRun(
            states={"optimize": "success", "expire": "success", "orphan": "failed"}
        ),
    )

    assert result["status"] == "CIRCUIT_OPEN"
    assert result["reason"] == "ACTION_STATE_MISMATCH"


def test_table_gate_allows_only_confirmed_table_local_failure():
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    task_ids = ["optimize", "expire", "orphan"]
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            ("optimize", module.RESULT_XCOM_KEY): confirmed_table_failure(
                module, table=table, operation="optimize"
            ),
        }
    )

    result = module._table_gate(
        table=table,
        action_task_ids=task_ids,
        ti=ti,
        dag_run=FakeDagRun(
            states={"optimize": "failed", "expire": "upstream_failed", "orphan": "skipped"}
        ),
    )

    assert result == {"table": table, "status": "TABLE_FAILED", "circuit_open": False}


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("category", None),
        ("category", "OTHER"),
        ("phase", None),
        ("phase", "PRE_SUBMIT"),
        ("query_id", None),
        ("query_id", ""),
        ("query_state", None),
        ("query_state", "FINISHED"),
        ("structured_error_type", None),
        ("structured_error_type", "INTERNAL_ERROR"),
        ("structured_error_name", None),
        ("structured_error_name", ""),
        ("structured_error_name", "unsafe error"),
    ],
)
def test_table_gate_rejects_incomplete_or_invalid_table_failure_evidence(
    field, replacement
):
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    task_id = module.action_task_id(1, table, "optimize")
    result = confirmed_table_failure(module, table=table, operation="optimize")
    if replacement is None:
        result.pop(field)
    else:
        result[field] = replacement
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            (task_id, module.RESULT_XCOM_KEY): result,
        }
    )

    control = module._table_gate(
        table=table,
        action_task_ids=[
            task_id,
            module.action_task_id(1, table, "expire_snapshots"),
            module.action_task_id(1, table, "remove_orphan_files"),
        ],
        ti=ti,
        dag_run=FakeDagRun(states={task_id: "failed"}),
    )

    assert control["status"] == "CIRCUIT_OPEN"
    assert control["reason"] == "UNCONFIRMED_TABLE_FAILURE"


def test_minimal_failed_result_opens_circuit_and_blocks_next_table_executor(monkeypatch):
    module = load_maintenance_module()
    failed_table, next_table = module.CANONICAL_TABLES[:2]
    failed_task = module.action_task_id(1, failed_table, "optimize")
    failed_gate = module.gate_task_id(1, failed_table)
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(
                module, [failed_table, next_table]
            ),
            (failed_task, module.RESULT_XCOM_KEY): action_result(
                module, table=failed_table, operation="optimize", status="FAILED"
            ),
        }
    )
    control = module._table_gate(
        table=failed_table,
        action_task_ids=[
            failed_task,
            module.action_task_id(1, failed_table, "expire_snapshots"),
            module.action_task_id(1, failed_table, "remove_orphan_files"),
        ],
        ti=ti,
        dag_run=FakeDagRun(states={failed_task: "failed"}),
    )
    ti.values[(failed_gate, "return_value")] = control

    monkeypatch.setattr(
        module,
        "execute_maintenance_action",
        lambda *args, **kwargs: pytest.fail("executor must not be reached"),
    )

    with pytest.raises(FakeAirflowSkipException):
        module._run_action(
            table=next_table,
            operation="optimize",
            previous_task_id=module.PLAN_TASK_ID,
            previous_gate_task_id=failed_gate,
            ti=ti,
            dag_run=FakeDagRun(),
        )

    assert control["status"] == "CIRCUIT_OPEN"
    assert control["reason"] == "UNCONFIRMED_TABLE_FAILURE"


def test_table_gate_requires_a_failed_task_instance_for_table_local_failure():
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    task_id = module.action_task_id(1, table, "optimize")
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            (task_id, module.RESULT_XCOM_KEY): confirmed_table_failure(
                module, table=table, operation="optimize"
            ),
        }
    )

    control = module._table_gate(
        table=table,
        action_task_ids=[
            task_id,
            module.action_task_id(1, table, "expire_snapshots"),
            module.action_task_id(1, table, "remove_orphan_files"),
        ],
        ti=ti,
        dag_run=FakeDagRun(states={task_id: "success"}),
    )

    assert control["status"] == "CIRCUIT_OPEN"
    assert control["reason"] == "UNCONFIRMED_TABLE_FAILURE"


def test_table_gate_opens_circuit_for_unknown_action_status():
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    task_ids = ["optimize", "expire", "orphan"]
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            ("optimize", module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation="optimize", status="PENDING"
            ),
            ("expire", module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation="expire_snapshots"
            ),
            ("orphan", module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation="remove_orphan_files"
            ),
        }
    )

    result = module._table_gate(
        table=table,
        action_task_ids=task_ids,
        ti=ti,
        dag_run=FakeDagRun(states={task_id: "success" for task_id in task_ids}),
    )

    assert result["status"] == "CIRCUIT_OPEN"
    assert result["reason"] == "UNKNOWN_ACTION_STATUS"


def test_table_gate_rejects_malformed_plan_and_action_topology():
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    ti = FakeTaskInstance(
        {(module.PLAN_TASK_ID, "return_value"): {"tables": [table]}}
    )

    invalid_plan = module._table_gate(
        table=table,
        action_task_ids=["optimize", "expire", "orphan"],
        ti=ti,
        dag_run=FakeDagRun(),
    )
    assert invalid_plan["reason"] == "INVALID_IMMUTABLE_PLAN"

    ti = FakeTaskInstance(
        {(module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table])}
    )
    invalid_actions = module._table_gate(
        table=table,
        action_task_ids=["only_one"],
        ti=ti,
        dag_run=FakeDagRun(),
    )
    assert invalid_actions["reason"] == "INVALID_ACTION_TOPOLOGY"


def test_final_report_handles_wrong_gate_control_as_controlled_failure():
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            (module.gate_task_id(1, table), "return_value"): {
                "table": "wrong_table",
                "status": "SUCCEEDED",
                "circuit_open": False,
            },
        }
    )

    with pytest.raises(RuntimeError, match=table):
        module._final_report(ti=ti)


@pytest.mark.parametrize(
    ("selection", "control"),
    [
        ((0, 1), {}),
        ((0, 1), {"table": "bronze_kma_vilage_fcst", "status": "SUCCEEDED"}),
        ((0, 1), {"table": "bronze_kma_vilage_fcst", "circuit_open": False}),
        ((0, 1), gate_control("wrong_table", "SUCCEEDED", False)),
        ((0, 1), gate_control("bronze_kma_vilage_fcst", "NOT_SELECTED", False)),
        ((1,), gate_control("bronze_kma_vilage_fcst", "SUCCEEDED", False)),
    ],
)
def test_invalid_previous_gate_skips_before_executor(monkeypatch, selection, control):
    module = load_maintenance_module()
    previous_table, table = module.CANONICAL_TABLES[:2]
    selected_tables = [module.CANONICAL_TABLES[index] for index in selection]
    previous_gate = module.gate_task_id(1, previous_table)
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(
                module, selected_tables
            ),
            (previous_gate, "return_value"): control,
        }
    )
    called = False

    def executor(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("executor must not be reached")

    monkeypatch.setattr(module, "execute_maintenance_action", executor)

    with pytest.raises(FakeAirflowSkipException):
        module._run_action(
            table=table,
            operation="optimize",
            previous_task_id=module.PLAN_TASK_ID,
            previous_gate_task_id=previous_gate,
            ti=ti,
        )
    assert called is False


def test_invalid_previous_gate_opens_current_gate_circuit_before_selection():
    module = load_maintenance_module()
    previous_table, table = module.CANONICAL_TABLES[:2]
    previous_gate = module.gate_task_id(1, previous_table)
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            (previous_gate, "return_value"): gate_control(
                previous_table, "SUCCEEDED", False
            ),
        }
    )

    result = module._table_gate(
        table=table,
        action_task_ids=["optimize", "expire", "orphan"],
        previous_gate_task_id=previous_gate,
        ti=ti,
        dag_run=FakeDagRun(),
    )

    assert result["status"] == "CIRCUIT_OPEN"
    assert result["reason"] == "INVALID_PREVIOUS_GATE_CONTROL"


@pytest.mark.parametrize(
    ("selection", "control"),
    [
        ((0, 1), gate_control("bronze_kma_vilage_fcst", "TABLE_FAILED", False)),
        ((1,), gate_control("bronze_kma_vilage_fcst", "NOT_SELECTED", False)),
    ],
)
def test_valid_closed_previous_gate_allows_next_table_executor(
    monkeypatch, selection, control
):
    module = load_maintenance_module()
    previous_table, table = module.CANONICAL_TABLES[:2]
    previous_gate = module.gate_task_id(1, previous_table)
    selected_tables = [module.CANONICAL_TABLES[index] for index in selection]
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(
                module, selected_tables
            ),
            (previous_gate, "return_value"): control,
        }
    )
    called = False

    def executor(*args, **kwargs):
        nonlocal called
        called = True
        return {"status": "SUCCEEDED", "circuit_breaker": False}

    monkeypatch.setattr(module, "execute_maintenance_action", executor)

    result = module._run_action(
        table=table,
        operation="optimize",
        previous_task_id=module.PLAN_TASK_ID,
        previous_gate_task_id=previous_gate,
        ti=ti,
    )

    assert result["status"] == "SUCCEEDED"
    assert called is True


@pytest.mark.parametrize(
    ("operation", "predecessor", "result", "state"),
    [
        ("expire_snapshots", "optimize", None, "success"),
        ("expire_snapshots", "optimize", "malformed", "success"),
        (
            "expire_snapshots",
            "optimize",
            {"status": "SUCCEEDED"},
            "success",
        ),
        (
            "expire_snapshots",
            "optimize",
            action_result(
                None,
                table="bronze_kma_vilage_fcst",
                operation="remove_orphan_files",
            ),
            "success",
        ),
        (
            "expire_snapshots",
            "optimize",
            action_result(
                None,
                table="bronze_kma_vilage_fcst",
                operation="optimize",
                plan_hash="b" * 64,
            ),
            "success",
        ),
        (
            "expire_snapshots",
            "optimize",
            action_result(
                None,
                table="bronze_kma_vilage_fcst",
                operation="optimize",
                plan_id="manual__stale",
            ),
            "success",
        ),
        (
            "expire_snapshots",
            "optimize",
            action_result(
                None,
                table="wrong_table",
                operation="optimize",
            ),
            "success",
        ),
        (
            "expire_snapshots",
            "optimize",
            action_result(
                None,
                table="bronze_kma_vilage_fcst",
                operation="optimize",
                circuit_breaker=True,
            ),
            "success",
        ),
        (
            "remove_orphan_files",
            "expire_snapshots",
            action_result(
                None,
                table="bronze_kma_vilage_fcst",
                operation="expire_snapshots",
                status="UNKNOWN",
            ),
            "success",
        ),
        (
            "remove_orphan_files",
            "expire_snapshots",
            action_result(
                None,
                table="bronze_kma_vilage_fcst",
                operation="expire_snapshots",
                status="SUCCEEDED_WITH_STOP",
            ),
            "success",
        ),
        (
            "remove_orphan_files",
            "expire_snapshots",
            action_result(
                None,
                table="bronze_kma_vilage_fcst",
                operation="expire_snapshots",
            ),
            "failed",
        ),
    ],
)
def test_later_action_rejects_invalid_predecessor_before_executor(
    monkeypatch, operation, predecessor, result, state
):
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    previous_task_id = module.action_task_id(1, table, predecessor)
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            (previous_task_id, module.RESULT_XCOM_KEY): result,
        }
    )
    called = False

    def executor(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("executor must not be reached")

    monkeypatch.setattr(module, "execute_maintenance_action", executor)

    with pytest.raises(FakeAirflowSkipException):
        module._run_action(
            table=table,
            operation=operation,
            previous_task_id=previous_task_id,
            ti=ti,
            dag_run=FakeDagRun(states={previous_task_id: state}),
        )

    assert called is False


@pytest.mark.parametrize(
    ("operation", "predecessor", "status"),
    [
        ("expire_snapshots", "optimize", "SUCCEEDED"),
        ("remove_orphan_files", "expire_snapshots", "SUCCEEDED"),
    ],
)
def test_later_action_runs_only_after_a_valid_successful_predecessor(
    monkeypatch, operation, predecessor, status
):
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    previous_task_id = module.action_task_id(1, table, predecessor)
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            (previous_task_id, module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation=predecessor, status=status
            ),
        }
    )
    calls = []

    def executor(*args, **kwargs):
        calls.append((args, kwargs))
        return action_result(module, table=table, operation=operation)

    monkeypatch.setattr(module, "execute_maintenance_action", executor)

    result = module._run_action(
        table=table,
        operation=operation,
        previous_task_id=previous_task_id,
        ti=ti,
        dag_run=FakeDagRun(states={previous_task_id: "success"}),
    )

    assert result["status"] == "SUCCEEDED"
    assert len(calls) == 1


def test_later_action_rejects_a_noncanonical_predecessor_task_before_executor(monkeypatch):
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    wrong_predecessor = module.action_task_id(1, table, "remove_orphan_files")
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            (wrong_predecessor, module.RESULT_XCOM_KEY): action_result(
                module, table=table, operation="remove_orphan_files"
            ),
        }
    )

    monkeypatch.setattr(
        module,
        "execute_maintenance_action",
        lambda *args, **kwargs: pytest.fail("executor must not be reached"),
    )

    with pytest.raises(FakeAirflowSkipException):
        module._run_action(
            table=table,
            operation="expire_snapshots",
            previous_task_id=wrong_predecessor,
            ti=ti,
            dag_run=FakeDagRun(states={wrong_predecessor: "success"}),
        )


def test_valid_missing_predecessor_synthesizes_identity_preserving_result_without_executor(
    monkeypatch,
):
    module = load_maintenance_module()
    table = module.CANONICAL_TABLES[0]
    previous_task_id = module.action_task_id(1, table, "optimize")
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(module, [table]),
            (previous_task_id, module.RESULT_XCOM_KEY): action_result(
                module,
                table=table,
                operation="optimize",
                status="SKIPPED_MISSING",
            ),
        }
    )

    monkeypatch.setattr(
        module,
        "execute_maintenance_action",
        lambda *args, **kwargs: pytest.fail("executor must not be reached"),
    )

    result = module._run_action(
        table=table,
        operation="expire_snapshots",
        previous_task_id=previous_task_id,
        ti=ti,
        dag_run=FakeDagRun(states={previous_task_id: "success"}),
    )

    assert result == action_result(
        module,
        table=table,
        operation="expire_snapshots",
        status="SKIPPED_MISSING",
    )
    assert ti.values[(None, module.RESULT_XCOM_KEY)] == result


def test_missing_predecessor_evidence_opens_gate_circuit_and_blocks_later_table_executor(
    monkeypatch,
):
    module = load_maintenance_module()
    first_table, later_table = module.CANONICAL_TABLES[:2]
    optimize = module.action_task_id(1, first_table, "optimize")
    expire = module.action_task_id(1, first_table, "expire_snapshots")
    orphan = module.action_task_id(1, first_table, "remove_orphan_files")
    first_gate = module.gate_task_id(1, first_table)
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(
                module, [first_table, later_table]
            ),
        }
    )
    called = False

    def executor(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("executor must not be reached")

    monkeypatch.setattr(module, "execute_maintenance_action", executor)

    with pytest.raises(FakeAirflowSkipException):
        module._run_action(
            table=first_table,
            operation="expire_snapshots",
            previous_task_id=optimize,
            ti=ti,
            dag_run=FakeDagRun(states={optimize: "success"}),
        )

    gate = module._table_gate(
        table=first_table,
        action_task_ids=[optimize, expire, orphan],
        ti=ti,
        dag_run=FakeDagRun(
            states={optimize: "success", expire: "skipped", orphan: "upstream_failed"}
        ),
    )
    ti.values[(first_gate, "return_value")] = gate

    with pytest.raises(FakeAirflowSkipException):
        module._run_action(
            table=later_table,
            operation="optimize",
            previous_task_id=module.PLAN_TASK_ID,
            previous_gate_task_id=first_gate,
            ti=ti,
            dag_run=FakeDagRun(),
        )

    assert gate["status"] == "CIRCUIT_OPEN"
    assert called is False


def test_hard_kill_propagates_through_nonselected_table_blocks_later_table_and_fails_report(
    monkeypatch,
):
    module = load_maintenance_module()
    first_table, nonselected_table, later_table = module.CANONICAL_TABLES[:3]
    first_actions = [
        module.action_task_id(1, first_table, operation) for operation in module.OPERATIONS
    ]
    first_gate = module.gate_task_id(1, first_table)
    second_gate = module.gate_task_id(2, nonselected_table)
    third_gate = module.gate_task_id(3, later_table)
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(
                module, [first_table, later_table]
            ),
        }
    )
    first_control = module._table_gate(
        table=first_table,
        action_task_ids=first_actions,
        ti=ti,
        dag_run=FakeDagRun(states={first_actions[0]: "failed"}),
    )
    ti.values[(first_gate, "return_value")] = first_control
    second_control = module._table_gate(
        table=nonselected_table,
        action_task_ids=["unused-1", "unused-2", "unused-3"],
        previous_gate_task_id=first_gate,
        ti=ti,
        dag_run=FakeDagRun(),
    )
    ti.values[(second_gate, "return_value")] = second_control
    called = False

    def executor(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("executor must not be reached")

    monkeypatch.setattr(module, "execute_maintenance_action", executor)

    with pytest.raises(FakeAirflowSkipException):
        module._run_action(
            table=later_table,
            operation="optimize",
            previous_task_id=module.PLAN_TASK_ID,
            previous_gate_task_id=second_gate,
            ti=ti,
            dag_run=FakeDagRun(),
        )

    third_control = module._table_gate(
        table=later_table,
        action_task_ids=["later-1", "later-2", "later-3"],
        previous_gate_task_id=second_gate,
        ti=ti,
        dag_run=FakeDagRun(),
    )
    ti.values[(third_gate, "return_value")] = third_control

    with pytest.raises(RuntimeError, match=f"{first_table}.*{later_table}"):
        module._final_report(ti=ti)

    assert first_control["status"] == "CIRCUIT_OPEN"
    assert second_control["status"] == "CIRCUIT_OPEN"
    assert third_control["status"] == "CIRCUIT_OPEN"
    assert called is False


def test_confirmed_table_failure_allows_next_table_but_final_report_fails(monkeypatch):
    module = load_maintenance_module()
    failed_table, next_table = module.CANONICAL_TABLES[:2]
    failed_gate = module.gate_task_id(1, failed_table)
    next_gate = module.gate_task_id(2, next_table)
    failed_action = module.action_task_id(1, failed_table, "optimize")
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): immutable_plan_payload(
                module, [failed_table, next_table]
            ),
            (failed_action, module.RESULT_XCOM_KEY): confirmed_table_failure(
                module, table=failed_table, operation="optimize"
            ),
        }
    )
    failed_control = module._table_gate(
        table=failed_table,
        action_task_ids=[
            failed_action,
            module.action_task_id(1, failed_table, "expire_snapshots"),
            module.action_task_id(1, failed_table, "remove_orphan_files"),
        ],
        ti=ti,
        dag_run=FakeDagRun(
            states={
                failed_action: "failed",
                module.action_task_id(1, failed_table, "expire_snapshots"): "upstream_failed",
                module.action_task_id(1, failed_table, "remove_orphan_files"): "skipped",
            }
        ),
    )
    ti.values[(failed_gate, "return_value")] = failed_control
    calls = []

    def executor(*args, **kwargs):
        calls.append((args, kwargs))
        return action_result(module, table=next_table, operation="optimize")

    monkeypatch.setattr(module, "execute_maintenance_action", executor)
    next_result = module._run_action(
        table=next_table,
        operation="optimize",
        previous_task_id=module.PLAN_TASK_ID,
        previous_gate_task_id=failed_gate,
        ti=ti,
        dag_run=FakeDagRun(),
    )
    ti.values[(next_gate, "return_value")] = gate_control(next_table, "SUCCEEDED", False)

    with pytest.raises(RuntimeError, match=failed_table):
        module._final_report(ti=ti)

    assert failed_control == {
        "table": failed_table,
        "status": "TABLE_FAILED",
        "circuit_open": False,
    }
    assert next_result["status"] == "SUCCEEDED"
    assert len(calls) == 1
