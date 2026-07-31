"""Airflow DAG: serial Iceberg maintenance checkpoints for Weather and Traffic."""

from __future__ import annotations

from datetime import timedelta
import os
import sys

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.pools import (  # noqa: E402
    TRINO_TRAFFIC_HEAVY_POOL as TRAFFIC_TRINO_HEAVY_POOL,
)
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from weather_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402
from weather_ingest.iceberg_maintenance import (  # noqa: E402
    OPERATIONS,
    collect_maintenance_inventory,
    execute_maintenance_action,
    maintenance_plan_payload,
    resolve_maintenance_plan,
)
from weather_ingest.run_manifest import MANIFEST_TABLE  # noqa: E402
from weather_lineage import enable_lineage_if_configured  # noqa: E402


KST = "Asia/Seoul"
record_weather_problem = problem_failure_callback(domain="weather")

CANONICAL_TABLES = (
    "bronze_kma_vilage_fcst",
    "bronze_seoul_traffic_incident",
    "bronze_seoul_traffic_incident_request_audit",
    MANIFEST_TABLE,
    "silver_kma_vilage_fcst",
    "gold_weather_forecast_summary",
    "dim_weather_place",
    "gold_weather_forecast_by_place",
    "silver_seoul_traffic_incident",
    "gold_traffic_incident_summary",
)
TRAFFIC_TABLES = frozenset(
    {
        "bronze_seoul_traffic_incident",
        "bronze_seoul_traffic_incident_request_audit",
        "silver_seoul_traffic_incident",
        "gold_traffic_incident_summary",
    }
)


def maintenance_pool_for_table(table: str) -> str:
    if table not in CANONICAL_TABLES:
        raise ValueError("maintenance table is outside canonical allowlist")
    return TRAFFIC_TRINO_HEAVY_POOL if table in TRAFFIC_TABLES else TRINO_HEAVY_POOL


DEFAULT_PARAM_VALUES = {
    "target": "dev",
    "retention": "7d",
    "tables": CANONICAL_TABLES,
}
DEFAULT_PARAMS = {
    "target": Param("dev", enum=["dev", "prod"]),
    "retention": Param("7d", enum=["7d"]),
    "tables": Param(
        list(CANONICAL_TABLES),
        type="array",
        minItems=1,
        uniqueItems=True,
        items={"type": "string", "enum": list(CANONICAL_TABLES)},
    ),
}
PLAN_TASK_ID = "preflight"
RESULT_XCOM_KEY = "maintenance_result"


def action_task_id(index: int, table: str, operation: str) -> str:
    return f"maint_{index:02d}_{table}__{operation}"


def gate_task_id(index: int, table: str) -> str:
    return f"gate_{index:02d}_{table}"


def _preflight(**context):
    params = context["params"]
    dag_run = context["dag_run"]
    target = str(params["target"])
    validate_dev_runtime("weather", requested_target=target)
    plan = resolve_maintenance_plan(
        target=target,
        retention=str(params["retention"]),
        tables=params["tables"],
        allowed_tables=CANONICAL_TABLES,
        dag_run_id=str(dag_run.run_id),
    )
    payload = maintenance_plan_payload(plan)
    payload["inventory"] = collect_maintenance_inventory(
        plan,
        allowed_tables=CANONICAL_TABLES,
    )
    return payload


def _pull_result(ti, task_id):
    return ti.xcom_pull(task_ids=task_id, key=RESULT_XCOM_KEY)


def _is_immutable_plan_payload(payload) -> bool:
    if not isinstance(payload, dict):
        return False
    plan_id = payload.get("plan_id")
    plan_hash = payload.get("plan_hash")
    tables = payload.get("tables")
    if not isinstance(plan_id, str) or not plan_id:
        return False
    if (
        not isinstance(plan_hash, str)
        or len(plan_hash) != 64
        or any(character not in "0123456789abcdef" for character in plan_hash)
    ):
        return False
    if not isinstance(tables, (list, tuple)) or not tables:
        return False
    selected = tuple(tables)
    if (
        any(not isinstance(table, str) or table not in CANONICAL_TABLES for table in selected)
        or len(set(selected)) != len(selected)
    ):
        return False
    return tuple(table for table in CANONICAL_TABLES if table in selected) == selected


def _safe_control_reason(value, fallback):
    candidate = str(value)
    if candidate and all(character.isupper() or character.isdigit() or character == "_" for character in candidate):
        return candidate
    return fallback


def _valid_control_reason(value) -> bool:
    return isinstance(value, str) and value == _safe_control_reason(value, "")


def _previous_gate_control(ti, plan_payload, table, previous_gate_task_id):
    try:
        current_index = CANONICAL_TABLES.index(table)
    except ValueError:
        return "INVALID", None
    if current_index == 0:
        return ("ROOT", None) if previous_gate_task_id is None else ("INVALID", None)

    previous_table = CANONICAL_TABLES[current_index - 1]
    expected_task_id = gate_task_id(current_index, previous_table)
    if previous_gate_task_id != expected_task_id:
        return "INVALID", None
    control = ti.xcom_pull(task_ids=previous_gate_task_id)
    if (
        not isinstance(control, dict)
        or control.get("table") != previous_table
        or not isinstance(control.get("circuit_open"), bool)
        or not isinstance(control.get("status"), str)
    ):
        return "INVALID", None
    if control["circuit_open"]:
        if (
            control["status"] != "CIRCUIT_OPEN"
            or control.get("source_table") not in CANONICAL_TABLES
            or not _valid_control_reason(control.get("reason"))
        ):
            return "INVALID", None
        return "CIRCUIT", control

    expected_statuses = (
        {"SUCCEEDED", "SKIPPED_MISSING", "TABLE_FAILED"}
        if previous_table in plan_payload["tables"]
        else {"NOT_SELECTED"}
    )
    if control["status"] not in expected_statuses:
        return "INVALID", None
    return "CLOSED", control


def _run_action(
    *, table, operation, previous_task_id, previous_gate_task_id=None, **context
):
    ti = context["ti"]
    plan_payload = ti.xcom_pull(task_ids=PLAN_TASK_ID)
    if not _is_immutable_plan_payload(plan_payload):
        raise AirflowSkipException("immutable maintenance plan is unavailable")
    previous_kind, _previous_control = _previous_gate_control(
        ti,
        plan_payload,
        table,
        previous_gate_task_id,
    )
    if previous_kind in {"INVALID", "CIRCUIT"}:
        raise AirflowSkipException("maintenance circuit is open")

    if table not in plan_payload.get("tables", ()):
        raise AirflowSkipException("table is not selected by the immutable plan")

    previous = _valid_predecessor_action_result(
        ti=ti,
        plan_payload=plan_payload,
        table=table,
        operation=operation,
        previous_task_id=previous_task_id,
    )
    if previous is None:
        raise AirflowSkipException("previous maintenance action is unavailable")
    if previous.get("status") == "SKIPPED_MISSING":
        result = {
            "plan_id": plan_payload["plan_id"],
            "plan_hash": plan_payload["plan_hash"],
            "table": table,
            "operation": operation,
            "status": "SKIPPED_MISSING",
            "circuit_breaker": False,
        }
        ti.xcom_push(key=RESULT_XCOM_KEY, value=result)
        return result

    result = execute_maintenance_action(
        plan_payload,
        allowed_tables=CANONICAL_TABLES,
        table=table,
        operation=operation,
    )
    ti.xcom_push(key=RESULT_XCOM_KEY, value=result)
    if result["status"] in {"FAILED", "UNKNOWN", "SUCCEEDED_WITH_STOP"}:
        raise RuntimeError(
            f"maintenance action failed: table={table}, operation={operation}, "
            f"status={result['status']}, category={result.get('category')}"
        )
    return result


def _normalize_task_state(state):
    if state is None:
        return None
    return str(getattr(state, "value", state)).lower()


def _task_states(ti, task_ids):
    task_ids = list(task_ids)
    if not task_ids:
        return {}
    dag_id = getattr(ti, "dag_id", None)
    run_id = getattr(ti, "run_id", None)
    get_task_states = getattr(ti, "get_task_states", None)
    if (
        not isinstance(dag_id, str)
        or not dag_id
        or not isinstance(run_id, str)
        or not run_id
        or not callable(get_task_states)
    ):
        return {}

    states_by_run = get_task_states(
        dag_id=dag_id,
        task_ids=task_ids,
        run_ids=[run_id],
    )
    if not isinstance(states_by_run, dict):
        return {}
    run_states = states_by_run.get(run_id)
    if not isinstance(run_states, dict):
        return {}
    return {
        task_id: _normalize_task_state(run_states.get(task_id))
        for task_id in task_ids
    }


def _task_state(ti, task_id):
    return _task_states(ti, [task_id]).get(task_id)


def _valid_predecessor_action_result(
    *, ti, plan_payload, table, operation, previous_task_id
):
    try:
        table_index = CANONICAL_TABLES.index(table) + 1
        operation_index = OPERATIONS.index(operation)
    except ValueError:
        return None
    if operation_index == 0:
        return {} if previous_task_id == PLAN_TASK_ID else None

    previous_operation = OPERATIONS[operation_index - 1]
    expected_task_id = action_task_id(table_index, table, previous_operation)
    if previous_task_id != expected_task_id:
        return None
    result = _pull_result(ti, expected_task_id)
    if not isinstance(result, dict):
        return None
    if _action_identity_reason(plan_payload, table, previous_operation, result) is not None:
        return None
    if result.get("status") not in {"SUCCEEDED", "SKIPPED_MISSING"}:
        return None
    if result["circuit_breaker"] is not False:
        return None
    if _task_state(ti, expected_task_id) != "success":
        return None
    return result


def _open_circuit(*, table, reason, source_table=None):
    return {
        "table": table,
        "status": "CIRCUIT_OPEN",
        "circuit_open": True,
        "source_table": source_table or table,
        "reason": _safe_control_reason(reason, "UNKNOWN_CIRCUIT"),
    }


def _action_identity_reason(plan_payload, table, operation, result):
    if result.get("plan_id") != plan_payload["plan_id"]:
        return "ACTION_PLAN_ID_MISMATCH"
    if result.get("plan_hash") != plan_payload["plan_hash"]:
        return "ACTION_PLAN_HASH_MISMATCH"
    if result.get("table") != table:
        return "ACTION_TABLE_MISMATCH"
    if result.get("operation") != operation:
        return "ACTION_OPERATION_MISMATCH"
    if not isinstance(result.get("circuit_breaker"), bool):
        return "MALFORMED_ACTION_RESULT"
    return None


def _is_confirmed_table_local_failure(result, state):
    return (
        result.get("status") == "FAILED"
        and result.get("circuit_breaker") is False
        and result.get("category") == "TABLE_OPERATION"
        and result.get("phase") == "SUBMITTED"
        and isinstance(result.get("query_id"), str)
        and bool(result["query_id"])
        and result.get("query_state") == "FAILED"
        and result.get("structured_error_type") == "USER_ERROR"
        and isinstance(result.get("structured_error_name"), str)
        and bool(result["structured_error_name"])
        and _valid_control_reason(result.get("structured_error_name"))
        and state == "failed"
    )


def _table_gate(*, table, action_task_ids, previous_gate_task_id=None, **context):
    ti = context["ti"]
    plan_payload = ti.xcom_pull(task_ids=PLAN_TASK_ID)
    if not _is_immutable_plan_payload(plan_payload):
        return _open_circuit(table=table, reason="INVALID_IMMUTABLE_PLAN")
    previous_kind, previous_control = _previous_gate_control(
        ti,
        plan_payload,
        table,
        previous_gate_task_id,
    )
    if previous_kind == "INVALID":
        return _open_circuit(table=table, reason="INVALID_PREVIOUS_GATE_CONTROL")
    if previous_kind == "CIRCUIT":
        return _open_circuit(
            table=table,
            reason=previous_control["reason"],
            source_table=previous_control["source_table"],
        )

    if table not in plan_payload["tables"]:
        return {"table": table, "status": "NOT_SELECTED", "circuit_open": False}
    if (
        len(action_task_ids) != len(OPERATIONS)
        or any(not isinstance(task_id, str) or not task_id for task_id in action_task_ids)
        or len(set(action_task_ids)) != len(action_task_ids)
    ):
        return _open_circuit(table=table, reason="INVALID_ACTION_TOPOLOGY")

    task_states = _task_states(ti, action_task_ids)
    statuses = []
    table_failed = False
    for task_id, operation in zip(action_task_ids, OPERATIONS, strict=True):
        result = _pull_result(ti, task_id)
        state = task_states.get(task_id)
        if table_failed:
            if result is not None or state not in {"skipped", "upstream_failed"}:
                return _open_circuit(
                    table=table,
                    reason="DOWNSTREAM_ACTION_AFTER_TABLE_FAILURE",
                )
            continue
        if not isinstance(result, dict):
            return _open_circuit(table=table, reason="UNRECORDED_TASK_FAILURE")
        identity_reason = _action_identity_reason(
            plan_payload,
            table,
            operation,
            result,
        )
        if identity_reason is not None:
            return _open_circuit(table=table, reason=identity_reason)
        status = result.get("status")
        if status not in {"SUCCEEDED", "SKIPPED_MISSING", "FAILED", "UNKNOWN", "SUCCEEDED_WITH_STOP"}:
            return _open_circuit(table=table, reason="UNKNOWN_ACTION_STATUS")
        if result["circuit_breaker"]:
            return _open_circuit(
                table=table,
                reason="ACTION_CIRCUIT",
            )
        if status in {"UNKNOWN", "SUCCEEDED_WITH_STOP"}:
            return _open_circuit(table=table, reason=f"ACTION_{status}")
        if status in {"SUCCEEDED", "SKIPPED_MISSING"}:
            if state != "success":
                return _open_circuit(table=table, reason="ACTION_STATE_MISMATCH")
            statuses.append(status)
            continue
        if not _is_confirmed_table_local_failure(result, state):
            return _open_circuit(table=table, reason="UNCONFIRMED_TABLE_FAILURE")
        table_failed = True

    if table_failed:
        return {"table": table, "status": "TABLE_FAILED", "circuit_open": False}
    if statuses and all(status == "SKIPPED_MISSING" for status in statuses):
        return {"table": table, "status": "SKIPPED_MISSING", "circuit_open": False}
    if len(statuses) == len(OPERATIONS) and all(status == "SUCCEEDED" for status in statuses):
        return {"table": table, "status": "SUCCEEDED", "circuit_open": False}
    return _open_circuit(table=table, reason="INCOMPLETE_ACTION_EVIDENCE")


def _final_report(**context):
    ti = context["ti"]
    plan_payload = ti.xcom_pull(task_ids=PLAN_TASK_ID)
    if not _is_immutable_plan_payload(plan_payload):
        raise RuntimeError("Iceberg maintenance immutable plan is unavailable")
    failures = []
    for index, table in enumerate(CANONICAL_TABLES, start=1):
        gate = ti.xcom_pull(task_ids=gate_task_id(index, table))
        if table not in plan_payload["tables"]:
            continue
        if (
            not isinstance(gate, dict)
            or gate.get("table") != table
            or gate.get("circuit_open") is not False
            or gate.get("status") not in {"SUCCEEDED", "SKIPPED_MISSING"}
        ):
            failures.append(table)
    if failures:
        raise RuntimeError(
            "Iceberg maintenance did not complete safely: " + ", ".join(failures)
        )
    return {"status": "SUCCEEDED"}


def _default_schedule() -> str | None:
    if "ASK_SEOUL_ICEBERG_MAINTENANCE_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_ICEBERG_MAINTENANCE_SCHEDULE"] or None
    return "0 4 * * 0"


with DAG(
    dag_id="ask_seoul_iceberg_maintenance",
    description="Weekly metadata cleanup for weather/traffic Iceberg tables.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule=_default_schedule(),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    params=DEFAULT_PARAMS,
    tags=["maintenance", "ask_seoul", "iceberg", "weather", "traffic"],
) as dag:
    preflight = PythonOperator(
        task_id=PLAN_TASK_ID,
        python_callable=_preflight,
        on_failure_callback=record_weather_problem,
    )

    previous = preflight
    gate_task_ids = []
    for index, table in enumerate(CANONICAL_TABLES, start=1):
        action_tasks = []
        previous_action_id = PLAN_TASK_ID
        for operation in OPERATIONS:
            action = PythonOperator(
                task_id=action_task_id(index, table, operation),
                python_callable=_run_action,
                op_kwargs={
                    "table": table,
                    "operation": operation,
                    "previous_task_id": previous_action_id,
                    "previous_gate_task_id": gate_task_ids[-1]
                    if gate_task_ids
                    else None,
                },
                pool=maintenance_pool_for_table(table),
                pool_slots=1,
                weight_rule="absolute",
                retries=0,
                on_failure_callback=record_weather_problem,
            )
            previous >> action
            previous = action
            previous_action_id = action.task_id
            action_tasks.append(action)

        gate = PythonOperator(
            task_id=gate_task_id(index, table),
            python_callable=_table_gate,
            op_kwargs={
                "table": table,
                "action_task_ids": [task.task_id for task in action_tasks],
                "previous_gate_task_id": gate_task_ids[-1] if gate_task_ids else None,
            },
            trigger_rule=TriggerRule.ALL_DONE,
            on_failure_callback=record_weather_problem,
        )
        previous >> gate
        previous = gate
        gate_task_ids.append(gate.task_id)

    final_report = PythonOperator(
        task_id="final_report",
        python_callable=_final_report,
        trigger_rule=TriggerRule.ALL_DONE,
        on_failure_callback=record_weather_problem,
    )
    previous >> final_report


enable_lineage_if_configured(dag)
