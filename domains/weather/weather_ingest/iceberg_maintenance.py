"""Weather-owned immutable Iceberg maintenance plan helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
from numbers import Integral
import os
import re
import sys
from typing import Any

from common.runtime_guard import validate_dev_runtime
from weather_ingest.trino_query_metrics import (
    TelemetryCursor,
    collect_iceberg_fingerprint,
    query_id_from_cursor,
    sql_string,
)


APPROVED_DEV_CATALOG = "iceberg_dev"
APPROVED_DEV_SCHEMA = "weather_traffic_bronze"
FIXED_RETENTION = "7d"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
OPERATIONS = ("optimize", "expire_snapshots", "remove_orphan_files")
MEMORY_WARNING_BYTES = 1_503_238_554
MEMORY_STOP_BYTES = 1_717_986_918


class MaintenancePlanError(ValueError):
    """Raised before connecting when a maintenance request is unsafe."""


@dataclass(frozen=True)
class MaintenancePlan:
    plan_id: str
    plan_hash: str
    target: str
    catalog: str
    schema: str
    retention: str
    retain_last: int
    tables: tuple[str, ...]


def _normalize_requested_tables(
    tables: str | Iterable[str] | None,
) -> tuple[str, ...]:
    if isinstance(tables, str):
        return tuple(item.strip() for item in tables.split(",") if item.strip())
    if tables is None:
        return ()
    return tuple(str(item).strip() for item in tables if str(item).strip())


def _normalize_tables(tables: str | Iterable[str] | None) -> tuple[str, ...]:
    """Temporarily preserve the legacy import while using the new policy parser."""

    return _normalize_requested_tables(tables)


def _validate_canonical_tables(allowed_tables: Sequence[str]) -> tuple[str, ...]:
    if isinstance(allowed_tables, (str, bytes, bytearray)) or not isinstance(
        allowed_tables, Sequence
    ):
        raise MaintenancePlanError(
            "maintenance canonical allowlist must be a deterministic ordered sequence"
        )

    canonical = tuple(allowed_tables)
    if not canonical:
        raise MaintenancePlanError(
            "maintenance canonical allowlist requires at least one table"
        )
    if any(
        not isinstance(table, str) or not _IDENTIFIER.fullmatch(table)
        for table in canonical
    ):
        raise MaintenancePlanError(
            "maintenance canonical allowlist entries must be exact safe string identifiers"
        )
    if len(set(canonical)) != len(canonical):
        raise MaintenancePlanError(
            "maintenance canonical allowlist contains a duplicate"
        )
    return canonical


def _plan_hash(
    *,
    plan_id: str,
    target: str,
    catalog: str,
    schema: str,
    retention: str,
    retain_last: int,
    tables: tuple[str, ...],
) -> str:
    hash_input = {
        "plan_id": plan_id,
        "target": target,
        "catalog": catalog,
        "schema": schema,
        "retention": retention,
        "retain_last": retain_last,
        "tables": list(tables),
    }
    return hashlib.sha256(
        json.dumps(hash_input, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _validate_maintenance_plan(
    plan: MaintenancePlan,
    *,
    allowed_tables: Sequence[str],
) -> None:
    """Reject forged raw plans before any SQL statement or cursor use."""
    if not isinstance(plan, MaintenancePlan):
        raise MaintenancePlanError("maintenance plan has an invalid type")
    if (
        plan.target != "dev"
        or plan.catalog != APPROVED_DEV_CATALOG
        or plan.schema != APPROVED_DEV_SCHEMA
        or plan.retention != FIXED_RETENTION
        or not isinstance(plan.retain_last, int)
        or isinstance(plan.retain_last, bool)
        or plan.retain_last != 1
    ):
        raise MaintenancePlanError("maintenance plan invariants are invalid")
    if not isinstance(plan.plan_id, str) or not plan.plan_id:
        raise MaintenancePlanError("maintenance plan ID is invalid")
    if not isinstance(plan.tables, tuple):
        raise MaintenancePlanError("maintenance plan tables must be a canonical tuple")
    canonical = _validate_canonical_tables(allowed_tables)
    _validate_canonical_tables(plan.tables)
    if (
        any(table not in canonical for table in plan.tables)
        or plan.tables != tuple(table for table in canonical if table in plan.tables)
    ):
        raise MaintenancePlanError(
            "maintenance plan tables are outside canonical allowlist order"
        )
    expected_hash = _plan_hash(
        plan_id=plan.plan_id,
        target=plan.target,
        catalog=plan.catalog,
        schema=plan.schema,
        retention=plan.retention,
        retain_last=plan.retain_last,
        tables=plan.tables,
    )
    if plan.plan_hash != expected_hash:
        raise MaintenancePlanError("maintenance plan hash is invalid")


def resolve_maintenance_plan(
    *,
    target: str,
    retention: str,
    tables: str | Iterable[str] | None,
    allowed_tables: Sequence[str],
    dag_run_id: str,
    env: Mapping[str, str] | None = None,
) -> MaintenancePlan:
    values = env if env is not None else os.environ
    validate_dev_runtime("weather", env=values, requested_target=target)
    if target != "dev":
        raise MaintenancePlanError("maintenance target must be exactly dev")
    if str(values.get("TRINO_DEV_ICEBERG_CATALOG", "")).strip() != APPROVED_DEV_CATALOG:
        raise MaintenancePlanError("maintenance catalog must be the approved dev catalog")
    if str(values.get("ASK_SEOUL_SCHEMA", "")).strip() != APPROVED_DEV_SCHEMA:
        raise MaintenancePlanError(
            "maintenance source schema must use the approved dev schema"
        )
    if retention != FIXED_RETENTION:
        raise MaintenancePlanError("maintenance retention must be 7d")

    canonical = _validate_canonical_tables(allowed_tables)
    requested = _normalize_requested_tables(tables)
    if not requested:
        raise MaintenancePlanError("maintenance requires at least one table")
    if len(set(requested)) != len(requested):
        raise MaintenancePlanError("maintenance table selection contains a duplicate")
    unknown = sorted(set(requested) - set(canonical))
    if unknown:
        raise MaintenancePlanError("maintenance table is outside canonical allowlist")
    if not all(_IDENTIFIER.fullmatch(table) for table in requested):
        raise MaintenancePlanError("maintenance table identifier is invalid")

    ordered = tuple(table for table in canonical if table in set(requested))
    plan_hash = _plan_hash(
        plan_id=dag_run_id,
        target="dev",
        catalog=APPROVED_DEV_CATALOG,
        schema=APPROVED_DEV_SCHEMA,
        retention=FIXED_RETENTION,
        retain_last=1,
        tables=ordered,
    )
    return MaintenancePlan(
        plan_id=dag_run_id,
        plan_hash=plan_hash,
        target="dev",
        catalog=APPROVED_DEV_CATALOG,
        schema=APPROVED_DEV_SCHEMA,
        retention=FIXED_RETENTION,
        retain_last=1,
        tables=ordered,
    )


def maintenance_plan_payload(plan: MaintenancePlan) -> dict[str, Any]:
    payload = asdict(plan)
    payload["tables"] = list(plan.tables)
    return payload


def exact_table_exists(
    cursor: Any,
    *,
    catalog: str,
    schema: str,
    table: str,
) -> bool:
    if (
        catalog != APPROVED_DEV_CATALOG
        or schema != APPROVED_DEV_SCHEMA
        or not _IDENTIFIER.fullmatch(table)
    ):
        raise MaintenancePlanError("exact table probe is outside approved dev scope")
    cursor.execute(
        f"SELECT 1 FROM {catalog}.information_schema.tables "
        f"WHERE table_schema = {sql_string(schema)} "
        f"AND table_name = {sql_string(table)} LIMIT 1"
    )
    return cursor.fetchone() is not None


def _qualified_table(plan: MaintenancePlan, table: str) -> str:
    if table not in plan.tables or not _IDENTIFIER.fullmatch(table):
        raise MaintenancePlanError("table is not selected by the immutable plan")
    return f"{plan.catalog}.{plan.schema}.{table}"


def operation_sql(plan: MaintenancePlan, table: str, operation: str) -> str:
    qualified = _qualified_table(plan, table)
    if operation == "optimize":
        command = "optimize"
    elif operation == "expire_snapshots":
        command = (
            "expire_snapshots("
            f"retention_threshold => {sql_string(plan.retention)}, "
            f"retain_last => {plan.retain_last})"
        )
    elif operation == "remove_orphan_files":
        command = (
            "remove_orphan_files("
            f"retention_threshold => {sql_string(plan.retention)})"
        )
    else:
        raise MaintenancePlanError("unsupported maintenance operation")
    return f"ALTER TABLE {qualified} EXECUTE {command}"


def structured_error_type(exc: Exception) -> str | None:
    return _sanitized_error_identifier(getattr(exc, "error_type", None))


def structured_error_name(exc: Exception) -> str | None:
    return _sanitized_error_identifier(getattr(exc, "error_name", None))


def _sanitized_error_identifier(value: Any) -> str | None:
    normalized = str(value).upper() if value else None
    return normalized if normalized and _IDENTIFIER.fullmatch(normalized) else None


def query_state_from_cursor(cursor: Any) -> str | None:
    stats = getattr(cursor, "stats", None) or {}
    state = stats.get("state") if isinstance(stats, Mapping) else None
    return str(state).strip().upper() if state else None


def classify_action_exception(
    exc: Exception,
    *,
    phase: str,
    query_id: str | None,
    query_state: str | None,
    query_error_type: str | None,
) -> dict[str, Any]:
    normalized_error_type = _sanitized_error_identifier(
        query_error_type
    ) or structured_error_type(exc)
    normalized_error_name = structured_error_name(exc)
    confirmed_user_failure = (
        phase == "SUBMITTED"
        and query_id is not None
        and query_state == "FAILED"
        and normalized_error_type == "USER_ERROR"
        and normalized_error_name is not None
    )
    if normalized_error_name and (
        "MEMORY" in normalized_error_name or "OOM" in normalized_error_name
    ):
        category = "MEMORY_LIMIT"
        circuit_breaker = True
    elif confirmed_user_failure:
        category = "TABLE_OPERATION"
        circuit_breaker = False
    else:
        category = "UNKNOWN"
        circuit_breaker = True
    return {
        "category": category,
        "circuit_breaker": circuit_breaker,
        "error_type": type(exc).__name__,
        "phase": phase,
        "query_id": query_id,
        "query_state": query_state,
        "structured_error_type": normalized_error_type,
        "structured_error_name": normalized_error_name,
    }


def _json_safe_int(value: Any, *, nonnegative: bool = False) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, Integral):
        normalized = int(value)
    elif isinstance(value, Decimal) and value == value.to_integral_value():
        normalized = int(value)
    else:
        return None
    if nonnegative and normalized < 0:
        return None
    return normalized


def _procedure_output(
    cursor: Any,
    rows: list[tuple[Any, ...]],
    operation: str,
) -> dict[str, Any]:
    description = cursor.description or []
    if not all(isinstance(column, (tuple, list)) and column for column in description):
        return {}
    names = [str(column[0]).lower() for column in description]
    if names == ["metric_name", "metric_value"]:
        output: dict[str, Any] = {}
        required = _OPERATION_REQUIRED_METRICS[operation]
        for row in rows:
            if len(row) != 2:
                return {}
            metric_name = str(row[0])
            if not _IDENTIFIER.fullmatch(metric_name):
                return {}
            if metric_name not in required:
                continue
            if metric_name in output:
                return {}
            normalized = _json_safe_int(row[1], nonnegative=True)
            if normalized is not None:
                output[metric_name] = normalized
        return output
    return {}


_OPERATION_REQUIRED_METRICS = {
    "optimize": {
        "rewritten_data_files_count",
        "removed_delete_files_count",
        "added_data_files_count",
    },
    "expire_snapshots": set(),
    "remove_orphan_files": {
        "processed_manifests_count",
        "active_files_count",
        "scanned_files_count",
        "deleted_files_count",
        "deleted_bytes",
    },
}


def required_procedure_metrics_present(operation: str, output: dict[str, Any]) -> bool:
    required = _OPERATION_REQUIRED_METRICS[operation]
    return required.issubset(output) and all(
        _json_safe_int(output[name], nonnegative=True) is not None
        for name in required
    )


def maintenance_postconditions_hold(
    operation: str,
    before: dict[str, Any],
    after: dict[str, Any],
) -> bool:
    before_refs = {ref["name"]: ref for ref in before["refs"]}
    after_refs = {ref["name"]: ref for ref in after["refs"]}
    if operation == "optimize":
        return (
            before_refs.get("main", {}).get("snapshot_id") == before["snapshot_id"]
            and after_refs.get("main", {}).get("snapshot_id") == after["snapshot_id"]
            and before["exact_row_count"] == after["exact_row_count"]
            and {key: value for key, value in before_refs.items() if key != "main"}
            == {key: value for key, value in after_refs.items() if key != "main"}
        )
    return (
        before["snapshot_id"] == after["snapshot_id"]
        and before_refs == after_refs
        and before["file_count"] == after["file_count"]
        and before["file_bytes"] == after["file_bytes"]
        and before["physical_record_count"] == after["physical_record_count"]
    )


def collect_maintenance_fingerprint(
    cursor: Any,
    qualified_table: str,
    *,
    include_exact_row_count: bool,
) -> dict[str, Any]:
    current = collect_iceberg_fingerprint(cursor, qualified_table)
    physical_record_count = current.pop("record_count")
    catalog, schema, table = qualified_table.split(".")
    cursor.execute(
        "SELECT name, type, snapshot_id FROM "
        f'{catalog}.{schema}."{table}$refs" ORDER BY name'
    )
    refs = _normalized_refs(cursor.fetchall(), current["snapshot_id"])
    exact_row_count = None
    if include_exact_row_count:
        cursor.execute("SELECT count(*) FROM " + qualified_table)
        exact_row_count = int(cursor.fetchone()[0])
    return {
        **current,
        "physical_record_count": physical_record_count,
        "refs": refs,
        "exact_row_count": exact_row_count,
    }


def _normalized_refs(rows: list[tuple[Any, ...]], current_snapshot_id: Any) -> list[dict[str, Any]]:
    current_snapshot = _json_safe_int(current_snapshot_id)
    if current_snapshot is None:
        raise MaintenancePlanError("maintenance fingerprint has no current snapshot")
    refs: list[dict[str, Any]] = []
    names: set[str] = set()
    for row in rows:
        if len(row) != 3:
            raise MaintenancePlanError("maintenance ref evidence is malformed")
        name, ref_type, snapshot_id = row
        if not isinstance(name, str) or not _IDENTIFIER.fullmatch(name):
            raise MaintenancePlanError("maintenance ref name is invalid")
        normalized_type = _sanitized_error_identifier(ref_type)
        normalized_snapshot = _json_safe_int(snapshot_id)
        if normalized_type not in {"BRANCH", "TAG"} or normalized_snapshot is None:
            raise MaintenancePlanError("maintenance ref evidence is invalid")
        if name in names:
            raise MaintenancePlanError("maintenance ref name is duplicate")
        names.add(name)
        refs.append(
            {
                "name": name,
                "type": normalized_type,
                "snapshot_id": normalized_snapshot,
            }
        )
    main_refs = [ref for ref in refs if ref["name"] == "main"]
    if len(main_refs) != 1 or main_refs[0]["type"] != "BRANCH":
        raise MaintenancePlanError("maintenance refs require exactly one main BRANCH")
    if main_refs[0]["snapshot_id"] != current_snapshot:
        raise MaintenancePlanError("maintenance main ref must match current snapshot")
    return refs


def run_maintenance_action(
    plan: MaintenancePlan,
    *,
    table: str,
    operation: str,
    workload_cursor: Any,
    metrics_cursor: Any,
    fingerprint_cursor: Any,
    allowed_tables: Sequence[str],
) -> dict[str, Any]:
    _validate_maintenance_plan(plan, allowed_tables=allowed_tables)
    qualified = _qualified_table(plan, table)
    if operation not in OPERATIONS:
        raise MaintenancePlanError("unsupported maintenance operation")
    base = {
        "plan_id": plan.plan_id,
        "plan_hash": plan.plan_hash,
        "table": table,
        "operation": operation,
    }
    phase = "PRE_SUBMIT"
    try:
        if not exact_table_exists(
            workload_cursor,
            catalog=plan.catalog,
            schema=plan.schema,
            table=table,
        ):
            return {**base, "status": "SKIPPED_MISSING", "circuit_breaker": False}

        before = collect_maintenance_fingerprint(
            fingerprint_cursor,
            qualified,
            include_exact_row_count=operation == "optimize",
        )
        telemetry = TelemetryCursor(workload_cursor, metrics_cursor)
        phase = "SUBMITTED"
        telemetry.execute(operation_sql(plan, table, operation))
        rows = telemetry.fetchall()
        phase = "ACKNOWLEDGED"
        output = _procedure_output(workload_cursor, rows, operation)
        metrics = dict(telemetry.records[-1])
        state = metrics.get("state")
        metrics["state"] = str(state).strip().upper() if state is not None else None
        metrics["peak_user_memory_bytes"] = _json_safe_int(
            metrics.get("peak_user_memory_bytes"), nonnegative=True
        )
        try:
            after = collect_maintenance_fingerprint(
                fingerprint_cursor,
                qualified,
                include_exact_row_count=operation == "optimize",
            )
        except MaintenancePlanError:
            return {
                **base,
                "status": "FAILED",
                "category": "LOGICAL_INVARIANT",
                "circuit_breaker": True,
                "phase": phase,
                "query_id": metrics.get("query_id"),
                "query_state": metrics.get("state"),
                "metrics": metrics,
                "procedure_output": output,
                "before": before,
            }

        peak = metrics.get("peak_user_memory_bytes")
        if (
            metrics.get("query_id") is None
            or metrics.get("state") != "FINISHED"
            or peak is None
            or not required_procedure_metrics_present(operation, output)
        ):
            return {
                **base,
                "status": "UNKNOWN",
                "category": "REQUIRED_TELEMETRY_UNAVAILABLE",
                "circuit_breaker": True,
                "phase": phase,
                "query_id": metrics.get("query_id"),
                "query_state": metrics.get("state"),
                "metrics": metrics,
                "procedure_output": output,
                "before": before,
                "after": after,
            }

        if not maintenance_postconditions_hold(operation, before, after):
            return {
                **base,
                "status": "FAILED",
                "category": "LOGICAL_INVARIANT",
                "circuit_breaker": True,
                "phase": phase,
                "query_id": metrics.get("query_id"),
                "query_state": metrics.get("state"),
                "metrics": metrics,
                "procedure_output": output,
                "before": before,
                "after": after,
            }

        phase = "VERIFIED"
        if peak >= MEMORY_WARNING_BYTES:
            return {
                **base,
                "status": "SUCCEEDED_WITH_STOP",
                "category": "MEMORY_STOP" if peak >= MEMORY_STOP_BYTES else "MEMORY_WARNING",
                "circuit_breaker": True,
                "phase": phase,
                "query_id": metrics.get("query_id"),
                "query_state": metrics.get("state"),
                "metrics": metrics,
                "procedure_output": output,
                "before": before,
                "after": after,
            }

        return {
            **base,
            "status": "SUCCEEDED",
            "circuit_breaker": False,
            "phase": phase,
            "query_id": metrics.get("query_id"),
            "query_state": metrics.get("state"),
            "metrics": metrics,
            "procedure_output": output,
            "before": before,
            "after": after,
        }
    except Exception as exc:  # noqa: BLE001 - classification is the public contract
        failure = classify_action_exception(
            exc,
            phase=phase,
            query_id=query_id_from_cursor(workload_cursor),
            query_state=query_state_from_cursor(workload_cursor),
            query_error_type=structured_error_type(exc),
        )
        return {
            **base,
            "status": "UNKNOWN" if failure["circuit_breaker"] else "FAILED",
            **failure,
        }


def _metadata_table_name(plan: MaintenancePlan, table: str, suffix: str) -> str:
    _qualified_table(plan, table)
    return f'{plan.catalog}.{plan.schema}."{table}${suffix}"'


def inspect_maintenance_plan(
    plan: MaintenancePlan,
    cursor: Any,
    *,
    allowed_tables: Sequence[str],
) -> dict[str, dict[str, Any]]:
    _validate_maintenance_plan(plan, allowed_tables=allowed_tables)
    inventory = {}
    for table in plan.tables:
        if not exact_table_exists(
            cursor,
            catalog=plan.catalog,
            schema=plan.schema,
            table=table,
        ):
            inventory[table] = {"status": "MISSING"}
            continue
        qualified = _qualified_table(plan, table)
        current = collect_maintenance_fingerprint(
            cursor,
            qualified,
            include_exact_row_count=False,
        )
        refs = current.pop("refs")
        current.pop("exact_row_count")
        cursor.execute(
            "SELECT count(*), min(committed_at), max(committed_at) FROM "
            + _metadata_table_name(plan, table, "snapshots")
        )
        snapshot_count, oldest, newest = cursor.fetchone()
        cursor.execute(
            "SELECT count(*) FROM "
            + _metadata_table_name(plan, table, "metadata_log_entries")
        )
        metadata_log_entries = cursor.fetchone()[0]
        inventory[table] = {
            "status": "EXISTS",
            "snapshot_count": int(snapshot_count),
            "oldest_snapshot_at": str(oldest) if oldest is not None else None,
            "newest_snapshot_at": str(newest) if newest is not None else None,
            "metadata_log_entries": int(metadata_log_entries),
            "current": current,
            "refs": refs,
            "orphan_candidate_estimate": "UNAVAILABLE_WITHOUT_EXECUTION",
        }
    return inventory


def collect_maintenance_inventory(
    plan: MaintenancePlan,
    *,
    allowed_tables: Sequence[str],
    connection_factory=None,
) -> dict[str, dict[str, Any]]:
    _validate_maintenance_plan(plan, allowed_tables=allowed_tables)
    factory = connection_factory if connection_factory is not None else _connect_trino
    connection = None
    cursor = None
    try:
        connection = factory()
        cursor = connection.cursor()
        return inspect_maintenance_plan(plan, cursor, allowed_tables=allowed_tables)
    finally:
        if _close_all((cursor, connection)) and sys.exc_info()[0] is None:
            raise MaintenancePlanError("maintenance resource cleanup failed")


def maintenance_plan_from_payload(
    payload: Mapping[str, Any],
    *,
    allowed_tables: Sequence[str],
    env: Mapping[str, str] | None = None,
) -> MaintenancePlan:
    required = {
        "plan_id",
        "plan_hash",
        "target",
        "catalog",
        "schema",
        "retention",
        "retain_last",
        "tables",
    }
    if not required.issubset(payload):
        raise MaintenancePlanError("maintenance plan payload is incomplete")
    rebuilt = resolve_maintenance_plan(
        target=str(payload["target"]),
        retention=str(payload["retention"]),
        tables=payload["tables"],
        allowed_tables=allowed_tables,
        dag_run_id=str(payload["plan_id"]),
        env=env,
    )
    expected = maintenance_plan_payload(rebuilt)
    received = {key: payload[key] for key in expected}
    if received != expected:
        raise MaintenancePlanError("maintenance plan payload hash or fields changed")
    return rebuilt


def execute_maintenance_action(
    plan_payload: Mapping[str, Any],
    *,
    allowed_tables: Sequence[str],
    table: str,
    operation: str,
    connection_factory=None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    plan = maintenance_plan_from_payload(
        plan_payload,
        allowed_tables=allowed_tables,
        env=env,
    )
    _validate_maintenance_plan(plan, allowed_tables=allowed_tables)
    factory = connection_factory if connection_factory is not None else _connect_trino
    connection = None
    cursors: list[Any] = []
    try:
        connection = factory()
        workload_cursor = connection.cursor()
        cursors.append(workload_cursor)
        metrics_cursor = connection.cursor()
        cursors.append(metrics_cursor)
        fingerprint_cursor = connection.cursor()
        cursors.append(fingerprint_cursor)
        return run_maintenance_action(
            plan,
            table=table,
            operation=operation,
            workload_cursor=workload_cursor,
            metrics_cursor=metrics_cursor,
            fingerprint_cursor=fingerprint_cursor,
            allowed_tables=allowed_tables,
        )
    finally:
        if _close_all((*reversed(cursors), connection)) and sys.exc_info()[0] is None:
            raise MaintenancePlanError("maintenance resource cleanup failed")


def _close_if_supported(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if callable(close):
        close()


def _close_all(resources: Sequence[Any]) -> bool:
    """Attempt every close without allowing cleanup to disclose driver details."""
    failed = False
    for resource in resources:
        try:
            _close_if_supported(resource)
        except Exception:  # noqa: BLE001 - cleanup must continue for every resource
            failed = True
    return failed


def _connect_trino():
    import trino.dbapi

    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=APPROVED_DEV_CATALOG,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )


def run_maintenance(
    target: str = "dev",
    tables: Iterable[str] = (),
    *,
    retention: str = "7d",
    ignore_missing: bool = True,
) -> dict[str, str]:
    """Fail closed until callers migrate to the immutable maintenance API."""

    del target, tables, retention, ignore_missing
    raise MaintenancePlanError(
        "legacy run_maintenance is disabled; use the immutable maintenance API"
    )
