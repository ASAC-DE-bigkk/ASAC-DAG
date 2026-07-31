from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "domains" / "weather"))

from common.runtime_guard import RuntimeTargetError  # noqa: E402
from weather_ingest.iceberg_maintenance import (  # noqa: E402
    APPROVED_DEV_CATALOG,
    APPROVED_DEV_SCHEMA,
    APPROVED_PROD_CATALOG,
    MaintenancePlanError,
    _connect_trino,
    classify_action_exception,
    collect_maintenance_fingerprint,
    collect_maintenance_inventory,
    execute_maintenance_action,
    exact_table_exists,
    inspect_maintenance_plan,
    maintenance_plan_payload,
    operation_sql,
    resolve_maintenance_plan,
    run_maintenance_action,
    run_maintenance,
)


ALLOWED_TABLES = (
    "bronze_kma_vilage_fcst",
    "bronze_seoul_traffic_incident",
    "bronze_collection_run_manifest",
)


def dev_env():
    return {
        "ASK_SEOUL_TARGET": "dev",
        "DBT_TARGET": "dev",
        "TRINO_DEV_ICEBERG_CATALOG": APPROVED_DEV_CATALOG,
        "ASK_SEOUL_SCHEMA": APPROVED_DEV_SCHEMA,
        "WEATHER_SCHEMA": "weather",
        "R2_DEV_BUCKET_NAME": "seoul-dev",
        "R2_DEV_ENDPOINT": "https://dev.invalid",
        "R2_DEV_ACCESS_KEY_ID": "dev-access",
        "R2_DEV_SECRET_ACCESS_KEY": "dev-secret",
    }


def prod_env():
    return {
        "ASK_SEOUL_TARGET": "prod",
        "DBT_TARGET": "prod",
        "TRINO_ICEBERG_CATALOG": APPROVED_PROD_CATALOG,
        "ASK_SEOUL_SCHEMA": APPROVED_DEV_SCHEMA,
        "WEATHER_SCHEMA": "weather",
        "R2_BUCKET_NAME": "seoul",
        "R2_ENDPOINT": "https://prod.invalid",
        "R2_ACCESS_KEY_ID": "prod-access",
        "R2_SECRET_ACCESS_KEY": "prod-secret",
    }


def test_resolve_plan_rejects_non_dev_before_connect():
    with pytest.raises(RuntimeTargetError, match="requested runtime target"):
        resolve_maintenance_plan(
            target="prod",
            retention="7d",
            tables=ALLOWED_TABLES,
            allowed_tables=ALLOWED_TABLES,
            dag_run_id="manual__unsafe",
            env=dev_env(),
        )


@pytest.mark.parametrize(
    ("tables", "message"),
    [
        ((), "at least one"),
        (("bronze_kma_vilage_fcst", "bronze_kma_vilage_fcst"), "duplicate"),
        (("commerce_orders",), "outside canonical allowlist"),
    ],
)
def test_resolve_plan_rejects_unsafe_table_selection(tables, message):
    with pytest.raises(MaintenancePlanError, match=message):
        resolve_maintenance_plan(
            target="dev",
            retention="7d",
            tables=tables,
            allowed_tables=ALLOWED_TABLES,
            dag_run_id="manual__policy",
            env=dev_env(),
        )


@pytest.mark.parametrize(
    ("allowed_tables", "message"),
    [
        ((), "canonical allowlist requires at least one table"),
        (
            ("bronze_kma_vilage_fcst", "bronze_kma_vilage_fcst"),
            "canonical allowlist contains a duplicate",
        ),
        (
            ("bronze_kma_vilage_fcst", "unsafe-table"),
            "canonical allowlist entries must be exact safe string identifiers",
        ),
        (
            ("bronze_kma_vilage_fcst", 7),
            "canonical allowlist entries must be exact safe string identifiers",
        ),
    ],
)
def test_resolve_plan_rejects_unsafe_canonical_allowlist_before_connect(
    monkeypatch, allowed_tables, message
):
    def fail_if_connected():
        raise AssertionError("canonical validation must not connect")

    monkeypatch.setattr(
        "weather_ingest.iceberg_maintenance._connect_trino", fail_if_connected
    )

    with pytest.raises(MaintenancePlanError, match=message):
        resolve_maintenance_plan(
            target="dev",
            retention="7d",
            tables=(ALLOWED_TABLES[0],),
            allowed_tables=allowed_tables,
            dag_run_id="manual__unsafe_canonical",
            env=dev_env(),
        )


@pytest.mark.parametrize(
    "allowed_tables",
    [
        "bronze_kma_vilage_fcst",
        b"bronze_kma_vilage_fcst",
        {"bronze_kma_vilage_fcst"},
        frozenset({"bronze_kma_vilage_fcst"}),
        (table for table in ALLOWED_TABLES),
    ],
    ids=("string", "bytes", "set", "frozenset", "generator"),
)
def test_resolve_plan_rejects_non_sequence_canonical_before_connect(
    monkeypatch, allowed_tables
):
    def fail_if_connected():
        raise AssertionError("canonical validation must not connect")

    monkeypatch.setattr(
        "weather_ingest.iceberg_maintenance._connect_trino", fail_if_connected
    )

    with pytest.raises(
        MaintenancePlanError,
        match="canonical allowlist must be a deterministic ordered sequence",
    ):
        resolve_maintenance_plan(
            target="dev",
            retention="7d",
            tables=(ALLOWED_TABLES[0],),
            allowed_tables=allowed_tables,
            dag_run_id="manual__unordered_canonical",
            env=dev_env(),
        )


def test_resolve_plan_reorders_subset_to_canonical_order():
    plan = resolve_maintenance_plan(
        target="dev",
        retention="7d",
        tables=("bronze_collection_run_manifest", "bronze_kma_vilage_fcst"),
        allowed_tables=ALLOWED_TABLES,
        dag_run_id="manual__ordered",
        env=dev_env(),
    )

    assert plan.tables == (
        "bronze_kma_vilage_fcst",
        "bronze_collection_run_manifest",
    )
    assert plan.catalog == "iceberg_dev"
    assert plan.schema == "weather_traffic_bronze"
    assert len(plan.plan_hash) == 64


def test_resolve_plan_hash_is_stable_for_logical_subset_request_order():
    first = resolve_maintenance_plan(
        target="dev",
        retention="7d",
        tables=("bronze_collection_run_manifest", "bronze_kma_vilage_fcst"),
        allowed_tables=ALLOWED_TABLES,
        dag_run_id="manual__stable_hash",
        env=dev_env(),
    )
    second = resolve_maintenance_plan(
        target="dev",
        retention="7d",
        tables=("bronze_kma_vilage_fcst", "bronze_collection_run_manifest"),
        allowed_tables=ALLOWED_TABLES,
        dag_run_id="manual__stable_hash",
        env=dev_env(),
    )

    assert first.tables == second.tables == (
        "bronze_kma_vilage_fcst",
        "bronze_collection_run_manifest",
    )
    assert first.plan_hash == second.plan_hash


def test_resolve_plan_requires_fixed_retention_and_schema():
    with pytest.raises(MaintenancePlanError, match="retention must be 7d"):
        resolve_maintenance_plan(
            target="dev",
            retention="6d",
            tables=(ALLOWED_TABLES[0],),
            allowed_tables=ALLOWED_TABLES,
            dag_run_id="manual__retention",
            env=dev_env(),
        )

    unsafe_env = dev_env()
    unsafe_env["ASK_SEOUL_SCHEMA"] = "ask_seoul"
    with pytest.raises(MaintenancePlanError, match="approved schema"):
        resolve_maintenance_plan(
            target="dev",
            retention="7d",
            tables=(ALLOWED_TABLES[0],),
            allowed_tables=ALLOWED_TABLES,
            dag_run_id="manual__schema",
            env=unsafe_env,
        )

    with pytest.raises(MaintenancePlanError, match="target must be dev or prod"):
        resolve_maintenance_plan(
            target="DEV",
            retention="7d",
            tables=(ALLOWED_TABLES[0],),
            allowed_tables=ALLOWED_TABLES,
            dag_run_id="manual__target_case",
            env=dev_env(),
        )


def test_resolve_plan_accepts_prod_target_with_prod_catalog():
    plan = resolve_maintenance_plan(
        target="prod",
        retention="7d",
        tables=(ALLOWED_TABLES[0],),
        allowed_tables=ALLOWED_TABLES,
        dag_run_id="manual__prod",
        env=prod_env(),
    )

    assert plan.target == "prod"
    assert plan.catalog == APPROVED_PROD_CATALOG
    assert plan.schema == APPROVED_DEV_SCHEMA


def test_resolve_plan_rejects_prod_target_with_dev_catalog(monkeypatch):
    """Caught by the shared runtime_guard cross-check before this module's own
    (redundant, defense-in-depth) catalog check ever runs."""

    def fail_if_connected():
        raise AssertionError("catalog mismatch must not connect")

    monkeypatch.setattr(
        "weather_ingest.iceberg_maintenance._connect_trino", fail_if_connected
    )

    bad_env = prod_env()
    bad_env["TRINO_ICEBERG_CATALOG"] = APPROVED_DEV_CATALOG
    with pytest.raises(RuntimeTargetError, match="catalog"):
        resolve_maintenance_plan(
            target="prod",
            retention="7d",
            tables=(ALLOWED_TABLES[0],),
            allowed_tables=ALLOWED_TABLES,
            dag_run_id="manual__prod_bad_catalog",
            env=bad_env,
        )


def test_connect_trino_uses_requested_catalog(monkeypatch):
    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return "connection"

    monkeypatch.setattr("trino.dbapi.connect", fake_connect)

    assert _connect_trino(catalog=APPROVED_PROD_CATALOG) == "connection"
    assert captured["catalog"] == APPROVED_PROD_CATALOG


def test_resolve_plan_keeps_weather_dbt_schema_independent_from_bronze_maintenance():
    env = dev_env()
    env["WEATHER_SCHEMA"] = "weather"

    plan = resolve_maintenance_plan(
        target="dev",
        retention="7d",
        tables=(ALLOWED_TABLES[0],),
        allowed_tables=ALLOWED_TABLES,
        dag_run_id="manual__independent_weather_schema",
        env=env,
    )

    assert plan.schema == APPROVED_DEV_SCHEMA


class ExistsCursor:
    def __init__(self, row):
        self.row = row
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)

    def fetchone(self):
        return self.row


def test_exact_table_exists_does_not_discover_other_tables():
    cursor = ExistsCursor((1,))

    assert exact_table_exists(
        cursor,
        catalog="iceberg_dev",
        schema="weather_traffic_bronze",
        table="bronze_kma_vilage_fcst",
    )
    statement = cursor.statements[0]
    assert "information_schema.tables" in statement
    assert "table_name = 'bronze_kma_vilage_fcst'" in statement
    assert "SHOW TABLES" not in statement


def test_exact_table_exists_rejects_unapproved_scope_before_query():
    cursor = ExistsCursor((1,))
    with pytest.raises(MaintenancePlanError):
        exact_table_exists(
            cursor,
            catalog="iceberg_dev; DROP SCHEMA x",
            schema="weather_traffic_bronze",
            table="bronze_kma_vilage_fcst",
        )
    assert cursor.statements == []


def test_legacy_run_maintenance_fails_closed_without_connecting(monkeypatch):
    def fail_if_connected():
        raise AssertionError("legacy bridge must not connect")

    monkeypatch.setattr(
        "weather_ingest.iceberg_maintenance._connect_trino", fail_if_connected
    )

    with pytest.raises(MaintenancePlanError, match="immutable maintenance API"):
        run_maintenance()


def sample_action_plan():
    return resolve_maintenance_plan(
        target="dev",
        retention="7d",
        tables=("bronze_kma_vilage_fcst",),
        allowed_tables=ALLOWED_TABLES,
        dag_run_id="manual__action",
        env=dev_env(),
    )


def test_operation_sql_preserves_canonical_order_contract():
    plan = sample_action_plan()
    table = "bronze_kma_vilage_fcst"

    assert operation_sql(plan, table, "optimize").endswith("EXECUTE optimize")
    assert "expire_snapshots(retention_threshold => '7d', retain_last => 1)" in operation_sql(
        plan, table, "expire_snapshots"
    )
    assert "remove_orphan_files(retention_threshold => '7d')" in operation_sql(
        plan, table, "remove_orphan_files"
    )


class StructuredTrinoError(RuntimeError):
    def __init__(self, *, error_name, error_type):
        super().__init__("sanitized")
        self.error_name = error_name
        self.error_type = error_type


@pytest.mark.parametrize(
    ("error", "category", "circuit_breaker"),
    [
        (ConnectionError("connection lost"), "UNKNOWN", True),
        (TimeoutError("query timed out"), "UNKNOWN", True),
        (
            StructuredTrinoError(
                error_name="EXCEEDED_GLOBAL_MEMORY_LIMIT",
                error_type="USER_ERROR",
            ),
            "MEMORY_LIMIT",
            True,
        ),
        (ValueError("invalid procedure argument"), "UNKNOWN", True),
    ],
)
def test_classify_action_exception(error, category, circuit_breaker):
    result = classify_action_exception(
        error,
        phase="SUBMITTED",
        query_id=None,
        query_state=None,
        query_error_type=None,
    )

    assert result == {
        "category": category,
        "circuit_breaker": circuit_breaker,
        "error_type": type(error).__name__,
        "phase": "SUBMITTED",
        "query_id": None,
        "query_state": None,
        "structured_error_type": getattr(error, "error_type", None),
        "structured_error_name": getattr(error, "error_name", None),
    }


class ActionCursor:
    def __init__(
        self,
        *,
        stats=None,
        mutation_rows=(),
        mutation_description=(),
        snapshots=(),
        files=(),
        refs=(),
        exact_counts=(),
        exists=True,
        execute_error=None,
        fetchall_error=None,
    ):
        self.stats = stats if stats is not None else {}
        self.mutation_rows = list(mutation_rows)
        self.mutation_description = list(mutation_description)
        self.snapshots = list(snapshots)
        self.files = list(files)
        self.refs = list(refs)
        self.exact_counts = list(exact_counts)
        self.exists = exists
        self.execute_error = execute_error
        self.fetchall_error = fetchall_error
        self.statements = []
        self.description = None

    def execute(self, statement):
        self.statements.append(statement)
        if statement.startswith("ALTER TABLE"):
            self.description = self.mutation_description
            if self.execute_error is not None:
                raise self.execute_error

    def fetchone(self):
        statement = self.statements[-1]
        if "information_schema.tables" in statement:
            return (1,) if self.exists else None
        if "$snapshots" in statement:
            return self.snapshots.pop(0)
        if "$files" in statement:
            return self.files.pop(0)
        if statement.startswith("SELECT count(*) FROM iceberg_dev."):
            return (self.exact_counts.pop(0),)
        raise AssertionError(f"unexpected fetchone: {statement}")

    def fetchall(self):
        statement = self.statements[-1]
        if "$refs" in statement:
            return self.refs.pop(0)
        if statement.startswith("ALTER TABLE"):
            if self.fetchall_error is not None:
                raise self.fetchall_error
            return list(self.mutation_rows)
        raise AssertionError(f"unexpected fetchall: {statement}")


class MetricsCursor:
    def __init__(self, *, row=("q_maintenance", "FINISHED", 1024), error=None):
        self.row = row
        self.error = error
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)
        if self.error is not None:
            raise self.error

    def fetchall(self):
        return [
            ("query_id", "varchar"),
            ("state", "varchar"),
            ("peak_user_memory_bytes", "bigint"),
        ]

    def fetchone(self):
        return self.row


def action_fingerprint_inputs(*, refs=None, exact_counts=()):
    return {
        "snapshots": [(11, "2026-07-18T00:00:00Z")] * 2,
        "files": [(4, 8192, 400)] * 2,
        "refs": refs
        if refs is not None
        else [[("main", "BRANCH", 11)], [("main", "BRANCH", 11)]],
        "exact_counts": exact_counts,
    }


ORPHAN_METRICS = (
    ("processed_manifests_count", 3),
    ("active_files_count", 10),
    ("scanned_files_count", 20),
    ("deleted_files_count", 1),
    ("deleted_bytes", 128),
)
OPTIMIZE_METRICS = (
    ("rewritten_data_files_count", 3),
    ("removed_delete_files_count", 0),
    ("added_data_files_count", 3),
)


def run_action(operation, *, workload=None, metrics=None, fingerprint=None):
    inputs = action_fingerprint_inputs(
        exact_counts=(400, 400) if operation == "optimize" else ()
    )
    workload = workload or ActionCursor(
        stats={
            "queryId": "q_maintenance",
            "state": "FINISHED",
            "peakMemoryBytes": 1024,
        },
        mutation_rows=ORPHAN_METRICS if operation == "remove_orphan_files" else OPTIMIZE_METRICS,
        mutation_description=[("metric_name",), ("metric_value",)],
        **inputs,
    )
    return run_maintenance_action(
        sample_action_plan(),
        table="bronze_kma_vilage_fcst",
        operation=operation,
        workload_cursor=workload,
        metrics_cursor=metrics or MetricsCursor(),
        fingerprint_cursor=fingerprint or workload,
        allowed_tables=ALLOWED_TABLES,
    )


def test_run_action_returns_query_metrics_procedure_output_and_fingerprints():
    result = run_action("remove_orphan_files")

    assert result["status"] == "SUCCEEDED"
    assert result["query_id"] == "q_maintenance"
    assert result["metrics"]["state"] == "FINISHED"
    assert result["metrics"]["peak_user_memory_bytes"] == 1024
    assert result["procedure_output"]["deleted_files_count"] == 1
    assert result["procedure_output"]["deleted_bytes"] == 128


def test_run_action_rejects_non_finished_query_state():
    result = run_action(
        "remove_orphan_files",
        metrics=MetricsCursor(row=("q_maintenance", "FAILED", 1024)),
    )

    assert result["status"] == "UNKNOWN"
    assert result["category"] == "REQUIRED_TELEMETRY_UNAVAILABLE"
    assert result["circuit_breaker"] is True


@pytest.mark.parametrize("metric", [name for name, _ in ORPHAN_METRICS])
@pytest.mark.parametrize("bad_value", ["omitted", None])
def test_run_action_rejects_each_missing_or_invalid_orphan_metric(metric, bad_value):
    rows = [(name, value) for name, value in ORPHAN_METRICS if name != metric]
    if bad_value is None:
        rows.append((metric, None))
    result = run_action(
        "remove_orphan_files",
        workload=ActionCursor(
            stats={"queryId": "q_maintenance", "state": "FINISHED", "peakMemoryBytes": 1024},
            mutation_rows=rows,
            mutation_description=[("metric_name",), ("metric_value",)],
            **action_fingerprint_inputs(),
        ),
    )

    assert result["status"] == "UNKNOWN"
    assert result["category"] == "REQUIRED_TELEMETRY_UNAVAILABLE"


@pytest.mark.parametrize(
    "rows",
    [
        tuple((name, value) for name, value in OPTIMIZE_METRICS if name != "added_data_files_count"),
        (("rewritten_data_files_count", "3"),) + OPTIMIZE_METRICS[1:],
    ],
)
def test_run_action_rejects_missing_or_non_numeric_optimize_metric(rows):
    result = run_action(
        "optimize",
        workload=ActionCursor(
            stats={"queryId": "q_maintenance", "state": "FINISHED", "peakMemoryBytes": 1024},
            mutation_rows=rows,
            mutation_description=[("metric_name",), ("metric_value",)],
            **action_fingerprint_inputs(exact_counts=(400, 400)),
        ),
    )

    assert result["status"] == "UNKNOWN"
    assert result["category"] == "REQUIRED_TELEMETRY_UNAVAILABLE"


def test_run_action_fails_closed_when_refs_drift():
    result = run_action(
        "remove_orphan_files",
        workload=ActionCursor(
            stats={"queryId": "q_maintenance", "state": "FINISHED", "peakMemoryBytes": 1024},
            mutation_rows=ORPHAN_METRICS,
            mutation_description=[("metric_name",), ("metric_value",)],
            **action_fingerprint_inputs(
                refs=[[('main', 'BRANCH', 11)], [('main', 'BRANCH', 12)]]
            ),
        ),
    )

    assert result["status"] == "FAILED"
    assert result["category"] == "LOGICAL_INVARIANT"
    assert result["circuit_breaker"] is True


def test_run_action_fails_closed_when_optimize_exact_count_drifts():
    result = run_action(
        "optimize",
        workload=ActionCursor(
            stats={"queryId": "q_maintenance", "state": "FINISHED", "peakMemoryBytes": 1024},
            mutation_rows=OPTIMIZE_METRICS,
            mutation_description=[("metric_name",), ("metric_value",)],
            **action_fingerprint_inputs(exact_counts=(400, 401)),
        ),
    )

    assert result["status"] == "FAILED"
    assert result["category"] == "LOGICAL_INVARIANT"


def test_run_action_returns_unknown_after_post_submit_fetchall_failure():
    result = run_action(
        "remove_orphan_files",
        workload=ActionCursor(
            stats={"queryId": "q_maintenance", "state": "RUNNING", "peakMemoryBytes": 1024},
            mutation_rows=ORPHAN_METRICS,
            mutation_description=[("metric_name",), ("metric_value",)],
            fetchall_error=ConnectionError("raw endpoint must not leak"),
            **action_fingerprint_inputs(),
        ),
    )

    assert result["status"] == "UNKNOWN"
    assert result["phase"] == "SUBMITTED"
    assert result["query_id"] == "q_maintenance"
    assert "raw endpoint" not in str(result)


def test_run_action_returns_unknown_after_post_submit_telemetry_failure(monkeypatch):
    class RaisingTelemetry:
        def __init__(self, workload_cursor, metrics_cursor):
            self._workload_cursor = workload_cursor

        def execute(self, statement):
            return self._workload_cursor.execute(statement)

        def fetchall(self):
            raise ConnectionError("telemetry URI must not leak")

    monkeypatch.setattr("weather_ingest.iceberg_maintenance.TelemetryCursor", RaisingTelemetry)

    result = run_action("remove_orphan_files")

    assert result["status"] == "UNKNOWN"
    assert result["phase"] == "SUBMITTED"
    assert "telemetry URI" not in str(result)


def test_run_action_returns_unknown_after_post_submit_fingerprint_failure():
    class PostFingerprintFailureCursor(ActionCursor):
        def fetchone(self):
            if len([statement for statement in self.statements if "$snapshots" in statement]) > 1:
                raise ConnectionError("r2://secret/object")
            return super().fetchone()

    workload = PostFingerprintFailureCursor(
        stats={"queryId": "q_maintenance", "state": "FINISHED", "peakMemoryBytes": 1024},
        mutation_rows=ORPHAN_METRICS,
        mutation_description=[("metric_name",), ("metric_value",)],
        **action_fingerprint_inputs(),
    )
    result = run_action("remove_orphan_files", workload=workload)

    assert result["status"] == "UNKNOWN"
    assert result["phase"] == "ACKNOWLEDGED"
    assert "r2://" not in str(result)


def test_run_action_marks_confirmed_terminal_non_memory_user_error_table_local():
    error = StructuredTrinoError(error_name="INVALID_PROCEDURE_ARGUMENT", error_type="USER_ERROR")
    result = run_action(
        "remove_orphan_files",
        workload=ActionCursor(
            stats={"queryId": "q_failed", "state": "FAILED"},
            mutation_rows=ORPHAN_METRICS,
            mutation_description=[("metric_name",), ("metric_value",)],
            execute_error=error,
            **action_fingerprint_inputs(),
        ),
    )

    assert result["status"] == "FAILED"
    assert result["category"] == "TABLE_OPERATION"
    assert result["circuit_breaker"] is False
    assert result["phase"] == "SUBMITTED"
    assert result["query_id"] == "q_failed"
    assert result["query_state"] == "FAILED"
    assert result["structured_error_type"] == "USER_ERROR"
    assert result["structured_error_name"] == "INVALID_PROCEDURE_ARGUMENT"


def test_classify_action_exception_sanitizes_direct_query_error_type():
    result = classify_action_exception(
        ValueError("must remain private"),
        phase="SUBMITTED",
        query_id="q_failed",
        query_state="FAILED",
        query_error_type="r2://bucket/private-object-key",
    )

    assert result["category"] == "UNKNOWN"
    assert result["circuit_breaker"] is True
    assert result["structured_error_type"] is None
    assert "r2://" not in str(result)


@pytest.mark.parametrize(
    (
        "phase",
        "query_id",
        "query_state",
        "query_error_type",
        "error_type",
        "error_name",
    ),
    [
        ("PRE_SUBMIT", "q_failed", "FAILED", "USER_ERROR", "USER_ERROR", "INVALID_ARGUMENT"),
        ("SUBMITTED", None, "FAILED", "USER_ERROR", "USER_ERROR", "INVALID_ARGUMENT"),
        ("SUBMITTED", "q_failed", None, "USER_ERROR", "USER_ERROR", "INVALID_ARGUMENT"),
        ("SUBMITTED", "q_failed", "FINISHED", "USER_ERROR", "USER_ERROR", "INVALID_ARGUMENT"),
        ("SUBMITTED", "q_failed", "FAILED", None, None, "INVALID_ARGUMENT"),
        ("SUBMITTED", "q_failed", "FAILED", "INTERNAL_ERROR", "INTERNAL_ERROR", "INVALID_ARGUMENT"),
        ("SUBMITTED", "q_failed", "FAILED", "USER_ERROR", "USER_ERROR", None),
        ("SUBMITTED", "q_failed", "FAILED", "USER_ERROR", "USER_ERROR", "r2://private/object"),
    ],
)
def test_classify_action_exception_requires_complete_table_local_evidence(
    phase, query_id, query_state, query_error_type, error_type, error_name
):
    error = StructuredTrinoError(error_name=error_name, error_type=error_type)

    result = classify_action_exception(
        error,
        phase=phase,
        query_id=query_id,
        query_state=query_state,
        query_error_type=query_error_type,
    )

    assert result["category"] == "UNKNOWN"
    assert result["circuit_breaker"] is True


def test_run_action_marks_missing_table_without_alter():
    workload = ActionCursor(exists=False)

    result = run_maintenance_action(
        sample_action_plan(),
        table="bronze_kma_vilage_fcst",
        operation="optimize",
        workload_cursor=workload,
        metrics_cursor=MetricsCursor(),
        fingerprint_cursor=workload,
        allowed_tables=ALLOWED_TABLES,
    )

    assert result == {
        "plan_id": "manual__action",
        "plan_hash": sample_action_plan().plan_hash,
        "table": "bronze_kma_vilage_fcst",
        "operation": "optimize",
        "status": "SKIPPED_MISSING",
        "circuit_breaker": False,
    }
    assert not any(statement.startswith("ALTER TABLE") for statement in workload.statements)


class InventoryCursor:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)

    def fetchone(self):
        statement = self.statements[-1]
        if "information_schema.tables" in statement:
            return (1,)
        if "ORDER BY committed_at DESC LIMIT 1" in statement:
            return (11, "2026-07-18T00:00:00Z")
        if "$files" in statement:
            return (4, 8192, 400)
        if "min(committed_at)" in statement:
            return (780, "2026-07-10T00:00:00Z", "2026-07-18T00:00:00Z")
        if "$metadata_log_entries" in statement:
            return (87,)
        raise AssertionError(f"unexpected inventory query: {statement}")

    def fetchall(self):
        if '"bronze_kma_vilage_fcst$refs"' in self.statements[-1]:
            return [("main", "BRANCH", 11)]
        raise AssertionError(f"unexpected inventory query: {self.statements[-1]}")


def test_preflight_inventory_reads_only_selected_table_metadata_without_orphan_guess():
    cursor = InventoryCursor()
    inventory = inspect_maintenance_plan(
        sample_action_plan(), cursor, allowed_tables=ALLOWED_TABLES
    )

    assert inventory["bronze_kma_vilage_fcst"] == {
        "status": "EXISTS",
        "snapshot_count": 780,
        "oldest_snapshot_at": "2026-07-10T00:00:00Z",
        "newest_snapshot_at": "2026-07-18T00:00:00Z",
        "metadata_log_entries": 87,
        "current": {
            "table": "iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst",
            "snapshot_id": 11,
            "snapshot_committed_at": "2026-07-18T00:00:00Z",
            "file_count": 4,
            "file_bytes": 8192,
            "physical_record_count": 400,
        },
        "refs": [{"name": "main", "type": "BRANCH", "snapshot_id": 11}],
        "orphan_candidate_estimate": "UNAVAILABLE_WITHOUT_EXECUTION",
    }
    joined = "\n".join(cursor.statements)
    assert "SHOW TABLES" not in joined
    assert "bronze_kma_vilage_fcst" in joined


@pytest.mark.parametrize(
    ("peak", "category"),
    [
        (1_503_238_554, "MEMORY_WARNING"),
        (1_717_986_918, "MEMORY_STOP"),
    ],
)
def test_run_action_stops_after_successful_memory_threshold(peak, category):
    result = run_action(
        "remove_orphan_files",
        metrics=MetricsCursor(row=("q_maintenance", "FINISHED", peak)),
    )

    assert result["status"] == "SUCCEEDED_WITH_STOP"
    assert result["category"] == category
    assert result["circuit_breaker"] is True
    assert result["phase"] == "VERIFIED"


def test_execute_action_rejects_changed_payload_before_connection():
    plan = resolve_maintenance_plan(
        target="dev",
        retention="7d",
        tables=(ALLOWED_TABLES[0],),
        allowed_tables=ALLOWED_TABLES,
        dag_run_id="manual__payload",
        env=dev_env(),
    )
    payload = maintenance_plan_payload(plan)
    payload["plan_hash"] = "b" * 64

    def must_not_connect():
        raise AssertionError("payload validation must precede connection")

    with pytest.raises(MaintenancePlanError, match="payload hash or fields changed"):
        execute_maintenance_action(
            payload,
            allowed_tables=ALLOWED_TABLES,
            table=ALLOWED_TABLES[0],
            operation="optimize",
            connection_factory=must_not_connect,
            env=dev_env(),
        )


@pytest.mark.parametrize(
    ("stats", "metrics_row"),
    [
        ({"state": "FINISHED", "peakMemoryBytes": 1024}, ("q_maintenance", "FINISHED", 1024)),
        (
            {"queryId": "q_maintenance", "state": "FINISHED", "peakMemoryBytes": 1024},
            ("q_maintenance", "FINISHED", "invalid"),
        ),
    ],
)
def test_run_action_rejects_missing_query_id_or_invalid_peak(stats, metrics_row):
    result = run_action(
        "remove_orphan_files",
        workload=ActionCursor(
            stats=stats,
            mutation_rows=ORPHAN_METRICS,
            mutation_description=[("metric_name",), ("metric_value",)],
            **action_fingerprint_inputs(),
        ),
        metrics=MetricsCursor(row=metrics_row),
    )

    assert result["status"] == "UNKNOWN"
    assert result["category"] == "REQUIRED_TELEMETRY_UNAVAILABLE"
    assert result["circuit_breaker"] is True


def test_run_action_fails_closed_when_optimize_non_main_ref_drifts():
    result = run_action(
        "optimize",
        workload=ActionCursor(
            stats={"queryId": "q_maintenance", "state": "FINISHED", "peakMemoryBytes": 1024},
            mutation_rows=OPTIMIZE_METRICS,
            mutation_description=[("metric_name",), ("metric_value",)],
            **action_fingerprint_inputs(
                refs=[
                    [("main", "BRANCH", 11), ("release", "BRANCH", 7)],
                    [("main", "BRANCH", 11), ("release", "TAG", 7)],
                ],
                exact_counts=(400, 400),
            ),
        ),
    )

    assert result["status"] == "FAILED"
    assert result["category"] == "LOGICAL_INVARIANT"


@pytest.mark.parametrize(
    "forged",
    [
        replace(sample_action_plan(), retention="1d"),
        replace(sample_action_plan(), retain_last=2),
        replace(sample_action_plan(), plan_hash="f" * 64),
        replace(sample_action_plan(), catalog=APPROVED_PROD_CATALOG),
        replace(sample_action_plan(), target="staging"),
    ],
)
def test_raw_forged_plan_is_rejected_before_action_or_inventory_cursor_use(forged):
    cursor = ActionCursor()

    with pytest.raises(MaintenancePlanError):
        run_maintenance_action(
            forged,
            table="bronze_kma_vilage_fcst",
            operation="optimize",
            workload_cursor=cursor,
            metrics_cursor=MetricsCursor(),
            fingerprint_cursor=cursor,
            allowed_tables=ALLOWED_TABLES,
        )
    with pytest.raises(MaintenancePlanError):
        inspect_maintenance_plan(forged, cursor, allowed_tables=ALLOWED_TABLES)

    assert cursor.statements == []


def test_collect_fingerprint_rejects_duplicate_or_invalid_main_refs():
    fingerprint = ActionCursor(
        snapshots=[(11, "2026-07-18T00:00:00Z")],
        files=[(4, 8192, 400)],
        refs=[
            [("main", "BRANCH", 11), ("main", "BRANCH", 11)],
        ],
    )
    with pytest.raises(MaintenancePlanError, match="duplicate"):
        collect_maintenance_fingerprint(
            fingerprint,
            "iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst",
            include_exact_row_count=False,
        )

    missing_main = ActionCursor(
        snapshots=[(11, "2026-07-18T00:00:00Z")],
        files=[(4, 8192, 400)],
        refs=[[("release", "TAG", 11)]],
    )
    with pytest.raises(MaintenancePlanError, match="main"):
        collect_maintenance_fingerprint(
            missing_main,
            "iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst",
            include_exact_row_count=False,
        )


def test_collect_fingerprint_requires_main_branch_at_current_snapshot():
    fingerprint = ActionCursor(
        snapshots=[(11, "2026-07-18T00:00:00Z")],
        files=[(4, 8192, 400)],
        refs=[[("main", "BRANCH", 12)]],
    )

    with pytest.raises(MaintenancePlanError, match="current snapshot"):
        collect_maintenance_fingerprint(
            fingerprint,
            "iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst",
            include_exact_row_count=False,
        )


def test_run_action_normalizes_json_safe_required_procedure_and_ref_evidence():
    result = run_action(
        "remove_orphan_files",
        workload=ActionCursor(
            stats={"queryId": "q_maintenance", "state": "FINISHED", "peakMemoryBytes": Decimal("1024")},
            mutation_rows=[
                ("processed_manifests_count", Decimal("3")),
                ("active_files_count", Decimal("10")),
                ("scanned_files_count", Decimal("20")),
                ("deleted_files_count", Decimal("1")),
                ("deleted_bytes", Decimal("128")),
                ("unexpected_metric", "r2://private/object"),
            ],
            mutation_description=[("metric_name", "varchar"), ("metric_value", "bigint")],
            **action_fingerprint_inputs(
                refs=[
                    [("main", "BRANCH", Decimal("11"))],
                    [("main", "BRANCH", Decimal("11"))],
                ]
            ),
        ),
        metrics=MetricsCursor(row=("q_maintenance", " FINISHED ", Decimal("1024"))),
    )

    assert result["status"] == "SUCCEEDED"
    assert result["metrics"]["state"] == "FINISHED"
    assert result["metrics"]["peak_user_memory_bytes"] == 1024
    assert result["procedure_output"] == {
        "processed_manifests_count": 3,
        "active_files_count": 10,
        "scanned_files_count": 20,
        "deleted_files_count": 1,
        "deleted_bytes": 128,
    }
    assert result["before"]["refs"][0]["snapshot_id"] == 11
    assert "r2://" not in json.dumps(result)
    json.dumps(result)


class ClosableConnection:
    def __init__(self, cursors):
        self.cursors = list(cursors)
        self.closed = False

    def cursor(self):
        return self.cursors.pop(0)

    def close(self):
        self.closed = True


class ClosableActionCursor(ActionCursor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.closed = False
        self.close_error = None

    def close(self):
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class ClosableMetricsCursor(MetricsCursor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.closed = False

    def close(self):
        self.closed = True


def test_execute_action_uses_distinct_cursors_and_closes_factory_owned_resources():
    workload = ClosableActionCursor(
        stats={"queryId": "q_maintenance", "state": "FINISHED", "peakMemoryBytes": 1024},
        mutation_rows=ORPHAN_METRICS,
        mutation_description=[("metric_name", "varchar"), ("metric_value", "bigint")],
    )
    metrics = ClosableMetricsCursor()
    fingerprint = ClosableActionCursor(**action_fingerprint_inputs())
    connection = ClosableConnection([workload, metrics, fingerprint])
    plan = resolve_maintenance_plan(
        target="dev",
        retention="7d",
        tables=(ALLOWED_TABLES[0],),
        allowed_tables=ALLOWED_TABLES,
        dag_run_id="manual__close_success",
        env=dev_env(),
    )

    result = execute_maintenance_action(
        maintenance_plan_payload(plan),
        allowed_tables=ALLOWED_TABLES,
        table=ALLOWED_TABLES[0],
        operation="remove_orphan_files",
        connection_factory=lambda: connection,
        env=dev_env(),
    )

    assert result["status"] == "SUCCEEDED"
    assert workload.closed and metrics.closed and fingerprint.closed and connection.closed


def test_execute_action_closes_factory_owned_resources_when_runner_raises(monkeypatch):
    cursors = [ClosableActionCursor(), ClosableMetricsCursor(), ClosableActionCursor()]
    connection = ClosableConnection(cursors)
    plan = resolve_maintenance_plan(
        target="dev",
        retention="7d",
        tables=(ALLOWED_TABLES[0],),
        allowed_tables=ALLOWED_TABLES,
        dag_run_id="manual__close_error",
        env=dev_env(),
    )

    def raise_from_runner(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("must not escape cleanup")

    monkeypatch.setattr(
        "weather_ingest.iceberg_maintenance.run_maintenance_action", raise_from_runner
    )
    with pytest.raises(RuntimeError, match="must not escape cleanup"):
        execute_maintenance_action(
            maintenance_plan_payload(plan),
            allowed_tables=ALLOWED_TABLES,
            table=ALLOWED_TABLES[0],
            operation="remove_orphan_files",
            connection_factory=lambda: connection,
            env=dev_env(),
        )

    assert all(cursor.closed for cursor in cursors)
    assert connection.closed


def test_execute_action_closes_acquired_cursor_when_later_cursor_creation_fails():
    first = ClosableActionCursor()

    class PartialConnection:
        def __init__(self):
            self.calls = 0
            self.closed = False

        def cursor(self):
            self.calls += 1
            if self.calls == 1:
                return first
            raise RuntimeError("second cursor unavailable")

        def close(self):
            self.closed = True

    connection = PartialConnection()
    plan = resolve_maintenance_plan(
        target="dev",
        retention="7d",
        tables=(ALLOWED_TABLES[0],),
        allowed_tables=ALLOWED_TABLES,
        dag_run_id="manual__close_partial",
        env=dev_env(),
    )

    with pytest.raises(RuntimeError, match="second cursor unavailable"):
        execute_maintenance_action(
            maintenance_plan_payload(plan),
            allowed_tables=ALLOWED_TABLES,
            table=ALLOWED_TABLES[0],
            operation="remove_orphan_files",
            connection_factory=lambda: connection,
            env=dev_env(),
        )

    assert first.closed and connection.closed


def test_collect_inventory_closes_factory_owned_cursor_and_connection():
    cursor = InventoryCursor()
    cursor.closed = False
    cursor.close = lambda: setattr(cursor, "closed", True)
    connection = ClosableConnection([cursor])

    inventory = collect_maintenance_inventory(
        sample_action_plan(),
        allowed_tables=ALLOWED_TABLES,
        connection_factory=lambda: connection,
    )

    assert inventory["bronze_kma_vilage_fcst"]["status"] == "EXISTS"
    assert cursor.closed and connection.closed


def test_matching_hash_noncanonical_plan_is_rejected_before_action_or_inventory_cursor_use():
    forged = resolve_maintenance_plan(
        target="dev",
        retention="7d",
        tables=("bronze_kma_vilage_fcst", "bronze_seoul_traffic_incident"),
        allowed_tables=(
            "bronze_seoul_traffic_incident",
            "bronze_kma_vilage_fcst",
            "bronze_collection_run_manifest",
        ),
        dag_run_id="manual__noncanonical",
        env=dev_env(),
    )
    cursor = ActionCursor()

    with pytest.raises(MaintenancePlanError, match="canonical allowlist"):
        run_maintenance_action(
            forged,
            table="bronze_kma_vilage_fcst",
            operation="optimize",
            workload_cursor=cursor,
            metrics_cursor=MetricsCursor(),
            fingerprint_cursor=cursor,
            allowed_tables=ALLOWED_TABLES,
        )
    with pytest.raises(MaintenancePlanError, match="canonical allowlist"):
        inspect_maintenance_plan(forged, cursor, allowed_tables=ALLOWED_TABLES)

    assert cursor.statements == []


def test_factory_cleanup_attempts_every_resource_and_returns_sanitized_failure():
    workload = ClosableActionCursor(
        stats={"queryId": "q_maintenance", "state": "FINISHED", "peakMemoryBytes": 1024},
        mutation_rows=ORPHAN_METRICS,
        mutation_description=[("metric_name", "varchar"), ("metric_value", "bigint")],
    )
    metrics = ClosableMetricsCursor()
    fingerprint = ClosableActionCursor(**action_fingerprint_inputs())
    connection = ClosableConnection([workload, metrics, fingerprint])
    fingerprint.close_error = RuntimeError("r2://private/close-error")
    plan = sample_action_plan()

    with pytest.raises(MaintenancePlanError, match="resource cleanup failed") as error:
        execute_maintenance_action(
            maintenance_plan_payload(plan),
            allowed_tables=ALLOWED_TABLES,
            table=ALLOWED_TABLES[0],
            operation="remove_orphan_files",
            connection_factory=lambda: connection,
            env=dev_env(),
        )

    assert "r2://" not in str(error.value)
    assert workload.closed and metrics.closed and fingerprint.closed and connection.closed


def test_factory_cleanup_preserves_primary_action_error_after_close_failure(monkeypatch):
    cursors = [ClosableActionCursor(), ClosableMetricsCursor(), ClosableActionCursor()]
    cursors[2].close_error = RuntimeError("cleanup failure")
    connection = ClosableConnection(cursors)
    plan = sample_action_plan()

    def primary_failure(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("primary action failure")

    monkeypatch.setattr(
        "weather_ingest.iceberg_maintenance.run_maintenance_action", primary_failure
    )
    with pytest.raises(RuntimeError, match="primary action failure"):
        execute_maintenance_action(
            maintenance_plan_payload(plan),
            allowed_tables=ALLOWED_TABLES,
            table=ALLOWED_TABLES[0],
            operation="remove_orphan_files",
            connection_factory=lambda: connection,
            env=dev_env(),
        )

    assert all(cursor.closed for cursor in cursors)
    assert connection.closed


def test_inventory_cleanup_attempts_every_resource_after_close_failure():
    cursor = InventoryCursor()
    cursor.closed = False

    def fail_close():
        cursor.closed = True
        raise RuntimeError("inventory close failure")

    cursor.close = fail_close
    connection = ClosableConnection([cursor])

    with pytest.raises(MaintenancePlanError, match="resource cleanup failed"):
        collect_maintenance_inventory(
            sample_action_plan(),
            allowed_tables=ALLOWED_TABLES,
            connection_factory=lambda: connection,
        )

    assert cursor.closed and connection.closed
