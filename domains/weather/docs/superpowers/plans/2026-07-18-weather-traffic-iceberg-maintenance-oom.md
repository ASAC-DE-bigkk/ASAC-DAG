# Weather·Traffic Iceberg Maintenance Partial/OOM Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Weather·Traffic dev Iceberg maintenance를 canonical `table × operation` 단위로 완전 직렬화하고, partial failure·query evidence·재개 경계를 Airflow task state로 남긴다.

**Architecture:** `weather_ingest/iceberg_maintenance.py`는 dev-only plan 검증, exact-table preflight, 한 operation 실행, Trino metric과 Iceberg fingerprint 수집을 소유한다. `weather_iceberg_maintenance.py`는 canonical 10개 table에 대해 정적 task chain을 만들고 table-local failure는 다음 table로 격리하되 infrastructure/UNKNOWN은 circuit breaker로 이후 mutation을 중단한다. 실시간 container RSS watcher는 Docker runtime 권한이 필요한 보호된 dev capacity gate로 분리하며 DAG 코드에 Docker socket 의존성을 추가하지 않는다.

**Tech Stack:** Python 3.12, Apache Airflow 3, `trino.dbapi`, Trino 482 Iceberg connector, pytest

## Global Constraints

- 이 plan은 ASAC-DAG #419 전용이다.
- 변경은 `domains/weather/**` 안에서만 수행한다.
- `dbt/**`, Traffic transform Python/dbt 모델, ASAC-DBT PR #254 관련 파일은 수정·실행하지 않는다.
- Commerce, Citydata, Transit, Culture 파일·table은 탐색·실행·수정하지 않는다.
- runtime target은 정확히 `dev`, catalog는 `iceberg_dev`, schema는 `weather_traffic_bronze`다.
- `.env`, secret, API key, token 값을 읽거나 출력하지 않는다.
- maintenance, W2, recovery, backfill, recollect는 이 implementation 단계에서 실행하지 않는다.
- DAG ID `ask_seoul_iceberg_maintenance`, schedule `0 4 * * 0`, `max_active_runs=1`, canonical table 순서와 `optimize → expire_snapshots → remove_orphan_files` 순서를 유지한다.
- table/operation 병렬화와 dynamic task mapping을 금지한다.
- mutation task는 Weather resource adapter의 `TRINO_HEAVY_POOL`을 사용한다. #426 merge 이후
  이 상수는 `trino_weather_heavy`로 해석되며, `pool_slots=1`, `weight_rule="absolute"`,
  `retries=0`을 유지한다.
- Trino 9 GiB container, 약 4.95 GiB heap, query 2 GB/total 4 GB, headroom 2 GB, resource group concurrency 1을 올리거나 제거하지 않는다.
- 자동 retry는 query 제출 전 preflight에만 허용한다. mutation 제출 후 재개는 query terminal state와 plan hash를 확인한 수동 승인 절차다.
- `ALL_DONE` gate는 예외로 circuit을 표현하지 않는다. immutable plan, 이전 gate control, 현재 action XCom과 Airflow TI state를 함께 검사하고 `circuit_open`을 이후 모든 gate에 전달한다.
- mutation 제출 시도 이후 `fetchall`, telemetry, post-fingerprint 예외는 query ID와 terminal state로 확정되지 않는 한 항상 `UNKNOWN`이다.
- DAG 내부 preflight와 trigger 전 external capacity gate를 구분한다. paused/active-run,
  `trino_weather_heavy`·`trino_traffic_heavy`·legacy `trino_heavy`의 idle, 실제 Trino
  running/queued query 0, container health/RSS watcher는 외부 gate 책임이며, DAG 내부는
  dev plan/hash/exact-table inventory만 검증한다.
- 사용자 명시 승인 전 `git commit`, `git push`, PR 생성, DAG unpause를 실행하지 않는다.

---

## File Structure

- Modify: `domains/weather/weather_ingest/iceberg_maintenance.py`
  - dev-only immutable plan, exact table existence probe, operation SQL, action execution/result classification
- Modify: `domains/weather/weather_iceberg_maintenance.py`
  - Airflow Param, preflight, 30개 정적 mutation task, 10개 table gate, final report
- Create: `domains/weather/tests/test_weather_iceberg_maintenance.py`
  - 네트워크 없는 helper policy/action 단위 테스트
- Modify: `domains/weather/tests/test_weather_iceberg_maintenance_dag.py`
  - 정적 DAG topology, pool, retry, callback, pause contract 테스트
- Keep unchanged: `domains/weather/weather_ingest/trino_query_metrics.py`
  - 기존 `TelemetryCursor`, `collect_query_metrics`, `collect_iceberg_fingerprint` 재사용
- Keep unchanged: root `docker-compose.yml`, `trino/**`, `dbt/**`, Traffic transform files

---

### Task 1: Dev-only immutable maintenance plan

**Files:**
- Create: `domains/weather/tests/test_weather_iceberg_maintenance.py`
- Modify: `domains/weather/weather_ingest/iceberg_maintenance.py`

**Interfaces:**
- Consumes: `common.runtime_guard.validate_dev_runtime`, canonical table tuple supplied by the DAG
- Produces: `MaintenancePlan`, `resolve_maintenance_plan()`, `maintenance_plan_payload()`, `exact_table_exists()`

- [ ] **Step 1: Write failing policy tests**

Create `domains/weather/tests/test_weather_iceberg_maintenance.py` with these imports and policy cases:

```python
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
    MaintenancePlanError,
    exact_table_exists,
    resolve_maintenance_plan,
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
        "WEATHER_SCHEMA": APPROVED_DEV_SCHEMA,
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
    with pytest.raises(MaintenancePlanError, match="approved dev schema"):
        resolve_maintenance_plan(
            target="dev",
            retention="7d",
            tables=(ALLOWED_TABLES[0],),
            allowed_tables=ALLOWED_TABLES,
            dag_run_id="manual__schema",
            env=unsafe_env,
        )

    unsafe_weather_env = dev_env()
    unsafe_weather_env["WEATHER_SCHEMA"] = "weather"
    with pytest.raises(MaintenancePlanError, match="approved dev schema"):
        resolve_maintenance_plan(
            target="dev",
            retention="7d",
            tables=(ALLOWED_TABLES[0],),
            allowed_tables=ALLOWED_TABLES,
            dag_run_id="manual__weather_schema",
            env=unsafe_weather_env,
        )

    with pytest.raises(MaintenancePlanError, match="target must be exactly dev"):
        resolve_maintenance_plan(
            target="DEV",
            retention="7d",
            tables=(ALLOWED_TABLES[0],),
            allowed_tables=ALLOWED_TABLES,
            dag_run_id="manual__target_case",
            env=dev_env(),
        )


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
```

Also add RED cases proving the canonical `allowed_tables` input itself is a non-empty ordered
`Sequence[str]`: reject duplicate entries, invalid/non-string entries, empty sequences, and
string/set/frozenset/generator inputs. Prove identical `dag_run_id` and logical selected subset
produce the exact same `plan_hash` regardless of requested-table order.

- [ ] **Step 2: Run the tests and verify the policy API is missing**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_iceberg_maintenance.py -q
```

Expected: collection fails because `APPROVED_DEV_CATALOG`, `MaintenancePlanError`, `exact_table_exists`, and `resolve_maintenance_plan` do not exist.

- [ ] **Step 3: Implement immutable plan resolution**

Replace target/catalog/schema fallback and `_normalize_tables()` policy in `iceberg_maintenance.py` with:

```python
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
import hashlib
import json
import os
import re
from typing import Any

from common.runtime_guard import validate_dev_runtime
from weather_ingest.trino_query_metrics import sql_string


APPROVED_DEV_CATALOG = "iceberg_dev"
APPROVED_DEV_SCHEMA = "weather_traffic_bronze"
FIXED_RETENTION = "7d"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def _validate_canonical_tables(allowed_tables: Sequence[str]) -> tuple[str, ...]:
    if isinstance(allowed_tables, (str, bytes, bytearray)) or not isinstance(
        allowed_tables, Sequence
    ):
        raise MaintenancePlanError(
            "maintenance canonical allowlist must be a deterministic ordered sequence"
        )
    canonical = tuple(allowed_tables)
    if not canonical:
        raise MaintenancePlanError("maintenance canonical allowlist requires at least one table")
    if any(
        not isinstance(table, str) or not _IDENTIFIER.fullmatch(table)
        for table in canonical
    ):
        raise MaintenancePlanError(
            "maintenance canonical allowlist entries must be exact safe string identifiers"
        )
    if len(set(canonical)) != len(canonical):
        raise MaintenancePlanError("maintenance canonical allowlist contains a duplicate")
    return canonical


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
    if any(
        str(values.get(name, "")).strip() != APPROVED_DEV_SCHEMA
        for name in ("ASK_SEOUL_SCHEMA", "WEATHER_SCHEMA")
    ):
        raise MaintenancePlanError("maintenance schema aliases must use the approved dev schema")
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
    hash_input = {
        "plan_id": dag_run_id,
        "target": "dev",
        "catalog": APPROVED_DEV_CATALOG,
        "schema": APPROVED_DEV_SCHEMA,
        "retention": FIXED_RETENTION,
        "retain_last": 1,
        "tables": list(ordered),
    }
    plan_hash = hashlib.sha256(
        json.dumps(hash_input, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
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
```

Replace `_connect_trino()` with a fixed-dev connection factory. It has no target
argument and therefore no prod fallback:

```python
def _connect_trino():
    import trino.dbapi

    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=APPROVED_DEV_CATALOG,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
```

Only `execute_maintenance_action()` and the read-only preflight inventory may call
this factory, and both must first validate an immutable plan.

- [ ] **Step 4: Run policy tests**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_iceberg_maintenance.py -q
```

Expected: all Task 1 tests pass.

- [ ] **Step 5: Review checkpoint**

Run `git diff --check` and inspect only the two Task 1 files. Do not commit without explicit user approval.

---

### Task 2: One-operation executor with evidence and circuit classification

**Files:**
- Modify: `domains/weather/tests/test_weather_iceberg_maintenance.py`
- Modify: `domains/weather/weather_ingest/iceberg_maintenance.py`
- Keep unchanged: `domains/weather/weather_ingest/trino_query_metrics.py`

**Interfaces:**
- Consumes: `MaintenancePlan`, `TelemetryCursor`, `collect_iceberg_fingerprint`
- Produces: `OPERATIONS`, `operation_sql()`, `classify_action_exception()`, `run_maintenance_action()`

**Binding safety amendment:** 아래 예시 코드는 골격일 뿐이며 다음 계약이 우선한다.

- 실행 phase를 `PRE_SUBMIT`, `SUBMITTED`, `ACKNOWLEDGED`, `VERIFIED`로 명시적으로 추적한다.
- `execute()` 호출을 시작한 뒤 발생한 예외는 query ID와 terminal `FAILED` state 및 구조화된
  non-memory user error가 모두 확인된 경우에만 `TABLE_OPERATION`이다. 그 외에는
  `UNKNOWN`/`circuit_breaker=True`다. exception 문자열 substring만으로 table-local을
  판정하지 않는다.
- exception 결과에도 판정에 사용한 `phase`, `query_id`, `query_state`, sanitized
  `structured_error_type`, `structured_error_name`을 보존한다. non-circuit `FAILED`는 이
  evidence가 완전하지 않으면 만들 수 없다.
- 성공에는 query ID, terminal state `FINISHED`, peak user memory가 모두 필요하다.
  Trino procedure output의 실제 `metric_name, metric_value` N행 형식을 dict로
  정규화한다. `optimize` 성공에는 공식 3개 metric, `remove_orphan_files` 성공에는 공식
  5개 metric이 모두 non-null, non-negative integer로 있어야 한다.
- fingerprint는 `$refs`를 포함한다. `expire_snapshots`와 `remove_orphan_files`는 current
  snapshot/ref가 불변이어야 한다. `optimize`는 non-main refs가 불변이어야 하며, 전후
  exact `SELECT count(*)` 결과로 logical row 불변을 검증한다. `$files.record_count` 합계는
  logical row invariant로 사용하지 않는다.
- query peak가 1.4 GiB 이상이면 이미 성공한 action은 `SUCCEEDED_WITH_STOP`으로 기록하고
  circuit을 연다. `BLOCKED_OVERSIZE`는 mutation 제출 전 external gate 차단에만 사용한다.
- orphan 후보 수는 procedure 실행 없이 확정할 수 없으므로 inventory에
  `orphan_candidate_estimate="UNAVAILABLE_WITHOUT_EXECUTION"`을 명시한다.

- [ ] **Step 1: Add failing action tests**

Append tests that use injected cursors and assert exact operation/evidence behavior:

The initial RED set must include the happy path below plus non-`FINISHED` state, each orphan
metric omitted/`None`, missing/non-numeric optimize metric, `$refs` drift, optimize exact-count
drift, post-submit `fetchall`/telemetry/post-fingerprint failure, and confirmed table-local failure
evidence cases. It must also cover missing-table no-mutation, exact selected-table inventory with no
`SHOW TABLES`, invalid/missing query ID and peak, both memory-stop thresholds, changed payload
rejection before connection, direct unsanitized `query_error_type`, missing/invalid structured error
name, and optimize non-main ref drift. Run this complete RED set before Step 3 changes production
code.

```python
from weather_ingest.iceberg_maintenance import (  # noqa: E402
    MaintenancePlan,
    classify_action_exception,
    inspect_maintenance_plan,
    operation_sql,
    run_maintenance_action,
)


def sample_plan():
    return MaintenancePlan(
        plan_id="manual__action",
        plan_hash="a" * 64,
        target="dev",
        catalog="iceberg_dev",
        schema="weather_traffic_bronze",
        retention="7d",
        retain_last=1,
        tables=("bronze_kma_vilage_fcst",),
    )


def test_operation_sql_preserves_canonical_order_contract():
    plan = sample_plan()
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
        stats,
        mutation_rows,
        mutation_description,
        snapshots=(),
        files=(),
        refs=(),
        exact_counts=(),
        exists=True,
    ):
        self.stats = stats
        self.mutation_rows = list(mutation_rows)
        self.mutation_description = list(mutation_description)
        self.snapshots = list(snapshots)
        self.files = list(files)
        self.refs = list(refs)
        self.exact_counts = list(exact_counts)
        self.exists = exists
        self.statements = []
        self.description = None

    def execute(self, statement):
        self.statements.append(statement)
        if statement.startswith("ALTER TABLE"):
            self.description = self.mutation_description

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
            return list(self.mutation_rows)
        raise AssertionError(f"unexpected fetchall: {statement}")


class MetricsCursor:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)

    def fetchall(self):
        return [
            ("query_id", "varchar"),
            ("state", "varchar"),
            ("peak_user_memory_bytes", "bigint"),
        ]

    def fetchone(self):
        return ("q_maintenance", "FINISHED", 1024)


def test_run_action_returns_query_metrics_procedure_output_and_fingerprints(monkeypatch):
    workload = ActionCursor(
        stats={
            "queryId": "q_maintenance",
            "state": "FINISHED",
            "peakMemoryBytes": 1024,
        },
        mutation_rows=[
            ("processed_manifests_count", 3),
            ("active_files_count", 10),
            ("scanned_files_count", 20),
            ("deleted_files_count", 1),
            ("deleted_bytes", 128),
        ],
        mutation_description=[("metric_name",), ("metric_value",)],
        snapshots=[
            (11, "2026-07-18T00:00:00Z"),
            (11, "2026-07-18T00:00:00Z"),
        ],
        files=[
            (4, 8192, 400),
            (4, 8192, 400),
        ],
        refs=[
            [("main", "BRANCH", 11)],
            [("main", "BRANCH", 11)],
        ],
    )

    result = run_maintenance_action(
        sample_plan(),
        allowed_tables=ALLOWED_TABLES,
        table="bronze_kma_vilage_fcst",
        operation="remove_orphan_files",
        workload_cursor=workload,
        metrics_cursor=MetricsCursor(),
        fingerprint_cursor=workload,
    )

    assert result["status"] == "SUCCEEDED"
    assert result["query_id"] == "q_maintenance"
    assert result["metrics"]["state"] == "FINISHED"
    assert result["metrics"]["peak_user_memory_bytes"] == 1024
    assert result["procedure_output"]["deleted_files_count"] == 1
    assert result["procedure_output"]["deleted_bytes"] == 128
    assert set(result["procedure_output"]) >= {
        "processed_manifests_count",
        "active_files_count",
        "scanned_files_count",
        "deleted_files_count",
        "deleted_bytes",
    }


def test_run_action_marks_missing_table_without_mutation():
    workload = ActionCursor(
        stats={},
        mutation_rows=[],
        mutation_description=[],
        exists=False,
    )
    result = run_maintenance_action(
        sample_plan(),
        allowed_tables=ALLOWED_TABLES,
        table="bronze_kma_vilage_fcst",
        operation="optimize",
        workload_cursor=workload,
        metrics_cursor=MetricsCursor(),
        fingerprint_cursor=workload,
    )

    assert result["status"] == "SKIPPED_MISSING"
    assert not any("ALTER TABLE" in statement for statement in workload.statements)


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


def test_preflight_inventory_reads_only_selected_exact_table_metadata():
    cursor = InventoryCursor()

    inventory = inspect_maintenance_plan(
        sample_plan(), cursor, allowed_tables=ALLOWED_TABLES
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
```

- [ ] **Step 2: Run the focused tests and verify the action API is absent**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_iceberg_maintenance.py -q
```

Expected: new action tests fail because action interfaces are not implemented.

- [ ] **Step 3: Implement operation SQL and sanitized classification**

Add these contracts to `iceberg_maintenance.py`:

```python
from weather_ingest.trino_query_metrics import (
    TelemetryCursor,
    collect_iceberg_fingerprint,
    query_id_from_cursor,
    sql_string,
)


OPERATIONS = ("optimize", "expire_snapshots", "remove_orphan_files")
MEMORY_WARNING_BYTES = 1_503_238_554
MEMORY_STOP_BYTES = 1_717_986_918


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


def classify_action_exception(
    exc: Exception,
    *,
    phase: str,
    query_id: str | None,
    query_state: str | None,
    query_error_type: str | None,
) -> dict[str, Any]:
    normalized_error_type = query_error_type or structured_error_type(exc)
    normalized_error_name = structured_error_name(exc)
    confirmed_user_failure = (
        phase == "SUBMITTED"
        and query_id is not None
        and query_state == "FAILED"
        and normalized_error_type == "USER_ERROR"
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


def _procedure_output(cursor: Any, rows: list[tuple[Any, ...]]) -> dict[str, Any]:
    names = [str(column[0]).lower() for column in (cursor.description or [])]
    if names == ["metric_name", "metric_value"]:
        output = {}
        for row in rows:
            if len(row) != 2:
                return {}
            metric_name = str(row[0])
            if not _IDENTIFIER.fullmatch(metric_name) or metric_name in output:
                return {}
            output[metric_name] = row[1]
        return output
    if len(rows) == 1 and len(names) == len(rows[0]):
        return {name: value for name, value in zip(names, rows[0], strict=True)}
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
        isinstance(output[name], int)
        and not isinstance(output[name], bool)
        and output[name] >= 0
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
            and {k: v for k, v in before_refs.items() if k != "main"}
            == {k: v for k, v in after_refs.items() if k != "main"}
        )
    return (
        before["snapshot_id"] == after["snapshot_id"]
        and before_refs == after_refs
        and before["file_count"] == after["file_count"]
        and before["file_bytes"] == after["file_bytes"]
        and before["physical_record_count"] == after["physical_record_count"]
    )


def query_state_from_cursor(cursor: Any) -> str | None:
    stats = getattr(cursor, "stats", None) or {}
    state = stats.get("state") if isinstance(stats, Mapping) else None
    return str(state).upper() if state else None


def structured_error_type(exc: Exception) -> str | None:
    value = getattr(exc, "error_type", None)
    normalized = str(value).upper() if value else None
    return normalized if normalized and _IDENTIFIER.fullmatch(normalized) else None


def structured_error_name(exc: Exception) -> str | None:
    value = getattr(exc, "error_name", None)
    normalized = str(value).upper() if value else None
    return normalized if normalized and _IDENTIFIER.fullmatch(normalized) else None
```

Never include raw exception text, credential, URI, or raw object key in the returned result.

- [ ] **Step 4: Implement the injected one-action executor**

Add `run_maintenance_action()` with this exact result flow:

```python
def run_maintenance_action(
    plan: MaintenancePlan,
    *,
    allowed_tables: Sequence[str],
    table: str,
    operation: str,
    workload_cursor: Any,
    metrics_cursor: Any,
    fingerprint_cursor: Any,
) -> dict[str, Any]:
    _validate_maintenance_plan(plan, allowed_tables=allowed_tables)
    qualified = _qualified_table(plan, table)
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
        output = _procedure_output(workload_cursor, rows)
        metrics = telemetry.records[-1]
        after = collect_maintenance_fingerprint(
            fingerprint_cursor,
            qualified,
            include_exact_row_count=operation == "optimize",
        )

        if (
            metrics.get("query_id") is None
            or metrics.get("state") != "FINISHED"
            or metrics.get("peak_user_memory_bytes") is None
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

        peak = metrics.get("peak_user_memory_bytes")
        phase = "VERIFIED"
        if isinstance(peak, int) and peak >= MEMORY_WARNING_BYTES:
            return {
                **base,
                "status": "SUCCEEDED_WITH_STOP",
                "category": (
                    "MEMORY_STOP" if peak >= MEMORY_STOP_BYTES else "MEMORY_WARNING"
                ),
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
```

Add bounded read-only inventory functions. They query only immutable-plan table
names and never enumerate a catalog or schema:

```python
def _metadata_table_name(plan: MaintenancePlan, table: str, suffix: str) -> str:
    _qualified_table(plan, table)
    return f'{plan.catalog}.{plan.schema}."{table}${suffix}"'


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
    refs = [
        {"name": str(name), "type": str(ref_type), "snapshot_id": snapshot_id}
        for name, ref_type, snapshot_id in cursor.fetchall()
    ]
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
        return inspect_maintenance_plan(
            plan,
            cursor,
            allowed_tables=allowed_tables,
        )
    finally:
        cleanup_failed = _close_all((cursor, connection))
        if cleanup_failed and sys.exc_info()[0] is None:
            raise MaintenancePlanError("maintenance resource cleanup failed")
```

Add payload reconstruction and the connection-owning wrapper with these exact
interfaces:

```python
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
    cursors = []
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
            allowed_tables=allowed_tables,
            table=table,
            operation=operation,
            workload_cursor=workload_cursor,
            metrics_cursor=metrics_cursor,
            fingerprint_cursor=fingerprint_cursor,
        )
    finally:
        cleanup_failed = _close_all((*reversed(cursors), connection))
        if cleanup_failed and sys.exc_info()[0] is None:
            raise MaintenancePlanError("maintenance resource cleanup failed")
```

The optional `inventory` field produced by preflight is evidence only. It is not
part of the mutation parameters; all required plan fields are rebuilt and compared
before `connection_factory()` is called.

The 1.4 GiB warning is a post-query stop gate; it prevents the next mutation while preserving that the completed action succeeded. Live 7 GiB container RSS cancellation, paused state, active-run count, all three heavy pools idle, actual Trino running/queued query count zero, health/restart/OOM checks, and pre-submit `BLOCKED_OVERSIZE` remain the external protected-canary gate responsibility.

- [ ] **Step 5: Run helper and existing telemetry tests**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_iceberg_maintenance.py domains/weather/tests/test_trino_query_metrics.py -q
```

Expected: all tests pass, with missing metric values remaining `None`, never zero.

- [ ] **Step 6: Review checkpoint**

Run `git diff --check`. Confirm `SHOW TABLES`, prod catalog fallback, raw exception message logging, and automatic mutation retry are absent. Do not commit without explicit user approval.

---

### Task 3: Static Airflow checkpoint chain

**Files:**
- Modify: `domains/weather/tests/test_weather_iceberg_maintenance_dag.py`
- Modify: `domains/weather/weather_iceberg_maintenance.py`

**Interfaces:**
- Consumes: Task 1 plan payload, Task 2 action result, `TRINO_HEAVY_POOL`
- Produces: `_preflight()`, `_run_action()`, `_table_gate()`, `_final_report()`, static task IDs

**Binding circuit amendment:**

- `_table_gate()` never raises to encode an open circuit. It always returns a control record with
  `circuit_open`, `status`, `source_table`, and sanitized reason.
- Each gate receives `previous_gate_task_id`, reads the immutable plan, previous gate control,
  action XComs, and action TI states from `dag_run.get_task_instance(task_id)`.
- A table absent from `plan_payload["tables"]` alone is `NOT_SELECTED`. A selected table with no
  action evidence is not `NOT_SELECTED`; it is `UNKNOWN` with an open circuit.
- 이전 gate circuit을 plan selection보다 먼저 검사한다. 열린 circuit 사이에 non-selected
  table이 있어도 그 gate는 `NOT_SELECTED`로 덮지 않고 동일 circuit을 전파한다.
- An action task in `failed`, `upstream_failed`, or an unexpected terminal state without an
  earlier observed non-circuit `FAILED` result opens the circuit. Missing downstream results are
  tolerated only after a confirmed table-local failure in the same table.
- The first action of each table receives the previous gate task ID. If its control has
  `circuit_open=true`, it raises `AirflowSkipException` before opening Trino. The table gate then
  propagates the same circuit, so later `ALL_DONE` gates cannot revive mutations.
- `_final_report()` fails for any selected table gate with `TABLE_FAILED`, `CIRCUIT_OPEN`, missing
  control, or incomplete status. Non-selected tables do not count as success or failure.

- [ ] **Step 1: Upgrade Airflow fakes and write failing topology tests**

Extend the fake operator with dependency state:

```python
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
```

Install fakes for `airflow.models.param.Param`,
`airflow.utils.trigger_rule.TriggerRule`, and
`airflow.exceptions.AirflowSkipException`:

```python
class FakeParam:
    def __init__(self, default, **schema):
        self.default = default
        self.schema = schema


class FakeTriggerRule:
    ALL_DONE = "all_done"


class FakeAirflowSkipException(Exception):
    pass


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
```

Add all these module names to `_FAKE_MODULE_NAMES` so the autouse fixture restores
the real import state after every test.

Replace the old single-`maintain` assertions with:

```python
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

    previous = "preflight"
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


def test_mutation_tasks_keep_weather_failure_callback():
    module = load_maintenance_module()

    action_tasks = [
        task
        for task_id, task in module.dag.task_dict.items()
        if task_id.startswith("maint_")
    ]
    assert action_tasks
    assert all(
        task.kwargs["on_failure_callback"] is module.record_weather_problem
        for task in action_tasks
    )
```

- [ ] **Step 2: Run DAG tests and verify the old single-task design fails**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_iceberg_maintenance_dag.py -q
```

Expected: tests fail because static task IDs, Param objects, pool wiring, and gates do not exist.

- [ ] **Step 3: Define canonical params and safe task IDs**

In `weather_iceberg_maintenance.py`:

```python
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

from common.runtime_guard import validate_dev_runtime  # noqa: E402
from weather_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402
from weather_ingest.iceberg_maintenance import (  # noqa: E402
    OPERATIONS,
    collect_maintenance_inventory,
    execute_maintenance_action,
    maintenance_plan_payload,
    resolve_maintenance_plan,
)


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
DEFAULT_PARAM_VALUES = {
    "target": "dev",
    "retention": "7d",
    "tables": CANONICAL_TABLES,
}
DEFAULT_PARAMS = {
    "target": Param("dev", enum=["dev"]),
    "retention": Param("7d", enum=["7d"]),
    "tables": Param(
        list(CANONICAL_TABLES),
        type="array",
        minItems=1,
        uniqueItems=True,
        items={"type": "string", "enum": list(CANONICAL_TABLES)},
    ),
}


def action_task_id(index: int, table: str, operation: str) -> str:
    return f"maint_{index:02d}_{table}__{operation}"


def gate_task_id(index: int, table: str) -> str:
    return f"gate_{index:02d}_{table}"
```

- [ ] **Step 4: Implement Airflow callables**

Implement the Airflow callables with the following exact bodies:

```python
RESULT_XCOM_KEY = "maintenance_result"
PLAN_TASK_ID = "preflight"


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


def _run_action(
    *, table, operation, previous_task_id, previous_gate_task_id=None, **context
):
    ti = context["ti"]
    plan_payload = ti.xcom_pull(task_ids=PLAN_TASK_ID)
    if previous_gate_task_id is not None:
        previous_gate = ti.xcom_pull(task_ids=previous_gate_task_id)
        if not isinstance(previous_gate, dict) or previous_gate.get("circuit_open"):
            raise AirflowSkipException("maintenance circuit is open")

    if table not in plan_payload["tables"]:
        raise AirflowSkipException("table is not selected by the immutable plan")

    if previous_task_id != PLAN_TASK_ID:
        previous = _pull_result(ti, previous_task_id)
        if previous and previous.get("status") == "SKIPPED_MISSING":
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


def _task_state(dag_run, task_id):
    task_instance = dag_run.get_task_instance(task_id)
    state = getattr(task_instance, "state", None)
    return str(getattr(state, "value", state)).lower() if state is not None else None


def _open_circuit(*, table, reason, source_table=None):
    return {
        "table": table,
        "status": "CIRCUIT_OPEN",
        "circuit_open": True,
        "source_table": source_table or table,
        "reason": reason,
    }


def _table_gate(
    *, table, action_task_ids, previous_gate_task_id=None, **context
):
    ti = context["ti"]
    dag_run = context["dag_run"]
    plan_payload = ti.xcom_pull(task_ids=PLAN_TASK_ID)
    if previous_gate_task_id is not None:
        previous_gate = ti.xcom_pull(task_ids=previous_gate_task_id)
        if not isinstance(previous_gate, dict):
            return _open_circuit(table=table, reason="MISSING_PREVIOUS_GATE_CONTROL")
        if previous_gate.get("circuit_open"):
            return _open_circuit(
                table=table,
                reason=str(previous_gate.get("reason", "UPSTREAM_CIRCUIT")),
                source_table=str(previous_gate.get("source_table", table)),
            )

    if table not in plan_payload["tables"]:
        return {"table": table, "status": "NOT_SELECTED", "circuit_open": False}

    results = [_pull_result(ti, task_id) for task_id in action_task_ids]
    observed = [result for result in results if isinstance(result, dict)]
    for result in observed:
        if result.get("circuit_breaker"):
            return _open_circuit(
                table=table,
                reason=str(result.get("category", "ACTION_CIRCUIT")),
            )

    table_failure_index = next(
        (
            index
            for index, result in enumerate(results)
            if isinstance(result, dict)
            and result.get("status") == "FAILED"
            and not result.get("circuit_breaker")
        ),
        None,
    )
    for index, (task_id, result) in enumerate(zip(action_task_ids, results, strict=True)):
        state = _task_state(dag_run, task_id)
        if isinstance(result, dict):
            continue
        expected_after_table_failure = (
            table_failure_index is not None
            and index > table_failure_index
            and state in {"skipped", "upstream_failed"}
        )
        if not expected_after_table_failure:
            return _open_circuit(table=table, reason="UNRECORDED_TASK_FAILURE")

    if table_failure_index is not None:
        return {"table": table, "status": "TABLE_FAILED", "circuit_open": False}
    if all(result.get("status") == "SKIPPED_MISSING" for result in observed):
        return {"table": table, "status": "SKIPPED_MISSING", "circuit_open": False}
    if len(observed) != len(action_task_ids) or any(
        result.get("status") != "SUCCEEDED" for result in observed
    ):
        return _open_circuit(table=table, reason="INCOMPLETE_ACTION_EVIDENCE")
    return {"table": table, "status": "SUCCEEDED", "circuit_open": False}


def _final_report(**context):
    ti = context["ti"]
    plan_payload = ti.xcom_pull(task_ids=PLAN_TASK_ID)
    failures = []
    for index, table in enumerate(CANONICAL_TABLES, start=1):
        gate = ti.xcom_pull(task_ids=gate_task_id(index, table))
        if table not in plan_payload["tables"]:
            continue
        if gate is None or gate.get("status") not in {"SUCCEEDED", "SKIPPED_MISSING"}:
            failures.append(table)
    if failures:
        raise RuntimeError(
            "Iceberg maintenance did not complete safely: " + ", ".join(failures)
        )
    return {"status": "SUCCEEDED"}
```

`execute_maintenance_action()` must reconstruct `MaintenancePlan` from the payload and verify the SHA-256 hash before opening Trino. Hash mismatch fails before connection.

- [ ] **Step 5: Build the static DAG**

Replace the single `maintain` task with:

```python
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
                    "previous_gate_task_id": gate_task_ids[-1] if gate_task_ids else None,
                },
                pool=TRINO_HEAVY_POOL,
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
```

Do not add TaskGroups or mapped tasks. Keep `enable_lineage_if_configured(dag)` unchanged; OpenLineage timeout handling remains outside #419.

- [ ] **Step 6: Run DAG tests**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_iceberg_maintenance_dag.py -q
```

Expected: canonical 30 mutation tasks, 10 gates, preflight, final report are present and completely serial.

- [ ] **Step 7: Review checkpoint**

Inspect the rendered dependency assertions. Confirm no task imports or calls dbt, Traffic transform, W2, recovery, backfill, recollect, or another domain. Do not commit without explicit user approval.

---

### Task 4: Partial-failure, ACK-loss, and final-report contracts

**Files:**
- Modify: `domains/weather/tests/test_weather_iceberg_maintenance_dag.py`
- Modify: `domains/weather/tests/test_weather_iceberg_maintenance.py`
- Modify only if tests expose a defect: `domains/weather/weather_iceberg_maintenance.py`
- Modify only if tests expose a defect: `domains/weather/weather_ingest/iceberg_maintenance.py`

**Interfaces:**
- Consumes: Task 2 action result schema and Task 3 XCom key/task IDs
- Produces: regression proof for table-local continuation and global circuit breaker

The tests in this task must exercise scheduler state, not XCom values alone. Fake `dag_run`
objects expose task states for every action and gate.

- [ ] **Step 1: Add callable-level fake TaskInstance tests**

Use a fake XCom store:

```python
class FakeTaskInstance:
    def __init__(self, values=None):
        self.values = dict(values or {})
        self.current_task_id = None

    def xcom_pull(self, task_ids, key="return_value"):
        return self.values.get((task_ids, key))

    def xcom_push(self, key, value):
        self.values[(self.current_task_id, key)] = value


class FakeDagRun:
    def __init__(self, states):
        self.states = dict(states)

    def get_task_instance(self, task_id):
        return types.SimpleNamespace(state=self.states.get(task_id))


def test_table_gate_allows_next_table_after_terminal_table_failure():
    module = load_maintenance_module()
    task_ids = ["optimize", "expire", "orphan"]
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): {"tables": ["bronze_kma_vilage_fcst"]},
            ("optimize", module.RESULT_XCOM_KEY): {
                "status": "FAILED",
                "circuit_breaker": False,
            }
        }
    )

    result = module._table_gate(
        table="bronze_kma_vilage_fcst",
        action_task_ids=task_ids,
        previous_gate_task_id=None,
        ti=ti,
        dag_run=FakeDagRun(
            {"optimize": "failed", "expire": "upstream_failed", "orphan": "upstream_failed"}
        ),
    )

    assert result == {
        "table": "bronze_kma_vilage_fcst",
        "status": "TABLE_FAILED",
        "circuit_open": False,
    }


def test_table_gate_opens_circuit_for_unknown_query_state():
    module = load_maintenance_module()
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): {"tables": ["bronze_collection_run_manifest"]},
            ("orphan", module.RESULT_XCOM_KEY): {
                "status": "UNKNOWN",
                "circuit_breaker": True,
            }
        }
    )

    result = module._table_gate(
        table="bronze_collection_run_manifest",
        action_task_ids=["optimize", "expire", "orphan"],
        previous_gate_task_id=None,
        ti=ti,
        dag_run=FakeDagRun({"orphan": "failed"}),
    )
    assert result["status"] == "CIRCUIT_OPEN"
    assert result["circuit_open"] is True


def test_missing_table_short_circuits_later_operations_without_query(monkeypatch):
    module = load_maintenance_module()
    plan = {"plan_id": "run", "plan_hash": "a" * 64, "tables": ["missing"]}
    ti = FakeTaskInstance(
        {
            (module.PLAN_TASK_ID, "return_value"): plan,
            ("optimize", module.RESULT_XCOM_KEY): {
                "status": "SKIPPED_MISSING",
                "circuit_breaker": False,
            },
        }
    )
    ti.current_task_id = "expire"
    called = False

    def fail_if_called(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(module, "execute_maintenance_action", fail_if_called)

    result = module._run_action(
        table="missing",
        operation="expire_snapshots",
        previous_task_id="optimize",
        ti=ti,
    )

    assert result["status"] == "SKIPPED_MISSING"
    assert called is False
```

- [ ] **Step 2: Add hash mismatch and metric-unavailable tests**

Append these two cases:

```python
from weather_ingest.iceberg_maintenance import (  # noqa: E402
    execute_maintenance_action,
    maintenance_plan_payload,
)


def test_changed_plan_payload_fails_before_connection():
    plan = resolve_maintenance_plan(
        target="dev",
        retention="7d",
        tables=(ALLOWED_TABLES[0],),
        allowed_tables=ALLOWED_TABLES,
        dag_run_id="manual__hash",
        env=dev_env(),
    )
    payload = maintenance_plan_payload(plan)
    payload["retention"] = "8d"
    called = False

    def connection_factory():
        nonlocal called
        called = True
        raise AssertionError("connection must not be opened")

    with pytest.raises(MaintenancePlanError):
        execute_maintenance_action(
            payload,
            allowed_tables=ALLOWED_TABLES,
            table=ALLOWED_TABLES[0],
            operation="optimize",
            connection_factory=connection_factory,
            env=dev_env(),
        )
    assert called is False


def test_missing_required_query_metrics_opens_circuit():
    workload = ActionCursor(
        stats={},
        mutation_rows=[
            ("rewritten_data_files_count", 1),
            ("removed_delete_files_count", 1),
            ("added_data_files_count", 1),
        ],
        mutation_description=[("metric_name",), ("metric_value",)],
        snapshots=[
            (11, "2026-07-18T00:00:00Z"),
            (12, "2026-07-18T00:01:00Z"),
        ],
        files=[(4, 8192, 400), (1, 8192, 400)],
        refs=[
            [("main", "BRANCH", 11)],
            [("main", "BRANCH", 12)],
        ],
        exact_counts=[400, 400],
    )

    result = run_maintenance_action(
        sample_plan(),
        table="bronze_kma_vilage_fcst",
        operation="optimize",
        workload_cursor=workload,
        metrics_cursor=MetricsCursor(),
        fingerprint_cursor=workload,
        allowed_tables=CANONICAL_TABLES,
    )

    assert result["status"] == "UNKNOWN"
    assert result["category"] == "REQUIRED_TELEMETRY_UNAVAILABLE"
    assert result["circuit_breaker"] is True
    assert result["query_id"] is None
    assert result["metrics"]["peak_user_memory_bytes"] is None
```

Also implement `test_hard_kill_without_xcom_circuit_propagates_across_later_tables`.
The fake scheduler sequence is mandatory: selected T1 has a `failed` action TI and no
result XCom; T1 gate returns `CIRCUIT_OPEN/UNRECORDED_TASK_FAILURE`; T2 first action raises
`AirflowSkipException` before the injected executor is called; T2 and T3 gates receive all
skipped action TIs and copy T1's circuit control; final report raises. Assert no executor call
occurred for T2 or T3.

Also implement `test_non_selected_gate_does_not_clear_open_circuit`: T1 selected gate control is
`CIRCUIT_OPEN`, T2 is absent from the immutable plan, and T3 is selected. Assert T2 gate propagates
T1's `source_table/reason` instead of returning `NOT_SELECTED`, T3 first action skips before the
executor, and T3 gate remains `CIRCUIT_OPEN`.

Add a parametrized `test_post_submit_evidence_failure_is_unknown` for `fetchall`, telemetry,
and post-fingerprint failures. Each fake has already entered `SUBMITTED`; unless it exposes a
query ID, terminal `FAILED` state, and structured `USER_ERROR`, assert `status="UNKNOWN"`,
`circuit_breaker=True`, and absence of raw exception text. Add the positive inverse case for a
confirmed terminal non-memory `USER_ERROR`, which alone may return table-local `FAILED`.

Before implementation is considered complete, add these focused failure tests:

- every non-`FINISHED` mutation query state is `UNKNOWN`/circuit even when query ID and peak exist;
- each required `remove_orphan_files` metric is independently omitted once and set to `None` once;
  all ten cases are `UNKNOWN`/circuit;
- missing or non-numeric required `optimize` metrics are `UNKNOWN`/circuit;
- `$refs` drift for expire/orphan and non-main ref drift for optimize are global invariant failures;
- optimize exact row count drift is a global invariant failure;
- confirmed table-local failure preserves `phase="SUBMITTED"`, query ID, `FAILED` state,
  `USER_ERROR`, and sanitized error name in its result; removing any one item converts it to
  `UNKNOWN`/circuit;
- raw exception messages, URIs, SQL, and object keys never appear in any result.

- [ ] **Step 3: Run all maintenance and telemetry tests**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_iceberg_maintenance.py domains/weather/tests/test_weather_iceberg_maintenance_dag.py domains/weather/tests/test_trino_query_metrics.py -q
```

Expected: all tests pass without network or Airflow service access.

- [ ] **Step 4: Compile only #419 Python files**

Run:

```powershell
python -m py_compile domains/weather/weather_iceberg_maintenance.py domains/weather/weather_ingest/iceberg_maintenance.py
```

Expected: exit code 0.

- [ ] **Step 5: Review checkpoint**

Run `git diff --check` and `git status --short`. Confirm only approved Weather files and docs changed. Do not commit without explicit user approval.

---

### Task 5: Read-only regression gates and protected dev handoff

**Files:**
- Modify only with reusable findings after an approved run: `LessonRun.md`
- No code changes in this task

**Interfaces:**
- Consumes: passing unit tests, exact branch revision, approved design document
- Produces: implementation review evidence and a separate canary approval request

- [ ] **Step 1: Run the complete in-scope test set**

Run from ASAC-DAG worktree:

```powershell
python -m pytest domains/weather/tests/test_weather_iceberg_maintenance.py domains/weather/tests/test_weather_iceberg_maintenance_dag.py domains/weather/tests/test_trino_query_metrics.py domains/weather/tests/test_weather_domain_boundary.py domains/weather/tests/test_airflowignore_weather_traffic_allowlist.py -q
```

Expected: all selected Weather/Traffic boundary and maintenance tests pass. Do not run dbt tests or Traffic transform tests for #419.

- [ ] **Step 2: Recheck protected root OOM invariants read-only**

From a clean root checkout whose submodules point to the intended refs, run:

```powershell
python -m pytest scripts/tests/test_trino_runtime_hardening.py -q
```

Expected: Trino 482, 9 GiB, 55% heap, 2 GB/4 GB query limits, 2 GB headroom,
global concurrency 1, `trino_traffic_heavy=1`, `trino_weather_heavy=1`, legacy
`trino_heavy=1` bootstrap이 유지된다. #419 mutation task의 resolved pool은 정확히
`trino_weather_heavy`여야 한다.

- [ ] **Step 3: Run Sol final implementation review before any maintenance**

The reviewer must inspect:

- canonical ordering and absence of parallel paths
- mutation `retries=0`
- exact dev catalog/schema guard before connection
- plan hash validation
- table-local versus circuit-breaker classification
- preservation of query ID, peak memory, procedure output, fingerprints
- propagation of a hard-kill/XCom-loss circuit through at least two later `ALL_DONE` gates
- terminal state `FINISHED`, orphan 5-metric completeness, `$refs`, and optimize exact row invariant
- no `dbt/**` or Traffic transform changes

Expected verdict: no unresolved high-severity ordering, idempotency, OOM, or scope regression.

- [ ] **Step 4: Stop at the protected dev canary approval boundary**

Do not execute maintenance in this implementation plan. Present:

- exact branch revision and diff
- passing test output
- trigger 전 DAG paused와 active run count 0, trigger 뒤 current run 외 active run 0을
  외부 read-only gate로 확인한 evidence
- current table inventory and selected canary candidate
- external 2-second container RSS/JVM watcher command and 7.0 GiB abort contract
- query peak 1.4 GiB stop gate and missing-metric behavior

Request separate approval for a one-table dev canary. Canary success does not authorize full maintenance or unpause.

- [ ] **Step 5: Commit/PR boundary**

Only after explicit user approval, prepare a commit and PR to `dev` using the shared templates. PR evidence must include issue #419, changed files, unit-test results, DAG paused state, no maintenance run, no dbt/Traffic transform changes, and the remaining canary gate. Do not push or create the PR without a separate explicit instruction.
