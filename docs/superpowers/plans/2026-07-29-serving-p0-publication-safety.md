# Serving P0 Publication Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** D1 snapshot publication을 last-known-good 보존·ledger·동적 byte-budget batch로 안전하고 빠르게 만들고, Traffic export를 검증 완료 Gold asset 뒤에만 실행한다.

**Architecture:** `HttpD1Client`는 SQL statement와 API batch를 byte-budget으로 구성하고 snapshot 후보본/이전본 전환을 제공한다. `publisher`는 제품별 publication unit에서 검증·catalog·smoke 실패를 보상하고 ledger를 기록한다. Traffic Gold의 마지막 성공 task가 terminal asset을 emit하고 export DAG가 이를 구독한다.

**Tech Stack:** Python 3.11, Airflow 3 Asset, Cloudflare D1 HTTP API, pytest, shell guard.

## Global Constraints

- dev D1만 사용하고 secret은 출력·문서화·커밋하지 않는다.
- `.airflowignore`, 사용자 개인 기록 md, 다른 도메인 코드는 수정하지 않는다.
- root는 dirty이므로 `scripts/safe-trigger-dag.sh` 한 파일만 명시적으로 수정하며 일괄 stage하지 않는다.
- 커밋·push·PR은 사용자 별도 승인 전까지 하지 않는다.

---

### Task 1: Byte-budget SQL statement와 제한 HTTP batch

**Files:**
- Modify: `common/serving/d1_client.py`
- Test: `common/serving/tests/test_d1_client.py`

**Interfaces:**
- Produces: `build_insert_statements(name, columns, rows, replace) -> list[str]`, each UTF-8 statement <= `80_000` bytes.
- Produces: `HttpD1Client._query_batch(statements) -> list[D1QueryMetric]`, max 4 statements and 256,000 request bytes.

- [ ] **Step 1: Write failing packing tests**

```python
def test_insert_statements_pack_small_rows_up_to_utf8_budget():
    statements = build_insert_statements("risk", [("text", "varchar")], [{"text": "가" * 1000}] * 100)
    assert all(len(sql.encode("utf-8")) <= 80_000 for sql in statements)
    assert sum(sql.count("(") - 1 for sql in statements) == 100

def test_oversized_single_row_fails_before_http():
    with pytest.raises(ValueError, match="80_000"):
        build_insert_statements("risk", [("text", "varchar")], [{"text": "가" * 30_000}])
```

- [ ] **Step 2: Run the focused test and observe the expected missing-symbol failure**

Run: `pytest common/serving/tests/test_d1_client.py -q`

- [ ] **Step 3: Implement pure UTF-8 statement packing**

```python
MAX_SQL_STATEMENT_BYTES = 80_000

def build_insert_statements(name, columns, rows, *, replace=False):
    # Render each literal once, flush before a candidate exceeds the budget.
    ...
```

- [ ] **Step 4: Add HTTP batch shape test and implement `_query_batch`**

```python
assert request_body["batch"] == [{"sql": first}, {"sql": second}]
assert len(request_body["batch"]) <= 4
assert len(json.dumps(request_body).encode("utf-8")) <= 256_000
```

- [ ] **Step 5: Run focused D1-client tests**

Run: `pytest common/serving/tests/test_d1_client.py -q`

### Task 2: Snapshot compensation and immutable publication ledger

**Files:**
- Modify: `common/serving/d1_client.py`
- Modify: `common/serving/publisher.py`
- Modify: `common/serving/dag_factory.py`
- Test: `common/serving/tests/test_d1_client.py`
- Test: `common/serving/tests/test_publisher.py`

**Interfaces:**
- Produces: `D1Client.restore_snapshot(name) -> None`, `D1Client.finalize_snapshot(name) -> None`.
- Produces: `D1Client.append_publication_ledger(record) -> None`.
- Produces: `ProductRecord.stage`, `rollback_status`, `sql_statement_count`, `http_batch_count`, `max_statement_bytes`.

- [ ] **Step 1: Write failing publisher tests for smoke/catalog compensation**

```python
with pytest.raises(PublicationError):
    publish([contract], source, d1_with_old_snapshot, FakeSmoke(status="failed"), source_run_id="r")
assert d1.table_rows(contract.model_name) == old_rows
assert d1.catalog_row(contract.model_name) == old_catalog
assert d1.ledger[-1]["outcome"] == "failed"
assert d1.ledger[-1]["rollback_status"] == "restored"
```

- [ ] **Step 2: Run focused publisher tests and observe expected failure**

Run: `pytest common/serving/tests/test_publisher.py -q`

- [ ] **Step 3: Implement candidate/previous lifecycle and product-local catalog commit**

```python
# snapshot: stage -> promote retaining __previous -> read-back -> smoke -> catalog
# any post-promote error: restore __previous, restore previous catalog row, ledger failed
# success: drop __previous, append immutable ledger success
```

- [ ] **Step 4: Add ledger schema/write tests, then implement D1 ledger insert**

```python
assert "INSERT INTO _publication_ledger" in query
assert row["publication_id"] == record.publication_id
assert row["outcome"] in {"published", "skipped_retained", "failed"}
```

- [ ] **Step 5: Extend factory XCom with publication stage, recovery, smoke, and batch metrics**

```python
assert xcom_record["stage"] == "completed"
assert xcom_record["http_batch_count"] >= 1
```

- [ ] **Step 6: Run all common serving tests**

Run: `pytest common/serving/tests -q`

### Task 3: Traffic Gold terminal asset and manual admission guard

**Files:**
- Modify: `domains/traffic/traffic_ingest/assets.py`
- Modify: `domains/traffic/traffic_gold_transform.py`
- Modify: `domains/traffic/traffic_serving_export.py`
- Modify: `domains/traffic/tests/test_traffic_transform_dag.py`
- Modify: `domains/traffic/tests/test_traffic_serving_export.py`
- Modify: `scripts/safe-trigger-dag.sh` (root harness)
- Test: `scripts/tests/test_safe_trigger_dag.py` or existing shell guard test location

**Interfaces:**
- Produces: `TRAFFIC_GOLD_PUBLICATION_READY_ASSET` and its `Asset` reference.
- Produces: `traffic_serving_export` scheduled by `schedule_asset(TRAFFIC_GOLD_PUBLICATION_READY_ASSET)`.

- [ ] **Step 1: Write failing DAG contract tests**

```python
assert module.TRAFFIC_GOLD_PUBLICATION_READY_ASSET in {
    asset.uri for asset in module.dag.task_dict["mark_traffic_gold_success"].outlets
}
assert serving_dag.kwargs["schedule"].uri == assets.TRAFFIC_GOLD_PUBLICATION_READY_ASSET
```

- [ ] **Step 2: Run focused Traffic DAG tests and observe expected failure**

Run: `pytest domains/traffic/tests/test_traffic_transform_dag.py domains/traffic/tests/test_traffic_serving_export.py -q`

- [ ] **Step 3: Implement asset constant, success-task outlet metadata, and export subscription**

```python
# Only mark_traffic_gold_success has the outlet; its upstream chain includes dbt_test_gold.
mark_success = PythonOperator(..., outlets=[TRAFFIC_GOLD_PUBLICATION_READY_ASSET_REF])
```

- [ ] **Step 4: Add `traffic_incident_landing` to the root traffic family and test it**

```bash
traffic_family=(
  traffic_incident_landing
  traffic_incident_bronze
  ...
)
```

- [ ] **Step 5: Run focused Traffic tests and shell guard test**

Run: `pytest domains/traffic/tests/test_traffic_transform_dag.py domains/traffic/tests/test_traffic_serving_export.py -q`

### Task 4: Compile, isolated dev deployment, and evidence

**Files:**
- Modify: none unless a failing verification proves a scoped defect.

- [ ] **Step 1: Run Python compilation and all touched unit suites**

Run: `python -m py_compile common/serving/*.py domains/traffic/traffic_gold_transform.py domains/traffic/traffic_serving_export.py && pytest common/serving/tests domains/traffic/tests/test_traffic_transform_dag.py domains/traffic/tests/test_traffic_serving_export.py -q`

- [ ] **Step 2: Deploy only the verified feature revision to the dev harness**

Run: use the repository's explicit-ref dev harness; do not use `scripts/deploy.sh` and do not update root submodule pointers.

- [ ] **Step 3: Verify a real dev D1 Traffic risk-window publication**

Run: after `scripts/safe-trigger-dag.sh traffic_serving_export` permits it, inspect the DAG run, `_catalog`, `_publication_ledger`, source/D1 row count, and XCom batch metrics.

- [ ] **Step 4: Verify a successful Gold terminal asset triggers exactly one export run**

Run: observe an existing scheduled/asset Gold cycle; do not manually trigger external-source DAGs directly.

- [ ] **Step 5: Report evidence and request explicit commit approval**

Report: branch SHA, changed files, test output, DAG run IDs, row counts, ledger/catalog evidence, and any remaining Gateway limitation.
