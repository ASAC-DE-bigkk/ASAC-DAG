# Weather·Traffic prod 승격 P0 DAG Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 제품 관측의 prod target·event identity와 Weather·Traffic raw manifest partition을 run 단위 계약으로 고정한다.

**Architecture:** `common.runtime_guard`가 target 해석의 단일 진입점이 되고, product observability는 그 target과 deterministic event identity를 사용한다. 각 landing module은 API 호출 전 `landing_load_date` anchor를 고정해 raw key와 manifest에 전달하며 checkpoint가 retry anchor를 보존한다.

**Tech Stack:** Python 3, pytest, Airflow callback context, Cloudflare R2 S3-compatible object store.

## Global Constraints

- `DBT_TARGET`은 authoritative target이며 legacy alias는 불일치만 거부한다.
- 관측 write 실패는 application task를 실패시키지 않되 metric·reconciler warning을 남긴다.
- `collected_at`은 실제 수집 시각이고 partition anchor로 재해석하지 않는다.
- Dashboard, Worker API, dbt Gold SQL, schedule, 실제 prod write는 변경하지 않는다.

---

### Task 1: Runtime target과 제품 이벤트 identity

**Files:**
- Modify: `common/runtime_guard.py`
- Modify: `common/ops/product_observability.py`
- Modify: `common/serving/dag_factory.py`
- Test: `common/tests/test_runtime_guard.py`
- Test: `common/tests/test_product_observability.py`
- Test: `common/serving/tests/test_dag_factory.py`

**Interfaces:**
- Produces: `resolve_runtime_target(env: Mapping[str, str] | None = None) -> str`.
- Produces: product event document field `event_id: str` and key segment `event_id=<sha256>`.

- [ ] **Step 1: Write failing resolver and identity tests**

```python
assert resolve_runtime_target({"DBT_TARGET": "prod"}) == "prod"
with pytest.raises(RuntimeTargetError, match="DBT_TARGET"):
    resolve_runtime_target({})
assert key_a != key_b  # two ProductRecord product_id values
assert retry_key == first_key
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `python -m pytest -q common/tests/test_runtime_guard.py common/tests/test_product_observability.py common/serving/tests/test_dag_factory.py -p no:cacheprovider`

Expected: failure because resolver/event_id and distinct-key behavior do not exist.

- [ ] **Step 3: Add the minimal resolver and deterministic event identity**

```python
identity = {
    "domain": domain, "layer": layer, "product_id": product_id,
    "dag_id": dag_id, "run_id": run_id, "task_id": task_id,
    "try_number": try_number, "publication_id": publication_id,
}
event_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
```

- [ ] **Step 4: Route event and health writes through the resolver**

```python
target = resolve_runtime_target()
_put_r2(key, payload, target=target)
```

On resolution or write failure, increment `product_observability.write_failed` and log the stable failure kind without exposing credentials.

- [ ] **Step 5: Run the focused tests and verify GREEN**

Run: `python -m pytest -q common/tests/test_runtime_guard.py common/tests/test_product_observability.py common/serving/tests/test_dag_factory.py -p no:cacheprovider`

Expected: PASS.

### Task 2: Weather fixed landing partition

**Files:**
- Modify: `domains/weather/weather_ingest/landing.py`
- Test: `domains/weather/tests/test_weather_landing_module.py`

**Interfaces:**
- Produces: `RunIdentity(..., landing_load_date: str | None = None)`.
- Produces: Weather XCom field `landing_load_date`.

- [ ] **Step 1: Write a cross-midnight failing test**

```python
batch = landing.collect(RunIdentity("weather", "run-1"), request)
assert "/load_date=2026-07-30/" in batch.manifest_key
assert all("/load_date=2026-07-30/" in item.raw_object_key for item in batch.raw_objects)
```

Use a clock returning 2026-07-30 23:59:59 KST then 2026-07-31 00:00:01 KST.

- [ ] **Step 2: Verify RED**

Run: `python -m pytest -q domains/weather/tests/test_weather_landing_module.py -p no:cacheprovider`

Expected: raw keys cross partitions while manifest stays in the first partition.

- [ ] **Step 3: Persist and consume the anchor before fetch**

```python
landing_load_date = run.landing_load_date or checkpoint.load_date or kst_date(self._clock())
self._save_checkpoint(run, request, raw_objects, landing_load_date=landing_load_date)
```

Pass the same anchor to raw key and manifest builders; preserve raw `collected_at`.

- [ ] **Step 4: Verify GREEN**

Run: `python -m pytest -q domains/weather/tests/test_weather_landing_module.py -p no:cacheprovider`

Expected: PASS.

### Task 3: Traffic Incident and Flow fixed landing partition

**Files:**
- Modify: `domains/traffic/traffic_ingest/landing_contracts.py`
- Modify: `domains/traffic/traffic_ingest/landing.py`
- Modify: `domains/traffic/traffic_ingest/flow_landing.py`
- Modify: `domains/traffic/traffic_ingest/flow_info.py`
- Test: `domains/traffic/tests/test_traffic_landing_module.py`
- Test: `domains/traffic/tests/test_traffic_flow_info.py`

**Interfaces:**
- Produces: Traffic XCom field `landing_load_date`.
- Produces: Flow `collect(..., landing_load_date: str | None = None)`.

- [ ] **Step 1: Write cross-midnight and cross-partition replay tests**

```python
assert {load_date(key) for key in raw_result["raw_object_keys"]} == {"2026-07-30"}
assert load_date(raw_result["manifest_key"]) == "2026-07-30"
with pytest.raises(TrafficSourceSchemaError, match="single load_date"):
    landing.replay([old_key, new_key], run=run)
```

- [ ] **Step 2: Verify RED**

Run: `python -m pytest -q domains/traffic/tests/test_traffic_landing_module.py domains/traffic/tests/test_traffic_flow_info.py -p no:cacheprovider`

Expected: fixtures expose object and manifest load_date divergence.

- [ ] **Step 3: Add one anchor per Incident/Flow collect run**

```python
raw_object_key = build_raw_object_key(
    collected_at, dag_run_id, link_id, landing_load_date=landing_load_date
)
```

Use a checkpoint anchor for Incident and an explicit argument for Flow; reject replay inputs spanning partitions.

- [ ] **Step 4: Verify GREEN**

Run: `python -m pytest -q domains/traffic/tests/test_traffic_landing_module.py domains/traffic/tests/test_traffic_flow_info.py -p no:cacheprovider`

Expected: PASS.

### Task 4: Integration verification and publish

**Files:**
- Modify: the files from Tasks 1–3 only

- [ ] **Step 1: Run targeted contract suite**

Run: `python -m pytest -q common/tests/test_product_observability.py common/tests/test_runtime_guard.py common/serving/tests/test_dag_factory.py domains/weather/tests/test_weather_landing_module.py domains/traffic/tests/test_traffic_landing_module.py domains/traffic/tests/test_traffic_flow_info.py -p no:cacheprovider`

Expected: PASS.

- [ ] **Step 2: Compile modified domains**

Run: `python -m compileall -q common domains/weather domains/traffic`

Expected: successful exit.

- [ ] **Step 3: Inspect and commit intentional paths**

Run: `git diff --check` followed by path-specific `git add` and a commit referencing #627 and #628.

- [ ] **Step 4: Push and open a draft PR to `dev`**

Run: `git push -u origin feat/prod-release-p0` and create a UTF-8 body draft PR with evidence, change contract, validation, data impact, and rollback.
