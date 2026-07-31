# Traffic Legacy Receipt Manifest Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** P0 이전 Traffic Incident pending receipt를 raw 재검증 후 표준 manifest로 복구하여 fail-closed Bronze 처리를 재개한다.

**Architecture:** receipt는 불변으로 둔다. materializer runtime이 manifest 없는 receipt만 `TrafficLanding.replay`로 복구하고, 복구된 batch의 메모리상 결과를 기존 loader에 전달한다.

**Tech Stack:** Python, Airflow, R2 S3-compatible storage, pytest.

## Global Constraints

- 외부 API 호출 금지; 기존 R2 raw만 읽는다.
- raw 및 receipt 기존 객체 삭제·수정 금지.
- manifest 없는 raw를 Bronze loader에 전달하지 않는다.
- Weather·Traffic Flow 및 P1 ops/control 변경 금지.

---

### Task 1: Legacy manifest 복구 경계 추가

**Files:**

- Modify: `domains/traffic/traffic_ingest/runtime.py`
- Modify: `domains/traffic/traffic_ingest/incident_pipeline.py`
- Test: `domains/traffic/tests/test_traffic_incident_materializer.py`

**Interfaces:**

- Consumes: `LandedSnapshot.raw_result`, `TrafficLanding.replay(raw_object_keys, run=...)`
- Produces: manifest가 보강된 실행 전용 raw result

- [ ] **Step 1: Write the failing test**

```python
def test_legacy_receipt_is_replayed_before_bronze_load():
    result = materializer.run(...)
    assert replayed_run == TrafficRun("traffic_incident_bronze", "legacy-run")
    assert loaded_raw_result["manifest_key"] == "raw/.../_manifest.json"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest -q domains/traffic/tests/test_traffic_incident_materializer.py -k legacy`

Expected: FAIL because no legacy recovery dependency exists.

- [ ] **Step 3: Write minimal implementation**

```python
if not raw_result.get("manifest_key"):
    raw_result = recover_legacy_manifest(raw_result, receipt.snapshot_run_id)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest -q domains/traffic/tests/test_traffic_incident_materializer.py -k legacy`

Expected: PASS.

### Task 2: Failure containment regression

**Files:**

- Modify: `domains/traffic/tests/test_traffic_incident_materializer.py`

- [ ] **Step 1: Write the failing test**

```python
def test_legacy_replay_failure_does_not_call_bronze_loader_or_ack_receipt():
    with pytest.raises(ValueError):
        materializer.run(...)
    assert load_calls == []
    assert pending_receipt_still_exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest -q domains/traffic/tests/test_traffic_incident_materializer.py -k legacy_replay_failure`

Expected: FAIL before the containment path is implemented.

- [ ] **Step 3: Write minimal implementation and run the focused test**

Run: `python -m pytest -q domains/traffic/tests/test_traffic_incident_materializer.py -k legacy_replay_failure`

Expected: PASS.

### Task 3: Regression and runtime verification

**Files:**

- Verify: `domains/traffic/tests`

- [ ] **Step 1: Run Traffic test suite**

Run: `python -m pytest -q domains/traffic/tests`

Expected: all selected tests pass.

- [ ] **Step 2: Verify DAG imports**

Run: `python -m py_compile domains/traffic/traffic_ingest/runtime.py domains/traffic/traffic_ingest/incident_pipeline.py`

Expected: exit 0.

- [ ] **Step 3: Commit, push, PR, merge, and deploy only after fresh verification**

Use path-specific staging. Deploy the merged `origin/dev` revision to the dev Airflow environment and inspect the existing pending Traffic receipt retry without triggering a new DAG run.
