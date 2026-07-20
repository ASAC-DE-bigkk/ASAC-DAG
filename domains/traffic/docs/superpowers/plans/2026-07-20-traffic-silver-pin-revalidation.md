# Traffic Silver Pin Revalidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Traffic catch-up 중 정상 supersession이 `dbt_test_silver` 실패와 Discord 장애 알림으로 나타나는 race를 제거한다.

**Architecture:** Traffic manifest의 latest publishable run과 immutable pin을 pool-slot 획득 직후 비교하는 fail-closed guard를 추가한다. `dbt_run_silver`와 `dbt_test_silver`의 기존 selector와 exact dbt test는 유지하고, newer Bronze가 확인된 경우에만 현재 DagRun을 expected supersession으로 skip한다.

**Tech Stack:** Python 3.11, Apache Airflow 3.2.2, pytest, dbt, Trino/Iceberg

## Global Constraints

- 수정·실행 범위는 `domains/traffic/**`만 허용한다.
- dev 환경만 사용하고 secret 또는 `.env`를 읽거나 출력하지 않는다.
- freshness threshold, full correctness, immutable pin, deferred coalescing을 유지한다.
- `.omc`, `.omx`, `.superpowers`, `__pycache__`, `.pytest_cache`를 stage하거나 push하지 않는다.

---

### Task 1: Latest-publishable guard

**Files:**
- Modify: `domains/traffic/traffic_ingest/transform_dag_support.py`
- Test: `domains/traffic/tests/test_traffic_transform_contract.py`

**Interfaces:**
- Consumes: manifest의 `latest_publishable_run_id() -> str`와 immutable `run_id: str`.
- Produces: `require_latest_publishable_incident_snapshot(manifest, run_id: str) -> str`.

- [ ] **Step 1: Write failing helper tests**

```python
class Manifest:
    def __init__(self, latest_run_id):
        self.latest_run_id = latest_run_id

    def latest_publishable_run_id(self):
        return self.latest_run_id


def test_latest_publishable_guard_accepts_current_pin():
    module = load_transform_module()
    assert (
        module.require_latest_publishable_incident_snapshot(
            Manifest("incident-new"), "incident-new"
        )
        == "incident-new"
    )

def test_latest_publishable_guard_skips_superseded_pin():
    module = load_transform_module()
    with pytest.raises(FakeAirflowSkipException, match="superseded"):
        module.require_latest_publishable_incident_snapshot(
            Manifest("incident-new"), "incident-old"
        )

def test_latest_publishable_guard_fails_closed_on_manifest_error():
    module = load_transform_module()

    class BrokenManifest:
        def latest_publishable_run_id(self):
            raise OSError("Trino unavailable")

    with pytest.raises(FakeAirflowFailException, match="verification failed"):
        module.require_latest_publishable_incident_snapshot(
            BrokenManifest(), "incident-old"
        )
```

- [ ] **Step 2: Run tests and verify RED**

Run: `pytest domains/traffic/tests/test_traffic_transform_contract.py -k latest_publishable_guard -q`

Expected: collection or attribute failure because the helper does not exist.

- [ ] **Step 3: Implement the minimal fail-closed helper**

```python
def require_latest_publishable_incident_snapshot(manifest, run_id: str) -> str:
    try:
        latest_run_id = str(manifest.latest_publishable_run_id())
    except Exception as exc:
        raise AirflowFailException(
            f"Traffic latest publishable snapshot verification failed: {run_id}"
        ) from exc
    if latest_run_id != run_id:
        raise AirflowSkipException(
            "superseded Traffic Incident snapshot: "
            f"pinned={run_id}, latest={latest_run_id}"
        )
    return run_id
```

- [ ] **Step 4: Run tests and verify GREEN**

Run: `pytest domains/traffic/tests/test_traffic_transform_contract.py -k latest_publishable_guard -q`

Expected: all selected tests pass.

### Task 2: Pool-scoped dbt pre-execution guard

**Files:**
- Modify: `domains/traffic/traffic_incident_transform.py`
- Test: `domains/traffic/tests/test_traffic_transform_contract.py`

**Interfaces:**
- Consumes: Task 1 helper and existing `transform_runtime.run_dbt_phase(..., pre_execution_guard=...)` seam.
- Produces: snapshot-required Silver phases that revalidate after acquiring `trino_traffic_heavy` and before invoking dbt.

- [ ] **Step 1: Write failing wrapper tests**

```python
def test_snapshot_required_phase_skips_before_dbt_when_pin_is_superseded(monkeypatch):
    module = load_transform_module()
    called = False

    class Manifest:
        def latest_publishable_run_id(self):
            return "incident-new"

    def execute_dbt_phase(**_kwargs):
        nonlocal called
        called = True
        return _successful_runtime_execution()

    monkeypatch.setattr(module, "build_traffic_manifest", Manifest)
    monkeypatch.setattr(
        module.transform_runtime,
        "collect_silver_snapshot_evidence",
        lambda: _silver_evidence(10),
    )
    monkeypatch.setattr(
        module.transform_runtime.traffic_dbt,
        "execute_dbt_phase",
        execute_dbt_phase,
    )

    with pytest.raises(FakeAirflowSkipException, match="superseded"):
        module.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_traffic_transform_incident_silver",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            snapshot_required=True,
            silver_persisted=False,
            silver_fence_mode="write",
            ti=_runtime_ti(),
            run_id="asset_triggered__stale",
            params={"target": "dev"},
        )

    assert called is False

def test_preflight_phase_does_not_query_latest_publishable_manifest(monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(
        module,
        "build_traffic_manifest",
        lambda: pytest.fail("preflight must not query the manifest"),
    )
    monkeypatch.setattr(
        module.transform_runtime.traffic_dbt,
        "execute_dbt_phase",
        lambda **_kwargs: _successful_runtime_execution(),
    )

    result = module.run_dbt_phase(
        dbt_command="source freshness",
        selector="ask_seoul_traffic_transform_source",
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        snapshot_required=False,
        silver_persisted=False,
        ti=_runtime_ti(task_id="dbt_source_freshness"),
        run_id="asset_triggered__preflight",
        params={"target": "dev"},
    )

    assert result["status"] == "success"
```

- [ ] **Step 2: Run tests and verify RED**

Run: `pytest domains/traffic/tests/test_traffic_transform_contract.py -k "superseded_before_dbt or preflight_does_not_query" -q`

Expected: stale phase invokes dbt or the new expectations are absent.

- [ ] **Step 3: Inject the guard only for snapshot-required phases**

```python
pre_execution_guard = None
if snapshot_required:
    ti = context["ti"]
    pinned_run_id = ti.xcom_pull(task_ids=snapshot_task_id)
    pre_execution_guard = lambda: require_latest_publishable_incident_snapshot(
        build_traffic_manifest(), str(pinned_run_id)
    )
```

Pass `pre_execution_guard=pre_execution_guard` to `transform_runtime.run_dbt_phase`.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `pytest domains/traffic/tests/test_traffic_transform_contract.py -k "latest_publishable_guard or superseded_before_dbt or preflight_does_not_query" -q`

Expected: all selected tests pass.

### Task 3: Contract and regression verification

**Files:**
- Verify: `domains/traffic/traffic_incident_transform.py`
- Verify: `domains/traffic/traffic_ingest/transform_dag_support.py`
- Verify: `domains/traffic/tests/test_traffic_transform_contract.py`

**Interfaces:**
- Consumes: Tasks 1-2.
- Produces: reviewable Traffic-only patch with unchanged selectors and DAG topology.

- [ ] **Step 1: Run Traffic transform tests**

Run: `pytest domains/traffic/tests/test_traffic_transform_contract.py domains/traffic/tests/test_traffic_transform_dag.py domains/traffic/tests/test_traffic_transform_failures.py -q`

Expected: all pass.

- [ ] **Step 2: Run Traffic suite and compile check**

Run: `pytest domains/traffic/tests -q`

Run: `python -m compileall -q domains/traffic`

Expected: both commands exit 0.

- [ ] **Step 3: Verify scope and artifacts**

Run: `git diff --check`

Run: `git status --short`

Expected: only intended `domains/traffic/**` files appear; no generated/cache/secret files appear.

- [ ] **Step 4: Review, commit, push, and open dev PR**

Use one focused commit, a UTF-8 PR body, and verify GitHub rendering. Do not create a new issue. Merge only after checks pass and confirm no other-domain files changed.
