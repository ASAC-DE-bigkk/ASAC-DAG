# Traffic Hot Path SLO Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Traffic의 매 asset cycle에서 전체 계약 반복 scan을 제거하고 여섯 D1 제품을 15분 안에 안전하게 발행하며, 전체 계약은 09:00 신뢰성 감사에서 보존한다.

**Architecture:** Incident/Flow/Gold는 pinned input을 O(1) admission으로 확인한 뒤 모델과 compound publication contract를 단일 `dbt build`로 실행한다. 전체 Bronze/Silver/Gold 계약은 별도 Traffic daily-assurance selector로 이동하고, 실패를 리포트 RED로 전달하되 last-known-good publication은 보존한다.

**Tech Stack:** Airflow 3 asset scheduling, Python 3, dbt Core, Trino/Iceberg, pytest, GitHub CLI, Docker Compose

## Global Constraints

- ASAC-DAG 변경은 `domains/traffic/**`로 제한한다.
- ASAC-DBT 변경은 `domains/traffic_weather/**`의 Traffic selector와 Traffic test로 제한한다.
- Weather 등 다른 도메인 DAG/model/test를 수정하거나 실행하지 않는다.
- `gold_traffic_incident_x_weather_current_hourly`는 Traffic 소유 D1 제품이므로 기존 Weather Gold를 읽기만 한다.
- root `.airflowignore`, 개인 `AGENTS.md`, `LessonRun.md`, `engineering-decision-log.md`는 수정·stage·commit하지 않는다.
- 수동 DAG trigger는 반드시 `scripts/safe-trigger-dag.sh`를 사용한다.
- prod가 아닌 dev target/schema만 사용한다.
- 동일 pin 성공 receipt의 replay는 heavy query 없이 skip하고, 실패 receipt는 성공으로 간주하지 않는다.

---

### Task 1: DBT hot selector 계약

**Files:**
- Modify: `domains/traffic_weather/selectors.yml`
- Create: `domains/traffic_weather/tests/traffic/hot_path/assert_traffic_incident_silver_publication_receipt.sql`
- Create: `domains/traffic_weather/tests/traffic/hot_path/assert_traffic_flow_silver_publication_receipt.sql`
- Create: `domains/traffic_weather/tests/traffic/hot_path/assert_traffic_gold_serving_publication_receipt.sql`
- Modify: `domains/traffic_weather/tests/traffic/test_gold_trigger_selectors.py`
- Create: `domains/traffic_weather/tests/traffic/test_hot_path_selectors.py`

**Interfaces:**
- Produces: selector `ask_seoul_traffic_transform_incident_hot_build`
- Produces: selector `ask_seoul_traffic_transform_flow_hot_build`
- Produces: selectors `ask_seoul_traffic_transform_gold_hot_build` and `ask_seoul_traffic_transform_gold_incident_hot_build`
- Produces: selector `ask_seoul_traffic_daily_assurance`

- [ ] **Step 1: Write failing selector tests**

```python
def test_incident_hot_selector_contains_only_two_models_and_one_receipt():
    nodes = dbt_ls("ask_seoul_traffic_transform_incident_hot_build")
    assert model_names(nodes) == {
        "silver_seoul_traffic_incident",
        "silver_seoul_traffic_incident_current",
    }
    assert test_names(nodes) == {
        "assert_traffic_incident_silver_publication_receipt"
    }


def test_gold_hot_selectors_match_six_d1_products():
    with_flow = model_names(
        dbt_ls("ask_seoul_traffic_transform_gold_hot_build")
    )
    incident_only = model_names(
        dbt_ls("ask_seoul_traffic_transform_gold_incident_hot_build")
    )
    assert with_flow == {
        "gold_traffic_incident_x_weather_current_hourly",
        "gold_traffic_flow_congestion_hotspots_hourly",
        "gold_traffic_flow_link_latest",
        "gold_traffic_flow_change_latest",
        "gold_traffic_flow_link_time_profile",
        "gold_traffic_flow_anomaly_current",
    }
    assert incident_only == {
        "gold_traffic_incident_x_weather_current_hourly"
    }
```

- [ ] **Step 2: Run selector tests and verify RED**

Run:

```powershell
python -m pytest domains/traffic_weather/tests/traffic/test_hot_path_selectors.py -q
```

Expected: FAIL because the four hot selectors and three receipt tests do not exist.

- [ ] **Step 3: Add compound receipt SQL**

Incident receipt must return a violation row when any of these is true:

```sql
-- configured manifest is not SUCCESS/publishable
-- current rows use a dag_run_id different from traffic_snapshot_dag_run_id
-- expected pinned Bronze ids MINUS current Silver ids is non-empty
-- current Silver ids MINUS expected pinned Bronze ids is non-empty
-- source_record_id/event_at/source_id is null
-- source_record_id is duplicated
```

Flow receipt applies the same pattern to
`(link_id, traffic_flow_snapshot_dag_run_id)` and validates
`observed_at/source_id`. Gold receipt unions six compact checks, each requiring
non-null and unique `product_row_id`, except
`gold_traffic_incident_active_latest` is not selected and therefore needs no
special key branch.

- [ ] **Step 4: Add exact named selectors**

Use `union` with explicit `fqn` entries and `resource_type` intersection.
Set `indirect_selection: empty` so parents and generic children are not
accidentally selected. The incident-only Gold selector contains the Weather
context Traffic leaf and Gold receipt; the receipt SQL skips Flow branches
when `traffic_flow_snapshot_dag_run_id` is empty.

`ask_seoul_traffic_daily_assurance` is a union of:

```yaml
- {method: selector, value: ask_seoul_traffic_transform_incident_preflight_contracts}
- {method: selector, value: ask_seoul_traffic_transform_silver}
- {method: selector, value: ask_seoul_traffic_transform_flow_silver_tests}
- {method: selector, value: ask_seoul_traffic_transform_common_admin}
- {method: selector, value: ask_seoul_traffic_transform_asac_axes_contract}
- {method: selector, value: ask_seoul_traffic_transform_gold_full_tests_without_commerce}
```

- [ ] **Step 5: Run selector tests and exact node listing**

Run:

```powershell
python -m pytest domains/traffic_weather/tests/traffic/test_hot_path_selectors.py domains/traffic_weather/tests/traffic/test_gold_trigger_selectors.py -q
dbt parse --project-dir domains/traffic_weather --profiles-dir domains/traffic_weather
dbt ls --project-dir domains/traffic_weather --profiles-dir domains/traffic_weather --selector ask_seoul_traffic_transform_incident_hot_build --resource-type model test
dbt ls --project-dir domains/traffic_weather --profiles-dir domains/traffic_weather --selector ask_seoul_traffic_transform_gold_hot_build --resource-type model test
```

Expected: tests PASS, parse exit 0, node listing matches the asserted exact sets.

- [ ] **Step 6: Commit DBT selector and receipts**

```powershell
git add -- domains/traffic_weather/selectors.yml domains/traffic_weather/tests/traffic/hot_path domains/traffic_weather/tests/traffic/test_hot_path_selectors.py domains/traffic_weather/tests/traffic/test_gold_trigger_selectors.py
git commit -m "perf(traffic): publication receipt 기반 hot selector 추가"
```

---

### Task 2: Incident/Flow transform 단일 build graph

**Files:**
- Modify: `domains/traffic/traffic_ingest/transform_specs.py`
- Modify: `domains/traffic/traffic_incident_transform.py`
- Modify: `domains/traffic/traffic_flow_transform.py`
- Modify: `domains/traffic/tests/test_traffic_transform_dag.py`
- Modify: `domains/traffic/tests/test_traffic_flow_transform_dag.py`
- Modify: `domains/traffic/tests/test_traffic_transform_contract.py`

**Interfaces:**
- Consumes: Task 1 incident/flow hot selectors
- Produces: `SILVER_DBT_PHASE_SPECS=(deps, dbt_build_incident_silver)`
- Produces: `FLOW_SILVER_DBT_PHASE_SPECS=(deps, dbt_build_flow_silver)`

- [ ] **Step 1: Write failing DAG graph tests**

```python
def test_incident_hot_graph_has_no_full_preflight_scan():
    assert tuple(spec.task_id for spec in SILVER_DBT_PHASE_SPECS) == (
        "dbt_deps",
        "dbt_build_incident_silver",
    )
    build = SILVER_DBT_PHASE_SPECS[1]
    assert build.dbt_command == "build"
    assert build.selector == "ask_seoul_traffic_transform_incident_hot_build"
    assert build.snapshot_required is True
    assert build.pin_critical is True


def test_flow_hot_graph_uses_one_pinned_build():
    assert tuple(spec.task_id for spec in FLOW_SILVER_DBT_PHASE_SPECS) == (
        "dbt_deps_flow_silver",
        "dbt_build_flow_silver",
    )
    assert FLOW_SILVER_DBT_PHASE_SPECS[1].selector == (
        "ask_seoul_traffic_transform_flow_hot_build"
    )
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```powershell
python -m pytest domains/traffic/tests/test_traffic_transform_dag.py domains/traffic/tests/test_traffic_flow_transform_dag.py -q
```

Expected: FAIL because the legacy source freshness/preflight and split Flow run/test tasks remain.

- [ ] **Step 3: Replace phase specs with one pinned build**

Incident build keeps:

```python
DbtPhaseSpec(
    "dbt_build_incident_silver",
    "build",
    "ask_seoul_traffic_transform_incident_hot_build",
    silver_persisted=True,
    fresh_parse=True,
    snapshot_required=True,
    pin_critical=True,
    silver_fence_mode="write",
)
```

Flow build keeps `snapshot_required=True`, `pin_critical=True`,
`silver_persisted=True`, and uses `dbt_command="build"`.

- [ ] **Step 4: Preserve pin and publication ordering**

The graph remains:

```text
validate_runtime -> deps -> resolve/admit/latest pin
  -> single build -> verify/publish asset -> metrics teardown
```

Do not move resolver after build. Do not publish an asset when dbt build or
write-evidence validation fails.

- [ ] **Step 5: Run focused and Traffic transform regressions**

Run:

```powershell
python -m pytest domains/traffic/tests/test_traffic_transform_dag.py domains/traffic/tests/test_traffic_flow_transform_dag.py domains/traffic/tests/test_traffic_transform_contract.py domains/traffic/tests/test_traffic_transform_failures.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit transform graph**

```powershell
git add -- domains/traffic/traffic_ingest/transform_specs.py domains/traffic/traffic_incident_transform.py domains/traffic/traffic_flow_transform.py domains/traffic/tests/test_traffic_transform_dag.py domains/traffic/tests/test_traffic_flow_transform_dag.py domains/traffic/tests/test_traffic_transform_contract.py
git commit -m "perf(traffic): Silver 변환을 pinned 단일 build로 축소"
```

---

### Task 3: Gold 여섯 제품 hot build

**Files:**
- Modify: `domains/traffic/traffic_ingest/transform_specs.py`
- Modify: `domains/traffic/traffic_gold_transform.py`
- Modify: `domains/traffic/tests/test_traffic_gold_transform_dag.py`
- Modify: `domains/traffic/tests/test_traffic_transform_contract.py`

**Interfaces:**
- Consumes: Task 1 Gold hot selectors
- Produces: `GOLD_DBT_PHASE_SPECS=(deps, dbt_build_gold_hot)`
- Preserves: incident-only vs Flow-present selector choice

- [ ] **Step 1: Write failing Gold graph tests**

```python
def test_gold_hot_graph_has_no_per_cycle_seed_or_full_test_tasks():
    assert tuple(spec.task_id for spec in GOLD_DBT_PHASE_SPECS) == (
        "dbt_deps_gold",
        "dbt_build_gold_hot",
    )
    build = GOLD_DBT_PHASE_SPECS[1]
    assert build.dbt_command == "build"
    assert build.selector == "ask_seoul_traffic_transform_gold_hot_build"
    assert build.selector_when_flow_missing == (
        "ask_seoul_traffic_transform_gold_incident_hot_build"
    )
    assert build.snapshot_required is True
    assert build.pin_critical is True
```

- [ ] **Step 2: Run focused Gold tests and verify RED**

Run:

```powershell
python -m pytest domains/traffic/tests/test_traffic_gold_transform_dag.py -q
```

Expected: FAIL because seed/common axis/run/test phases remain.

- [ ] **Step 3: Replace Gold phases with one build**

Use:

```python
DbtPhaseSpec(
    "dbt_build_gold_hot",
    "build",
    "ask_seoul_traffic_transform_gold_hot_build",
    selector_when_flow_missing=(
        "ask_seoul_traffic_transform_gold_incident_hot_build"
    ),
    silver_persisted=True,
    fresh_parse=True,
    snapshot_required=True,
    pin_critical=True,
    admin_dong_crosswalk_pin_required=True,
)
```

Citydata pin is no longer required because no selected D1 model reads Citydata.
The existing Silver evidence and incident manifest pre-execution guard remain.

- [ ] **Step 4: Remove hot-path cadence task without removing daily audit**

Remove `select_traffic_test_tier` from `traffic_gold_transform` graph. Keep
success marker write last. Do not advance the old cadence marker from a hot
run; Task 4 owns full-contract execution.

- [ ] **Step 5: Run Gold and admission regressions**

Run:

```powershell
python -m pytest domains/traffic/tests/test_traffic_gold_transform_dag.py domains/traffic/tests/test_traffic_transform_contract.py domains/traffic/tests/test_traffic_transform_failures.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit Gold graph**

```powershell
git add -- domains/traffic/traffic_ingest/transform_specs.py domains/traffic/traffic_gold_transform.py domains/traffic/tests/test_traffic_gold_transform_dag.py domains/traffic/tests/test_traffic_transform_contract.py
git commit -m "perf(traffic): Gold를 여섯 D1 제품 hot build로 제한"
```

---

### Task 4: 09:00 full-contract assurance

**Files:**
- Create: `domains/traffic/traffic_ingest/daily_contract_audit.py`
- Modify: `domains/traffic/traffic_reliability_report.py`
- Modify: `domains/traffic/traffic_ingest/reliability_report.py`
- Modify: `domains/traffic/tests/test_traffic_reliability_dag.py`
- Create: `domains/traffic/tests/test_traffic_daily_contract_audit.py`

**Interfaces:**
- Consumes: DBT selector `ask_seoul_traffic_daily_assurance`
- Produces: `run_daily_contract_audit(...) -> dict[str, object]`
- Produces XCom: task `audit_traffic_dbt_contracts`
- Report field: `contract_audit={status, selector, elapsed_seconds, failure}`

- [ ] **Step 1: Write failing audit result tests**

```python
def test_daily_audit_returns_red_result_instead_of_hiding_dbt_failure():
    result = run_daily_contract_audit(
        runner=lambda *_args, **_kwargs: CompletedProcess([], 1, "", "boom"),
        clock=iter([10.0, 13.5]).__next__,
    )
    assert result == {
        "status": "FAIL",
        "selector": "ask_seoul_traffic_daily_assurance",
        "elapsed_seconds": 3.5,
        "failure": "dbt_exit_1",
    }


def test_report_is_red_when_contract_audit_is_red():
    result = merge_contract_audit(pass_report(), {"status": "FAIL"})
    assert result["status"] == "FAIL"
    assert result["contract_audit"]["status"] == "FAIL"
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```powershell
python -m pytest domains/traffic/tests/test_traffic_daily_contract_audit.py domains/traffic/tests/test_traffic_reliability_dag.py -q
```

Expected: FAIL because the audit module/task/report field does not exist.

- [ ] **Step 3: Implement fail-visible audit wrapper**

Run one command:

```text
dbt test --selector ask_seoul_traffic_daily_assurance
```

Return PASS on exit 0. On nonzero exit, timeout, missing artifact or exception,
return a structured FAIL result with exception class only; do not include
credentials, full environment or secret-bearing command arguments.

- [ ] **Step 4: Add the fourth serial reliability task**

The graph becomes:

```text
audit_traffic_dbt_contracts -> collect_traffic_data_plane
  -> compose_traffic_pipeline_reliability -> deliver_traffic_pipeline_reliability
```

The audit task uses `trino_traffic_heavy` with one slot. Compose pulls audit and
data-plane XCom, merges the audit status, and deliver still runs because the
wrapper returns RED instead of raising for a data-contract failure. Airflow
infrastructure failure still invokes `record_traffic_problem`.

- [ ] **Step 5: Run reliability regressions**

Run:

```powershell
python -m pytest domains/traffic/tests/test_traffic_daily_contract_audit.py domains/traffic/tests/test_traffic_reliability_dag.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit daily assurance**

```powershell
git add -- domains/traffic/traffic_ingest/daily_contract_audit.py domains/traffic/traffic_reliability_report.py domains/traffic/traffic_ingest/reliability_report.py domains/traffic/tests/test_traffic_daily_contract_audit.py domains/traffic/tests/test_traffic_reliability_dag.py
git commit -m "feat(traffic): 전체 계약을 09시 신뢰성 감사로 분리"
```

---

### Task 5: 전체 Traffic 검증과 PR

**Files:**
- Verify only; no other-domain source changes
- PR body files outside repository or untracked temporary location

- [ ] **Step 1: Run DBT Traffic tests and parse**

```powershell
python -m pytest domains/traffic_weather/tests/traffic -q
dbt parse --project-dir domains/traffic_weather --profiles-dir domains/traffic_weather
```

Expected: PASS and parse exit 0.

- [ ] **Step 2: Run DAGS Traffic suite**

```powershell
python -m compileall -q domains/traffic
python -m pytest domains/traffic/tests -q
```

Expected: PASS.

- [ ] **Step 3: Verify scope**

```powershell
git diff --name-only origin/dev...HEAD
```

Expected: only allowed Traffic paths and the DAGS design/plan documents.
`git diff --check` returns exit 0. `.airflowignore` is absent.

- [ ] **Step 4: Push and create DBT PR**

Push `perf/traffic-hot-path-slo`, create UTF-8 Korean PR body with purpose,
measured bottleneck, selector node sets, tests and rollback. Base is `dev`.
Verify title/body with `gh pr view`.

- [ ] **Step 5: Push and create DAGS PR**

Push the DAGS branch and create a `dev` PR that references the DBT PR/revision.
Verify title/body encoding and ensure no other-domain diff.

- [ ] **Step 6: Merge DBT then DAGS**

Merge only after required checks pass. Record both merge commit SHAs. Do not
create a root pointer PR.

---

### Task 6: dev 재배포와 실제 Traffic asset 1 cycle

**Files:**
- Runtime-only: existing local dev Compose override
- No tracked root file changes

- [ ] **Step 1: Update clean deploy worktrees**

```powershell
git -C C:\Users\Dell3571\Desktop\Projects\ask-seoul-worktrees\dbt-serving-p0-publication-safety fetch origin dev
git -C C:\Users\Dell3571\Desktop\Projects\ask-seoul-worktrees\dbt-serving-p0-publication-safety checkout --detach origin/dev
git -C C:\Users\Dell3571\Desktop\Projects\ask-seoul-worktrees\dags-serving-p0-publication-safety fetch origin dev
git -C C:\Users\Dell3571\Desktop\Projects\ask-seoul-worktrees\dags-serving-p0-publication-safety checkout --detach origin/dev
```

Verify each HEAD equals the recorded PR merge commit.

- [ ] **Step 2: Rebuild the dev harness**

Use the existing serving-P0 override:

```powershell
docker compose -f docker-compose.yml -f docker-compose.serving-p0.override.yml up -d --build
docker compose -f docker-compose.yml -f docker-compose.serving-p0.override.yml ps
```

Expected: Airflow scheduler/API/worker and Trino containers are healthy.

- [ ] **Step 3: Verify Traffic DAG imports and task graphs**

List only Traffic DAGs and inspect:

- `traffic_incident_transform`
- `traffic_flow_transform`
- `traffic_gold_transform`
- `traffic_serving_export`
- `traffic_bronze_reliability_report`

Expected: no Traffic import error; task IDs match the new hot graphs.

- [ ] **Step 4: Obtain a real Traffic asset cycle**

Prefer the next scheduler-created Traffic asset event. If a manual collection
is necessary, first run:

```powershell
bash scripts/safe-trigger-dag.sh traffic_incident_landing --check-only
bash scripts/safe-trigger-dag.sh traffic_incident_landing
```

Never directly trigger a transform DAG without its input asset event.

- [ ] **Step 5: Verify one complete cycle**

Capture run ids and durations for:

```text
Traffic Landing
Incident/Flow Bronze
Incident/Flow Transform
Traffic core Gold
Traffic D1 export
```

Expected: every required task succeeds, Gold success marker precedes export,
six D1 products pass gate/write/row-count/`_catalog`, and the previous serving
snapshot remains available until replacement succeeds.

- [ ] **Step 6: Report the result**

Report exact merge SHAs, deployment revisions, Airflow run IDs, task durations,
row counts and SLO comparison. A single cycle can prove deployment correctness
and the observed 15-minute budget, but not p95; p95 remains a daily report
measurement after enough steady-state samples.
