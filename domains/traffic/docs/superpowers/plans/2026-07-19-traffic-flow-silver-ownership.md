# Traffic Flow Silver Ownership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** publishable Flow Bronze를 전용 DAG에서 Flow Silver로 materialize한 뒤에만 Traffic full Gold 경로를 실행한다.

**Architecture:** `traffic_flow_transform`은 Flow Bronze와 matching Incident Silver Asset의 AND 조건을 소비하고 Flow Silver model/test 성공 후 `iceberg://traffic/flow/silver` Asset을 발행한다. Gold DAG는 Incident Silver 또는 materialized Flow Silver만 소비하며 기존 incident-only/full selector routing과 pinned-row fail-closed 계약을 유지한다.

**Tech Stack:** Python 3.11/3.12, Apache Airflow 3.2 Assets, dbt-core 1.10, dbt-trino 1.10, Trino Iceberg, pytest

## Global Constraints

- dev의 Weather/Traffic만 실행·수정·검증한다.
- Commerce, Citydata, Transit, Culture 파일을 수정·생성하지 않는다.
- 기존 pinned Flow pre-hook, incremental MERGE, canonical grain, event/ingest lineage와 Gold admission을 완화하지 않는다.
- maintenance, W2, recovery, backfill은 pause 상태를 유지하고 실행하지 않는다.
- `.env`, secret, `.omc`, `.omx`, `__pycache__`, `.pytest_cache`를 읽거나 stage하지 않는다.
- 새 issue를 만들지 않고 #419를 닫지 않는다.
- 두 PR은 `dev` base를 유지하고 금지 도메인 diff 0과 CI 성공 후에만 merge한다.

---

### Task 1: ASAC-DBT Flow Silver 전용 selector와 zero-row gate

**Files:**
- Modify: `ASAC-DBT/domains/traffic_weather/selectors.yml`
- Create: `ASAC-DBT/domains/traffic_weather/tests/traffic/assert_silver_seoul_traffic_flow_pinned_rows.sql`
- Modify: `ASAC-DBT/domains/traffic_weather/tests/traffic/test_gold_trigger_selectors.py`

**Interfaces:**
- Consumes: `var('traffic_flow_snapshot_dag_run_id')`, `silver_seoul_traffic_flow`
- Produces: `ask_seoul_traffic_transform_flow_silver_model`, `ask_seoul_traffic_transform_flow_silver_tests`

- [ ] **Step 1: exact selector와 zero-row gate failing test 작성**

```python
FLOW_SILVER_MODEL = "ask_seoul_traffic_transform_flow_silver_model"
FLOW_SILVER_TESTS = "ask_seoul_traffic_transform_flow_silver_tests"

def test_flow_silver_selectors_resolve_one_model_and_pinned_row_gate(resolved_selector_project):
    assert _resolved_names(resolved_selector_project, FLOW_SILVER_MODEL, "model") == {
        "silver_seoul_traffic_flow"
    }
    tests = _resolved_names(resolved_selector_project, FLOW_SILVER_TESTS, "test")
    assert "assert_silver_seoul_traffic_flow_pinned_rows" in tests
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic_weather/tests/traffic/test_gold_trigger_selectors.py -q`

Expected: 두 selector가 없어 FAIL.

- [ ] **Step 3: model/test selector와 pinned-row singular test 구현**

```sql
{% set flow_run_id = var('traffic_flow_snapshot_dag_run_id', '') or '' %}

select '{{ flow_run_id | replace("'", "''") }}' as missing_flow_run_id
where '{{ flow_run_id | replace("'", "''") }}' = ''
   or not exists (
       select 1
       from {{ ref('silver_seoul_traffic_flow') }}
       where cast(dag_run_id as varchar) = '{{ flow_run_id | replace("'", "''") }}'
   )
```

```yaml
- name: ask_seoul_traffic_transform_flow_silver_model
  definition:
    intersection:
      - {method: fqn, value: silver_seoul_traffic_flow, indirect_selection: empty}
      - {method: resource_type, value: model}

- name: ask_seoul_traffic_transform_flow_silver_tests
  definition:
    intersection:
      - {method: fqn, value: silver_seoul_traffic_flow, children: 1, indirect_selection: empty}
      - {method: resource_type, value: test}
```

- [ ] **Step 4: GREEN과 parse 확인**

Run: `python -m pytest domains/traffic_weather/tests/traffic/test_gold_trigger_selectors.py -q`

Run: `dbt parse --project-dir domains/traffic_weather --target dev --no-partial-parse`

Expected: exact model 1개, pinned-row gate 포함, parse 성공.

- [ ] **Step 5: DBT commit**

```bash
git add domains/traffic_weather/selectors.yml domains/traffic_weather/tests/traffic/assert_silver_seoul_traffic_flow_pinned_rows.sql domains/traffic_weather/tests/traffic/test_gold_trigger_selectors.py
git commit -m "fix(traffic): materialize Flow Silver before Gold"
```

### Task 2: Flow Silver Asset 계약과 exact pair resolver

**Files:**
- Modify: `ASAC-DAG/domains/traffic/traffic_ingest/assets.py`
- Modify: `ASAC-DAG/domains/traffic/traffic_ingest/transform_dag_support.py`
- Modify: `ASAC-DAG/domains/traffic/tests/test_traffic_assets.py`
- Modify: `ASAC-DAG/domains/traffic/tests/test_traffic_transform_contract.py`

**Interfaces:**
- Consumes: `Flow Bronze Asset`, `Incident Silver Asset`, Flow Bronze manifest
- Produces: `TRAFFIC_FLOW_SILVER_ASSET`, `flow_silver_events()`, `resolve_traffic_flow_silver_snapshot_run()`

- [ ] **Step 1: Asset metadata와 matching pair failing tests 작성**

```python
def test_flow_silver_events_require_materialized_contract():
    events = flow_silver_events({"triggering_asset_events": {TRAFFIC_FLOW_SILVER_ASSET: [event]}})
    assert events[0]["flow_dag_run_id"] == "flow-42"

def test_flow_silver_resolver_pins_only_matching_incident_parent(monkeypatch):
    incident = resolve_traffic_flow_silver_snapshot_run(
        context={
            "ti": ti,
            "triggering_asset_events": {
                TRAFFIC_INCIDENT_SILVER_ASSET: [silver_event],
                TRAFFIC_FLOW_BRONZE_ASSET: [flow_event],
            },
        },
        flow_manifest_factory=lambda: FlowManifest(),
        flow_xcom_key=FLOW_SNAPSHOT_XCOM_KEY,
    )
    assert incident == "incident-42"
    assert pushed[FLOW_SNAPSHOT_XCOM_KEY] == "flow-42"
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_assets.py domains/traffic/tests/test_traffic_transform_contract.py -q`

Expected: 새 Asset/helper가 없어 FAIL.

- [ ] **Step 3: exact Asset validator와 resolver 구현**

```python
TRAFFIC_FLOW_SILVER_ASSET = "iceberg://traffic/flow/silver"
FLOW_SILVER_ASSET_CONTRACT = "traffic_flow_silver.v1"

def flow_silver_events(context):
    return _validated_events(
        context,
        asset_uri=TRAFFIC_FLOW_SILVER_ASSET,
        required_fields=FLOW_SILVER_FIELDS,
        source_id="seoul_traffic_flow",
        run_field="flow_dag_run_id",
        duplicate_run_field="flow_run_id",
    )
```

resolver는 최신 Incident Silver event를 선택하고 같은 `parent_incident_run_id`의 최신 Flow Bronze만 고정하며 manifest identity를 재검증한다. pair가 없으면 `AirflowFailException`을 발생시킨다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_assets.py domains/traffic/tests/test_traffic_transform_contract.py -q`

Expected: 정상 pair, stale pair, malformed metadata, manifest mismatch test 모두 PASS.

### Task 3: 전용 `traffic_flow_transform` DAG

**Files:**
- Create: `ASAC-DAG/domains/traffic/traffic_flow_transform.py`
- Modify: `ASAC-DAG/domains/traffic/traffic_ingest/transform_specs.py`
- Modify: `ASAC-DAG/domains/traffic/tests/traffic_transform_test_support.py`
- Create: `ASAC-DAG/domains/traffic/tests/test_traffic_flow_transform_dag.py`
- Modify: `ASAC-DAG/domains/traffic/tests/test_traffic_entrypoint_architecture.py`
- Modify: `ASAC-DAG/domains/traffic/tests/test_traffic_lineage.py`

**Interfaces:**
- Consumes: `FLOW_SILVER_DBT_PHASE_SPECS`, exact Incident/Flow XCom pair
- Produces: validated `TRAFFIC_FLOW_SILVER_ASSET`

- [ ] **Step 1: DAG graph와 publish gate failing test 작성**

```python
def test_flow_transform_owns_only_flow_silver():
    module = load_flow_transform_module()
    assert module.dag.dag_id == "traffic_flow_transform"
    assert {asset.uri for asset in module.dag.kwargs["schedule"].assets} == {
        module.TRAFFIC_FLOW_BRONZE_ASSET,
        module.TRAFFIC_INCIDENT_SILVER_ASSET,
    }
    assert "dbt_run_gold" not in module.dag.task_ids
    assert module.dag.task_dict["dbt_test_flow_silver"].downstream_task_ids == {
        "publish_traffic_flow_silver_asset"
    }
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_flow_transform_dag.py -q`

Expected: entrypoint와 specs가 없어 FAIL.

- [ ] **Step 3: 최소 DAG와 phase specs 구현**

```python
FLOW_SILVER_DBT_PHASE_SPECS = (
    DbtPhaseSpec("dbt_deps_flow_silver", "deps", workload=DbtWorkload.LOCAL, threads=None),
    DbtPhaseSpec("dbt_run_flow_silver", "run", "ask_seoul_traffic_transform_flow_silver_model", fresh_parse=True, snapshot_required=True, pin_critical=True),
    DbtPhaseSpec("dbt_test_flow_silver", "test", "ask_seoul_traffic_transform_flow_silver_tests", silver_persisted=True, snapshot_required=True, pin_critical=True),
)
```

DAG 순서는 `validate -> resolve -> deps -> run -> test -> publish -> metrics`이고, schedule은 Flow Bronze와 Incident Silver Asset의 AND expression이다. publish task는 두 run ID와 `traffic_flow_silver.v1` 계약을 metadata로 발행한다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_flow_transform_dag.py domains/traffic/tests/test_traffic_entrypoint_architecture.py domains/traffic/tests/test_traffic_lineage.py -q`

Expected: graph, pool, failure callback, exact metadata, entrypoint size와 lineage test PASS.

### Task 4: Gold 입력을 materialized Flow Silver로 교체

**Files:**
- Modify: `ASAC-DAG/domains/traffic/traffic_gold_transform.py`
- Modify: `ASAC-DAG/domains/traffic/traffic_ingest/transform_dag_support.py`
- Modify: `ASAC-DAG/domains/traffic/tests/test_traffic_gold_transform_dag.py`
- Modify: `ASAC-DAG/domains/traffic/tests/test_traffic_transform_contract.py`
- Modify: `ASAC-DAG/domains/traffic/README.md`

**Interfaces:**
- Consumes: `flow_silver_events()`
- Preserves: incident-only 17-model routing, full 21-model routing, Flow pinned-row pre-hook

- [ ] **Step 1: Bronze가 Gold를 직접 열지 못하는 failing test 작성**

```python
def test_gold_is_triggered_by_materialized_flow_silver_not_flow_bronze():
    module = load_gold_transform_module()
    assets = {asset.uri for asset in module.dag.kwargs["schedule"].assets}
    assert module.TRAFFIC_FLOW_SILVER_ASSET in assets
    assert module.TRAFFIC_FLOW_BRONZE_ASSET not in assets
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_gold_transform_dag.py domains/traffic/tests/test_traffic_transform_contract.py -q`

Expected: Gold가 아직 Flow Bronze를 schedule하므로 FAIL.

- [ ] **Step 3: Gold schedule/resolver 교체**

Gold schedule을 `Incident Silver OR Flow Silver`로 바꾸고 resolver가 Flow Silver metadata의 parent를 Incident Silver와 비교하도록 한다. 기존 manifest 재검증과 selector routing은 유지한다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_gold_transform_dag.py domains/traffic/tests/test_traffic_transform_contract.py -q`

Expected: incident-only, compatible Flow Silver, stale Flow Silver 세 경로 PASS.

- [ ] **Step 5: DAGS commit**

```bash
git add domains/traffic
git commit -m "fix(traffic): add dedicated Flow Silver transform"
```

### Task 5: 회귀, PR, merge, exact dev 재배포

**Files:**
- No forbidden-domain source changes

**Interfaces:**
- Consumes: DAGS/DBT feature revisions
- Produces: merged `dev` revisions and local exact-revision deployment

- [ ] **Step 1: 전체 정적·단위 회귀**

Run: `python -m pytest domains/traffic/tests -q`

Run: `python -m compileall -q domains/traffic`

Run: `python -m pytest domains/traffic_weather/tests/traffic -q`

Run: `dbt parse --project-dir domains/traffic_weather --target dev --no-partial-parse`

Expected: intentional skip 외 실패 0.

- [ ] **Step 2: scope와 cache 확인**

Run: `git diff --check`

Run: `git diff --name-only origin/dev...HEAD`

Expected: DAGS는 `domains/traffic/**`와 기존 허용 Weather maintenance diff만, DBT는 Traffic monoproject diff만 포함하고 forbidden-domain 파일과 cache가 0개다.

- [ ] **Step 3: 두 branch push와 draft PR 갱신**

Run: `git push origin feat/traffic-gold-trigger-selectors`

Run: `git push origin feat/traffic-compaction-fencing`

Expected: ASAC-DBT #261과 ASAC-DAG #429 head가 새 commit을 가리킨다.

- [ ] **Step 4: feature revision smoke**

Flow Bronze와 matching Incident Silver Asset pair로 `traffic_flow_transform`을 실행한다. Expected: pinned Flow Silver row 수가 0보다 크고 Flow Silver Asset이 발행되며, 이어진 Gold run이 21개 model 경로를 통과한다. maintenance/W2/recovery/backfill은 계속 paused다.

- [ ] **Step 5: CI 성공 후 ready/merge**

Run: `gh pr ready 261 --repo ASAC-DE-bigkk/ASAC-DBT`

Run: `gh pr ready 429 --repo ASAC-DE-bigkk/ASAC-DAG`

Run: `gh pr checks 261 --repo ASAC-DE-bigkk/ASAC-DBT --watch`

Run: `gh pr checks 429 --repo ASAC-DE-bigkk/ASAC-DAG --watch`

Expected: required check 성공 후 두 PR을 `dev`에 merge하며 #419는 OPEN 유지.

- [ ] **Step 6: exact merged revision 재배포**

clean harness에서 DAGS/DBT를 각각 `origin/dev`에 detach한 뒤 다음 명령을 사용한다.

```bash
docker compose -f docker-compose.yml -f docker-compose.traffic-weather-lineage.yml -f .runtime/dev/docker-compose.generated.yml up -d --build
```

Expected: container healthy, DAG import error 0, Weather/Traffic allowlist 유지, 정상 Traffic 수집/Bronze/Silver/Gold만 활성화.
