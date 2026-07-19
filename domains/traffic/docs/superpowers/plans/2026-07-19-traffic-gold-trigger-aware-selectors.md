# Traffic Gold Trigger-Aware Selectors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 각 Traffic Gold DagRun이 실제로 pin한 Flow 입력에 맞는 named dbt selector를 사용해 incident-only와 flow-present 경로를 모두 성공시킨다.

**Architecture:** dbt selector가 node membership을 소유하고 Airflow는 resolver XCom의 Flow run ID 존재 여부만 selector routing에 사용한다. Flow가 있으면 기존 21개 Gold 계약을 그대로 실행하고, 없으면 Flow 전용 4개 모델과 그 test만 제외한다.

**Tech Stack:** Python 3.11/3.12, Apache Airflow 3.2, dbt-core 1.10, dbt-trino 1.10, pytest, Trino Iceberg

## Global Constraints

- dev의 Weather/Traffic만 실행·수정·검증한다.
- Commerce, Citydata, Transit, Culture 파일은 수정·생성하지 않는다. 기존 Traffic Gold source read만 허용한다.
- Flow pinned-row pre-hook, canonical grain, incremental MERGE, event/ingest lineage, idempotency를 낮추지 않는다.
- maintenance, W2, recovery, backfill은 pause 상태를 유지하고 실행하지 않는다.
- `.env`, secret, token, password를 읽거나 출력하지 않는다.
- `.omc`, `.omx`, `__pycache__`를 stage·commit·push하지 않는다.
- 새 issue를 만들지 않고 #419를 이 PR로 닫지 않는다.
- PR base는 `dev`이며 금지 도메인 diff가 0일 때만 merge한다.

---

### Task 1: ASAC-DBT incident-only named selector 계약

**Files:**
- Modify: `domains/traffic_weather/selectors.yml`
- Create: `domains/traffic_weather/tests/traffic/test_gold_trigger_selectors.py`

**Interfaces:**
- Produces: `ask_seoul_traffic_transform_flow_gold_scope`
- Produces: `ask_seoul_traffic_transform_gold_incident_models`
- Produces: incident `gate`, `hourly`, `full` test selectors

- [ ] **Step 1: selector 구조 failing test 작성**

```python
from pathlib import Path
import yaml


SELECTORS = Path(__file__).resolve().parents[2] / "selectors.yml"


def test_incident_gold_selectors_reuse_full_contract_and_exclude_flow_scope():
    values = yaml.safe_load(SELECTORS.read_text(encoding="utf-8"))["selectors"]
    selectors = {item["name"]: item["definition"] for item in values}
    expected = {
        "ask_seoul_traffic_transform_flow_gold_scope",
        "ask_seoul_traffic_transform_gold_incident_models",
        "ask_seoul_traffic_transform_gold_incident_gate_tests",
        "ask_seoul_traffic_transform_gold_incident_hourly_tests",
        "ask_seoul_traffic_transform_gold_incident_full_tests",
    }
    assert expected <= selectors.keys()
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic_weather/tests/traffic/test_gold_trigger_selectors.py -q`

Expected: incident selector 이름이 없어 FAIL.

- [ ] **Step 3: 기존 selector 상속과 exclude 구현**

```yaml
- name: ask_seoul_traffic_transform_flow_gold_scope
  definition:
    union:
      - {method: fqn, value: gold_traffic_flow_link_latest, children: true}
      - {method: fqn, value: gold_traffic_flow_change_latest, children: true}
      - {method: fqn, value: gold_traffic_flow_congestion_hotspots_hourly, children: true}
      - {method: fqn, value: gold_traffic_flow_link_time_profile, children: true}

- name: ask_seoul_traffic_transform_gold_incident_models
  definition:
    intersection:
      - {method: selector, value: ask_seoul_traffic_transform_gold_models}
      - exclude:
          - {method: selector, value: ask_seoul_traffic_transform_flow_gold_scope}
```

세 test selector도 기존 tier selector와 `flow_gold_scope`의 set difference로 정의한다.

- [ ] **Step 4: GREEN 및 exact node set 확인**

Run: `python -m pytest domains/traffic_weather/tests/traffic/test_gold_trigger_selectors.py -q`

Run: `dbt parse --project-dir domains/traffic_weather --target dev`

Run: `dbt ls --project-dir domains/traffic_weather --selector ask_seoul_traffic_transform_gold_incident_models --resource-type model`

Expected: parse 성공, incident model 17개, 제외된 model은 정확히 Flow 전용 4개.

- [ ] **Step 5: DBT commit**

```bash
git add domains/traffic_weather/selectors.yml domains/traffic_weather/tests/traffic/test_gold_trigger_selectors.py
git commit -m "fix(traffic): route incident-only Gold selectors"
```

### Task 2: ASAC-DAG Flow-aware selector routing

**Files:**
- Modify: `domains/traffic/traffic_ingest/transform_specs.py`
- Modify: `domains/traffic/traffic_ingest/transform_dag_support.py`
- Modify: `domains/traffic/traffic_ingest/transform_runtime.py`
- Modify: `domains/traffic/traffic_gold_transform.py`
- Modify: `domains/traffic/tests/test_traffic_transform_contract.py`

**Interfaces:**
- Adds: `DbtPhaseSpec.selector_when_flow_missing`
- Adds: `DbtPhaseSpec.selector_by_test_tier_when_flow_missing`
- Preserves: existing selector when compatible Flow XCom is non-empty

- [ ] **Step 1: no-flow와 flow-present failing tests 작성**

```python
def test_gold_run_uses_incident_selector_when_flow_snapshot_is_missing(monkeypatch):
    captured = execute_phase_with_flow(monkeypatch, flow_run_id=None)
    assert captured["selector"] == "ask_seoul_traffic_transform_gold_incident_models"


def test_gold_run_keeps_full_selector_when_flow_snapshot_is_present(monkeypatch):
    captured = execute_phase_with_flow(monkeypatch, flow_run_id="flow-42")
    assert captured["selector"] == "ask_seoul_traffic_transform_gold"
```

GATE/HOURLY/FULL test tier도 Flow 유무별 selector를 각각 검증한다.

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_contract.py -q`

Expected: alternate selector 인자가 없거나 기존 full selector가 전달되어 FAIL.

- [ ] **Step 3: 최소 routing 구현**

```python
if not flow_run_id and selector_when_flow_missing is not None:
    selector = selector_when_flow_missing
    if selector_by_test_tier_when_flow_missing is not None:
        selector_by_test_tier = selector_by_test_tier_when_flow_missing
```

`GOLD_DBT_PHASE_SPECS`의 `dbt_run_gold`와 `dbt_test_gold`에만 alternate selector를 설정한다.
Silver, seed, common dimension phase는 기본값으로 기존 동작을 유지한다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_contract.py domains/traffic/tests/test_traffic_gold_transform_dag.py -q`

Expected: 전체 PASS.

- [ ] **Step 5: DAGS commit**

```bash
git add domains/traffic/traffic_ingest/transform_specs.py domains/traffic/traffic_ingest/transform_dag_support.py domains/traffic/traffic_ingest/transform_runtime.py domains/traffic/traffic_gold_transform.py domains/traffic/tests/test_traffic_transform_contract.py
git commit -m "fix(traffic): route Gold by pinned Flow input"
```

### Task 3: 회귀 및 feature revision smoke

**Files:**
- No production file additions

- [ ] **Step 1: DAGS 전체 Traffic 회귀**

Run: `python -m pytest domains/traffic/tests -q`

Expected: intentional skip 외 실패 0.

- [ ] **Step 2: Weather maintenance 회귀**

Run: `python -m pytest domains/weather/tests/test_weather_iceberg_maintenance.py domains/weather/tests/test_weather_iceberg_maintenance_dag.py -q`

Expected: 실패 0.

- [ ] **Step 3: compile과 scope 확인**

Run: `python -m compileall -q domains/traffic domains/weather/weather_iceberg_maintenance.py`

Run: `git diff --check && git diff --name-only origin/dev...HEAD`

Expected: compile 성공, 금지 도메인 파일 0, tracked cache 0.

- [ ] **Step 4: runtime mount를 두 feature commit으로 맞추고 Airflow reserialize**

Expected: Weather/Traffic DAG 18개, import error 0, maintenance/W2/recovery/backfill paused.

- [ ] **Step 5: incident-only Gold checkpoint**

동일 Silver Asset event run을 재실행한다. Expected: Flow model 4개가 selected node에서 제외되고 Gold run/test/marker가 성공한다.

- [ ] **Step 6: Flow Bronze 재활성화 및 compatible Flow checkpoint**

새 compatible Flow event가 만든 Gold run을 관찰한다. Expected: 기존 21개 model selector, Flow pinned row guard 성공, Gold test 성공.

- [ ] **Step 7: admission idempotency 확인**

동일 `(incident, flow, citydata, evidence)` identity를 재실행한다. Expected: admission skip, heavy dbt phase 미실행, Iceberg snapshot 불변.

### Task 4: PR, merge, 최신 dev 재배포, 24시간 관찰

- [ ] **Step 1: verification-before-completion과 code review**

DAGS/DBT diff, tests, dbt node set, Airflow task log, Trino memory를 검토한다.

- [ ] **Step 2: DBT와 DAGS ready PR 생성**

공용 UTF-8 PR template을 사용하고 base는 `dev`로 둔다. 새 issue와 #419 close 문구를 넣지 않는다.

- [ ] **Step 3: CI와 금지 도메인 diff 확인 후 merge**

Expected: required checks PASS, Commerce/Citydata/Transit/Culture 파일 diff 0.

- [ ] **Step 4: clean runtime을 각 `origin/dev` merge revision으로 detach 후 재배포**

Run: `docker compose up -d --build`

Expected: Airflow, Trino, Postgres, Marquez healthy; import error 0.

- [ ] **Step 5: 정상 파이프라인 활성화**

Traffic landing/Bronze/Silver/Gold와 허용된 Weather 수집/Bronze/transform을 정상 운영 상태로 둔다.
maintenance/W2/recovery/backfill은 별도 해결 전까지 paused로 유지한다.

- [ ] **Step 6: 24시간 Weather/Traffic 안정성 관찰**

매 시간 active run, failed task, `trino_traffic_heavy`/`trino_weather_heavy` 적체, source freshness,
Traffic duplicate/lineage, 일일 reliability를 읽기 전용 점검한다. 새 실패는 즉시 증거와 원인만 보고한다.

