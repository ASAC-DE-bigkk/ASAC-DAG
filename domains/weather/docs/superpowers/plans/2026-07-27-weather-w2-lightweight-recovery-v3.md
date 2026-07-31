# Weather W2 용신동 경량 Recovery v3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 기존 Silver를 변경하지 않고 용신동만 staging에 6시간 단위로 누적한 뒤 최종 검증 후 Canonical Gold에 한 번 반영하는 resumable Recovery v3를 구현한다.

**Architecture:** ASAC-DBT에는 snapshot-pinned 용신동 staging model, 전용 incremental strategy, 창·최종 계약과 publish operation을 추가한다. ASAC-DAG에는 checkpoint v3 state machine과 pinned input/baseline resolution을 추가하고, per-window stage/test와 final validate/publish/verify phase만 orchestration한다.

**Tech Stack:** Python 3, Airflow 3, dbt Core, dbt-trino, Trino Iceberg, pytest

## Global Constraints

- Bronze와 기존 Silver relation은 읽기 전용이며 rebuild/full refresh/schema 변경을 금지한다.
- 처리 단위는 최대 6시간이다.
- target은 `admin_dong_code=1123053600`, `nx=61`, `ny=127`로 제한한다.
- 기존 checkpoint v2 Variable은 수정하거나 삭제하지 않는다.
- 실제 D1 snapshot/publish/`_catalog`/Worker 전환은 수행하지 않는다.
- Canonical transform은 recovery `verified` 전까지 paused 상태를 유지한다.
- 모든 수동 DAG trigger는 `scripts/safe-trigger-dag.sh`를 사용한다.
- production code보다 실패 테스트를 먼저 작성하고 RED를 확인한다.

---

### Task 1: checkpoint v3 순수 계약

**Files:**
- Modify: `ASAC-DAG/domains/weather/weather_ingest/w2_recovery.py`
- Modify: `ASAC-DAG/domains/weather/tests/test_weather_w2_observation_recovery.py`

**Interfaces:**
- Produces: `RecoveryPins`, `RecoveryBaseline`, `StagedCheckpoint`, `staged_checkpoint_payload()`, `load_staged_checkpoint()`, `advance_staged_checkpoint()`

- [ ] **Step 1: v2와 독립적인 v3 payload 실패 테스트 작성**

```python
def test_staged_checkpoint_keeps_pins_and_selected_windows():
    payload = staged_checkpoint_payload(
        windows,
        selected_labels=[windows[0].label],
        completed_labels=[],
        checkpoint_id="yongsin-0711-0724-light-v3",
        pins=RecoveryPins(11, 12, 13, 14),
        baseline=RecoveryBaseline(100, "a1", "2026-07-27 02:33:08.181476"),
    )
    assert payload["contract_version"] == 3
    assert payload["state"] == "staging"
    assert payload["pins"]["silver_grid_snapshot_id"] == 13
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/weather/tests/test_weather_w2_observation_recovery.py -q`

Expected: import 또는 symbol-not-defined failure

- [ ] **Step 3: dataclass와 엄격한 parser 최소 구현**

v3 parser는 range, checkpoint ID, target, positive snapshot IDs, baseline, selected/completed subset, 상태 전이를 검증한다. v2 `checkpoint_payload()`와 `completed_window_labels()`는 그대로 유지한다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest domains/weather/tests/test_weather_w2_observation_recovery.py -q`

Expected: all tests pass

### Task 2: pinned staging model과 downgrade-safe merge

**Files:**
- Create: `ASAC-DBT/domains/traffic_weather/macros/weather/weather_w2_recovery_stage.sql`
- Create: `ASAC-DBT/domains/traffic_weather/models/weather/special/recovery/weather_w2_observation_recovery_stage.sql`
- Modify: `ASAC-DBT/domains/traffic_weather/models/weather/special/recovery/_recovery.yml`
- Modify: `ASAC-DBT/domains/traffic_weather/tests/weather/test_weather_w2_repair_contract.py`

**Interfaces:**
- Consumes vars: `weather_w2_recovery_checkpoint_id`, `weather_w2_recovery_target_admin_dong_code`, `weather_w2_bridge_pin_snapshot_id`, `weather_w2_silver_grid_pin_snapshot_id`, 기존 repair/crosswalk vars
- Produces relation: `weather_w2_observation_recovery_stage`

- [ ] **Step 1: static contract 실패 테스트 작성**

테스트는 model이 pinned Silver/bridge macro를 사용하고, 용신동 target filter를 가지며, custom strategy가 checkpoint+natural key merge와 winner comparison을 포함하는지 검사한다.

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic_weather/tests/weather/test_weather_w2_repair_contract.py -q`

Expected: missing stage model/macro assertion failure

- [ ] **Step 3: stage macro와 model 최소 구현**

stage model은 Gold와 같은 payload에 `checkpoint_id`, `window_start_at`, `window_cutoff_at`을 추가한다. custom incremental SQL은 동일 checkpoint/natural key에서 `weather_w2_gold_winner_is_not_older(source, dest)`일 때만 update한다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest domains/traffic_weather/tests/weather/test_weather_w2_repair_contract.py -q`

Expected: all tests pass

### Task 3: 단일 per-window stage 계약

**Files:**
- Create: `ASAC-DBT/domains/traffic_weather/tests/weather/special/recovery/stage/assert_weather_w2_recovery_stage_window.sql`
- Modify: `ASAC-DBT/domains/traffic_weather/selectors.yml`
- Modify: `ASAC-DBT/domains/traffic_weather/models/weather/special/recovery/_recovery.yml`
- Modify: `ASAC-DBT/domains/traffic_weather/tests/weather/test_weather_w2_canonical_selectors.py`
- Modify: `ASAC-DBT/domains/traffic_weather/tests/weather/test_weather_w2_repair_contract.py`

**Interfaces:**
- Produces selector: `ask_seoul_weather_w2_recovery_stage_window_contract`

- [ ] **Step 1: selector와 SQL 계약 실패 테스트 작성**

테스트는 하나의 singular test가 wrong target, wrong grid, null grain/lineage, duplicate grain, missing expected, payload mismatch failure reason을 모두 포함하는지 확인한다.

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic_weather/tests/weather/test_weather_w2_canonical_selectors.py domains/traffic_weather/tests/weather/test_weather_w2_repair_contract.py -q`

Expected: selector/path/failure-reason assertion failure

- [ ] **Step 3: singular test와 selector 구현**

expected는 pinned Silver와 pinned bridge에서 같은 winner ordering으로 계산하고 stage는 현재 checkpoint와 현재 창으로 제한한다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest domains/traffic_weather/tests/weather/test_weather_w2_canonical_selectors.py domains/traffic_weather/tests/weather/test_weather_w2_repair_contract.py -q`

Expected: all tests pass

### Task 4: staging 최종 winner·lineage 검증과 Gold publish operation

**Files:**
- Create: `ASAC-DBT/domains/traffic_weather/tests/weather/special/recovery/stage/assert_weather_w2_recovery_stage_final_reconciles.sql`
- Create: `ASAC-DBT/domains/traffic_weather/tests/weather/special/recovery/stage/assert_weather_w2_recovery_stage_no_downgrade.sql`
- Create: `ASAC-DBT/domains/traffic_weather/tests/weather/special/recovery/stage/assert_weather_w2_recovery_stage_lineage.sql`
- Extend: `ASAC-DBT/domains/traffic_weather/macros/weather/weather_w2_recovery_stage.sql`
- Modify: `ASAC-DBT/domains/traffic_weather/selectors.yml`
- Modify: `ASAC-DBT/domains/traffic_weather/tests/weather/test_weather_w2_repair_contract.py`

**Interfaces:**
- Produces selectors: `ask_seoul_weather_w2_recovery_stage_final`, `ask_seoul_weather_w2_recovery_stage_winner`, `ask_seoul_weather_w2_recovery_stage_lineage`
- Produces operation: `weather_w2_publish_recovery_stage`

- [ ] **Step 1: final selector와 publish SQL 실패 테스트 작성**

테스트는 8 winner bucket, 4 lineage bucket vars, checkpoint filter, target-only source, delete/full-refresh 부재, single `merge into`를 검사한다.

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic_weather/tests/weather/test_weather_w2_repair_contract.py -q`

Expected: missing final tests/operation assertion failure

- [ ] **Step 3: final stage tests와 publish operation 구현**

publish operation은 stage target 행만 Gold natural key로 merge하고 insert 또는 non-downgrade update만 허용한다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest domains/traffic_weather/tests/weather/test_weather_w2_repair_contract.py -q`

Expected: all tests pass

### Task 5: Airflow v3 orchestration과 fingerprint gate

**Files:**
- Modify: `ASAC-DAG/domains/weather/weather_w2_observation_recovery.py`
- Modify: `ASAC-DAG/domains/weather/tests/test_weather_w2_observation_recovery.py`

**Interfaces:**
- Consumes DBT selectors/operation from Tasks 2–4
- Produces: checkpoint init, per-window stage/test, final bucket tests, publish/verify state machine

- [ ] **Step 1: phase ordering·resume·publish 차단 실패 테스트 작성**

```python
def test_v3_stages_and_checkpoints_each_window_before_final_publish(monkeypatch):
    result = recover_observation_windows(**context)
    assert events[:2] == ["stage-window-0000", "test-window-0000"]
    assert "checkpoint-window-0000" in events
    assert events.index("prepublish-validated") < events.index("publish-gold")
    assert events[-1] == "checkpoint-verified"
```

추가 테스트는 final failure가 publish를 호출하지 않고, publish 후 verify failure가 자동 republish되지 않으며, v3 resume가 저장된 pins/selected windows를 재사용하는지 검증한다.

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/weather/tests/test_weather_w2_observation_recovery.py -q`

Expected: 기존 v2 phase ordering으로 assertion failure

- [ ] **Step 3: v3 orchestration 최소 구현**

Trino metadata query로 Iceberg pins, source watermark, Gold non-target row count/checksum을 초기화한다. DBT vars에는 저장된 pins/checkpoint/target을 전달한다. `run-operation`도 기존 isolated artifact executor를 사용한다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest domains/weather/tests/test_weather_w2_observation_recovery.py -q`

Expected: all tests pass

### Task 6: compile·local fixture·실데이터 전 dry-run

**Files:**
- No source edit in this task. 실패가 발생하면 해당 source를 소유한 Task 1–5의 RED-GREEN cycle로 돌아간다.

**Interfaces:**
- Produces: 실행 가능한 DBT manifest와 Airflow import

- [ ] **Step 1: Python compile와 전체 관련 pytest 실행**

Run:

```text
python -m compileall domains/weather/weather_ingest/w2_recovery.py domains/weather/weather_w2_observation_recovery.py
python -m pytest domains/weather/tests/test_weather_w2_observation_recovery.py -q
python -m pytest domains/traffic_weather/tests/weather/test_weather_w2_repair_contract.py domains/traffic_weather/tests/weather/test_weather_w2_canonical_selectors.py -q
```

- [ ] **Step 2: 격리 DBT project parse**

Run: local Airflow/dbt container에서 worktree를 mount하여 `dbt deps`와 `dbt parse --no-partial-parse --target dev`

Expected: exit 0, parse errors 0

- [ ] **Step 3: 테스트 checkpoint ID로 첫 두 창 benchmark**

수동 trigger 전:

```text
scripts/safe-trigger-dag.sh weather_w2_observation_recovery --check-only
```

그 뒤 같은 script를 사용해 v3 conf를 trigger한다. direct `airflow dags trigger`는 사용하지 않는다.

- [ ] **Step 4: benchmark gate 판정**

두 창의 stage/test/checkpoint, Silver snapshot 불변, Canonical Gold snapshot 불변, 창당 시간을 기록한다. 목표 3분/창을 크게 넘으면 장기 실행과 publish를 중단한다.

### Task 7: 전체 recovery와 최종 검증

**Files:**
- No source edit expected
- Update: 사용자 scratch handoff/progress 기록만 갱신하며 git에 포함하지 않음

**Interfaces:**
- Produces: verified checkpoint v3와 검증 증거

- [ ] **Step 1: v3 checkpoint로 전체 staging 재개**

safe-trigger script와 동일 checkpoint ID를 사용한다.

- [ ] **Step 2: pre-publish 결과 확인**

winner 8 buckets, lineage 4 buckets, final stage reconciliation, non-target baseline이 모두 통과해야 한다.

- [ ] **Step 3: target-scoped publish와 post-publish FULL bridge test**

Gold merge 후
`ask_seoul_weather_w2_recovery_post_publish_bridge_contract`로 전 동
missing=0·extra=0·invalid_actual_bridge=0을 확인하고, non-target fingerprint와
current-wide 용신동 포함을 검증해 checkpoint를 `verified`로 닫는다. 그 뒤
canonical DAG를 unpause하고 다음 정상 Bronze asset run에서
`ask_seoul_weather_w2_canonical_contracts` 전체 10종 GREEN을 확인한다.

- [ ] **Step 4: non-target invariant와 snapshot 기록**

baseline과 post-publish non-target row count/checksum을 비교하고 Gold/stage/crosswalk/bridge/Silver snapshot IDs와 source watermark를 기록한다.

### Task 8: 최종 검증과 커밋

**Files:**
- Commit only files explicitly listed in Tasks 1–5 plus these design/plan docs
- Exclude: `LessonRun.md`, `engineering-decision-log.md`, scratch monitor files, secrets, unrelated dirty files

- [ ] **Step 1: fresh full verification**

관련 DAG pytest, DBT static tests, dbt parse, Airflow DAG import, targeted Trino validation을 모두 다시 실행한다.

- [ ] **Step 2: diff와 secret scan**

Run: `git status --short`, `git diff --check`, 변경 경로 대상 secret pattern scan

- [ ] **Step 3: 경로 지정 커밋**

`git add -A`와 `git add .`을 사용하지 않고 승인된 파일 경로만 staging하여 ASAC-DAG와 ASAC-DBT에 각각 커밋한다.

- [ ] **Step 4: push/PR 금지 확인**

remote push와 PR 생성은 수행하지 않고 local commit hash만 보고한다.
# 실행 보완: target source preflight

실데이터 RED에서 manifest anchor만으로 선택된 창에 용신동 pinned Silver 행이
0개인 사례가 확인됐다. 다음 TDD 보완을 Task 5에 포함한다.

1. checkpoint v3에 ordered `known_gaps[{window, reason}]`를 추가하되 기존 v3
   payload에서 필드가 없으면 빈 목록으로 호환한다.
2. pinned Silver snapshot과 최신 publishable manifest를 결합하는 target row-count
   preflight를 창마다 dbt보다 먼저 실행한다.
3. 0행은 stage 성공으로 처리하지 않고 known gap checkpoint로 기록한다.
4. dbt 0행 가드는 제거하지 않는다.
5. completed와 known gap의 합집합이 selected와 같아야 final 단계로 전이한다.
6. 회귀 테스트는 source SQL의 snapshot pin, scope `(61,127)`, manifest join,
   dbt skip, ordered state transition을 확인한다.
