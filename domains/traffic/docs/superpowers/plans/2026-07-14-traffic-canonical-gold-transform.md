# Traffic Canonical Gold Transform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 기존 Traffic scheduled transform이 pinned complete snapshot으로 source summary와 canonical 행정동·평가시간 Gold를 함께 생성·검증하게 한다.

**Architecture:** 기존 `dbt_run_gold`와 `dbt_test_gold` Airflow task를 유지해 task graph, watcher, metrics terminal artifact 계약을 보존한다. 조건부 dependency가 있는 `dbt_run_silver`와 resolved Gold 계약이 필요한 `dbt_test_gold`는 각각 같은 task/try target-path와 같은 `traffic_snapshot_dag_run_id`로 `dbt parse --no-partial-parse`를 먼저 실행한다. Gold를 조기 참조하는 교차 테스트는 common admin과 Silver gate에서 제외하고 Gold 재빌드 후 한 번만 실행한다.

**Tech Stack:** Python 3.11, Airflow 3 operator contract, dbt-core 1.10.22, pytest, Docker dev runtime.

**Implementation outcome:** 계획 실행 중 dev 실환경에서 common admin gate의 stale Gold fan-out과 fresh target의 Silver conditional dependency를 추가로 발견해 RED 테스트로 고정한 뒤 해결했다. Weather 소유권 이동 때 누락된 Traffic recovery manifest import도 회귀 범위 안에서 복구했다.

## Global Constraints

- 변경·생성 파일은 `domains/traffic/**` 안에만 둔다.
- `origin/dev`의 task/try별 dbt artifact, failure classification, ONE_FAILED watcher, ALL_DONE metrics 계약을 유지한다.
- `dbt deps`에는 `--target-path`를 전달하지 않는다.
- parse와 test는 같은 dev target, pinned snapshot vars, task/try target-path를 사용한다.
- 신규 Gold를 참조하는 fan-out/snapshot 교차 테스트는 Gold 생성 전 Silver gate에서 제외한다.
- prod schedule·bucket·schema, full refresh, backfill, destructive delete를 사용하지 않는다.

---

### Task 1: Gold selector와 fresh parse 계약을 RED로 고정

**Files:**
- Modify: `domains/traffic/tests/test_traffic_transform_dbt_selection.py`
- Test: `domains/traffic/tests/test_traffic_transform_dbt_selection.py`

**Interfaces:**
- Consumes: 기존 `load_transform_module()`, `run_dbt_phase()`, fake Airflow operator.
- Produces: `fresh_parse: bool` op_kwarg와 parse/test 동일 target-path를 요구하는 회귀 테스트.

- [x] **Step 1: selector RED assertion 추가**

`test_traffic_transform_bootstraps_asac_axes_before_silver`에 다음 계약을 추가한다.

```python
assert "gold_traffic_incident_current_by_admin_dong_hourly" in task_commands["dbt_run_gold"]
assert "gold_traffic_incident_current_by_admin_dong_hourly" in task_commands["dbt_test_gold"]
assert dag.task_dict["dbt_test_gold"].kwargs["op_kwargs"]["fresh_parse"] is True
assert "assert_gold_traffic_current_by_admin_dong_hourly_fanout_reconciles" in task_commands["dbt_test_gold"]
assert "assert_gold_traffic_current_by_admin_dong_hourly_snapshot_reconciles" in task_commands["dbt_test_gold"]
assert "--exclude assert_gold_traffic_current_by_admin_dong_hourly_fanout_reconciles" in task_commands["dbt_test_silver"]
assert "assert_gold_traffic_current_by_admin_dong_hourly_snapshot_reconciles" in task_commands["dbt_test_silver"]
```

selector와 exclude는 raw substring이 아니라 `shlex.split()` token을 `--select`와
`--exclude` 경계로 나눠 exact set으로 검증한다. Silver exclude set에는 기존
`assert_gold_traffic_counts_match_silver`도 반드시 남아 있어야 한다.

- [x] **Step 2: 동일 target-path RED test 추가**

```python
def test_gold_contract_test_fresh_parses_in_same_task_artifact(monkeypatch):
    module = load_transform_module()
    commands = []
    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=2,
        xcom_pull=lambda task_ids: "snapshot-a",
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, **_kwargs: (
            commands.append(command)
            or types.SimpleNamespace(returncode=0, stdout="", stderr="")
        ),
    )

    result = module.run_dbt_phase(
        dbt_args="test --select gold_traffic_incident_current_by_admin_dong_hourly",
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=True,
        fresh_parse=True,
        ti=ti,
        run_id="manual__a",
        params={"target": "dev"},
    )

    assert [command[1] for command in commands] == ["parse", "test"]
    assert "--no-partial-parse" in commands[0]
    assert commands[0][commands[0].index("--target") + 1] == "dev"
    assert commands[1][commands[1].index("--target") + 1] == "dev"
    assert commands[0][commands[0].index("--vars") + 1] == (
        '{"traffic_snapshot_dag_run_id": "snapshot-a"}'
    )
    assert commands[1][commands[1].index("--vars") + 1] == (
        '{"traffic_snapshot_dag_run_id": "snapshot-a"}'
    )
    parse_target = Path(commands[0][commands[0].index("--target-path") + 1])
    test_target = Path(commands[1][commands[1].index("--target-path") + 1])
    assert parse_target == test_target == Path(result["artifact_path"]).parent
    assert Path(result["artifact_path"]).parts[-4:] == (
        "manual__a", "dbt_test_gold", "try2", "run_results.json"
    )
```

- [x] **Step 3: parse 실패 RED test 추가**

`fresh_parse=True`에서 parse subprocess가 실패하면 test subprocess를 호출하지 않고, 기존
failure classification/XCom record가 `snapshot-a`와
`traffic-transform/manual__a/dbt_test_gold/try2/run_results.json`을 보존해야 한다.

- [x] **Step 4: RED 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_dbt_selection.py -q`

Expected: selector, Silver exclude, command chain, parse failure assertion이 예상 이유로 실패하고
기존 테스트는 통과한다.

### Task 2: 기존 Gold task 안에서 parse 후 test 실행

**Files:**
- Modify: `domains/traffic/traffic_incident_transform.py`
- Test: `domains/traffic/tests/test_traffic_transform_dbt_selection.py`

**Interfaces:**
- Consumes: `run_dbt_phase(..., fresh_parse: bool = False)`, `_artifact_path()`.
- Produces: `dbt_task(..., fresh_parse: bool = False)`와 동일 task/try parse-test chain.

- [x] **Step 1: `run_dbt_phase`에 선택적 fresh parse 추가**

signature를 다음처럼 확장한다.

```python
def run_dbt_phase(*, dbt_args: str, snapshot_task_id: str,
                  silver_persisted: bool, fresh_parse: bool = False,
                  **context) -> dict[str, str]:
```

기존 command 생성 규칙을 지역 함수로 재사용하고, `fresh_parse=True`일 때 아래 순서로 실행한다.

```python
phase_args = ["parse --no-partial-parse", dbt_args] if fresh_parse else [dbt_args]
for current_args in phase_args:
    completed = subprocess.run(build_command(current_args), ...)
    print stdout/stderr
    if completed.returncode != 0:
        break
```

마지막 성공 command가 test이므로 기존 artifact path의 `run_results.json`과 metrics 계약이 유지된다. parse 실패도 기존 `classify_dbt_failure` 경로로 전달한다.

- [x] **Step 2: `dbt_task`에 op_kwarg 전달**

```python
def dbt_task(task_id: str, dbt_args: str, *, silver_persisted: bool = False,
             fresh_parse: bool = False) -> PythonOperator:
    ...
    "fresh_parse": fresh_parse,
```

- [x] **Step 3: Gold selector 갱신**

```python
dbt_run_gold = dbt_task(
    "dbt_run_gold",
    "run --select gold_traffic_incident_summary "
    "gold_traffic_incident_current_by_admin_dong_hourly",
    silver_persisted=True,
)

dbt_test_gold = dbt_task(
    "dbt_test_gold",
    "test --select gold_traffic_incident_summary "
    "gold_traffic_incident_current_by_admin_dong_hourly "
    "assert_gold_traffic_counts_match_silver "
    "assert_gold_traffic_row_counts_positive "
    "assert_gold_traffic_current_by_admin_dong_hourly_admin_join_reconciles "
    "assert_gold_traffic_current_by_admin_dong_hourly_admin_stamp_exact "
    "assert_gold_traffic_current_by_admin_dong_hourly_fanout_reconciles "
    "assert_gold_traffic_current_by_admin_dong_hourly_grain_unique "
    "assert_gold_traffic_current_by_admin_dong_hourly_hourly_completeness "
    "assert_gold_traffic_current_by_admin_dong_hourly_product_row_id_reproducible "
    "assert_gold_traffic_current_by_admin_dong_hourly_snapshot_reconciles "
    "assert_gold_traffic_current_by_admin_dong_hourly_zero_requires_complete",
    silver_persisted=True,
    fresh_parse=True,
)
```

`dbt_test_silver`의 기존 `--exclude assert_gold_traffic_counts_match_silver` 뒤에 다음 두
Gold-current 교차 테스트를 추가해 이전 cycle의 stale Gold를 Silver gate가 읽지 않게 한다.

```text
assert_gold_traffic_current_by_admin_dong_hourly_fanout_reconciles
assert_gold_traffic_current_by_admin_dong_hourly_snapshot_reconciles
```

- [x] **Step 4: GREEN 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_dbt_selection.py -q`

Expected: 25 tests, 0 failures. 실제 dbt selector 검증에서 Silver는 두 교차 테스트를 제외한
33 tests, Gold는 기존 8 + canonical 25인 33 tests를 실행한다.

### Task 3: 실패·artifact 회귀 및 dev runtime 검증

**Files:**
- Modify if needed: `domains/traffic/tests/test_traffic_transform_dbt_selection.py`
- Create: `domains/traffic/docs/retrospectives/2026-07-14-traffic-canonical-gold-transform-dev-validation.md`

**Interfaces:**
- Consumes: Task 2의 parse/test chain과 기존 watcher/metrics tests.
- Produces: Issue #333과 PR에 사용할 PASS/FAIL/NOT_RUN 및 비용 대리 지표.

- [x] **Step 1: Traffic 전체 Python test**

Run: `python -m pytest domains/traffic/tests -q`

Expected: 모든 Traffic test PASS.

- [x] **Step 2: compile과 DAG import**

Run: `python -m py_compile domains/traffic/traffic_incident_transform.py`

Run in the dev Airflow image: import `domains/traffic/traffic_incident_transform.py` and verify the DAG has `dbt_run_gold`, `dbt_test_gold`, `publish_dbt_run_metrics`, and the failure watcher.

- [x] **Step 3: 실제 dbt command shape 검증**

ASAC-DBT PR #174 worktree를 dev Airflow image에 mount하고 동일 pinned snapshot으로 `parse --no-partial-parse`, canonical Gold compile/test를 같은 target-path에서 실행한다. 기존 성공 증적을 재사용할 때는 commit SHA와 artifact path가 동일한지 확인한다.

- [x] **Step 4: 회고 기록**

실제 비용이 아닌 대리 지표임을 명시하고 Python wall time, command 수, dbt/Trino 기존 측정값, 최종 row, 재시도, 미실행 항목을 `domains/traffic/docs/retrospectives/` 아래에 기록한다.

- [x] **Step 5: 최종 gate**

Run: `git diff --check`

Run: 변경 파일 scope/secret heuristic 검사.

Expected: 변경 파일은 `domains/traffic/**`뿐이고 secret hit는 0이다.
