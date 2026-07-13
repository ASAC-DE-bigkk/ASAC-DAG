# Weather·Traffic 공용 행정동 차원 재물질화 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development and execute every RED/GREEN checkpoint in order. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Weather·Traffic 정상 transform이 공용 행정동 dimension을 현재 `common` 원천에서 재생성·검증한 뒤 Silver/Gold를 실행하게 한다.

**Architecture:** 두 DAG에 package dimension 전용 run/test phase를 명시적으로 추가한다. 기존 operator factory, failure callback, pinned snapshot 전달, domain Silver/Gold selector는 변경하지 않는다.

**Tech Stack:** Python 3.11, Airflow DAG/PythonOperator/BashOperator, dbt-core 1.10, pytest, Trino/Iceberg DEV.

## Global Constraints

- 기준 브랜치는 ASAC-DAG 최신 `origin/dev@dd4e0c9`이다. 작업 중 원격이 전진해, 겹치는 파일이 없음을 확인하고 `4fa60b1`에서 fast-forward했다.
- 실행 순서는 `seed asac_axes → run dim_admin_dong → test dim_admin_dong → 기존 domain 단계`다.
- run selector는 `asac_axes.dim_admin_dong` 하나만 대상으로 한다.
- 기존 Weather failure callbacks와 Traffic pinned snapshot/failure-classification 계약을 유지한다.
- 기존 Weather·Traffic Silver/Gold selector는 변경하지 않는다.
- dev 환경만 사용하고 prod write, destructive full-refresh, backfill은 하지 않는다.
- 사용자의 별도 승인 전에는 commit, push, PR을 만들지 않는다.

---

### Task 1: Weather normal transform에 공용 dimension gate 추가

**Files:**

- Modify: `domains/weather/tests/test_weather_transform_dbt_selection.py`
- Modify: `domains/weather/weather_vilage_fcst_transform.py`

**Interfaces:**

- Consumes: `dbt_command(args)`와 `dbt_seed_asac_axes`
- Produces: `dbt_run_common_admin_dong_dimension`, `dbt_test_common_admin_dong_dimension`

- [x] **Step 1: 실패하는 task-order/selector 테스트 작성**

`expected_task_order`에서 axes seed 직후 두 task를 기대하고, run/test command, 기존 failure callback, 새 task의 사람이 읽는 실패 단계명을 검증한다.

- [x] **Step 2: RED 확인**

Run: `python -m pytest domains/weather/tests/test_weather_transform_dbt_selection.py -q -p no:cacheprovider`

Expected: FAIL because `dbt_run_common_admin_dong_dimension` and `dbt_test_common_admin_dong_dimension` do not exist.

- [x] **Step 3: 최소 구현**

```python
dbt_run_common_admin_dong_dimension = BashOperator(
    task_id="dbt_run_common_admin_dong_dimension",
    bash_command=dbt_command("run --select asac_axes.dim_admin_dong"),
    on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
)

dbt_test_common_admin_dong_dimension = BashOperator(
    task_id="dbt_test_common_admin_dong_dimension",
    bash_command=dbt_command("test --select asac_axes.dim_admin_dong"),
    on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
)
```

두 task를 `dbt_seed_asac_axes`와 `dbt_seed_place_mapping` 사이에 연결한다.
`transform_stage_name`은 두 task를 `공용 행정동 차원 실행/검증`으로 분류한다.

- [x] **Step 4: GREEN 확인**

Run: `python -m pytest domains/weather/tests/test_weather_transform_dbt_selection.py -q -p no:cacheprovider`

Expected: PASS.

### Task 2: Traffic normal transform에 공용 dimension gate 추가

**Files:**

- Modify: `domains/traffic/tests/test_traffic_transform_dbt_selection.py`
- Modify: `domains/traffic/traffic_incident_transform.py`

**Interfaces:**

- Consumes: `dbt_task(task_id, dbt_args, silver_persisted=False)`와 `dbt_seed_asac_axes`
- Produces: 동일한 두 dimension task, 기존 pinned snapshot 전달과 failure classification 유지

- [x] **Step 1: 실패하는 task-order/selector/failure-classification 테스트 작성**

bootstrap order와 `classified_task_ids`에 두 task를 추가하고, run/test selector, `snapshot_task_id`, `silver_persisted=False`를 검증한다.

- [x] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_dbt_selection.py -q -p no:cacheprovider`

Expected: FAIL because the dimension tasks do not exist.

- [x] **Step 3: 최소 구현**

```python
dbt_run_common_admin_dong_dimension = dbt_task(
    "dbt_run_common_admin_dong_dimension",
    "run --select asac_axes.dim_admin_dong",
)
dbt_test_common_admin_dong_dimension = dbt_task(
    "dbt_test_common_admin_dong_dimension",
    "test --select asac_axes.dim_admin_dong",
)
```

두 task를 `dbt_seed_asac_axes`와 `dbt_run_silver` 사이에 연결한다.

- [x] **Step 4: GREEN 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_dbt_selection.py -q -p no:cacheprovider`

Expected: PASS.

### Task 3: 회귀 및 DEV 계약 검증

**Files:**

- No additional tracked implementation files

- [x] **Step 1: 관련 회귀 suite**

Run: `python -m pytest domains/weather/tests/test_weather_transform_dbt_selection.py domains/traffic/tests/test_traffic_transform_dbt_selection.py -q -p no:cacheprovider`

Expected: all selected tests PASS.

- [x] **Step 2: changed DAG compile**

Run: `python -m compileall -q domains/weather/weather_vilage_fcst_transform.py domains/traffic/traffic_incident_transform.py`

Expected: exit 0.

- [x] **Step 3: diff hygiene**

Run: `git diff --check` and review `git diff --stat`, exact selectors, task order, callbacks, snapshot arguments.

- [x] **Step 4: approved DEV runtime evidence**

Run the two scoped dbt dimension run/test phases through the approved DEV runtime, then query:

```sql
SHOW CREATE VIEW iceberg_dev.weather.dim_admin_dong;
SHOW CREATE VIEW iceberg_dev.traffic.dim_admin_dong;
```

Verify both definitions reference `iceberg_dev.common.bronze_admin_dong_master`, each relation has 426 rows, `admin_dong_code` null/duplicate is 0, and the revision equals the common source latest revision.

- [x] **Step 5: commit/push/PR boundary**

Do not commit, push, or open a PR until the user explicitly approves those actions.

## Execution Evidence

- Baseline: Weather·Traffic transform selection tests `15 passed`.
- Weather RED: 새 task 부재로 `1 failed, 3 passed`; GREEN: `5 passed`.
- Traffic RED: 새 task 부재로 `2 failed, 9 passed`; GREEN: `11 passed`.
- Domain regression: 최초 `94 passed`; 최신 `origin/dev@dd4e0c9` 동기화 후 추가 reliability tests를 포함해 `102 passed` (Windows Airflow 지원 경고 1건).
- Final targeted transform tests: `16 passed`.
- Changed DAG in-memory compile: 2 files exit 0.
- DEV Weather: axes seeds 3/3, dimension view 1/1, attached tests 3/3 PASS.
- DEV Traffic: axes seeds 3/3, dimension view 1/1, attached tests 3/3 PASS.
- Trino: 두 view 모두 `iceberg_dev.common.bronze_admin_dong_master` 참조, 개인 `dev_*` 참조 없음, 각각 426행, code null/duplicate 0, latest revision `2025-04-01` 일치.
- Docker Compose가 다른 worktree의 빈 submodule을 bind한 상태여서 scheduler `/tmp`의 격리 dbt 복사본으로 같은 env/network에서 실행했고, 검증 뒤 복사본을 삭제했다. Compose와 해당 worktree는 변경하지 않았다.
- 작업 중 `origin/dev`가 11커밋 전진했으며 이번 변경 파일과 overlap 0건을 확인한 뒤 `dd4e0c9`로 fast-forward했다.
- Commit/push/PR: 2026-07-13 사용자 명시 승인을 받아 게시 단계 진행.
