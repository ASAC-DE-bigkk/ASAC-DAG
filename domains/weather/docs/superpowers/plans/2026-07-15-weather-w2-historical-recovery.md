# Weather W2 과거 Observation 복구 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development and execute every RED/GREEN checkpoint in order. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** #196의 91개 과거 Observation 누락을 24시간 이하의 직렬 W2 repair로 복구하고 재개 가능한 manual Airflow DAG를 제공한다.

**Architecture:** `weather_ingest/w2_recovery.py`가 timestamp/window/checkpoint와 DBT command를 순수 함수로 제공한다. `weather_w2_observation_recovery.py`가 해당 함수를 사용해 하나의 pool-held PythonOperator 안에서 DBT writer를 순서대로 실행하고 Airflow Variable checkpoint를 갱신한다.

**Tech Stack:** Airflow 3, Python 3.11, dbt 1.10, dbt-trino 1.10, Trino/Iceberg dev.

## 전역 제약

- `dev`와 `iceberg_dev.weather`만 쓴다.
- W1 30분 lookback과 2GB Trino query cap을 변경하지 않는다.
- recovery window는 KST timestamp(6), inclusive, 최대 24시간이다.
- `trino_heavy` pool slot은 seed부터 final test까지 보유한다.
- DBT 명령은 `--threads 1`이고 `--full-refresh`를 사용하지 않는다.

---

### Task 1: 순수 recovery 계약

**Files:**
- Create: `domains/weather/weather_ingest/w2_recovery.py`
- Create: `domains/weather/tests/test_weather_w2_observation_recovery.py`

**Interfaces:**
- Produces: `RepairWindow`, `split_repair_windows`, `checkpoint_payload`, `window_dbt_vars`, `dbt_command`.
- Consumes: KST 범위 문자열과 완료 window checkpoint.

- [ ] **Step 1: 실패하는 window/checkpoint test 작성**

```python
def test_splits_inclusive_range_into_at_most_24_hour_windows():
    windows = split_repair_windows("2026-07-02 00:00:00.000000", "2026-07-03 00:00:00.000000")
    assert [window.label for window in windows] == [
        "2026-07-02 00:00:00.000000__2026-07-02 23:59:59.999999",
        "2026-07-03 00:00:00.000000__2026-07-03 00:00:00.000000",
    ]
```

- [ ] **Step 2: test가 helper 부재로 실패하는지 확인**

Run: `python -m pytest -q domains/weather/tests/test_weather_w2_observation_recovery.py`

Expected: import failure.

- [ ] **Step 3: 최소 helper 구현**

`datetime.strptime(..., "%Y-%m-%d %H:%M:%S.%f")`로 KST-naive timestamp를 파싱하고, `timedelta(days=1) - timedelta(microseconds=1)`로 inclusive window를 생성한다. checkpoint payload에는 range와 completed window label을 보존한다.

- [ ] **Step 4: helper test 통과 확인**

Run: `python -m pytest -q domains/weather/tests/test_weather_w2_observation_recovery.py`

Expected: PASS.

### Task 2: Manual recovery DAG

**Files:**
- Create: `domains/weather/weather_w2_observation_recovery.py`
- Modify: `domains/weather/tests/test_weather_w2_observation_recovery.py`

**Interfaces:**
- Consumes: `RepairWindow`와 DBT command helpers.
- Produces: manual DAG `weather_w2_observation_recovery`.

- [ ] **Step 1: 실패하는 DAG contract test 작성**

```python
def test_recovery_dag_serializes_w2_writers_and_finishes_with_global_reconciliation():
    module = load_dag_module()
    task = module.dag.get_task("recover_observation_windows")
    assert module.dag.schedule is None
    assert task.pool == TRINO_HEAVY_POOL
    assert task.pool_slots == 1
    assert "--threads" in module.WINDOW_DBT_ARGS
    assert "assert_weather_observation_publishable_and_counts_reconcile" in module.FINAL_DBT_ARGS
```

- [ ] **Step 2: contract test 실패 확인**

Run: `python -m pytest -q domains/weather/tests/test_weather_w2_observation_recovery.py`

Expected: module import failure.

- [ ] **Step 3: 최소 DAG 구현**

`PythonOperator`가 checkpoint를 읽고 preflight DBT command, 미완료 window command, final test를 `subprocess.run(..., check=True)`로 실행한다. 모든 command는 list argv와 `DBT_PROJECT_DIR`/`DBT_PROFILES_DIR` 환경을 사용한다.

- [ ] **Step 4: contract test 통과 확인**

Run: `python -m pytest -q domains/weather/tests/test_weather_w2_observation_recovery.py`

Expected: PASS.

### Task 3: Runtime recovery와 최종 검증

**Files:**
- Modify: 없음

- [ ] **Step 1: compile/parse와 domain test 실행**

Run: `python -m pytest -q domains/weather/tests/test_weather_w2_observation_recovery.py domains/weather/tests/test_weather_transform_dbt_selection.py`

Expected: PASS.

- [ ] **Step 2: dev manual DAG 실행**

Run: `airflow dags trigger weather_w2_observation_recovery --run-id manual__weather_w2_issue_196_recovery`

Expected: 13개 KST window가 checkpoint와 함께 serial로 성공하고 final reconciliation이 PASS.

- [ ] **Step 3: issue evidence 확인**

Run: `dbt test --select assert_weather_observation_publishable_and_counts_reconcile --target dev`

Expected: PASS=1, ERROR=0.

- [ ] **Step 4: 커밋·PR·머지**

Run: `git add`로 새 weather recovery 파일과 test만 stage하고, `fix(weather): recover historical W2 observations (#196)`으로 commit한다. dev base PR을 만들고 checks와 runtime evidence를 확인한 뒤 merge한다.
