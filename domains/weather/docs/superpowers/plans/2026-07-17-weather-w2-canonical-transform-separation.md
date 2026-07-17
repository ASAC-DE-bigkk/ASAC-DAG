# Weather canonical W2 transform DAG 분리 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Legacy Weather transform과 독립된 asset-triggered canonical W2 transform DAG를 만들고, 그것이 소비할 최소 DBT selector 계약을 추가한다.

**Architecture:** `weather_vilage_fcst_transform`의 legacy Silver/Gold/place mart chain은 바꾸지 않는다. 새 `weather_w2_canonical_transform`은 동일한 publishable Weather Bronze asset event에서 snapshot을 pin한 뒤 selector-only DBT 실행으로 observation, native grid, canonical Gold와 그 계약 test만 수행한다. W1 bridge seed와 bridge model은 기존 guarded 별도 경계를 유지하며 새 selector와 새 DAG에서 실행하지 않는다.

**Tech Stack:** Airflow 3 Python DAG, `weather_dbt_execution`, dbt selector YAML, pytest, Trino/Iceberg dev target.

## Global Constraints

- Weather/Traffic 경로만 수정·실행한다.
- dev target만 허용하며 prod, shared schema, `--full-refresh`, 대량 backfill을 사용하지 않는다.
- `.omc/`, `.omx/`, `__pycache__/`, `.env`와 secret은 stage·commit·push하지 않는다.
- 새 DAG는 `weather_vilage_fcst_transform`에 task를 추가하지 않는다.
- W1 `weather_admin_dong_grid_bridge_history` seed와 `bridge_weather_admin_dong_grid`는 guarded 선행 상태로만 소비하고 새 selector에서 제외한다.
- canonical grain, revision var, publishable snapshot identity, idempotent DBT incremental 계약을 낮추지 않는다.

---

### Task 1: ASAC-DBT #244 canonical W2 selector 계약

**Files:**
- Create: `domains/traffic_weather/tests/weather/test_weather_w2_canonical_selectors.py`
- Modify: `domains/traffic_weather/selectors.yml`

**Interfaces:**
- Produces selector `ask_seoul_weather_w2_canonical_models` for `dbt run`.
- Produces selector `ask_seoul_weather_w2_canonical_contracts` for `dbt test`.
- Consumes the existing W2 model paths and never selects W1 seed/bridge paths.

- [ ] **Step 1: Write the failing selector-contract test**

```python
def test_canonical_w2_model_selector_has_only_three_owned_models():
    selectors = selectors_by_name()
    assert path_values(selectors["ask_seoul_weather_w2_canonical_models"]) == {
        "models/weather/special/silver/silver_kma_vilage_fcst_observation.sql",
        "models/weather/special/silver/silver_kma_vilage_fcst_grid.sql",
        "models/weather/special/gold/gold_weather_forecast_by_admin_dong.sql",
    }
```

The companion contract test asserts the ten canonical Gold contract SQL paths and explicitly asserts that no value contains `weather_admin_dong_grid_bridge_history` or `bridge_weather_admin_dong_grid`.

- [ ] **Step 2: Verify the test is red**

Run: `pytest domains/traffic_weather/tests/weather/test_weather_w2_canonical_selectors.py -q`

Expected: FAIL because neither canonical selector exists.

- [ ] **Step 3: Add exact-path selector unions**

```yaml
  - name: ask_seoul_weather_w2_canonical_models
    description: Weather canonical W2 observation, grid, Gold model만 선택한다.
    definition:
      union:
        - {method: path, value: models/weather/special/silver/silver_kma_vilage_fcst_observation.sql, indirect_selection: empty}
        - {method: path, value: models/weather/special/silver/silver_kma_vilage_fcst_grid.sql, indirect_selection: empty}
        - {method: path, value: models/weather/special/gold/gold_weather_forecast_by_admin_dong.sql, indirect_selection: empty}
```

Add a second `union` selector containing only the ten `assert_gold_weather_forecast_by_admin_dong_*` contract paths required by ASAC-DAG #357, including the repair reconciliation contract path under `tests/weather/special/recovery/reconciliation/`.

- [ ] **Step 4: Verify green and dbt selection**

Run: `pytest domains/traffic_weather/tests/weather/test_weather_w2_canonical_selectors.py -q`

Expected: PASS.

Run in the dev dbt runtime: `dbt parse --target dev`, then `dbt ls --selector ask_seoul_weather_w2_canonical_models --output json` and `dbt ls --selector ask_seoul_weather_w2_canonical_contracts --output json`.

Expected: the first list has the three owned models; the second has the ten contract tests; neither list contains W1 seed/bridge or recovery workset resources.

- [ ] **Step 5: Commit the DBT issue branch**

```bash
git add domains/traffic_weather/selectors.yml domains/traffic_weather/tests/weather/test_weather_w2_canonical_selectors.py
git commit -m "feat(weather): add canonical W2 transform selectors"
```

### Task 2: ASAC-DAG #357 independent canonical W2 DAG

**Files:**
- Modify: `domains/weather/tests/weather_transform_test_support.py`
- Create: `domains/weather/tests/test_weather_w2_canonical_transform_dag.py`
- Create: `domains/weather/tests/test_weather_w2_canonical_transform_execution.py`
- Create: `domains/weather/weather_w2_canonical_transform.py`

**Interfaces:**
- Produces DAG ID `weather_w2_canonical_transform`.
- Consumes `WEATHER_BRONZE_ASSET`, `weather_snapshot_dag_run_id`, `ask_seoul_weather_w2_canonical_models`, and `ask_seoul_weather_w2_canonical_contracts`.
- Produces task order `validate_dev_runtime -> resolve_weather_snapshot_run -> dbt_deps -> dbt_run_w2_canonical_models -> dbt_test_w2_canonical_contracts -> publish_dbt_run_metrics`.

- [ ] **Step 1: Extend the fake-DAG loader and write failing DAG tests**

```python
def load_transform_module(filename="weather_vilage_fcst_transform.py", module_name=None):
    module_path = Path(__file__).resolve().parents[1] / filename
    name = module_name or f"{module_path.stem}_under_test"
    # existing fake-Airflow import flow, using module_path and name
```

```python
def test_canonical_w2_dag_has_a_small_independent_phase_chain():
    module = load_transform_module(
        "weather_w2_canonical_transform.py",
        "weather_w2_canonical_transform_under_test",
    )
    assert module.DAG_ID == "weather_w2_canonical_transform"
    assert tuple(spec.selector for spec in module.DBT_PHASE_SPECS) == (
        None,
        "ask_seoul_weather_w2_canonical_models",
        "ask_seoul_weather_w2_canonical_contracts",
    )
```

The DAG test also asserts dev-only parameterization, `max_active_runs=1`, shared `TRINO_HEAVY_POOL` with `weight_rule="absolute"`, Bronze asset schedule, failure callbacks, teardown metrics, and no W1 seed/bridge task IDs.

- [ ] **Step 2: Verify the tests are red**

Run: `pytest domains/weather/tests/test_weather_w2_canonical_transform_dag.py domains/weather/tests/test_weather_w2_canonical_transform_execution.py -q`

Expected: FAIL with the canonical DAG module absent.

- [ ] **Step 3: Implement the independent DAG**

Implement `weather_w2_canonical_transform.py` without importing another DAG module. Reuse the proven lower-level `weather_dbt_execution` and `weather_dbt_failure` modules; copy the existing publishable snapshot validation semantics so the new DAG rejects missing, malformed, non-publishable, or mismatched Bronze events and passes both `weather_w2_canonical_revision_date` and `weather_snapshot_dag_run_id` to model/test phases.

```python
DBT_PHASE_SPECS = (
    DbtPhaseSpec("dbt_deps", "deps", include_project_vars=False),
    DbtPhaseSpec("dbt_run_w2_canonical_models", "run", "ask_seoul_weather_w2_canonical_models"),
    DbtPhaseSpec("dbt_test_w2_canonical_contracts", "test", "ask_seoul_weather_w2_canonical_contracts"),
)
```

Use pipeline name `weather-w2-canonical-transform`; set `schedule=[Asset(WEATHER_BRONZE_ASSET)]`; keep all DBT model/test work at one pool slot; publish only the current attempt's `run_results.json` as a non-gating teardown.

- [ ] **Step 4: Verify green and legacy isolation**

Run: `pytest domains/weather/tests/test_weather_w2_canonical_transform_dag.py domains/weather/tests/test_weather_w2_canonical_transform_execution.py domains/weather/tests/test_weather_transform_dag.py domains/weather/tests/test_weather_transform_execution.py -q`

Expected: PASS. The legacy DAG still has exactly its original phase contract and the new DAG has no guarded W1 tasks.

Run: `python -m py_compile domains/weather/weather_w2_canonical_transform.py`

Expected: no output and exit code 0.

- [ ] **Step 5: Commit the DAG issue branch**

```bash
git add domains/weather/weather_w2_canonical_transform.py domains/weather/tests/weather_transform_test_support.py domains/weather/tests/test_weather_w2_canonical_transform_dag.py domains/weather/tests/test_weather_w2_canonical_transform_execution.py domains/weather/docs/superpowers/plans/2026-07-17-weather-w2-canonical-transform-separation.md
git commit -m "feat(weather): split canonical W2 transform DAG"
```

### Task 3: Issue alignment, PRs, merge, and dev redeploy

**Files:**
- Modify GitHub issue ASAC-DAG #357 body/title to describe the separate DAG boundary.
- Create DBT PR for #244 and DAG PR for #357, both with `dev` base.
- Do not modify the dirty root, legacy #357 worktree, or root submodule pointers.

- [ ] **Step 1: Align #357**

Update its description so acceptance requires a separate canonical W2 DAG, selector-only execution, same snapshot identity, and exclusion of W1 seed/bridge from normal collection flow.

- [ ] **Step 2: Publish and merge in dependency order**

Merge #244 first. Rebase or fast-forward the DAG branch only from merged `origin/dev` if the DAG test needs the selector contract available in shared dev; then publish and merge #357.

- [ ] **Step 3: Redeploy from clean refs**

Update only clean deployment worktrees to merged `origin/dev`, retain the local Weather/Traffic `.airflowignore`, and run `docker compose -f docker-compose.yml -f docker-compose.traffic-dev.override.yml up -d --build`.

- [ ] **Step 4: Verify dev-only behavior**

Check Docker health, Airflow parser allowlist, both normal and canonical Weather DAG import, canonical DAG pause state before any manual trigger, DBT parse/selector listing, and targeted unit tests. Do not run W2 recovery, maintenance, or a large recollection/backfill.

## Self-review

- #244 owns only DBT selector configuration and its selector contract test.
- #357 owns only the independent Weather DAG, its tests, and documentation.
- W1 bridge/seed guard, legacy transform task count, OOM remediation, W2 recovery, and maintenance remain outside this implementation.
- Every production behavior has a failing test step and a green verification step.
