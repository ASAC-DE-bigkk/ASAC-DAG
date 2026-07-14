# Weather benchmark 반복·실행 지문 계약 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Weather 소유 read-only 비용 대리 지표 benchmark에 3회 반복·Gold suite·실행 지문 비교 계약을 추가한다.

**Architecture:** `weather_traffic_cost_proxy.py`의 case registry에 Weather/Traffic Gold를 추가하고 `MIN_REPEAT = 3`을 collect entrypoint에서 검증한다. suite별 stable execution fingerprint와 bundle fingerprint를 생성하고 `compare_bundles`가 source snapshot과 execution fingerprint 모두 일치할 때만 median 비교를 수행하게 한다.

**Tech Stack:** Python 3.11, pytest, dbt-core/dbt-trino, Trino, Iceberg

## Global Constraints

- `domains/weather/**` 밖의 파일은 변경하지 않는다.
- `ASK_SEOUL_TARGET=dev` 이외의 collect는 계속 실패한다.
- benchmark는 dbt model run, table write, raw upload, backfill, prod write를 수행하지 않는다.
- 최소 반복 수는 정확히 3이며 source fingerprint 또는 execution fingerprint 불일치는 `comparable=false`여야 한다.
- Weather Gold `gold_weather_forecast_by_place`, Traffic Gold `gold_traffic_incident_current_by_admin_dong_hourly`을 사용한다.
- Traffic Gold는 `traffic_snapshot_dag_run_id`와 `seoul_traffic_incident` publishable snapshot resolver를 사용한다.

---

### Task 1: 반복 하한과 Gold benchmark suite

**Files:**
- Modify: `domains/weather/weather_ingest/weather_traffic_cost_proxy.py:38-80,315-347`
- Modify: `domains/weather/tests/test_weather_traffic_cost_proxy.py`

**Interfaces:**
- Consumes: `collect_bundle(label: str, repeat: int)`와 `CASES`.
- Produces: 3회 이상 반복된 Weather/Traffic Silver·Gold·watchdog suite bundle.

- [ ] **Step 1: Write failing tests**

```python
def test_collect_bundle_rejects_fewer_than_three_repeats(monkeypatch):
    monkeypatch.setattr(benchmark, "_ensure_dev_target", lambda: None)
    with pytest.raises(ValueError, match="repeat must be at least 3"):
        benchmark.collect_bundle("before", 2)

def test_gold_cases_use_the_scheduled_weather_and_canonical_traffic_models():
    assert benchmark.CASES["weather_gold"]["model"] == "gold_weather_forecast_by_place"
    assert benchmark.CASES["traffic_gold"]["model"] == "gold_traffic_incident_current_by_admin_dong_hourly"
    assert benchmark.CASES["traffic_gold"]["snapshot_var"] == "traffic_snapshot_dag_run_id"
```

- [ ] **Step 2: Run RED verification**

Run: `python -m pytest -q domains/weather/tests/test_weather_traffic_cost_proxy.py`

Expected: 최소 반복과 Gold case test가 현재 구현에 없어 FAIL.

- [ ] **Step 3: Implement minimal case registry and validation**

```python
MIN_REPEAT = 3

"weather_gold": {
    "domain": "weather",
    "project": "/opt/airflow/dbt/domains/weather",
    "model": "gold_weather_forecast_by_place",
    "source_tables": ["bronze_kma_vilage_fcst", "bronze_collection_run_manifest"],
},
"traffic_gold": {
    "domain": "traffic",
    "project": "/opt/airflow/dbt/domains/traffic",
    "model": "gold_traffic_incident_current_by_admin_dong_hourly",
    "snapshot_source_id": "seoul_traffic_incident",
    "snapshot_var": "traffic_snapshot_dag_run_id",
    "source_tables": [
        "bronze_seoul_traffic_incident",
        "bronze_seoul_traffic_incident_request_audit",
        "bronze_collection_run_manifest",
    ],
},
```

`collect_bundle`의 `_ensure_dev_target()` 직후 `repeat < MIN_REPEAT`이면 `ValueError("repeat must be at least 3")`를 raise한다.

- [ ] **Step 4: Run GREEN verification**

Run: `python -m pytest -q domains/weather/tests/test_weather_traffic_cost_proxy.py`

Expected: Gold registry와 repeat validation tests PASS.

### Task 2: stable execution fingerprint 비교

**Files:**
- Modify: `domains/weather/weather_ingest/weather_traffic_cost_proxy.py:153-187,255-298,337-346,402-430`
- Modify: `domains/weather/tests/test_weather_traffic_cost_proxy.py`

**Interfaces:**
- Consumes: case mapping, `dbt_vars`, `_compile_command`, `catalog`, `schema`.
- Produces: suite `execution_fingerprint`와 bundle `execution_fingerprint`; mismatch reason `execution_fingerprint_mismatch`.

- [ ] **Step 1: Write failing comparison test**

```python
def test_compare_bundles_rejects_different_execution_fingerprints():
    before = {"fingerprint": {"weather": {"snapshot_id": 1}}, "execution_fingerprint": {"weather_silver": {"target": "dev"}}, "suites": []}
    after = {"fingerprint": {"weather": {"snapshot_id": 1}}, "execution_fingerprint": {"weather_silver": {"target": "dev", "schema": "other"}}, "suites": []}
    comparison = benchmark.compare_bundles(before, after)
    assert comparison == {"comparable": False, "reason": "execution_fingerprint_mismatch", "metrics": []}
```

- [ ] **Step 2: Run RED verification**

Run: `python -m pytest -q domains/weather/tests/test_weather_traffic_cost_proxy.py::test_compare_bundles_rejects_different_execution_fingerprints`

Expected: current comparator accepts matching source fingerprints, so assertion FAIL.

- [ ] **Step 3: Implement stable fingerprinting**

Add `_execution_fingerprint(name, case, dbt_vars, catalog, schema)` returning a JSON-serializable mapping containing `name`, `domain`, `source_tables`, `target="dev"`, `catalog`, `schema`, `dbt_bin=DBT_BIN`, and either `compile_command` for model cases or `report` for watchdog cases. Store it in each suite and aggregate `{suite["name"]: suite["execution_fingerprint"]}` as the bundle execution fingerprint. Before source fingerprint comparison in `compare_bundles`, reject unequal bundle execution fingerprints with `execution_fingerprint_mismatch`.

- [ ] **Step 4: Run GREEN verification**

Run: `python -m pytest -q domains/weather/tests/test_weather_traffic_cost_proxy.py`

Expected: existing source-fingerprint, metric, Traffic snapshot tests and the new execution-fingerprint test PASS.

### Task 3: documentation and focused verification

**Files:**
- Create: `domains/weather/docs/weather-traffic/2026-07-14-weather-benchmark-contract.md`
- Test: `domains/weather/tests/test_weather_traffic_cost_proxy.py`

**Interfaces:**
- Consumes: bundle schema from Tasks 1-2.
- Produces: a Korean record of dev-only execution, source/execution fingerprint gates, and any blocked live measurement.

- [ ] **Step 1: Write the record**

Document the exact six suites, `--repeat 3` minimum, source and execution fingerprint comparison rules, and that the concurrent Traffic transform session may cause a valid `fingerprint_mismatch` without any benchmark write.

- [ ] **Step 2: Run focused verification**

Run: `python -m pytest -q domains/weather/tests/test_weather_traffic_cost_proxy.py` and `python -m py_compile domains/weather/weather_ingest/weather_traffic_cost_proxy.py`.

Expected: focused tests and compile PASS when invoked as `python -m pytest`; the Windows `pytest.exe` launcher is not used because its entrypoint resolution failed before test collection.

- [ ] **Step 3: Commit**

```bash
git add domains/weather/weather_ingest/weather_traffic_cost_proxy.py domains/weather/tests/test_weather_traffic_cost_proxy.py domains/weather/docs
git commit -m "feat(weather): benchmark 반복과 실행 지문 계약을 추가한다 (#342)"
```
