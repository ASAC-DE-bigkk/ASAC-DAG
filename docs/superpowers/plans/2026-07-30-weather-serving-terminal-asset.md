# Weather D1 Terminal Asset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Weather Gold write와 계약 테스트가 성공한 경우에만 D1 4종 게시 DAG를 자동 실행한다.

**Architecture:** Weather place Gold를 소유한 `weather_vilage_fcst_transform`에 정상 경로 terminal marker task를 추가하고 전용 Asset을 발행한다. `weather_serving_export`는 그 Asset을 구독하며 공통 D1 Publisher 구현과 W2 canonical DAG는 변경하지 않는다.

**Tech Stack:** Python 3.11, Apache Airflow 3 Assets, pytest

## Global Constraints

- Weather 범위와 기존 `common.assets` 계약만 수정하고 Traffic 및 다른 도메인은 변경하지 않는다.
- terminal Asset은 `dbt_test_gold` 성공 뒤에만 발행한다.
- D1은 Gold run별 exactly-once가 아니라 트리거 시점의 최신 검증 Gold snapshot을 직렬 게시한다.
- `.airflowignore`, 공통 D1 Publisher 정책, prod credential은 변경하지 않는다.
- 현재 prod recollect/backfill이 끝날 때까지 새 revision을 runtime에 배포하지 않는다.

---

### Task 1: Weather Gold terminal Asset 계약

**Files:**
- Modify: `common/assets.py`
- Modify: `domains/weather/weather_vilage_fcst_transform.py`
- Test: `domains/weather/tests/test_weather_transform_dag.py`

**Interfaces:**
- Consumes: `WEATHER_BRONZE_ASSET`, `SNAPSHOT_TASK_ID`
- Produces: `WEATHER_GOLD_PUBLICATION_READY_ASSET`, `WEATHER_GOLD_PUBLICATION_READY_ASSET_REF`, `mark_weather_gold_publication_ready(**context) -> dict[str, str]`

- [ ] **Step 1: 성공 task의 outlet·순서·metadata를 검증하는 실패 테스트 작성**

```python
def test_weather_gold_terminal_asset_is_emitted_only_after_gold_contracts():
    marker = module.dag.task_dict["mark_weather_gold_publication_ready"]
    assert marker in module.dag.task_dict["dbt_test_gold"].downstream_list
    assert marker.outlets == [module.WEATHER_GOLD_PUBLICATION_READY_ASSET_REF]
    assert not module.dag.task_dict["publish_dbt_run_metrics"].outlets
```

- [ ] **Step 2: 테스트가 marker 부재로 실패하는지 확인**

Run: `python -m pytest -q domains/weather/tests/test_weather_transform_dag.py -p no:cacheprovider`

Expected: FAIL because `mark_weather_gold_publication_ready` does not exist.

- [ ] **Step 3: 최소 terminal marker 구현**

```python
WEATHER_GOLD_PUBLICATION_READY_ASSET = "iceberg://weather/gold/publication-ready"

def mark_weather_gold_publication_ready(**context) -> dict[str, str]:
    bronze_run_id = str(context["ti"].xcom_pull(task_ids=SNAPSHOT_TASK_ID) or "")
    if not bronze_run_id:
        raise AirflowFailException("weather Gold publication marker requires a Bronze snapshot")
    metadata = {
        "gold_dag_run_id": str(context.get("run_id") or ""),
        "bronze_dag_run_id": bronze_run_id,
    }
    context["outlet_events"][WEATHER_GOLD_PUBLICATION_READY_ASSET_REF].extra = metadata
    return metadata
```

- [ ] **Step 4: Weather transform 테스트 통과 확인**

Run: `python -m pytest -q domains/weather/tests/test_weather_transform_dag.py -p no:cacheprovider`

Expected: PASS.

### Task 2: Weather serving export Asset 구독

**Files:**
- Modify: `domains/weather/weather_serving_export.py`
- Test: `domains/weather/tests/test_weather_serving_export.py`

**Interfaces:**
- Consumes: `WEATHER_GOLD_PUBLICATION_READY_ASSET`
- Produces: `weather_serving_export` DAG의 Asset schedule

- [ ] **Step 1: 수동 schedule 기대값을 terminal Asset 기대값으로 변경**

```python
assert captured["schedule"] == FakeAsset("iceberg://weather/gold/publication-ready")
```

- [ ] **Step 2: 기존 구현에서 테스트 실패 확인**

Run: `python -m pytest -q domains/weather/tests/test_weather_serving_export.py -p no:cacheprovider`

Expected: FAIL because current schedule is `None`.

- [ ] **Step 3: wrapper schedule을 terminal Asset으로 변경**

```python
schedule=Asset(WEATHER_GOLD_PUBLICATION_READY_ASSET)
```

- [ ] **Step 4: serving wrapper 테스트 통과 확인**

Run: `python -m pytest -q domains/weather/tests/test_weather_serving_export.py -p no:cacheprovider`

Expected: PASS.

### Task 3: 범위·import 회귀 검증

**Files:**
- Verify only: `common/assets.py`
- Verify only: `domains/weather/**`

**Interfaces:**
- Consumes: Task 1·2 결과
- Produces: 배포 전 검증 증거

- [ ] **Step 1: Weather 관련 단위 테스트 실행**

Run: `python -m pytest -q domains/weather/tests/test_weather_serving_export.py domains/weather/tests/test_weather_transform_dag.py domains/weather/tests/test_weather_w2_canonical_transform_dag.py -p no:cacheprovider`

Expected: PASS.

- [ ] **Step 2: Python compile 및 diff whitespace 검증**

Run: `python -m py_compile common/assets.py domains/weather/weather_vilage_fcst_transform.py domains/weather/weather_serving_export.py`

Expected: exit code 0.

Run: `git diff --check`

Expected: no output.

- [ ] **Step 3: 변경 범위 확인**

Run: `git diff --name-only`

Expected: 설계·계획 문서, `common/assets.py`, Weather transform·serving wrapper·두 테스트 파일만 출력.

- [ ] **Step 4: 운영 배포는 recollect/backfill 완료 후 별도 승인 경계로 남김**

Expected: prod runtime에는 아직 변경이 없고, Weather backlog 완료 후 commit·PR·배포·asset canary 순서로 진행.
