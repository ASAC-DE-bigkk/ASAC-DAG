# Weather Place-Mart Transform 순서 교정 계획

> **작업 방식:** `superpowers:subagent-driven-development`와 TDD로 단계별 구현·검토한다.

## 목표

같은 Weather transform DAG run에서 최신 place mart가 먼저 준비된 뒤 기존 full Gold 포트폴리오가 실행되도록 phase 순서를 교정한다.

## 보존할 계약

- 기존 full selector `ask_seoul_weather_transform_gold`를 그대로 사용한다.
- 의도적인 Weather 소유 cross-domain serving Gold 모델을 제외하지 않는다.
- phase 순서만 `Silver → place mart → full Gold`로 바꾼다.
- 기존 task ID, task 수, failure callback, retry, pool, metrics/XCom provenance를 유지한다.
- 타 도메인 DAG·모델은 실행·수정·백필하지 않는다.
- recovery, maintenance, full refresh, 대량 backfill은 실행하지 않는다.

## Task 1: Weather phase ordering TDD

대상 파일:

- `domains/weather/tests/weather_transform_test_support.py`
- `domains/weather/tests/test_weather_transform_dag.py`
- `domains/weather/weather_vilage_fcst_transform.py`

기대 terminal phase:

```text
dbt_run_silver
→ dbt_test_silver
→ dbt_run_place_mart
→ dbt_test_place_mart
→ dbt_run_gold
→ dbt_test_gold
→ publish_dbt_run_metrics
```

구현 순서:

- [x] 현재 `Silver → Gold → place mart` 순서를 드러내는 실패 테스트를 작성한다.
- [x] `DBT_PHASE_SPECS` tuple 순서만 교정한다.
- [x] task dependency와 기존 full Gold selector를 검증한다.
- [x] Weather targeted tests를 통과시킨다.

Targeted test:

```powershell
$env:PYTHONUTF8='1'
python -m pytest domains/weather/tests/test_weather_transform_dag.py domains/weather/tests/test_weather_transform_execution.py -q
```

## Task 2: DAGS 범위 검증

- [x] Weather/Traffic 전체 Python suite를 실행한다.
- [ ] Python compile과 Airflow parser allowlist를 검증한다.
- [ ] dev에서 Weather small transform smoke를 `Silver → place mart → full Gold` 순서로 실행한다.
- [ ] diff에 Traffic DAG, 타 도메인, secret/cache 변경이 없는지 확인한다.

예정 커밋 범위:

```text
domains/weather/weather_vilage_fcst_transform.py
domains/weather/tests/weather_transform_test_support.py
domains/weather/tests/test_weather_transform_dag.py
domains/weather/docs/superpowers/plans/2026-07-17-weather-place-mart-transform-order.md
```
