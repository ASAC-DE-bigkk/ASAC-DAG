# Traffic/Weather dbt 실행 모듈 구현 계획

> **실행 방식:** 현재 세션에서 TDD로 순서대로 실행한다. 사용자 지시에 따라 commit/push/PR은 만들지 않는다.

**목표:** Traffic/Weather transform DAG가 root dbt monoproject를 사용하고, DAG 내부의 모델·테스트 membership literal 대신 dbt selector/tag 계약을 호출하도록 바꾼다.

**구조:** 각 도메인에 독립적인 dbt 실행 모듈을 둔다. 실행 모듈은 root project 경로, selector 표현, `dbt ls` non-empty preflight, `--indirect-selection=buildable`, task attempt별 target/log 경로를 소유한다. DAG는 phase/task ID, snapshot/vars, 실패 분류, callback과 task graph만 소유한다. Traffic과 Weather는 서로 import하지 않는다.

**기술:** Python 3, Airflow `PythonOperator`, dbt CLI, pytest

---

## 작업 1: Traffic selector 계약을 테스트로 고정

**파일**

- 수정: `domains/traffic/tests/test_traffic_transform_dbt_selection.py`
- 수정: `domains/traffic/tests/test_traffic_snapshot_recovery.py`

1. 40개/7개 tuple allowlist와 explicit model/test token 단언을 제거한다.
2. 각 task가 named selector 또는 단일 phase tag만 전달하는지 단언한다.
3. 선택 실행마다 동일 selector를 대상으로 `dbt ls`가 먼저 실행되고 empty selection은 실제 run/test 전에 중단되는지 단언한다.
4. 테스트를 실행해 현재 구현에서 RED임을 확인한다.

## 작업 2: Traffic domain-owned 실행 모듈 구현

**파일**

- 생성: `domains/traffic/traffic_dbt_execution.py`
- 수정: `domains/traffic/traffic_incident_transform.py`
- 수정: `domains/traffic/traffic_snapshot_recovery.py`

1. domain-owned project 기본값 `/opt/airflow/dbt/domains/traffic_weather`와 env override를
   구현한다.
2. run/task/try별 target/log 경로와 안전한 경로 segment 처리를 구현한다.
3. named selector/tag 표현, `--indirect-selection=buildable`, non-empty `dbt ls` preflight를 구현한다.
4. transform/recovery DAG의 explicit membership literal을 phase selector/tag로 교체한다.
5. 기존 run/test 분리, task ID/graph, snapshot vars, failure classification/callback을 보존한다.
6. Traffic 테스트를 GREEN으로 만든다.

## 작업 3: Weather selector 계약을 테스트로 고정

**파일**

- 수정: `domains/weather/tests/test_weather_transform_dbt_selection.py`

1. `EXPECTED_DBT_PHASES`의 explicit model/test 목록을 phase tag 계약으로 교체한다.
2. root project, preflight, buildable indirect selection, target/log 격리를 단언한다.
3. 테스트를 실행해 현재 구현에서 RED임을 확인한다.

## 작업 4: Weather domain-owned 실행 모듈 구현

**파일**

- 생성: `domains/weather/weather_dbt_execution.py`
- 수정: `domains/weather/weather_vilage_fcst_transform.py`

1. Traffic을 import하지 않는 Weather 전용 실행 모듈을 구현한다.
2. 기존 artifact reset/XCom provenance와 non-gating metrics teardown을 보존한다.
3. Weather DAG phase를 selector/tag 호출로 교체하고 테스트를 GREEN으로 만든다.

## 작업 5: Traffic/Weather OpenLineage opt-in 경계

**파일**

- 생성: `domains/traffic/traffic_lineage.py`
- 생성: `domains/weather/weather_lineage.py`
- 수정: `domains/traffic/traffic_incident_transform.py`
- 수정: `domains/traffic/traffic_snapshot_recovery.py`
- 수정: `domains/weather/weather_vilage_fcst_transform.py`
- 수정: 각 도메인의 transform/recovery 테스트

1. `AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE`가 정확히 `true`일 때만 provider 모듈을 동적 import하는 helper 테스트를 RED로 추가한다.
2. 기본 로컬/test 환경에서는 provider import가 발생하지 않고 DAG import가 성공하는지 확인한다.
3. opt-in 환경에서 provider/API가 없으면 명시적 `RuntimeError`로 실패하는지 확인한다.
4. Traffic transform/recovery와 Weather transform DAG에만 `enable_lineage()`를 적용하고 다른 domain이 helper를 import/호출하지 않음을 고정한다.

## 작업 6: 전체 검증

**파일**

- 검증: 위 production/test 파일 전체

1. Traffic/Weather 관련 pytest를 실행한다.
2. 변경 Python 파일 전체를 `py_compile`로 확인한다.
3. `git diff --check`, 범위 밖 변경, direct Traffic↔Weather import, landing/bronze 변경이 없는지 확인한다.
4. 변경 요약, RED/GREEN 증거, 테스트 결과, dbt 실제 selector integration 검증 잔여사항을 보고한다.
