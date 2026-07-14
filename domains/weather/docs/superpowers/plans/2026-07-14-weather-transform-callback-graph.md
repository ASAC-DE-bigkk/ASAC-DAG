# Weather Transform Callback Graph Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Weather transform의 메트릭 발행을 DAG 성공 callback으로 옮기고, 그래프를 단일 직렬 흐름으로 만든다.

**Architecture:** `publish_weather_transform_success_metrics(context)`가 Airflow의 단일 context 인자를 `publish_dbt_run_metrics(**context)`로 전달한다. task-level failure callback은 즉시 알림과 실패 상세 기록을 유지하며, graph leaf task는 제거한다.

**Tech Stack:** Python, Airflow 3.2, pytest

## Global Constraints

- Weather 도메인 파일만 변경한다.
- task-level 즉시 실패 알림과 R2 Problem 기록을 제거하지 않는다.
- prod, Traffic, R2/Trino 데이터 쓰기, backfill을 수행하지 않는다.

---

### Task 1: callback graph 회귀 계약

**Files:**
- Modify: `domains/weather/tests/test_weather_transform_dbt_selection.py`

- [ ] 실패 테스트: 마지막 dbt test가 유일한 leaf이며 기존 두 terminal task가 없고, DAG success callback이 wrapper를 가리키는지 검증한다.
- [ ] 실패 테스트: wrapper가 callback context를 keyword context로 풀어 기존 메트릭 함수를 호출하는지 검증한다.

### Task 2: DAG 전환

**Files:**
- Modify: `domains/weather/weather_vilage_fcst_transform.py`

- [ ] `publish_weather_transform_success_metrics(context)` wrapper를 추가한다.
- [ ] DAG에 `on_success_callback`을 등록한다.
- [ ] `ALL_DONE` 메트릭 task, `ONE_FAILED` watcher task, watcher edge와 전용 import를 제거한다.
- [ ] Weather transform pytest 및 Python compile을 실행한다.

### Task 3: dev 검증

**Files:**
- Runtime only: mounted Weather DAG

- [ ] Airflow import 오류에 Weather transform이 없는지 확인한다.
- [ ] `airflow tasks test weather_vilage_fcst_transform dbt_test_common_admin_dong_dimension`을 dev에서 실행한다.
