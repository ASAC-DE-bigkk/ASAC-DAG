# Weather Transform W2 Contract Variable Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 최신 DBT W2 canonical revision 계약을 Weather transform DAG의 모든 모델 해석 명령에 전달한다.

**Architecture:** DAG의 중앙 `dbt_command()`가 고정 JSON var를 shell-safe하게 구성한다. `dbt deps`만 명시적으로 제외하고, 나머지 task는 helper의 기본값으로 같은 계약을 받는다.

**Tech Stack:** Python, Airflow 3.2, dbt, pytest, Docker Compose

## Global Constraints

- Weather 도메인 파일만 수정한다.
- dev만 사용하고 prod, Traffic, R2 쓰기, backfill을 수행하지 않는다.
- `weather_w2_canonical_revision_date` 값은 승인된 `2025-04-01`로 고정한다.

---

### Task 1: 중앙 dbt 명령 계약

**Files:**
- Modify: `domains/weather/tests/test_weather_transform_dbt_selection.py`
- Modify: `domains/weather/weather_vilage_fcst_transform.py`

- [ ] 실패 테스트: `dbt deps`를 제외한 각 task command에 `--vars '{"weather_w2_canonical_revision_date":"2025-04-01"}'`가 있고, `dbt deps`에는 없음을 검증한다.
- [ ] 최소 구현: `WEATHER_DBT_VARS`와 `include_project_vars` 옵션을 `dbt_command()`에 추가하고 `dbt_deps`만 `False`로 호출한다.
- [ ] 검증: Weather transform selector pytest와 Python compile을 실행한다.

### Task 2: dev 배포 검증

**Files:**
- Runtime only: `docker-compose.yml`의 이미 병합된 공통 환경 설정

- [ ] 활성/queued DAG run이 없음을 확인한다.
- [ ] apiserver, scheduler, dag-processor, triggerer를 순차 재생성한다.
- [ ] `api.base_url`, 기존 TaskInstance `log_url`, 공용 행정동 dbt test를 dev에서 검증한다.
