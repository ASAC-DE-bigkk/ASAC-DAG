# Traffic/Weather dbt 실행 경계 강화 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Traffic/Weather dbt 실행을 domain-local helper로 제한하면서 OpenLineage 방출, artifact 소유권, 보존 정책, W1 smoke와 cost-proxy의 root monoproject 전환을 안전하게 완성한다.

**Architecture:** 양 도메인은 서로 import하지 않는 동일 계약의 execution helper를 유지한다. helper는 raw dbt preflight와 실제 dbt 실행을 분리하고, OpenLineage가 활성화된 실제 materialization 명령만 `dbt-ol`로 실행한다. DAG는 task graph·실패 분류·metrics 의미를 소유하고 selector membership은 dbt manifest가 소유한다.

**Tech Stack:** Python 3, Airflow `PythonOperator`, dbt Core, `openlineage-dbt` (`dbt-ol`), pytest

## Global Constraints

- 수정 범위는 `domains/traffic/**`, `domains/weather/**`이며 landing/Bronze 파일은 수정하지 않는다.
- commit, push, PR 생성은 하지 않는다.
- root dbt project 기본 경로는 `/opt/airflow/dbt`, override는 `ASK_SEOUL_DBT_PROJECT_DIR`이다.
- `ASK_SEOUL_DBT_OPENLINEAGE_*` 값은 실제 `dbt-ol` subprocess에서만 표준 `OPENLINEAGE_*`로 변환한다.
- `ASK_SEOUL_DBT_ARTIFACT_RETENTION_RUNS`는 기본 10이며 1 이상의 정수만 허용한다.
- Traffic/Weather task graph, callback, retry, pool, W1 isolated schema와 cleanup 순서를 유지한다.

---

### Task 1: execution helper parity 계약

**Files:**
- Modify: `domains/traffic/tests/test_traffic_dbt_execution.py`
- Modify: `domains/weather/tests/test_weather_dbt_execution.py`
- Modify: `domains/traffic/traffic_dbt_execution.py`
- Modify: `domains/weather/weather_dbt_execution.py`

**Interfaces:**
- Consumes: `execute_dbt_phase(...)`, `attempt_paths(...)`
- Produces: 분리된 preflight/execution target·log, command별 optional artifact 경로, observed artifact 및 `actual_attempted` 상태

- [ ] OpenLineage disabled/raw command, enabled `dbt-ol`, env 격리, parent ID 보존, missing configuration/wrapper 실패 테스트를 먼저 추가하고 RED를 확인한다.
- [ ] preflight와 execution 경로 분리, stale artifact reset/무시, `source freshness`의 `sources.json`, deps의 artifact 없음 테스트를 추가하고 RED를 확인한다.
- [ ] deps 성공 후 동일 pipeline run directory만 보존하는 retention 테스트와 invalid override fail-fast 테스트를 추가하고 RED를 확인한다.
- [ ] 양 helper에 같은 동작을 구현하고 각 도메인 테스트를 GREEN으로 만든다.

### Task 2: Traffic DAG artifact·pool 연결

**Files:**
- Create: `domains/traffic/traffic_ingest/common/resources.py`
- Modify: `domains/traffic/traffic_incident_transform.py`
- Modify: `domains/traffic/traffic_snapshot_recovery.py`
- Modify: `domains/traffic/traffic_dbt_failure.py`
- Modify: `domains/traffic/tests/test_traffic_transform_dbt_selection.py`
- Modify: `domains/traffic/tests/test_traffic_snapshot_recovery.py`
- Modify: `domains/traffic/tests/test_traffic_dbt_failure.py`

**Interfaces:**
- Consumes: execution helper의 observed `run_results_path`, `sources_path`, `manifest_path`
- Produces: metrics용 `run_results_path`, 실제 존재 artifact만 담는 failure/recovery record, `TRINO_HEAVY_POOL`

- [ ] 모든 dbt task의 pool과 command별 artifact provenance 테스트를 추가하고 RED를 확인한다.
- [ ] transform/recovery caller와 failure record를 실제 artifact 경로만 사용하도록 수정한다.
- [ ] 기존 phase ordering, callback, retry와 failure classification 테스트를 GREEN으로 유지한다.

### Task 3: Weather transform 및 W1 smoke 전환

**Files:**
- Modify: `domains/weather/weather_vilage_fcst_transform.py`
- Modify: `domains/weather/weather_w1_contract_smoke.py`
- Modify: `domains/weather/tests/test_weather_transform_dbt_selection.py`
- Modify: `domains/weather/tests/test_weather_w1_contract_smoke.py`

**Interfaces:**
- Consumes: Weather execution helper, tags `ask_seoul_weather_w1_inputs`, `ask_seoul_weather_w1_bridge`
- Produces: root project 기반 W1 `PythonOperator` phases와 isolated schema subprocess env

- [ ] Weather transform의 실제 artifact XCom/metrics 테스트를 새 필드로 갱신한다.
- [ ] W1이 Bash/model literal 대신 PythonOperator/tag를 사용하고 vars·schema·pool·cleanup을 보존하는 테스트를 RED로 추가한다.
- [ ] W1 `run_dbt_smoke_phase`와 task factory를 구현하고 GREEN을 확인한다.

### Task 4: cost-proxy compile 소유권

**Files:**
- Modify: `domains/weather/weather_ingest/weather_traffic_cost_proxy.py`
- Modify: `domains/weather/tests/test_weather_traffic_cost_proxy.py`

**Interfaces:**
- Consumes: root project env override와 raw dbt compile
- Produces: compile invocation별 고유 target/log/manifest, 동적 경로를 제외한 stable execution fingerprint

- [ ] CASE의 반복 project 필드 제거와 root project fingerprint 테스트를 RED로 추가한다.
- [ ] compile별 고유 경로, stale manifest reset, exact manifest read 테스트를 RED로 추가한다.
- [ ] raw dbt compile과 read-only benchmark 의미를 유지하며 구현한다.

### Task 5: boundary test 정리 및 전체 검증

**Files:**
- Modify: `domains/weather/tests/test_domain_boundary.py`
- Verify: `domains/weather/tests/test_weather_domain_boundary.py`

**Interfaces:**
- Consumes: mocked `changed_paths` unit tests와 AST dependency tests
- Produces: live git/worktree 상태에 의존하지 않는 deterministic suite

- [ ] `test_current_branch_changes_obey_the_domain_boundary`만 제거한다.
- [ ] targeted Traffic/Weather 테스트를 실행한다.
- [ ] `python -m pytest domains/traffic/tests domains/weather/tests -q -p no:cacheprovider`를 실행한다.
- [ ] `python -m compileall -q domains/traffic domains/weather`, `git diff --check`, 변경 범위 및 landing/Bronze 미변경을 확인한다.
