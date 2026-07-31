# Weather·Traffic 운영 이벤트 행수 출처 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Weather와 Traffic의 raw, Bronze, D1 제품 이벤트에 정본 기반 `row_count`와 `rows_source`를 기록한다.

**Architecture:** 공통 `product_observability.py`가 행수와 출처의 일관성을 검증하고, 각 도메인 DAG가 자신이 소유한 XCom과 manifest 구조에서 행수를 추출한다. 기존 SHA-256 `event_id`, fail-open R2 writer, Traffic receipt acknowledgement와 Asset 발행 순서는 유지한다.

**Tech Stack:** Python 3.11, Apache Airflow, pytest, Cloudflare R2

## Global Constraints

- `row_count=NULL`은 미측정이고 `row_count=0`은 실제 0건이다.
- 행수가 있으면 정본 `rows_source`가 필요하다.
- 실제 API, R2, D1, Trino에 쓰지 않고 단위테스트의 가짜 XCom만 사용한다.
- D1 `_ops_*`, reconciler, ops-dashboard, Gold 행수 조회는 범위 밖이다.
- commit, push, PR은 사용자 별도 승인 전까지 수행하지 않는다.

---

### Task 1: 공통 행수 출처 계약

**Files:**
- Modify: `common/ops/product_observability.py`
- Test: `common/tests/test_product_observability.py`

**Interfaces:**
- Consumes: 기존 `build_product_event(context, ...)`
- Produces: `build_product_event(..., row_count: int | None, rows_source: str = "not_observed")`

- [ ] **Step 1: 계약 실패 테스트 추가**

`row_count`와 `rows_source`의 유효·무효 조합, 실제 0건, `event_id` 멱등성을 테스트한다.

- [ ] **Step 2: 공통 계약 구현**

`product-observability/v2`와 닫힌 rows source 목록을 추가하고 모순된 조합을 `ValueError`로 거부한다.

- [ ] **Step 3: 공통 테스트 실행**

Run: `python -m pytest -q common/tests/test_product_observability.py`

Expected: 모든 테스트 PASS

### Task 2: Weather와 D1 배선

**Files:**
- Modify: `common/serving/dag_factory.py`
- Test: `common/serving/tests/test_dag_factory.py`
- Modify: `domains/weather/weather_vilage_fcst_bronze.py`
- Test: `domains/weather/tests/test_weather_bronze_module_wiring.py`

**Interfaces:**
- Consumes: KMA raw object `row_count`, Bronze verify task 반환값, `ProductRecord.published_row_count`
- Produces: `raw_manifest`, `bronze_run_manifest`, `publication_ledger` 출처의 제품 이벤트

- [ ] **Step 1: Weather raw·Bronze와 D1 출처 테스트 추가**

raw object 행수 합계, Bronze 검증 행수, 손상된 XCom의 `not_observed`, D1 publication ledger 출처를 검증한다.

- [ ] **Step 2: Weather 성공 콜백 구현**

기존 generic 성공 콜백을 Weather 전용 안전 추출 콜백으로 교체한다. 실패 콜백은 변경하지 않는다.

- [ ] **Step 3: D1 출처 배선**

`record_publication_events()`가 `rows_source="publication_ledger"`를 전달하게 한다.

- [ ] **Step 4: Weather와 serving 테스트 실행**

Run: `python -m pytest -q common/serving/tests/test_dag_factory.py domains/weather/tests/test_weather_bronze_module_wiring.py`

Expected: 모든 테스트 PASS

### Task 3: Traffic Incident 배선

**Files:**
- Modify: `domains/traffic/traffic_incident_landing.py`
- Modify: `domains/traffic/traffic_incident_bronze.py`
- Test: `domains/traffic/tests/test_traffic_incident_landing_dag.py`
- Test: `domains/traffic/tests/test_traffic_bronze_module_wiring.py`

**Interfaces:**
- Consumes: landing `raw_result.parsed_rows`, materializer가 보존한 검증 행수 합계
- Produces: Incident raw와 Bronze 제품 이벤트 및 materializer XCom의 집계 `row_count`

- [ ] **Step 1: Incident raw·Bronze 출처 테스트 추가**

정상 행수, 빈 materializer의 0건, 누락 행수의 `not_observed`를 검증한다.

- [ ] **Step 2: Incident raw 콜백 구현**

landing task XCom의 `raw_result.parsed_rows`를 안전하게 읽는다.

- [ ] **Step 3: Incident Bronze 집계와 콜백 구현**

materializer 반환 계약에 publishability와 독립적인 검증 행수 합계를 가산한다. acknowledgement 콜백 순서는 유지한다.

- [ ] **Step 4: Incident 테스트 실행**

Run: `python -m pytest -q domains/traffic/tests/test_traffic_incident_landing_dag.py domains/traffic/tests/test_traffic_bronze_module_wiring.py domains/traffic/tests/test_traffic_incident_materializer.py`

Expected: 모든 테스트 PASS

### Task 4: Traffic Flow 배선과 통합 회귀

**Files:**
- Modify: `domains/traffic/traffic_flow_bronze.py`
- Test: `domains/traffic/tests/test_traffic_flow_bronze_wiring.py`

**Interfaces:**
- Consumes: Flow landing `expected_rows`, Flow materializer `row_count`
- Produces: Flow raw와 Bronze 제품 이벤트

- [ ] **Step 1: Flow raw·Bronze 출처 테스트 추가**

정상 행수와 손상된 XCom의 `not_observed`를 검증한다.

- [ ] **Step 2: Flow 성공 콜백 구현**

현재 task XCom에서 단계별 필드를 읽되 누락·타입 오류를 fail-open 처리한다.

- [ ] **Step 3: 관련 테스트 통합 실행**

Run: `python -m pytest -q common/tests/test_product_observability.py common/serving/tests/test_dag_factory.py domains/weather/tests/test_weather_bronze_module_wiring.py domains/traffic/tests/test_traffic_incident_landing_dag.py domains/traffic/tests/test_traffic_bronze_module_wiring.py domains/traffic/tests/test_traffic_incident_materializer.py domains/traffic/tests/test_traffic_flow_bronze_wiring.py`

Expected: 모든 테스트 PASS
