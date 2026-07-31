# TOPIS Empty Response Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** TOPIS AccInfo의 빈 응답만 같은 page에서 한 번 재시도하고 반복 실패는 명시적으로 노출한다.

**Architecture:** parser가 빈 응답을 전용 예외로 분류하고 landing page fetch 경계가 그 예외만 한 번 재시도한다. raw/checkpoint 기록은 유효 응답 파싱 이후에만 수행한다.

**Tech Stack:** Python 3.11, pytest, Airflow DAG domain module

## Global Constraints

- business error와 malformed non-empty XML은 재시도하지 않는다.
- 빈 응답을 성공 또는 0건 snapshot으로 처리하지 않는다.
- prod 적재나 수동 DAG trigger 없이 단위 테스트로 검증한다.

---

### Task 1: AccInfo 빈 응답 제한 재시도

**Files:**
- Modify: `domains/traffic/traffic_ingest/acc_info.py`
- Modify: `domains/traffic/traffic_ingest/landing.py`
- Test: `domains/traffic/tests/test_traffic_landing_module.py`

**Interfaces:**
- Consumes: `TopisPageSource.fetch_page(start_index, end_index) -> tuple[int, bytes]`
- Produces: 빈 응답에만 최대 2회 호출하는 `TrafficLanding.collect`

- [ ] **Step 1: Write the failing tests**

`empty -> valid`, `empty -> empty`, `malformed -> valid`의 요청 횟수와 최종 결과를
검증한다.

- [ ] **Step 2: Run tests to verify RED**

Run: `python -m pytest domains/traffic/tests/test_traffic_landing_module.py -q`

Expected: `empty -> valid`가 현재 첫 빈 응답의 XML parse failure로 실패한다.

- [ ] **Step 3: Implement the minimal behavior**

AccInfo parser에서 빈 본문을 전용 예외로 분류하고 landing에서 그 예외만 한 번
재호출한다.

- [ ] **Step 4: Run focused and domain tests**

Run: `python -m pytest domains/traffic/tests/test_traffic_landing_module.py domains/traffic/tests/test_traffic_landing_runtime.py -q`

Expected: 모든 선택 테스트 PASS.

- [ ] **Step 5: Commit and publish**

변경 파일만 명시적으로 stage하고 `dev` 대상 PR을 생성한다.

