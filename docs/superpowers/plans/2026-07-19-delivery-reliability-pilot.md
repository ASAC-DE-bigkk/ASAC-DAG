# Delivery Reliability Pilot 구현 계획

> **에이전트 작업 지침:** 이 계획은 `superpowers:subagent-driven-development` 또는 `superpowers:executing-plans`로 task별 실행한다.

**목표:** 정규화된 read-only evidence로 최근 7일 Weather/Traffic run-level 납품 신뢰성 리포트를 결정적으로 생성한다.

**아키텍처:** 외부 의존성이 없는 common package가 `domain × scheduled_run_id`별 evidence 한 행을 검증하고 Bronze/transform/Gold 결과를 분류한 뒤 공급, 납품, SLA, completeness, freshness, recovery 지표를 집계한다. CLI는 정규화된 JSON evidence bundle을 읽고 Airflow, R2, Trino, warehouse table에 쓰지 않은 채 JSON, CSV 또는 Markdown을 stdout으로 출력한다.

**기술 스택:** Python 3, dataclasses, argparse, json, csv, pytest

## 공통 제약

- base branch는 `origin/dev`이며 `feat/431-delivery-reliability-pilot`에서만 작업한다.
- 수집, manifest, transform, Gold model, schedule, production state를 변경하지 않는다.
- Bronze publication gate인 `SUCCESS + is_publishable` 계약을 유지한다.
- 사용할 수 없는 evidence는 `NOT_AVAILABLE`로 보존하고 0으로 바꾸지 않는다.
- 내부 timestamp는 UTC를 사용하고 모든 출력 순서를 결정적으로 유지한다.

---

### Task 1: Evidence 계약과 상태 분류

**파일:**
- 생성: `common/delivery_reliability/__init__.py`
- 생성: `common/delivery_reliability/contract.py`
- 테스트: `common/tests/test_delivery_reliability_contract.py`

**인터페이스:**
- 입력: pilot evidence JSON 문서의 정규화된 dictionary.
- 출력: `DeliveryEvidence.from_mapping`, `DeliveryState`, 검증된 UTC timestamp.

- [ ] delivered, zero-row, partial, source failure, contract failure, missing evidence, duplicate grain 테스트를 작성한다.
- [ ] package가 없어 테스트가 실패하는지 확인한다.
- [ ] 최소 immutable evidence 계약과 classifier를 구현한다.
- [ ] 테스트 통과를 확인한다.

### Task 2: 최근 7일 집계 지표

**파일:**
- 생성: `common/delivery_reliability/aggregate.py`
- 테스트: `common/tests/test_delivery_reliability_aggregate.py`

**인터페이스:**
- 입력: 검증된 `DeliveryEvidence` 행.
- 출력: rate, latency distribution input, state count, MTTR recovery event를 포함한 `build_pilot_report(rows)`.

- [ ] supply rate, delivery rate, SLA rate, completeness, freshness, failure-to-next-delivery MTTR 테스트를 작성한다.
- [ ] aggregation API가 없어 테스트가 실패하는지 확인한다.
- [ ] 사용할 수 없는 지표를 `None`으로 보존하는 결정적 집계를 구현한다.
- [ ] 테스트 통과를 확인한다.

### Task 3: 검산 가능한 pilot 출력

**파일:**
- 생성: `common/delivery_reliability/render.py`
- 생성: `scripts/delivery_reliability_pilot.py`
- 테스트: `common/tests/test_delivery_reliability_render.py`

**인터페이스:**
- 입력: `runs`를 포함한 JSON object와 output format.
- 출력: run ID, timestamp, final row count, state, summary rate, MTTR을 포함한 안정적인 JSON, CSV 또는 Markdown.

- [ ] 안정적인 JSON, CSV, Markdown 출력과 잘못된 duplicate input 테스트를 작성한다.
- [ ] rendering/CLI 동작이 없어 테스트가 실패하는지 확인한다.
- [ ] renderer와 read-only CLI를 구현한다.
- [ ] 신규 및 기존 targeted test를 실행한다.

### Task 4: 검증과 handoff

**파일:**
- 검증 중 수정이 필요할 때만 Task 1-3에서 추가한 파일을 변경한다.

**인터페이스:**
- 입력: 전체 branch diff.
- 출력: Gate B evidence와 사용자 검증용 remote feature branch.

- [ ] changed-path compileall과 targeted pytest를 실행한다.
- [ ] `git diff --check`와 diff 기반 secret scan을 실행한다.
- [ ] read-only 동작과 domain 경계를 기준으로 diff를 검토한다.
- [ ] commit/push 전에 Gate B 결과를 보고한다.
