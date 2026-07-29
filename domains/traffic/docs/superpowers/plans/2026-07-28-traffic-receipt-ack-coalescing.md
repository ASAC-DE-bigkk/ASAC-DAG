# Traffic receipt acknowledgement coalescing fence 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:test-driven-development` for every behavior change and `superpowers:verification-before-completion` before publishing.

**Goal:** 이미 acknowledgement된 verified Traffic incident receipt가 stale batch 목록에서 manifest와 receipt write를 반복하지 않게 하여, 5분 수집 주기에서 Traffic Bronze의 불필요한 제어-plane 지연을 제거한다.

**Architecture:** `TrafficSnapshotReceipts`가 pending marker 존재 여부를 제공하고, `IncidentMaterializer`가 exact-preflight 성공 receipt만 manifest 시작 직전에 다시 확인한다. marker가 사라진 receipt는 side effect 없이 coalesce하며, marker가 남은 receipt는 asset callback 실패 복구를 위해 현행 verified-skip을 유지한다.

**Tech Stack:** Python 3.11, Apache Airflow 3.2.2, Cloudflare R2/S3 JSON receipts, Trino/Iceberg, pytest.

## 범위 제약

- 변경 파일은 `domains/traffic/**`로 제한한다.
- root `.airflowignore`, Weather 및 다른 도메인은 수정하지 않는다.
- 실행 중 DAG를 pause하지 않으며 수동 trigger는 `safe-trigger-dag.sh` 외에는 사용하지 않는다.
- secret과 사용자 개인 기록 md는 커밋하지 않는다.

## Task 1: receipt marker 조회 계약을 테스트로 고정

**Files:**

- Modify: `domains/traffic/tests/test_traffic_snapshot_receipt.py`
- Modify: `domains/traffic/traffic_ingest/snapshot_receipt.py`

- [ ] landed receipt의 pending marker는 `is_pending`이 true인지 실패 테스트로 작성한다.
- [ ] materialized만 기록한 상태도 acknowledgement 전까지 true인지 테스트한다.
- [ ] 정상 acknowledgement 후 false인지 테스트한다.
- [ ] `TrafficSnapshotReceipts.is_pending`을 side-effect-free strict read로 구현한다. missing-object만 false이며 R2 오류는 task failure로 전파한다.
- [ ] `pytest domains/traffic/tests/test_traffic_snapshot_receipt.py -q`를 통과시킨다.

## Task 2: stale batch coalescing fence를 테스트 우선으로 구현

**Files:**

- Modify: `domains/traffic/tests/test_traffic_incident_materializer.py`
- Modify: `domains/traffic/traffic_ingest/incident_pipeline.py`

- [ ] preflight 후 marker가 삭제된 verified receipt가 manifest/load/verify/receipt/asset 모두 생략되는 RED test를 작성한다.
- [ ] marker가 남아 있는 verified receipt는 기존 verified-skip의 manifest publish·receipt·asset 복구 동작을 보존하는 test를 고정한다.
- [ ] `ReceiptQueue` protocol과 test fake에 `is_pending`을 추가한다.
- [ ] `manifest.start` 전 fence를 최소 구현하고 관련 test를 GREEN으로 만든다.

## Task 3: 회귀 검증과 범위 감사

**Files:**

- Verify: `domains/traffic/tests/test_traffic_incident_materializer.py`
- Verify: `domains/traffic/tests/test_traffic_snapshot_receipt.py`
- Verify: `domains/traffic/tests/**`

- [ ] 두 대상 test를 우선 실행한다.
- [ ] `python -m compileall -q domains/traffic`를 실행한다.
- [ ] Traffic test suite를 실행한다.
- [ ] diff가 `domains/traffic/**` 외 경로를 건드리지 않고 `.airflowignore` 변경이 없는지 확인한다.

## Task 4: PR·병합·dev runtime 검증

- [ ] 공용 PR template을 확인하고 UTF-8 body file로 `dev` 대상 PR을 생성한다. 별도 이슈는 사용자 지시로 생략했다고 명시한다.
- [ ] required checks와 변경 범위를 확인한 뒤 PR을 병합한다.
- [ ] clean runtime checkout에서 DAG를 최신 `origin/dev` SHA로 맞추고 `docker compose up -d --build`로 재배포한다.
- [ ] Airflow import와 Traffic DAG 상태를 확인한다.
- [ ] 수동 trigger 없이 다음 scheduler cycle에서 Traffic Bronze -> Flow -> Transform/Gold의 성공 및 stale receipt 지연 제거를 확인한다.
