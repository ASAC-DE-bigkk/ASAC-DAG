# Traffic Batched Materialization Capacity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Traffic Incident 신규 receipt를 set-based Iceberg batch로 처리하고 15분 cron과 2-lane resource routing으로 backlog 없이 지속 실행한다.

**Architecture:** `IncidentMaterializer`가 pending receipt 전체를 준비한 뒤 manifest, Bronze/audit, verification을 batch adapter에 위임한다. Incident/Flow ingest와 snapshot-pinned transform은 별도 1-slot pool을 사용하며 로컬 dev Trino만 최대 query 2개를 허용한다.

**Tech Stack:** Python 3.11, Apache Airflow 3.2.2, Trino 482, Iceberg, Cloudflare R2, pytest, Docker Compose.

## Global Constraints

- root `.airflowignore`, Weather 및 다른 도메인 코드를 수정하지 않는다.
- 동일 table writer, snapshot pin, supersession, exact verification을 유지한다.
- secret과 개인 기록 md를 stage/commit/push하지 않는다.
- DAG 변경은 `domains/traffic/**`, root capacity 변경은 로컬 dev harness 파일로 제한한다.

---

### Task 1: Set-based Bronze/audit writer

**Files:**
- Modify: `domains/traffic/traffic_ingest/bronze.py`
- Modify: `domains/traffic/traffic_ingest/bronze_batch.py`
- Modify: `domains/traffic/tests/test_traffic_bronze_batch_atomicity.py`
- Modify: `domains/traffic/tests/test_traffic_bronze_idempotency.py`

**Interfaces:**
- Produces: `load_traffic_bronze_batches(raw_results_by_run_id, ...) -> dict[str, dict[str, object]]`
- Produces: `replace_seoul_traffic_bronze_snapshots(cursor, qualified_table, pages) -> dict[str, int]`

- [ ] 여러 run ID가 audit/Bronze 각각 한 DELETE·INSERT를 사용하는 실패 테스트를 작성한다.
- [ ] 모든 raw payload가 준비되기 전 cursor mutation이 0인지 실패 테스트를 작성한다.
- [ ] 기존 단일 receipt loader 계약을 유지하며 batch loader와 set-based writer를 최소 구현한다.
- [ ] zero-row audit, duplicate run ID, SQL escaping, per-run inserted count를 테스트한다.
- [ ] 대상 테스트를 GREEN으로 만든다.

### Task 2: Batch manifest와 materializer orchestration

**Files:**
- Modify: `domains/traffic/traffic_ingest/run_manifest.py`
- Modify: `domains/traffic/traffic_ingest/incident_pipeline.py`
- Modify: `domains/traffic/traffic_ingest/runtime.py`
- Modify: `domains/traffic/tests/test_traffic_run_manifest.py`
- Modify: `domains/traffic/tests/test_traffic_incident_materializer.py`

**Interfaces:**
- Produces: `TrafficRunManifest.start_many`, `publish_many`, `fail_many`
- Consumes: Task 1 batch loader
- Produces: materializer의 단일 preflight, 단일 batch load, 단일 batch verify 경로

- [ ] 여러 manifest row가 status당 한 MERGE를 사용하는 실패 테스트를 작성한다.
- [ ] 신규 receipt 여러 개가 loader/verifier를 각각 한 번만 호출하는 실패 테스트를 작성한다.
- [ ] verified receipt와 신규 receipt 혼합 batch의 결과·asset metadata 순서를 테스트한다.
- [ ] batch 실패 시 모든 active receipt의 FAILED manifest와 pending 보존을 테스트한다.
- [ ] batch manifest/orchestration/runtime adapter를 구현하고 GREEN으로 만든다.

### Task 3: Cron-only materializer와 2-lane pool routing

**Files:**
- Modify: `domains/traffic/traffic_ingest/assets.py`
- Modify: `domains/traffic/traffic_ingest/common/resources.py`
- Modify: `domains/traffic/traffic_incident_bronze.py`
- Modify: `domains/traffic/traffic_flow_bronze.py`
- Modify: `domains/traffic/traffic_incident_transform.py`
- Modify: `domains/traffic/traffic_flow_transform.py`
- Modify: `domains/traffic/traffic_gold_transform.py`
- Modify: `domains/traffic/traffic_ingest/transform_dag_support.py`
- Modify: `domains/traffic/tests/test_traffic_assets.py`
- Modify: Traffic pool wiring tests

**Interfaces:**
- Produces: `TRINO_INGEST_POOL`, `TRINO_TRANSFORM_POOL`, legacy `TRINO_HEAVY_POOL`

- [ ] `materializer_schedule()`이 dev cron string만 반환하는 실패 테스트를 작성한다.
- [ ] Incident/Flow Bronze가 ingest pool, Silver/Gold가 transform pool을 쓰는 실패 테스트를 작성한다.
- [ ] recovery/reliability/maintenance의 legacy pool 유지 테스트를 고정한다.
- [ ] 최소 production wiring 변경을 구현하고 관련 테스트를 GREEN으로 만든다.

### Task 4: Local dev Trino capacity

**Files:**
- Modify locally: root `docker-compose.yml`
- Modify locally: root `trino/resource-groups.json`
- Modify locally: root `.env.example`
- Modify locally: root runtime hardening/deploy verification tests

**Interfaces:**
- Produces: Trino container 12GB, root hard concurrency 2, ingest/transform pool 각각 1 slot

- [ ] root runtime test를 먼저 12GB·concurrency 2·신규 pool 기대값으로 변경해 RED를 확인한다.
- [ ] compose/resource group/example/pool bootstrap을 수정한다.
- [ ] root runtime test를 GREEN으로 만든다.

### Task 5: Verification, review, integration, deployment

**Files:**
- Verify: `domains/traffic/**`
- Verify: root dev harness

- [ ] focused tests와 `python -m compileall -q domains/traffic`를 실행한다.
- [ ] Traffic 전체 suite를 실행한다.
- [ ] Airflow 3 container import와 DAG pool/schedule을 확인한다.
- [ ] diff에 `.airflowignore`, Weather, 다른 도메인이 없는지 확인한다.
- [ ] code review에서 Critical/Important 지적을 해결한다.
- [ ] 경로 지정 commit 후 dev PR을 생성·병합한다.
- [ ] clean runtime을 merge SHA로 재배포한다.
- [ ] pending count/oldest age, materializer duration, Trino memory, pool concurrency와 새 downstream cycle을 검증한다.
