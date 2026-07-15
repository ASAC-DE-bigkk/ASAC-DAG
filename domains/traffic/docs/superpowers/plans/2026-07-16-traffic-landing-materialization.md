# Traffic Landing Materialization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development for every behavior change and superpowers:verification-before-completion before publishing.

**Goal:** 5분 Traffic raw landing을 Trino materialization에서 분리하고, receipt와 Asset으로 모든 snapshot을 멱등하게 Bronze까지 처리하면서 Flow/Transform snapshot 정합성과 reliability 경보 품질을 보장한다.

**Architecture:** `traffic_incident_landing`은 API/R2/ledger/receipt/Raw Asset만 소유한다. `traffic_incident_bronze`는 Raw Asset + 15분 fallback으로 pending receipt를 oldest-first materialize하고 `AssetAlias`로 실제 처리 시에만 Bronze Asset을 발행한다. Flow는 exact Incident Asset parent를 소비하며, Transform은 Incident 또는 compatible Flow 이벤트에서 deterministic snapshot pair를 선택한다.

**Tech Stack:** Python 3.11, Apache Airflow 3.2.2 Assets/AssetAlias/AssetOrTimeSchedule, Cloudflare R2(S3), Trino 482, Iceberg, pytest, dbt.

## Global Constraints

- [ ] production 변경은 `domains/traffic/**`에만 둔다.
- [ ] root dirty checkout과 다른 task worktree를 수정하지 않는다.
- [ ] Weather와 다른 도메인 코드는 수정하지 않고 회귀 테스트만 실행한다.
- [ ] 각 production behavior보다 실패 테스트를 먼저 작성하고 RED를 확인한다.
- [ ] secret 값을 fixture, log, receipt, 문서에 넣지 않는다.

---

## Task 1: Traffic Asset 계약을 한 곳에 둔다

**Files:**

- Create: `domains/traffic/traffic_ingest/assets.py`
- Create: `domains/traffic/tests/test_traffic_assets.py`

- [ ] Raw/Incident Bronze/Flow Bronze URI, alias, metadata required fields를 테스트로 정의한다.
- [ ] `pytest domains/traffic/tests/test_traffic_assets.py -q`를 실행해 import 실패 RED를 확인한다.
- [ ] triggering event 추출, 최신 event 선택, metadata 검증, conditional alias publish helper를 최소 구현한다.
- [ ] Airflow 3에서는 `AssetOrTimeSchedule(CronTriggerTimetable(...), Raw Asset)`을 만들고 로컬 Airflow 2 test runtime에서는 Asset-only schedule을 반환하는 명시적 compatibility branch를 둔다.
- [ ] 테스트가 GREEN인지 확인한다.

## Task 2: 결정적 receipt queue를 구현한다

**Files:**

- Create: `domains/traffic/traffic_ingest/snapshot_receipt.py`
- Create: `domains/traffic/tests/test_traffic_snapshot_receipt.py`
- Modify: `domains/traffic/traffic_ingest/runtime.py`

- [ ] LANDED/MATERIALIZED round-trip, oldest-first, retry idempotency, identity-safe key, divergent overwrite rejection, Asset success-before-ack ordering을 테스트한다.
- [ ] `pytest domains/traffic/tests/test_traffic_snapshot_receipt.py -q` RED를 확인한다.
- [ ] Airflow 비의존 dataclass와 injected `JsonStorage` queue를 구현한다.
- [ ] R2 builder는 기존 dev credential resolution을 재사용한다.
- [ ] 테스트가 GREEN인지 확인한다.

## Task 3: Landing lifecycle과 one-task DAG를 만든다

**Files:**

- Create: `domains/traffic/traffic_ingest/incident_pipeline.py`
- Create: `domains/traffic/traffic_incident_landing.py`
- Create: `domains/traffic/tests/test_traffic_incident_pipeline.py`
- Create: `domains/traffic/tests/test_traffic_incident_landing_dag.py`
- Modify: `domains/traffic/traffic_ingest/bronze_dag_support.py`

- [ ] ledger STARTED → runtime guard → collect → LANDED → ledger SUCCESS 순서와 failure ledger 보존을 테스트한다.
- [ ] one visible task, 5분 schedule, `max_active_runs=2`, no Trino/no pool, Raw Asset metadata를 테스트한다.
- [ ] 두 신규 테스트 파일 RED를 확인한다.
- [ ] lifecycle service와 얇은 DAG wrapper를 구현한다.
- [ ] 기존 scheduled ledger helper 상수는 Landing DAG ID를 명확히 분리한다.
- [ ] 신규 테스트 GREEN을 확인한다.

## Task 4: Incident Bronze를 one-task receipt materializer로 바꾼다

**Files:**

- Modify: `domains/traffic/traffic_ingest/incident_pipeline.py`
- Modify: `domains/traffic/traffic_incident_bronze.py`
- Modify: `domains/traffic/tests/test_traffic_bronze_module_wiring.py`
- Create: `domains/traffic/tests/test_traffic_incident_materializer.py`

- [ ] oldest-first, landing run ID 보존, first failure stop, success callback ack, latest-only asset metadata, empty queue no-publish를 테스트한다.
- [ ] 기존 scheduled DAG가 one task, `trino_heavy`, Asset+fallback, `max_active_runs=1`인지 테스트를 먼저 바꿔 RED를 확인한다.
- [ ] materializer service와 conditional `AssetAlias` publish를 구현한다.
- [ ] recollect/backfill DAG 계약은 기존 동작을 보존한다.
- [ ] 관련 테스트 GREEN을 확인한다.

## Task 5: Flow를 exact Incident Asset parent에 연결한다

**Files:**

- Modify: `domains/traffic/traffic_ingest/flow_info.py`
- Modify: `domains/traffic/traffic_ingest/flow_landing.py`
- Modify: `domains/traffic/traffic_flow_bronze.py`
- Modify: `domains/traffic/tests/test_traffic_flow_info.py`
- Modify: `domains/traffic/tests/test_traffic_flow_bronze.py`
- Modify: `domains/traffic/tests/test_traffic_flow_bronze_wiring.py`

- [ ] exact `incident_run_id` SQL, parent raw lineage, two visible tasks, no cron, conditional stale suppression, Flow Asset metadata를 테스트한다.
- [ ] 관련 테스트 RED를 확인한다.
- [ ] landing task와 heavy materialize/verify/publish task를 구현한다.
- [ ] parent가 latest Incident가 아닐 때 SUCCESS Bronze는 보존하고 alias event만 추가하지 않는다.
- [ ] 관련 테스트 GREEN을 확인한다.

## Task 6: Transform snapshot pair를 deterministic하게 선택한다

**Files:**

- Modify: `domains/traffic/traffic_ingest/transform_dag_support.py`
- Modify: `domains/traffic/traffic_incident_transform.py`
- Modify: `domains/traffic/tests/test_traffic_transform_contract.py`
- Modify: `domains/traffic/tests/test_traffic_transform_dag.py`

- [ ] Incident-only, exact Flow pair, batched Incident coalescing, stale Flow non-regression, OR Asset schedule을 테스트한다.
- [ ] 관련 테스트 RED를 확인한다.
- [ ] `SnapshotPair` resolver를 구현하고 기존 dbt var 이름을 유지한다.
- [ ] latest optional Flow manifest 조회 의존성을 제거한다.
- [ ] 관련 테스트 GREEN을 확인한다.

## Task 7: reliability 분모와 Discord identity를 수정한다

**Files:**

- Modify: `domains/traffic/traffic_ingest/run_ledger.py`
- Modify: `domains/traffic/traffic_ingest/reliability/config.py`
- Modify: `domains/traffic/traffic_ingest/reliability/report.py`
- Modify: `domains/traffic/traffic_ingest/reliability/discord.py`
- Modify: `domains/traffic/traffic_reliability_report.py`
- Modify: `domains/traffic/tests/test_traffic_run_ledger.py`
- Modify: `domains/traffic/tests/test_traffic_reliability_discord.py`
- Modify: `domains/traffic/tests/test_traffic_reliability_report_composition.py`
- Modify: `domains/traffic/tests/test_traffic_reliability_dag.py`

- [ ] `success + failed + running + grace == expected`와 terminal-in-grace를 테스트한다.
- [ ] discontiguous gap grouping과 같은 gap 확장 시 stable fingerprint를 테스트한다.
- [ ] cadence DAG가 Landing, publishability DAG가 Bronze인지 테스트한다.
- [ ] pending count/oldest age report를 테스트한다.
- [ ] 관련 테스트 RED를 확인하고 최소 구현 후 GREEN을 확인한다.

## Task 8: 구조·회귀 검증을 수행한다

**Files:**

- Modify: `domains/traffic/tests/test_traffic_entrypoint_architecture.py`
- Modify: `domains/traffic/README.md`
- Modify: `domains/traffic/docs/bronze-pipeline-reference.md`

- [ ] Traffic entrypoint task count와 import boundary를 architecture test로 고정한다.
- [ ] 운영 문서를 Landing → receipt → Bronze → Flow → Transform 구조로 갱신한다.
- [ ] `python -m compileall -q domains/traffic` 실행.
- [ ] `python -m pytest domains/traffic/tests domains/weather/tests -q` 실행; baseline `536 passed` 이상 확인.
- [ ] `git diff --name-only origin/dev...HEAD`가 `domains/traffic/**`만 포함하는지 확인.
- [ ] Airflow 3.2.2 container에서 `airflow dags list-import-errors`와 targeted DAG import를 확인.

## Task 9: PR을 만들고 검증 후 merge한다

- [ ] shared PR template을 UTF-8 body file로 복사해 `Fixes #390`, 변경 요약, 검증 결과, R2/table 영향 범위를 작성한다.
- [ ] 변경 scope를 stage하고 intentional commit을 만든다.
- [ ] `fix/390-traffic-landing-materialization`을 push한다.
- [ ] `dev` base ready PR을 생성하고 한글 encoding을 `gh pr view`로 확인한다.
- [ ] PR checks와 review를 확인하고 실패 시 원인을 수정·재검증한다.
- [ ] 모든 required check가 통과하면 merge하고 merge SHA를 기록한다.

## Task 10: 최신 DAG+DBT dev를 함께 재배포하고 smoke한다

- [ ] DAG와 DBT에 clean detached runtime worktree를 만들고 각 최신 `origin/dev` exact SHA를 기록한다.
- [ ] merge SHA가 DAG latest `origin/dev` ancestor인지 확인한다.
- [ ] clean root runtime worktree/override로 두 exact worktree를 mount한다.
- [ ] `docker compose up -d --build` 실행; `scripts/deploy.sh`는 사용하지 않는다.
- [ ] container health, Airflow import, DAG registration을 확인한다. unrelated domain import failure는 별도 기록하고 gate에서 제외한다.
- [ ] `traffic_incident_landing`, `traffic_incident_bronze`, `traffic_flow_bronze`, `traffic_incident_transform`을 unpause한다.
- [ ] landing smoke run ID, LANDED/MATERIALIZED receipt, Bronze/Flow parent, Silver/Gold snapshot 및 row count를 확인한다.
- [ ] 5분 scheduled landing과 15분 reliability report를 확인한다.
- [ ] in-app Airflow dashboard에서 최신 네 DAG 성공 run을 시각 확인한다.
- [ ] 결과와 exact SHAs, run IDs, table counts를 handoff한다.
