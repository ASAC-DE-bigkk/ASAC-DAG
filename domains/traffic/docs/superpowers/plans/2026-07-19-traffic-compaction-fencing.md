# Traffic Compaction Fencing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** R2 managed compaction과 Traffic Silver MERGE의 rewrite 경쟁을 차단하고, Traffic Silver/Gold transform을 독립 DAG와 early admission으로 운영해 반복 실패와 1-slot 낭비를 제거한다.

**Architecture:** 기존 dbt canonical MERGE와 fail-closed test는 유지한다. ASAC-DAG에서 Traffic maintenance를 기존 Traffic pool로 fencing하고, pure snapshot-evidence/admission module을 추가한 뒤 기존 transform을 Silver 전용으로 축소하고 Gold DAG를 분리한다. R2 table setting과 정확한 stale-row 복구는 소스 검증 후 dev에서만 수행한다.

**Tech Stack:** Python 3.12, Apache Airflow 3.2, dbt-trino, Trino Iceberg, pytest, Cloudflare R2 Data Catalog

## Global Constraints

- dev Weather/Traffic만 수정·실행·검증한다.
- Commerce, Citydata, Transit, Culture 파일과 DAG/model은 수정·탐색·실행하지 않는다.
- 기존 Gold가 소비하는 Citydata snapshot ID는 기존 adapter를 통한 read-only scalar 입력만 유지한다.
- `.env`, secret, token, password, webhook은 읽거나 출력하지 않는다.
- `source_record_id`, publishability, pinned snapshot, 단일 MERGE, full-history test, idempotency를 낮추지 않는다.
- Trino `hardConcurrencyLimit=1`과 Airflow `trino_traffic_heavy=1`을 유지한다.
- maintenance, W2, recovery, backfill은 pause를 유지하고 실행하지 않는다.
- 새 GitHub issue를 생성하지 않는다. commit/push/PR/merge는 이 승인된 브랜치에서 수행한다.

---

### Task 1: Traffic maintenance 작업을 Traffic writer pool로 fencing

**Files:**
- Modify: `domains/weather/weather_iceberg_maintenance.py`
- Modify: `domains/weather/tests/test_weather_iceberg_maintenance_dag.py`

**Interfaces:**
- Produces: `maintenance_pool_for_table(table: str) -> str`
- Preserves: #419 immutable plan, table × operation checkpoints, circuit breaker

- [ ] **Step 1: Traffic/Weather pool routing failing test 작성**

```python
def test_maintenance_routes_traffic_tables_to_traffic_writer_pool():
    module = load_maintenance_module()
    for table in module.TRAFFIC_TABLES:
        assert module.maintenance_pool_for_table(table) == "trino_traffic_heavy"
        for operation in module.OPERATIONS:
            task = module.dag.task_dict[module.action_task_id(
                module.CANONICAL_TABLES.index(table) + 1, table, operation
            )]
            assert task.kwargs["pool"] == "trino_traffic_heavy"


def test_maintenance_keeps_weather_tables_in_weather_pool():
    module = load_maintenance_module()
    weather_tables = set(module.CANONICAL_TABLES) - set(module.TRAFFIC_TABLES)
    assert all(
        module.maintenance_pool_for_table(table) == module.TRINO_HEAVY_POOL
        for table in weather_tables
    )
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/weather/tests/test_weather_iceberg_maintenance_dag.py -q`

Expected: `TRAFFIC_TABLES` 또는 `maintenance_pool_for_table` 부재로 FAIL.

- [ ] **Step 3: 최소 routing 구현**

```python
TRAFFIC_TRINO_HEAVY_POOL = "trino_traffic_heavy"
TRAFFIC_TABLES = frozenset(
    {
        "bronze_seoul_traffic_incident",
        "bronze_seoul_traffic_incident_request_audit",
        "silver_seoul_traffic_incident",
        "gold_traffic_incident_summary",
    }
)


def maintenance_pool_for_table(table: str) -> str:
    if table not in CANONICAL_TABLES:
        raise ValueError("maintenance table is outside canonical allowlist")
    return TRAFFIC_TRINO_HEAVY_POOL if table in TRAFFIC_TABLES else TRINO_HEAVY_POOL
```

각 action task의 `pool`을 `maintenance_pool_for_table(table)`로 설정한다.

- [ ] **Step 4: GREEN 및 #419 regression 확인**

Run: `python -m pytest domains/weather/tests/test_weather_iceberg_maintenance.py domains/weather/tests/test_weather_iceberg_maintenance_dag.py -q`

Expected: 전체 PASS.

- [ ] **Step 5: commit**

```bash
git add domains/weather/weather_iceberg_maintenance.py domains/weather/tests/test_weather_iceberg_maintenance_dag.py
git commit -m "fix(traffic): fence Silver maintenance writers"
```

### Task 2: Iceberg Silver snapshot fence pure module 추가

**Files:**
- Create: `domains/traffic/traffic_ingest/silver_snapshot_fence.py`
- Create: `domains/traffic/tests/test_traffic_silver_snapshot_fence.py`

**Interfaces:**
- Produces: `SilverSnapshotEvidence`, `collect_silver_snapshot_evidence()`, `assert_safe_post_write()`, `assert_snapshot_unchanged()`
- Consumes: Trino `$snapshots`, `$files` metadata only

- [ ] **Step 1: evidence parsing 및 race failing tests 작성**

```python
def test_post_write_allows_existing_compacted_files_but_rejects_new_ones():
    baseline = evidence(10, "append", ("s3://dev/data/compacted-old.parquet",))
    safe = evidence(11, "overwrite", ("s3://dev/data/compacted-old.parquet",))
    assert_safe_post_write(baseline, safe)
    raced = evidence(
        12,
        "replace",
        (
            "s3://dev/data/compacted-old.parquet",
            "s3://dev/data/compacted-new.parquet",
        ),
    )
    with pytest.raises(ExternalCompactionRace, match="new managed-compaction"):
        assert_safe_post_write(baseline, raced)


def test_test_fence_requires_exact_post_write_snapshot():
    expected = evidence(11, "overwrite", ())
    assert_snapshot_unchanged(expected, expected)
    with pytest.raises(ExternalCompactionRace, match="snapshot changed"):
        assert_snapshot_unchanged(expected, evidence(12, "replace", ()))
```

cursor test는 exact dev relation만 조회하고 `SHOW TABLES`나 다른 schema discovery를 하지 않는지 검증한다.

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_silver_snapshot_fence.py -q`

Expected: module import FAIL.

- [ ] **Step 3: immutable evidence와 strict parser 구현**

```python
@dataclass(frozen=True)
class SilverSnapshotEvidence:
    snapshot_id: int
    committed_at: str
    operation: str
    compacted_files: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "committed_at": self.committed_at,
            "operation": self.operation,
            "compacted_files": list(self.compacted_files),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "SilverSnapshotEvidence":
        required = {"snapshot_id", "committed_at", "operation", "compacted_files"}
        if set(value) != required:
            raise SnapshotFenceTelemetryError("snapshot evidence fields are invalid")
        snapshot_id = value["snapshot_id"]
        compacted_files = value["compacted_files"]
        if not isinstance(snapshot_id, int) or isinstance(snapshot_id, bool) or snapshot_id <= 0:
            raise SnapshotFenceTelemetryError("snapshot id is invalid")
        if not isinstance(compacted_files, list) or any(
            not isinstance(path, str) or not path for path in compacted_files
        ):
            raise SnapshotFenceTelemetryError("compacted file list is invalid")
        return cls(
            snapshot_id=snapshot_id,
            committed_at=str(value["committed_at"]),
            operation=str(value["operation"]),
            compacted_files=tuple(sorted(compacted_files)),
        )


def assert_safe_post_write(
    baseline: SilverSnapshotEvidence,
    current: SilverSnapshotEvidence,
) -> None:
    new_compacted = set(current.compacted_files) - set(baseline.compacted_files)
    if new_compacted:
        raise ExternalCompactionRace("new managed-compaction file appeared")
    if current.snapshot_id != baseline.snapshot_id and current.operation == "replace":
        raise ExternalCompactionRace("unexpected replace snapshot after Silver MERGE")
```

`collect_silver_snapshot_evidence()`는 exact `iceberg_dev.traffic.silver_seoul_traffic_incident` metadata table만 조회하고 cursor/connection을 항상 닫는다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_silver_snapshot_fence.py -q`

Expected: 전체 PASS.

- [ ] **Step 5: commit**

```bash
git add domains/traffic/traffic_ingest/silver_snapshot_fence.py domains/traffic/tests/test_traffic_silver_snapshot_fence.py
git commit -m "feat(traffic): detect external Silver rewrites"
```

### Task 3: Versioned transform admission과 Silver Asset 계약 추가

**Files:**
- Create: `domains/traffic/traffic_ingest/transform_admission.py`
- Create: `domains/traffic/tests/test_traffic_transform_admission.py`
- Modify: `domains/traffic/traffic_ingest/assets.py`
- Modify: `domains/traffic/tests/test_traffic_assets.py`

**Interfaces:**
- Produces: `TransformIdentity`, `TransformSuccessMarker`, `admission_decision()`
- Produces asset: `iceberg://traffic/incident/silver`

- [ ] **Step 1: marker strictness와 skip 조건 failing tests 작성**

```python
def test_silver_admission_skips_only_matching_identity_and_output_snapshot():
    identity = TransformIdentity.silver("incident-1")
    marker = TransformSuccessMarker(
        version=1,
        pipeline="silver",
        identity=identity,
        output_snapshot_id=44,
        compacted_files_fingerprint="a" * 64,
    )
    assert admission_decision(marker, identity, output_snapshot_id=44,
                              compacted_files_fingerprint="a" * 64).skip
    assert not admission_decision(marker, identity, output_snapshot_id=45,
                                  compacted_files_fingerprint="a" * 64).skip


def test_marker_rejects_unknown_fields_and_malformed_identity():
    with pytest.raises(TransformAdmissionError):
        TransformSuccessMarker.from_json('{"version":1,"extra":true}')
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_admission.py domains/traffic/tests/test_traffic_assets.py -q`

Expected: new module/asset constants 부재로 FAIL.

- [ ] **Step 3: pure domain marker와 asset 구현**

```python
TRAFFIC_INCIDENT_SILVER_ASSET = "iceberg://traffic/incident/silver"
TRAFFIC_INCIDENT_SILVER_ASSET_REF = Asset(TRAFFIC_INCIDENT_SILVER_ASSET)
TRAFFIC_INCIDENT_SILVER_MATERIALIZED_ALIAS = AssetAlias(
    "traffic_incident_silver_materialized"
)
```

marker JSON은 sorted compact JSON으로 직렬화하고 exact field set, positive snapshot IDs, non-empty run IDs, 64-char lowercase fingerprint를 검증한다. marker가 없으면 `RUN`, identity/evidence가 정확히 같으면 `SKIP`, 다르면 `RUN`, malformed이면 fail-closed 한다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_admission.py domains/traffic/tests/test_traffic_assets.py -q`

Expected: 전체 PASS.

- [ ] **Step 5: commit**

```bash
git add domains/traffic/traffic_ingest/transform_admission.py domains/traffic/tests/test_traffic_transform_admission.py domains/traffic/traffic_ingest/assets.py domains/traffic/tests/test_traffic_assets.py
git commit -m "feat(traffic): add transform admission contract"
```

### Task 4: dbt phase spec를 Silver와 Gold로 분리

**Files:**
- Modify: `domains/traffic/traffic_ingest/transform_specs.py`
- Modify: `domains/traffic/traffic_ingest/transform_dag_support.py`
- Modify: `domains/traffic/tests/test_traffic_transform_contract.py`

**Interfaces:**
- Produces: `SILVER_DBT_PHASE_SPECS`, `GOLD_DBT_PHASE_SPECS`
- Extends: `DbtPhaseSpec.citydata_snapshot_required`, `DbtPhaseSpec.silver_fence_mode`

- [ ] **Step 1: phase ownership failing test 작성**

```python
def test_transform_phase_specs_have_single_pipeline_owner():
    silver_ids = tuple(spec.task_id for spec in SILVER_DBT_PHASE_SPECS)
    gold_ids = tuple(spec.task_id for spec in GOLD_DBT_PHASE_SPECS)
    assert set(silver_ids).isdisjoint(gold_ids)
    assert silver_ids[-2:] == ("dbt_run_silver", "dbt_test_silver")
    assert gold_ids[-2:] == ("dbt_run_gold", "dbt_test_gold")
    assert all(not spec.citydata_snapshot_required for spec in SILVER_DBT_PHASE_SPECS)
    assert all(
        spec.citydata_snapshot_required
        for spec in GOLD_DBT_PHASE_SPECS
        if spec.snapshot_required
    )
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_contract.py -q`

Expected: split constants 부재로 FAIL.

- [ ] **Step 3: phase specs와 task adapter 확장**

Silver는 deps, source freshness, availability, Bronze contract, Silver run/test만 소유한다. Gold는 deps, seed/common dimension, Gold run/test를 소유한다. axes/admin run/test는 기존 test-tier selector로 FULL일 때만 실행하고 첫 bootstrap은 marker가 없으므로 FULL decision을 유지한다.

`build_dbt_phase_task()`는 `citydata_snapshot_required`와 `silver_fence_mode`를 `run_dbt_phase` op kwargs로 넘긴다. pool과 priority 계약은 바꾸지 않는다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_contract.py -q`

Expected: 전체 PASS.

- [ ] **Step 5: commit**

```bash
git add domains/traffic/traffic_ingest/transform_specs.py domains/traffic/traffic_ingest/transform_dag_support.py domains/traffic/tests/test_traffic_transform_contract.py
git commit -m "refactor(traffic): separate Silver and Gold phases"
```

### Task 5: 공용 dbt runtime에 snapshot fence를 연결

**Files:**
- Create: `domains/traffic/traffic_ingest/transform_runtime.py`
- Modify: `domains/traffic/traffic_incident_transform.py`
- Modify: `domains/traffic/tests/test_traffic_transform_contract.py`
- Modify: `domains/traffic/tests/test_traffic_transform_failures.py`

**Interfaces:**
- Produces: `run_dbt_phase()`, classified failure adapters, metrics adapter
- Consumes: Task 2 fence and Task 4 spec kwargs

- [ ] **Step 1: pre/post write와 test fence failing tests 작성**

```python
def test_silver_run_captures_baseline_and_returns_post_write_evidence(monkeypatch):
    monkeypatch.setattr(runtime, "collect_silver_snapshot_evidence", evidence_sequence(10, 11))
    result = runtime.run_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        snapshot_task_id="resolve_traffic_snapshot_run",
        silver_persisted=False,
        snapshot_required=True,
        citydata_snapshot_required=False,
        silver_fence_mode="write",
        threads=2,
        ti=successful_ti(task_id="dbt_run_silver", snapshot_run_id="incident-1"),
        run_id="manual__silver_fence",
        params={"target": "dev"},
    )
    assert result["silver_snapshot_evidence"]["snapshot_id"] == 11


def test_silver_test_rejects_snapshot_change_before_dbt(monkeypatch):
    ti = ti_with_run_result(snapshot_id=11)
    monkeypatch.setattr(runtime, "collect_silver_snapshot_evidence", lambda: evidence(12))
    with pytest.raises(FakeAirflowFailException, match="EXTERNAL_COMPACTION_RACE"):
        runtime.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_silver",
            snapshot_task_id="resolve_traffic_snapshot_run",
            silver_persisted=True,
            snapshot_required=True,
            citydata_snapshot_required=False,
            silver_fence_mode="verify",
            threads=2,
            ti=ti,
            run_id="manual__silver_test_fence",
            params={"target": "dev"},
        )
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_contract.py domains/traffic/tests/test_traffic_transform_failures.py -q`

Expected: fence adapter 부재로 FAIL.

- [ ] **Step 3: 기존 실행 코드를 공용 runtime으로 이동하고 fence 연결**

dbt subprocess, artifact isolation, failure classification, Discord callback 계약은 내용 변경 없이 이동한다. Silver write는 dbt 전후 evidence를 수집하고, verify는 `dbt_run_silver` XCom evidence와 dbt test 전후 current evidence를 비교한다. fence failure는 `AirflowFailException("EXTERNAL_COMPACTION_RACE: Silver snapshot changed")`로 즉시 드러낸다.

- [ ] **Step 4: GREEN 및 import regression 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_contract.py domains/traffic/tests/test_traffic_transform_failures.py domains/traffic/tests/test_traffic_dbt_execution_lineage.py -q`

Expected: 전체 PASS.

- [ ] **Step 5: commit**

```bash
git add domains/traffic/traffic_ingest/transform_runtime.py domains/traffic/traffic_incident_transform.py domains/traffic/tests/test_traffic_transform_contract.py domains/traffic/tests/test_traffic_transform_failures.py
git commit -m "feat(traffic): fence Silver dbt commits"
```

### Task 6: Silver/Gold DAG 분리와 early admission 배선

**Files:**
- Modify: `domains/traffic/traffic_incident_transform.py`
- Create: `domains/traffic/traffic_gold_transform.py`
- Modify: `domains/traffic/traffic_ingest/transform_dag_support.py`
- Modify: `domains/traffic/tests/traffic_transform_test_support.py`
- Modify: `domains/traffic/tests/test_traffic_transform_dag.py`
- Create: `domains/traffic/tests/test_traffic_gold_transform_dag.py`

**Interfaces:**
- `traffic_incident_transform`: Incident Bronze → Silver Asset
- `traffic_gold_transform`: Silver Asset 또는 compatible Flow Bronze → Gold
- Variable keys: `ask_seoul.traffic.silver_transform.last_success.v1`, `ask_seoul.traffic.gold_transform.last_success.v1`

- [ ] **Step 1: DAG schedule/order/admission failing tests 작성**

```python
def test_silver_dag_pins_and_admits_before_any_dbt_phase():
    dag = load_transform_module().dag
    assert dag.kwargs["schedule"].uri == TRAFFIC_INCIDENT_BRONZE_ASSET
    assert dag.task_dict["validate_dev_runtime"].downstream_task_ids == {
        "resolve_traffic_snapshot_run"
    }
    assert dag.task_dict["resolve_traffic_snapshot_run"].downstream_task_ids == {
        "admit_traffic_silver_snapshot"
    }
    assert dag.task_dict["admit_traffic_silver_snapshot"].downstream_task_ids == {
        "dbt_deps"
    }


def test_gold_dag_is_independent_and_owns_test_tier_marker():
    dag = load_gold_transform_module().dag
    assert {asset.uri for asset in dag.kwargs["schedule"].assets} == {
        TRAFFIC_INCIDENT_SILVER_ASSET,
        TRAFFIC_FLOW_BRONZE_ASSET,
    }
    assert "dbt_run_silver" not in dag.task_ids
    assert dag.task_dict["dbt_test_gold"].downstream_task_ids == {
        "mark_traffic_gold_success"
    }
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_dag.py domains/traffic/tests/test_traffic_gold_transform_dag.py -q`

Expected: Gold DAG와 admission task 부재로 FAIL.

- [ ] **Step 3: Silver DAG 구현**

Silver resolver는 Incident manifest만 pin하고 Citydata resolver를 호출하지 않는다. admission은 marker identity와 current Silver evidence가 모두 같을 때 `AirflowSkipException`으로 heavy chain을 skip한다. Silver test 성공 후 marker를 기록하고 alias outlet으로 exact Incident run ID, Silver snapshot ID, row contract metadata를 발행한다.

- [ ] **Step 4: Gold DAG 구현**

Gold resolver는 Silver event의 Incident parent, compatible Flow, 기존 Citydata scalar snapshot을 pin한다. Gold만 기존 test-tier selection/marking을 소유한다. 동일 tuple 성공 marker면 skip하고, 새 tuple이면 seed/common prerequisite tier와 Gold run/test를 실행한다. 기존 classified failure와 metrics teardown을 유지한다.

- [ ] **Step 5: GREEN 확인**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_dag.py domains/traffic/tests/test_traffic_gold_transform_dag.py domains/traffic/tests/test_traffic_transform_contract.py -q`

Expected: 전체 PASS.

- [ ] **Step 6: commit**

```bash
git add domains/traffic/traffic_incident_transform.py domains/traffic/traffic_gold_transform.py domains/traffic/traffic_ingest/transform_dag_support.py domains/traffic/tests/traffic_transform_test_support.py domains/traffic/tests/test_traffic_transform_dag.py domains/traffic/tests/test_traffic_gold_transform_dag.py
git commit -m "refactor(traffic): split Silver and Gold transform DAGs"
```

### Task 7: Traffic 범위 regression 및 Airflow/dbt 정적 검증

**Files:**
- Modify: `domains/traffic/README.md`

- [ ] **Step 1: Python unit regression**

Run: `python -m pytest domains/traffic/tests domains/weather/tests/test_weather_iceberg_maintenance.py domains/weather/tests/test_weather_iceberg_maintenance_dag.py -q`

Expected: 기존 intentional skip 외 실패 0.

- [ ] **Step 2: compile**

Run: `python -m compileall -q domains/traffic domains/weather/weather_iceberg_maintenance.py domains/weather/weather_ingest/iceberg_maintenance.py`

Expected: exit 0, tracked `__pycache__` 0.

- [ ] **Step 3: scope diff 확인**

Run: `git diff --name-only origin/dev...HEAD`

Expected: `domains/traffic/**`와 `domains/weather/weather_iceberg_maintenance.py`, 해당 Weather test만 포함.

- [ ] **Step 4: Traffic 운영 문서 갱신**

`domains/traffic/README.md`의 transform 흐름을 Incident Bronze → Silver Asset → Gold로 바꾸고,
Silver/Gold admission marker와 R2 automatic compaction 비활성 계약을 기록한다.

- [ ] **Step 5: container Airflow import와 dbt parse/compile**

Clean runtime mount에서 Weather/Traffic `.airflowignore` allowlist를 유지한 채 두 transform DAG import, `dbt parse`, Traffic selector compile을 실행한다. 금지 domain은 parse/test하지 않는다.

### Task 8: R2 containment, exact repair, dev smoke

- [ ] **Step 1: Traffic transform만 일시 pause하고 active run drain 확인**

Raw landing과 Bronze는 유지한다. maintenance/W2/recovery/backfill은 계속 pause한다.

- [ ] **Step 2: Cloudflare table-level automatic compaction 비활성화**

기존 Dashboard session으로 dev의 exact `silver_seoul_traffic_incident` table만 변경한다. secret/API token은 조회하지 않는다.

- [ ] **Step 3: duplicate/lineage preflight 재검증**

Expected: exact 7 key, 각 old/new 1행, Bronze unique, old lineage non-latest. 다르면 repair를 중단한다.

- [ ] **Step 4: exact stale lineage 단일 DELETE**

구 lineage run ID와 7개 `source_record_id` conjunction만 삭제한다. 신규 row나 다른 snapshot은 변경하지 않는다.

- [ ] **Step 5: post-repair targeted 검증**

Expected: duplicate 0, null key 0, latest mismatch 0, non-publishable lineage 0. current snapshot ID와 row count를 기록한다.

- [ ] **Step 6: feature branch runtime smoke**

Silver checkpoint 1회와 Gold checkpoint 1회를 실행한다. 동일 snapshot 재trigger는 admission에서 heavy phase를 skip해야 한다. maintenance는 실행하지 않는다.

### Task 9: review, PR, merge, 최신 dev 재배포

- [ ] **Step 1: verification-before-completion 및 code review**

전체 diff, tests, Airflow import, dbt compile, dev data evidence를 검토하고 P1 이상 이슈가 있으면 수정한다.

- [ ] **Step 2: push 및 ready PR 생성**

공용 PR template을 UTF-8 body file로 사용하고 base `dev`, issue link 없음, Weather/Traffic 영향과 검증 결과를 적는다.

- [ ] **Step 3: PR checks 확인 후 merge**

다른 domain file diff 0, required checks PASS일 때만 merge한다.

- [ ] **Step 4: clean deployment worktree를 최신 revision으로 맞춤**

ASAC-DAG와 ASAC-DBT를 각각 merge 후 `origin/dev` exact commit으로 detach한다. dirty user checkout은 건드리지 않는다.

- [ ] **Step 5: local dev 재배포**

`scripts/deploy.sh`는 사용하지 않고 `docker compose up -d --build`를 실행한다.

- [ ] **Step 6: 최종 smoke**

Weather/Traffic allowlist, DAG import, pool 1-slot, paused maintenance/recovery, Traffic Silver/Gold run, targeted dbt tests, final Trino query를 확인한다.
