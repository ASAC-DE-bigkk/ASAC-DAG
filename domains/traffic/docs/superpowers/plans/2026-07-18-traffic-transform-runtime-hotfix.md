# Traffic Transform Runtime Hotfix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Traffic transform runtime 병목과 재배포 후 package 유실 장애를 줄이면서 기존 Gold exact reconciliation fence, run-local artifact 격리, optional Flow 의미를 보존한다.

**Architecture:** Traffic stale Incident resolver는 `TrafficRunManifest.coalesce_many()` batch MERGE로 바꾸고 기존 `coalesce()`는 호환 wrapper로 유지한다. Traffic/Weather dbt 실행은 각 domain-owned `_dbt_execution` Module에 같은 self-heal 패턴을 mirror 적용하고, domain `common/resources.py`에서 공유하는 `LOCAL/TRINO` workload와 `threads=2`를 DAG phase spec에서 executor까지 명시적으로 전달한다. Traffic Gold cadence는 같은 transform DAG 안에서 frozen `TrafficTestDecision(tier/hour/day)`를 XCom에 고정하고 Airflow Variable ledger는 성공 후 그 frozen bucket만 mark한다. tier decision task는 `validate_dev_runtime` 뒤, 기존 `dbt_deps/source/contract/seed/admin` pre-snapshot phase 앞에 둔다. 기존 pre-snapshot phase 간 상대 순서와 resolver→Silver/Gold fence는 유지한다.

**Tech Stack:** Python 3.11, Apache Airflow DAG/PythonOperator/Variable/XCom, Trino Iceberg SQL MERGE, dbt CLI, pytest, Ruff, domain-local Traffic/Weather DAG modules.

## Global Constraints

- 설계/계획 문서는 한국어로 작성하고, 코드, 명령, 파일 경로, 식별자, 제품·라이브러리 고유명사는 원문 표기를 유지한다.
- ASAC-DAG 작업은 `domains/weather/**` 또는 `domains/traffic/**` 안에서만 수정·생성한다.
- 사용자의 명시 승인 없이 `git commit`, `git push`, PR 생성, destructive git 명령을 실행하지 않는다. 아래 commit step은 승인된 실행자가 작업 단위 완료 후 사용할 제안 명령이다.
- 기존 Traffic Gold exact-set 의미와 Gold run→test fence를 유지한다. selector를 축소하거나 run/test를 한 task로 합쳐 장애를 숨기지 않는다.
- `TrafficRunManifest`의 `COALESCED`, `is_publishable=false`, `failure_reason='replaced_by=<latest_run_id>'`, MERGE key `(source_id, dag_run_id, status)`, 동일 입력 재실행 멱등성을 유지한다.
- `require_publishable()`와 `latest_publishable_run_id()`의 COALESCED 제외 조건을 변경하지 않는다.
- run-local `dbt_packages`와 `target` 격리를 shared package cache로 되돌리지 않는다. self-heal은 사라진 동일 run-local package path만 재구성한다.
- Traffic과 Weather는 각각 독립 `_dbt_execution` Module을 소유한다. 이번 hotfix에서 새 cross-domain common Module을 만들지 않는다.
- `SnapshotPair.flow_run_id`는 nullable로 유지한다. stale Flow fallback과 `gold_traffic_incident_x_flow`의 `missing_flow` 보존 의미를 바꾸지 않는다.
- 기존 pre-snapshot `dbt_deps`, source freshness, contract/seed/admin phase 간 상대 순서를 바꾸지 않는다. `select_traffic_test_tier`는 `validate_dev_runtime` 직후에 배치해 frozen decision을 만들고, 그 뒤 기존 pre-snapshot chain을 그대로 이어간다.
- Traffic Gold test selector 이름은 ASAC-DBT #257과 정확히 맞춘다: `ask_seoul_traffic_transform_gold_gate_tests`, `ask_seoul_traffic_transform_gold_hourly_tests`, `ask_seoul_traffic_transform_gold_full_tests`.
- axes/admin 10개 Gold tests는 FULL tier에서만 실행한다. GATE/HOURLY에서는 `selector_by_test_tier`의 explicit `None` mapping으로 executor를 호출하지 않는 success no-op을 반환한다.
- Traffic test cadence marker는 선택 시점의 frozen hour/day bucket만 기록한다. 장시간 `dbt_test_gold`가 다음 hour/day로 넘어가도 mark 시점의 현재 시각으로 ledger를 재계산하지 않는다.
- Traffic test cadence ledger가 missing, inconsistent, read failure이면 FULL로 fail-closed한다. FULL marker는 hour를 먼저 쓰고 day를 나중에 써서 부분 write failure가 다음 run을 보수적으로 FULL로 돌리게 한다.
- dev 검증은 `seoul-dev` bucket과 dev schema를 우선 사용하고 secret 값을 출력하지 않는다.

---

## File Map

**Create**
- `domains/traffic/traffic_ingest/test_cadence.py` - Traffic Gold test tier enum, frozen `TrafficTestDecision`, Airflow Variable ledger key, tier 선택/mark helper를 소유한다.
- `domains/traffic/tests/test_traffic_test_cadence.py` - `TrafficTestTier` fail-closed 선택, frozen KST hour/day marker, missing/inconsistent/read-failure ledger, FULL marker write order/failure, selector mapping, marker update contract를 검증한다.

**Modify**
- `domains/traffic/traffic_ingest/run_manifest.py` - `TrafficRunManifest.coalesce_many()` batch Interface와 입력 normalization을 추가하고 기존 `coalesce()`를 wrapper로 유지한다.
- `domains/traffic/traffic_ingest/transform_dag_support.py` - stale Incident event loop를 `coalesce_many()` 1회 호출 adapter로 바꾼다. optional Flow 로직은 그대로 유지한다.
- `domains/traffic/traffic_ingest/transform_specs.py` - domain resource의 `DbtWorkload`, `TrafficTestTier` selector mapping field, `threads` field를 phase contract에 추가한다.
- `domains/traffic/traffic_incident_transform.py` - workload별 pool wiring, `threads` propagation, tier select/mark task, `dbt_test_gold` tier selector 해석을 추가한다.
- `domains/traffic/traffic_ingest/_dbt_execution/executor.py` - non-deps phase에서 package sentinel self-heal을 parse/ls 전에 수행한다.
- `domains/weather/weather_ingest/_dbt_execution/executor.py` - Traffic executor와 같은 package sentinel self-heal을 mirror 적용한다.
- `domains/traffic/traffic_ingest/common/resources.py` - Traffic domain pool constant를 `trino_traffic_heavy`로 바꾸고 `DbtWorkload` enum을 domain 공용 resource로 둔다.
- `domains/weather/weather_ingest/common/resources.py` - Weather domain pool constant를 `trino_weather_heavy`로 바꾸고 `DbtWorkload` enum을 domain 공용 resource로 둔다.
- `domains/weather/weather_vilage_fcst_transform.py` - Weather dbt transform phase spec에 `LOCAL/TRINO` workload와 `threads`를 추가하고 `dbt_deps`를 pool 밖으로 뺀다.
- `domains/weather/weather_w2_canonical_transform.py` - W2 canonical dbt transform에도 같은 workload/thread/pool contract를 적용한다.
- `domains/weather/weather_w2_observation_recovery.py` - Weather recovery가 `trino_weather_heavy` domain lane을 사용하는지 테스트 contract에 맞춘다.
- `domains/traffic/traffic_snapshot_recovery.py` - Traffic recovery dbt phases가 `trino_traffic_heavy`를 사용하고 필요한 경우 `threads=2`를 전달한다. `dbt_deps`는 recovery에서도 LOCAL로 둔다.

**Test**
- `domains/traffic/tests/test_traffic_run_manifest_module.py` - batch MERGE, normalization, SQL escaping, empty input, wrapper delegation, COALESCED publishability invariant를 검증한다.
- `domains/traffic/tests/test_traffic_transform_contract.py` - resolver batch adapter, tier selector propagation, optional Flow preservation, phase contract를 검증한다.
- `domains/traffic/tests/test_traffic_transform_dag.py` - Traffic pool/workload/priority/task ordering을 검증한다.
- `domains/traffic/tests/test_traffic_transform_failures.py` - dbt task failure callback과 LOCAL `dbt_deps` pool 예외를 검증한다.
- `domains/traffic/tests/traffic_transform_test_support.py` - Airflow fake에 `Variable`을 추가해 Traffic DAG import와 cadence task tests를 지원한다.
- `domains/traffic/tests/test_traffic_dbt_execution_artifacts.py` - Traffic package self-heal sentinel/failure behavior를 검증한다.
- `domains/traffic/tests/test_traffic_dbt_execution_lineage.py` - `threads`가 materialization command에만 붙는 기존 contract와 full propagation을 검증한다.
- `domains/weather/tests/test_weather_transform_dag.py` - Weather transform pool/workload/thread contract를 갱신한다.
- `domains/weather/tests/test_weather_transform_execution.py` - Weather transform `threads` propagation과 self-heal path를 검증한다.
- `domains/weather/tests/test_weather_dbt_execution.py` - Weather command-level thread validation을 유지/보강한다.
- `domains/weather/tests/test_weather_dbt_execution_artifacts.py` - Weather package self-heal sentinel/failure behavior를 검증한다.
- `domains/weather/tests/test_weather_w2_canonical_transform_dag.py` - W2 canonical workload/pool/thread contract를 검증한다.
- `domains/weather/tests/test_weather_w2_canonical_transform_execution.py` - W2 canonical `threads` propagation을 검증한다.
- `domains/weather/tests/test_weather_w2_observation_recovery.py` - W2 recovery pool constant 변경과 기존 serial recovery semantics를 검증한다.

## Existing Intent Summary

- `TrafficRunManifest.coalesce()`는 `c77f854d`에서 stale Bronze snapshot을 COALESCED 처리하기 위해 추가되었고, `require_publishable()`/`latest_publishable_run_id()`는 COALESCED row를 publishable latest 후보에서 제외한다.
- `traffic_ingest._dbt_execution`의 run-local path 격리는 `c219da9` 계열에서 task/try 간 stale artifact 혼입을 막기 위해 도입되었다. self-heal은 이 격리를 유지한 채 같은 run-local `dbt_packages` path만 복구해야 한다.
- Traffic transform의 pinned snapshot과 Citydata/Flow vars는 최근 `1c92946`, `5f3886d2`, `a2120475` 계열에서 강화되었다. test tier selector를 추가해도 `dbt_snapshot_variables()`의 optional Flow 조건부 emission은 그대로 둔다.

## Task 1: Traffic Manifest Batch Coalesce

**Files:**
- Modify: `domains/traffic/traffic_ingest/run_manifest.py:5-264`
- Test: `domains/traffic/tests/test_traffic_run_manifest_module.py:1-219`

**Interfaces:**
- Consumes: existing `TrafficRunManifest._cursor_factory`, `_sql_string()`, `_sql_int()`, `STATUS_COALESCED`, `MANIFEST_TABLE`.
- Produces: `TrafficRunManifest.coalesce_many(run_ids: Iterable[str], *, replacement_run_id: str) -> str | None`; existing `coalesce(run_id: str, *, replacement_run_id: str) -> str | None` delegates to it.

- [ ] **Step 1: Write failing batch MERGE test**

Add this test to `domains/traffic/tests/test_traffic_run_manifest_module.py` after `test_coalesce_records_replacement_identity_without_marking_snapshot_publishable`:

```python
def test_coalesce_many_records_many_replacements_in_one_merge():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    assert manifest.coalesce_many(
        ["scheduled__old-a", "scheduled__old-b"],
        replacement_run_id="scheduled__new",
    ) == "iceberg_dev.weather_traffic_bronze.bronze_collection_run_manifest"

    schema_statements = [
        statement for statement in cursor.statements if statement.startswith("CREATE SCHEMA")
    ]
    table_statements = [
        statement for statement in cursor.statements if statement.startswith("CREATE TABLE")
    ]
    mutations = [statement for statement in cursor.statements if statement.startswith("MERGE ")]
    assert len(schema_statements) == 1
    assert len(table_statements) == 1
    assert len(mutations) == 1
    mutation = mutations[0]
    assert "'scheduled__old-a'" in mutation
    assert "'scheduled__old-b'" in mutation
    assert mutation.count("'COALESCED'") == 2
    assert mutation.count("'replaced_by=scheduled__new'") == 2
    assert "false" in mutation
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_run_manifest_module.py::test_coalesce_many_records_many_replacements_in_one_merge -q
```

Expected: FAIL with `AttributeError: 'TrafficRunManifest' object has no attribute 'coalesce_many'`.

- [ ] **Step 3: Add failing normalization, escaping, empty, wrapper tests**

Add these tests to the same file:

```python
def test_coalesce_many_normalizes_duplicate_blank_and_replacement_ids():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.coalesce_many(
        [" old-a ", "", "old-a", "new-run", "old-b", "   "],
        replacement_run_id="new-run",
    )

    mutation = next(statement for statement in cursor.statements if statement.startswith("MERGE "))
    assert mutation.count("'old-a'") == 1
    assert mutation.count("'old-b'") == 1
    assert "'new-run'" not in mutation.split("VALUES", 1)[0]
    assert "replaced_by=new-run" in mutation


def test_coalesce_many_escapes_quoted_and_unicode_run_ids():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.coalesce_many(
        ["run'quote", "수동__교통"],
        replacement_run_id="new'run",
    )

    mutation = next(statement for statement in cursor.statements if statement.startswith("MERGE "))
    assert "'run''quote'" in mutation
    assert "'수동__교통'" in mutation
    assert "'replaced_by=new''run'" in mutation


def test_coalesce_many_empty_input_does_not_open_cursor():
    calls = []
    manifest = TrafficRunManifest(
        cursor_factory=lambda: calls.append("called") or (RecordingCursor(), "iceberg_dev", "weather_traffic_bronze")
    )

    assert manifest.coalesce_many([" ", "latest"], replacement_run_id="latest") is None
    assert calls == []


def test_single_coalesce_delegates_to_batch_interface(monkeypatch):
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (RecordingCursor(), "iceberg_dev", "weather_traffic_bronze")
    )
    calls = []

    def fake_coalesce_many(run_ids, *, replacement_run_id):
        calls.append((list(run_ids), replacement_run_id))
        return "qualified"

    monkeypatch.setattr(manifest, "coalesce_many", fake_coalesce_many)

    assert manifest.coalesce("old", replacement_run_id="new") == "qualified"
    assert calls == [(["old"], "new")]
```

- [ ] **Step 4: Run new tests to verify failures**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_run_manifest_module.py -q
```

Expected: FAIL only on new `coalesce_many`/delegation expectations.

- [ ] **Step 5: Implement `coalesce_many` and normalization**

In `domains/traffic/traffic_ingest/run_manifest.py`, change imports and methods:

```python
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Protocol
```

Replace `coalesce()` body and add `coalesce_many()`:

```python
    def coalesce(self, run_id: str, *, replacement_run_id: str) -> str | None:
        return self.coalesce_many([run_id], replacement_run_id=replacement_run_id)

    def coalesce_many(
        self,
        run_ids: Iterable[str],
        *,
        replacement_run_id: str,
    ) -> str | None:
        normalized_run_ids = _normalize_coalesced_run_ids(
            run_ids,
            replacement_run_id=replacement_run_id,
        )
        if not normalized_run_ids:
            return None

        cursor, catalog, schema = self._cursor_factory()
        qualified_schema = f"{catalog}.{schema}"
        qualified_table = f"{qualified_schema}.{MANIFEST_TABLE}"
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
        except Exception as exc:
            if "Namespace already exists" not in str(exc):
                raise
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {qualified_table} (
                source_id varchar,
                dag_id varchar,
                dag_run_id varchar,
                status varchar,
                is_publishable boolean,
                event_at timestamp(6),
                expected_rows integer,
                actual_rows integer,
                expected_raw_objects integer,
                actual_raw_objects integer,
                failure_reason varchar
            )
            WITH (format = 'PARQUET')
            """
        )
        event_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        columns = (
            "source_id, dag_id, dag_run_id, status, is_publishable, event_at, "
            "expected_rows, actual_rows, expected_raw_objects, actual_raw_objects, failure_reason"
        )
        values_rows = ", ".join(
            "("
            + ", ".join(
                (
                    _sql_string(self._source_id),
                    _sql_string("traffic_incident_bronze"),
                    _sql_string(run_id),
                    _sql_string(STATUS_COALESCED),
                    "false",
                    f"TIMESTAMP {_sql_string(event_at)}",
                    "NULL",
                    "NULL",
                    "NULL",
                    "NULL",
                    _sql_string(f"replaced_by={replacement_run_id}"),
                )
            )
            + ")"
            for run_id in normalized_run_ids
        )
        cursor.execute(
            f"""
            MERGE INTO {qualified_table} AS target
            USING (VALUES {values_rows}) AS incoming ({columns})
              ON target.source_id = incoming.source_id
             AND target.dag_run_id = incoming.dag_run_id
             AND target.status = incoming.status
            WHEN MATCHED THEN UPDATE SET
                dag_id = incoming.dag_id,
                is_publishable = incoming.is_publishable,
                event_at = incoming.event_at,
                expected_rows = incoming.expected_rows,
                actual_rows = incoming.actual_rows,
                expected_raw_objects = incoming.expected_raw_objects,
                actual_raw_objects = incoming.actual_raw_objects,
                failure_reason = incoming.failure_reason
            WHEN NOT MATCHED THEN INSERT ({columns})
            VALUES (
                incoming.source_id, incoming.dag_id, incoming.dag_run_id, incoming.status,
                incoming.is_publishable, incoming.event_at, incoming.expected_rows,
                incoming.actual_rows, incoming.expected_raw_objects, incoming.actual_raw_objects,
                incoming.failure_reason
            )
            """
        )
        return qualified_table
```

Add helper near `_sql_string()`:

```python
def _normalize_coalesced_run_ids(
    run_ids: Iterable[str],
    *,
    replacement_run_id: str,
) -> tuple[str, ...]:
    replacement = str(replacement_run_id).strip()
    unique: dict[str, None] = {}
    for value in run_ids:
        run_id = str(value).strip()
        if not run_id or run_id == replacement:
            continue
        unique.setdefault(run_id, None)
    return tuple(unique)
```

- [ ] **Step 6: Run manifest tests to verify pass**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_run_manifest_module.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit batch manifest work after approval**

Run only after user approval to commit:

```bash
git add domains/traffic/traffic_ingest/run_manifest.py domains/traffic/tests/test_traffic_run_manifest_module.py
git commit -m "fix(traffic): batch coalesced run manifest writes"
```

Expected: commit succeeds; no unrelated files staged.

## Task 2: Resolver Adapter Uses One Batch Call and Preserves Optional Flow

**Files:**
- Modify: `domains/traffic/traffic_ingest/transform_dag_support.py:45-101`
- Test: `domains/traffic/tests/test_traffic_transform_contract.py:63-288`

**Interfaces:**
- Consumes: Task 1 `TrafficRunManifest.coalesce_many(run_ids, *, replacement_run_id)`.
- Produces: Resolver calls `coalesce_many()` once for stale Incident events; stale Flow still returns `SnapshotPair(..., flow_run_id=None)`.

- [ ] **Step 1: Write failing resolver batch test**

Replace `test_traffic_snapshot_resolver_coalesces_older_asset_events` fake manifest with this batch-oriented assertion:

```python
def test_traffic_snapshot_resolver_coalesces_older_asset_events_in_one_batch(monkeypatch):
    module = load_transform_module()
    coalesced_batches = []
    monkeypatch.setattr(
        module,
        "resolve_citydata_crowding_snapshot_id",
        lambda: 8738321387624398062,
        raising=False,
    )

    class Manifest:
        def latest_publishable_run_id(self):
            return "traffic-new"

        def require_publishable(self, run_id):
            return run_id

        def coalesce_many(self, run_ids, *, replacement_run_id):
            coalesced_batches.append((list(run_ids), replacement_run_id))

    monkeypatch.setattr(module, "build_traffic_manifest", lambda: Manifest())

    def event(run_id, event_at):
        return types.SimpleNamespace(
            extra={
                "source_id": "seoul_traffic_incident",
                "bronze_run_id": run_id,
                "bronze_dag_run_id": run_id,
                "event_at": event_at,
                "load_date": "2026-07-15",
                "row_count": 7,
                "payload_hash": "a" * 64,
                "is_publishable": True,
            }
        )

    assert module.resolve_traffic_snapshot_run(
        triggering_asset_events={
            module.TRAFFIC_BRONZE_ASSET: [
                event("traffic-old-a", "2026-07-15T12:00:00+09:00"),
                event("traffic-old-b", "2026-07-15T12:00:30+09:00"),
                event("traffic-new", "2026-07-15T12:01:00+09:00"),
            ]
        }
    ) == "traffic-new"
    assert coalesced_batches == [(["traffic-old-a", "traffic-old-b"], "traffic-new")]
```

- [ ] **Step 2: Add 80 stale event adapter test**

Add:

```python
def test_traffic_snapshot_resolver_batches_large_stale_backlog(monkeypatch):
    module = load_transform_module()
    coalesced_batches = []
    monkeypatch.setattr(
        module,
        "resolve_citydata_crowding_snapshot_id",
        lambda: 8738321387624398062,
        raising=False,
    )

    class Manifest:
        def latest_publishable_run_id(self):
            return "traffic-latest"

        def require_publishable(self, run_id):
            return run_id

        def coalesce_many(self, run_ids, *, replacement_run_id):
            coalesced_batches.append((list(run_ids), replacement_run_id))

    monkeypatch.setattr(module, "build_traffic_manifest", lambda: Manifest())

    events = [
        types.SimpleNamespace(
            extra={
                "source_id": "seoul_traffic_incident",
                "bronze_run_id": f"traffic-old-{index}",
                "bronze_dag_run_id": f"traffic-old-{index}",
                "event_at": "2026-07-15T12:00:00+09:00",
                "load_date": "2026-07-15",
                "row_count": 7,
                "payload_hash": "a" * 64,
                "is_publishable": True,
            }
        )
        for index in range(80)
    ]
    events.append(
        types.SimpleNamespace(
            extra={
                "source_id": "seoul_traffic_incident",
                "bronze_run_id": "traffic-latest",
                "bronze_dag_run_id": "traffic-latest",
                "event_at": "2026-07-15T12:01:00+09:00",
                "load_date": "2026-07-15",
                "row_count": 7,
                "payload_hash": "b" * 64,
                "is_publishable": True,
            }
        )
    )

    assert module.resolve_traffic_snapshot_run(
        triggering_asset_events={module.TRAFFIC_BRONZE_ASSET: events}
    ) == "traffic-latest"
    assert len(coalesced_batches) == 1
    assert coalesced_batches[0][0] == [f"traffic-old-{index}" for index in range(80)]
    assert coalesced_batches[0][1] == "traffic-latest"
```

- [ ] **Step 3: Run resolver tests to verify failure**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_transform_contract.py::test_traffic_snapshot_resolver_coalesces_older_asset_events_in_one_batch domains\traffic\tests\test_traffic_transform_contract.py::test_traffic_snapshot_resolver_batches_large_stale_backlog -q
```

Expected: FAIL because resolver still calls `coalesce()` per stale event.

- [ ] **Step 4: Implement resolver adapter**

In `domains/traffic/traffic_ingest/transform_dag_support.py`, replace lines `78-84` loop with:

```python
    stale_incident_run_ids = [
        str(event["bronze_dag_run_id"])
        for event in incident_events
        if str(event["bronze_dag_run_id"]) != latest_incident_run_id
    ]
    incident_manifest.coalesce_many(
        stale_incident_run_ids,
        replacement_run_id=latest_incident_run_id,
    )
```

Leave lines `86-101` Flow selection unchanged.

- [ ] **Step 5: Add optional Flow preservation assertion**

In `test_stale_flow_asset_falls_forward_to_latest_incident_without_flow`, keep the existing expected pushes and ensure no Flow var is synthesized:

```python
    assert pushed == [
        (module.FLOW_SNAPSHOT_XCOM_KEY, None),
        (module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY, 8738321387624398062),
    ]
```

If the existing test already asserts this exact shape, keep it unchanged.

- [ ] **Step 6: Run Traffic transform contract tests**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_transform_contract.py -q
```

Expected: PASS for resolver and optional Flow tests.

- [ ] **Step 7: Commit resolver adapter work after approval**

Run only after user approval to commit:

```bash
git add domains/traffic/traffic_ingest/transform_dag_support.py domains/traffic/tests/test_traffic_transform_contract.py
git commit -m "fix(traffic): batch stale snapshot coalescing"
```

Expected: commit succeeds; no unrelated files staged.

## Task 3: Domain Pools, Workload Contract, and Traffic Threads Propagation

**Files:**
- Modify: `domains/traffic/traffic_ingest/common/resources.py:1`
- Modify: `domains/traffic/traffic_ingest/transform_specs.py:1-92`
- Modify: `domains/traffic/traffic_incident_transform.py:41-288`
- Modify: `domains/traffic/traffic_snapshot_recovery.py:34-370`
- Test: `domains/traffic/tests/test_traffic_transform_dag.py:23-45`
- Test: `domains/traffic/tests/test_traffic_transform_contract.py:582-709`
- Test: `domains/traffic/tests/test_traffic_transform_failures.py:16-44`
- Test: `domains/traffic/tests/test_traffic_snapshot_recovery_dag.py`

**Interfaces:**
- Consumes: existing `traffic_dbt.execute_dbt_phase(..., threads: int | None = None)`.
- Produces: `traffic_ingest.common.resources.DbtWorkload.LOCAL/TRINO`; `DbtPhaseSpec.threads`; Traffic dbt phases pass `threads` through `op_kwargs -> run_dbt_phase -> execute_dbt_phase`; `dbt_deps` has no heavy pool.

- [ ] **Step 1: Write failing Traffic pool/workload DAG assertion**

Update `domains/traffic/tests/test_traffic_transform_dag.py::test_traffic_transform_trino_tasks_do_not_inflate_pool_priority_from_chain`:

```python
def test_traffic_transform_trino_tasks_do_not_inflate_pool_priority_from_chain():
    module = load_transform_module()
    critical_task_ids = {
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_test_gold",
    }

    assert module.TRINO_HEAVY_POOL == "trino_traffic_heavy"
    for task_id in module.DBT_PHASE_TASK_IDS:
        task = module.dag.task_dict[task_id]
        if task_id == "dbt_deps":
            assert "pool" not in task.kwargs or task.kwargs["pool"] in (None, "default_pool")
            assert task.kwargs["op_kwargs"]["threads"] is None
        else:
            assert task.kwargs["pool"] == module.TRINO_HEAVY_POOL
            assert task.kwargs["op_kwargs"]["threads"] == 2
        assert task.kwargs["weight_rule"] == "absolute"
        expected_priority = (
            module.PIN_CRITICAL_PRIORITY
            if task_id in critical_task_ids
            else 1
        )
        assert task.kwargs["priority_weight"] == expected_priority

    resolver = module.dag.task_dict[module.SNAPSHOT_TASK_ID]
    assert resolver.kwargs["pool"] == module.TRINO_HEAVY_POOL
    assert resolver.kwargs["weight_rule"] == "absolute"
    assert resolver.kwargs["priority_weight"] == module.PIN_CRITICAL_PRIORITY
```

- [ ] **Step 2: Run Traffic DAG pool test to verify failure**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_transform_dag.py::test_traffic_transform_trino_tasks_do_not_inflate_pool_priority_from_chain -q
```

Expected: FAIL because pool is still `trino_heavy`, deps uses pool, and `threads` is absent.

- [ ] **Step 3: Implement Traffic pool constant and phase spec fields**

Change `domains/traffic/traffic_ingest/common/resources.py`:

```python
from enum import Enum


class DbtWorkload(str, Enum):
    LOCAL = "local"
    TRINO = "trino"


TRINO_HEAVY_POOL = "trino_traffic_heavy"


__all__ = ["DbtWorkload", "TRINO_HEAVY_POOL"]
```

Change `domains/traffic/traffic_ingest/transform_specs.py`:

```python
from dataclasses import dataclass
from traffic_ingest.common.resources import DbtWorkload
```

Update dataclass:

```python
@dataclass(frozen=True)
class DbtPhaseSpec:
    task_id: str
    dbt_command: str
    selector: str | None = None
    silver_persisted: bool = False
    fresh_parse: bool = False
    snapshot_required: bool = False
    pin_critical: bool = False
    workload: DbtWorkload = DbtWorkload.TRINO
    threads: int | None = 2
```

Set deps:

```python
DbtPhaseSpec("dbt_deps", "deps", workload=DbtWorkload.LOCAL, threads=None),
```

Update `__all__`:

```python
__all__ = ["DBT_PHASE_SPECS", "DBT_PHASE_TASK_IDS", "DbtPhaseSpec"]
```

- [ ] **Step 4: Implement Traffic DAG factory and run propagation**

In `domains/traffic/traffic_incident_transform.py`, import `DbtWorkload` from the same domain resource module:

```python
from traffic_ingest.common.resources import DbtWorkload, TRINO_HEAVY_POOL
from traffic_ingest.transform_specs import (
    DBT_PHASE_SPECS,
    DBT_PHASE_TASK_IDS,
    DbtPhaseSpec,
)
```

Add `threads` parameter to `run_dbt_phase()`:

```python
def run_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    snapshot_task_id: str,
    silver_persisted: bool,
    fresh_parse: bool = False,
    snapshot_required: bool = False,
    threads: int | None = None,
    **context,
) -> dict[str, object]:
```

Pass to executor:

```python
        threads=threads,
        fresh_parse=fresh_parse,
```

Replace `dbt_task()` body with kwargs assembly:

```python
def dbt_task(spec: DbtPhaseSpec) -> PythonOperator:
    operator_kwargs = {
        "task_id": spec.task_id,
        "python_callable": run_dbt_phase,
        "op_kwargs": {
            "dbt_command": spec.dbt_command,
            "selector": spec.selector,
            "snapshot_task_id": SNAPSHOT_TASK_ID,
            "silver_persisted": spec.silver_persisted,
            "fresh_parse": spec.fresh_parse,
            "snapshot_required": spec.snapshot_required,
            "threads": spec.threads,
        },
        "retries": 1,
        "retry_delay": DBT_RETRY_DELAY,
        "priority_weight": (PIN_CRITICAL_PRIORITY if spec.pin_critical else 1),
        "weight_rule": "absolute",
        "on_failure_callback": record_traffic_dbt_problem,
    }
    if spec.workload is DbtWorkload.TRINO:
        operator_kwargs["pool"] = TRINO_HEAVY_POOL
    return PythonOperator(**operator_kwargs)
```

- [ ] **Step 5: Update phase contract tests**

In `test_traffic_transform_contract.py`, update expected dataclass assertions to include workload and threads:

```python
    assert {
        spec.task_id: (spec.snapshot_required, spec.pin_critical, spec.workload.value, spec.threads)
        for spec in module.DBT_PHASE_SPECS
    } == {
        "dbt_deps": (False, False, "local", None),
        "dbt_source_freshness": (False, False, "trino", 2),
        "dbt_test_traffic_incident_availability": (False, False, "trino", 2),
        "dbt_test_traffic_bronze_source_contract": (False, False, "trino", 2),
        "dbt_seed_asac_axes": (False, False, "trino", 2),
        "dbt_run_common_admin_dong_dimension": (False, False, "trino", 2),
        "dbt_test_common_admin_dong_dimension": (False, False, "trino", 2),
        "dbt_test_asac_axes_seed_contract": (False, False, "trino", 2),
        "dbt_run_silver": (True, True, "trino", 2),
        "dbt_test_silver": (True, True, "trino", 2),
        "dbt_run_gold": (True, False, "trino", 2),
        "dbt_test_gold": (True, True, "trino", 2),
    }
```

Also assert every task `op_kwargs["threads"] == spec.threads`.

- [ ] **Step 6: Update failure callback test for LOCAL deps**

In `test_traffic_transform_failures.py`, replace pool assertion:

```python
        if task_id == "dbt_deps":
            assert "pool" not in task.kwargs or task.kwargs["pool"] in (None, "default_pool")
        else:
            assert task.kwargs["pool"] == module.TRINO_HEAVY_POOL
```

- [ ] **Step 7: Apply recovery pool/thread contract**

In `domains/traffic/traffic_snapshot_recovery.py`, import `DbtWorkload` from `traffic_ingest.common.resources` so recovery and transform share one domain-local workload vocabulary without depending on `transform_specs`:

```python
from traffic_ingest.common.resources import DbtWorkload, TRINO_HEAVY_POOL
```

Update recovery dataclass:

```python
    workload: DbtWorkload = DbtWorkload.TRINO
    threads: int | None = 2
```

Set `dbt_deps` to `LOCAL/None`, pass `threads=spec.threads`, and include pool only for TRINO phases.

- [ ] **Step 8: Run Traffic workload tests**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_transform_dag.py domains\traffic\tests\test_traffic_transform_contract.py domains\traffic\tests\test_traffic_transform_failures.py domains\traffic\tests\test_traffic_snapshot_recovery_dag.py -q
```

Expected: PASS.

- [ ] **Step 9: Commit Traffic workload work after approval**

Run only after user approval to commit:

```bash
git add domains/traffic/traffic_ingest/common/resources.py domains/traffic/traffic_ingest/transform_specs.py domains/traffic/traffic_incident_transform.py domains/traffic/traffic_snapshot_recovery.py domains/traffic/tests/test_traffic_transform_dag.py domains/traffic/tests/test_traffic_transform_contract.py domains/traffic/tests/test_traffic_transform_failures.py domains/traffic/tests/test_traffic_snapshot_recovery_dag.py
git commit -m "fix(traffic): split dbt workload pool and thread contracts"
```

Expected: commit succeeds; no unrelated files staged.

## Task 4: Weather Domain Pool, Workload Contract, and Threads Propagation

**Files:**
- Modify: `domains/weather/weather_ingest/common/resources.py:1`
- Modify: `domains/weather/weather_vilage_fcst_transform.py:58-421`
- Modify: `domains/weather/weather_w2_canonical_transform.py:59-357`
- Modify: `domains/weather/weather_w2_observation_recovery.py:30-350`
- Test: `domains/weather/tests/test_weather_transform_dag.py:18-71`
- Test: `domains/weather/tests/test_weather_transform_execution.py:76-130`
- Test: `domains/weather/tests/test_weather_w2_canonical_transform_dag.py`
- Test: `domains/weather/tests/test_weather_w2_canonical_transform_execution.py`
- Test: `domains/weather/tests/test_weather_w2_observation_recovery.py:270-285`

**Interfaces:**
- Consumes: existing `weather_dbt.execute_dbt_phase(..., threads: int | None = None)`.
- Produces: Weather transform/canonical/recovery use `trino_weather_heavy` for TRINO work; `dbt_deps` is LOCAL; non-deps transform phases pass `threads=2`.

- [ ] **Step 1: Write failing Weather transform pool/thread assertion**

Update `domains/weather/tests/test_weather_transform_dag.py::test_weather_dbt_factory_preserves_phase_contracts` loop:

```python
        if task_id == "dbt_deps":
            assert "pool" not in task.kwargs or task.kwargs["pool"] in (None, "default_pool")
            assert task.kwargs["op_kwargs"]["threads"] is None
        else:
            assert task.kwargs["pool"] == module.TRINO_HEAVY_POOL
            assert task.kwargs["op_kwargs"]["threads"] == 2
```

Update `test_weather_transform_trino_tasks_do_not_inflate_pool_priority_from_chain`:

```python
    assert module.TRINO_HEAVY_POOL == "trino_weather_heavy"
    for task_id in module.DBT_PHASE_TASK_IDS:
        task = module.dag.task_dict[task_id]
        if task_id == "dbt_deps":
            assert "pool" not in task.kwargs or task.kwargs["pool"] in (None, "default_pool")
        else:
            assert task.kwargs["pool"] == module.TRINO_HEAVY_POOL
        assert task.kwargs["weight_rule"] == "absolute"
```

- [ ] **Step 2: Run Weather DAG test to verify failure**

Run:

```powershell
pytest domains\weather\tests\test_weather_transform_dag.py::test_weather_dbt_factory_preserves_phase_contracts domains\weather\tests\test_weather_transform_dag.py::test_weather_transform_trino_tasks_do_not_inflate_pool_priority_from_chain -q
```

Expected: FAIL because Weather still uses `trino_heavy`, deps has pool, and `threads` is absent from op_kwargs.

- [ ] **Step 3: Implement Weather resource constant and transform spec**

Change `domains/weather/weather_ingest/common/resources.py`:

```python
from enum import Enum


class DbtWorkload(str, Enum):
    LOCAL = "local"
    TRINO = "trino"


TRINO_HEAVY_POOL = "trino_weather_heavy"


__all__ = ["DbtWorkload", "TRINO_HEAVY_POOL"]
```

In `domains/weather/weather_vilage_fcst_transform.py`, import:

```python
from weather_ingest.common.resources import DbtWorkload, TRINO_HEAVY_POOL
```

Update dataclass:

```python
@dataclass(frozen=True)
class DbtPhaseSpec:
    task_id: str
    dbt_command: str
    selector: str | None = None
    include_project_vars: bool = True
    workload: DbtWorkload = DbtWorkload.TRINO
    threads: int | None = 2
```

Set deps:

```python
DbtPhaseSpec(
    "dbt_deps",
    "deps",
    include_project_vars=False,
    workload=DbtWorkload.LOCAL,
    threads=None,
),
```

- [ ] **Step 4: Implement Weather transform factory/run propagation**

Add `threads` to `run_dbt_phase()`:

```python
def run_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    include_project_vars: bool = True,
    snapshot_task_id: str | None = None,
    threads: int | None = None,
    **context,
) -> dict[str, object]:
```

Pass to executor:

```python
            threads=threads,
            project_dir=DBT_PROJECT,
```

Replace `dbt_task()` with kwargs assembly equivalent to Traffic:

```python
def dbt_task(spec: DbtPhaseSpec) -> PythonOperator:
    operator_kwargs = {
        "task_id": spec.task_id,
        "python_callable": run_dbt_phase,
        "op_kwargs": {
            "dbt_command": spec.dbt_command,
            "selector": spec.selector,
            "include_project_vars": spec.include_project_vars,
            "snapshot_task_id": SNAPSHOT_TASK_ID,
            "threads": spec.threads,
        },
        "weight_rule": "absolute",
        "retries": 1,
        "retry_delay": DBT_RETRY_DELAY,
        "on_failure_callback": record_weather_problem,
    }
    if spec.workload is DbtWorkload.TRINO:
        operator_kwargs["pool"] = TRINO_HEAVY_POOL
    return PythonOperator(**operator_kwargs)
```

- [ ] **Step 5: Mirror W2 canonical transform**

Apply the same dataclass/factory/run propagation in `domains/weather/weather_w2_canonical_transform.py`, importing `DbtWorkload` from `weather_ingest.common.resources`, with deps set to `LOCAL/None` and model/test phases `TRINO/2`.

- [ ] **Step 6: Preserve W2 recovery domain lane**

In `domains/weather/weather_w2_observation_recovery.py`, no new workload spec is required because recovery has one TRINO task. After resource constant change, keep:

```python
pool=TRINO_HEAVY_POOL,
pool_slots=1,
```

Update test expected pool to `trino_weather_heavy` through module constant.

- [ ] **Step 7: Update Weather execution test for thread propagation**

In `domains/weather/tests/test_weather_transform_execution.py::test_weather_dbt_model_command_writes_isolated_artifact`, call:

```python
    result = module.run_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_weather_transform_silver",
        threads=2,
        ti=ti,
        run_id="scheduled/2026:07",
        params={"target": "dev"},
    )
```

Add assertion:

```python
    assert command[command.index("--threads") + 1] == "2"
    assert "--threads" not in ls_command
```

- [ ] **Step 8: Run Weather workload tests**

Run:

```powershell
pytest domains\weather\tests\test_weather_transform_dag.py domains\weather\tests\test_weather_transform_execution.py domains\weather\tests\test_weather_w2_canonical_transform_dag.py domains\weather\tests\test_weather_w2_canonical_transform_execution.py domains\weather\tests\test_weather_w2_observation_recovery.py -q
```

Expected: PASS.

- [ ] **Step 9: Commit Weather workload work after approval**

Run only after user approval to commit:

```bash
git add domains/weather/weather_ingest/common/resources.py domains/weather/weather_vilage_fcst_transform.py domains/weather/weather_w2_canonical_transform.py domains/weather/weather_w2_observation_recovery.py domains/weather/tests/test_weather_transform_dag.py domains/weather/tests/test_weather_transform_execution.py domains/weather/tests/test_weather_w2_canonical_transform_dag.py domains/weather/tests/test_weather_w2_canonical_transform_execution.py domains/weather/tests/test_weather_w2_observation_recovery.py
git commit -m "fix(weather): split dbt workload pool and thread contracts"
```

Expected: commit succeeds; no unrelated files staged.

## Task 5: Traffic and Weather Package Self-Heal Before Parse/LS

**Files:**
- Modify: `domains/traffic/traffic_ingest/_dbt_execution/executor.py:28-145`
- Modify: `domains/weather/weather_ingest/_dbt_execution/executor.py:28-145`
- Test: `domains/traffic/tests/test_traffic_dbt_execution_artifacts.py:34-314`
- Test: `domains/weather/tests/test_weather_dbt_execution_artifacts.py`

**Interfaces:**
- Consumes: `attempt_paths(...).packages_path`, `environment.raw_environment()`, `phase_commands()`.
- Produces: non-deps phase runs self-heal `dbt deps` before fresh parse/selector `dbt ls` when `<run-local dbt_packages>/asac_axes/dbt_project.yml` is missing and `packages.yml` exists.

- [ ] **Step 1: Write failing Traffic sentinel-present test**

Add to `domains/traffic/tests/test_traffic_dbt_execution_artifacts.py`:

```python
def test_non_deps_phase_does_not_self_heal_when_package_sentinel_exists(tmp_path):
    module = load_execution_module()
    paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        invocation_id="sentinel-present",
        dbt_command="run",
    )
    (tmp_path / "packages.yml").write_text("packages:\n  - package: asac_axes\n", encoding="utf-8")
    sentinel = Path(paths.packages_path) / "asac_axes" / "dbt_project.yml"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_text("name: asac_axes\n", encoding="utf-8")
    observed = []

    def runner(command, **_kwargs):
        observed.append(command)
        if command[1] == "ls":
            return completed(command, stdout='{"unique_id":"model.asac.silver","resource_type":"model"}\n')
        write_actual_artifacts(command)
        return completed(command)

    module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        invocation_id="sentinel-present",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[1] for command in observed] == ["ls", "run"]
```

- [ ] **Step 2: Write failing Traffic sentinel-missing self-heal tests**

Add:

```python
def test_non_deps_phase_self_heals_missing_packages_before_ls(tmp_path):
    module = load_execution_module()
    (tmp_path / "packages.yml").write_text("packages:\n  - package: asac_axes\n", encoding="utf-8")
    observed = []

    def runner(command, **kwargs):
        observed.append((command, kwargs))
        if command[1] == "deps":
            packages_path = Path(kwargs["env"]["DBT_PACKAGES_INSTALL_PATH"])
            sentinel = packages_path / "asac_axes" / "dbt_project.yml"
            sentinel.parent.mkdir(parents=True)
            sentinel.write_text("name: asac_axes\n", encoding="utf-8")
            return completed(command)
        if command[1] == "ls":
            return completed(command, stdout='{"unique_id":"model.asac.silver","resource_type":"model"}\n')
        write_actual_artifacts(command)
        return completed(command)

    execution = module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        invocation_id="self-heal",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[0][1] for command in observed] == ["deps", "ls", "run"]
    deps_command, deps_kwargs = observed[0]
    assert "--target-path" not in deps_command
    assert deps_kwargs["env"]["DBT_PACKAGES_INSTALL_PATH"] == execution.paths.packages_path


def test_non_deps_phase_stops_when_self_heal_deps_fails(tmp_path):
    module = load_execution_module()
    (tmp_path / "packages.yml").write_text("packages:\n  - package: asac_axes\n", encoding="utf-8")
    observed = []

    def runner(command, **_kwargs):
        observed.append(command)
        return completed(command, returncode=1, stderr="deps failed")

    execution = module.execute_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_traffic_transform_gold",
        invocation_id="self-heal-fails",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_test_gold",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[1] for command in observed] == ["deps"]
    assert execution.completed.returncode == 1
    assert execution.actual_attempted is False


def test_non_deps_phase_fails_closed_when_sentinel_still_missing_after_deps(tmp_path):
    module = load_execution_module()
    (tmp_path / "packages.yml").write_text("packages:\n  - package: asac_axes\n", encoding="utf-8")
    observed = []

    execution = module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        invocation_id="self-heal-missing-sentinel",
        pipeline="traffic-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=lambda command, **_kwargs: observed.append(command) or completed(command),
        environ={},
    )

    assert [command[1] for command in observed] == ["deps"]
    assert execution.completed.returncode == 2
    assert "package sentinel missing" in execution.completed.stderr
    assert execution.actual_attempted is False
```

- [ ] **Step 3: Run Traffic self-heal tests to verify failure**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_dbt_execution_artifacts.py::test_non_deps_phase_does_not_self_heal_when_package_sentinel_exists domains\traffic\tests\test_traffic_dbt_execution_artifacts.py::test_non_deps_phase_self_heals_missing_packages_before_ls domains\traffic\tests\test_traffic_dbt_execution_artifacts.py::test_non_deps_phase_stops_when_self_heal_deps_fails domains\traffic\tests\test_traffic_dbt_execution_artifacts.py::test_non_deps_phase_fails_closed_when_sentinel_still_missing_after_deps -q
```

Expected: at least sentinel-missing tests FAIL because no self-heal exists.

- [ ] **Step 4: Implement Traffic executor self-heal helpers**

In `domains/traffic/traffic_ingest/_dbt_execution/executor.py`, add imports:

```python
from pathlib import Path
```

Add helpers above `execute_dbt_phase()`:

```python
def _packages_yml_exists(project_dir: str) -> bool:
    return Path(project_dir, "packages.yml").is_file()


def _package_sentinel(packages_path: str) -> Path:
    return Path(packages_path) / "asac_axes" / "dbt_project.yml"


def _self_heal_deps_command(
    *,
    executable: str,
    target: str,
    log_path: str,
) -> list[str]:
    return [
        executable,
        "deps",
        "--target",
        target,
        "--no-use-colors",
        "--log-path",
        log_path,
    ]
```

In `execute_dbt_phase()`, after both `selected_ids: tuple[str, ...] = ()` and `actual_attempted = False` are initialized, and before `for stage, command in phase_commands(...)`, insert the self-heal block. This placement is required so every early return has defined `selected_ids` and `actual_attempted` values.

```python
    if phase != "deps" and _packages_yml_exists(resolved_project):
        sentinel = _package_sentinel(paths.packages_path)
        if not sentinel.is_file():
            deps_command = _self_heal_deps_command(
                executable=resolved_executable,
                target=target,
                log_path=paths.preflight_log_path,
            )
            completed = runner(
                deps_command,
                cwd=resolved_project,
                env=raw_env,
                check=False,
                capture_output=True,
                text=True,
            )
            attempts.append(completed)
            if completed.returncode != 0:
                return DbtExecution(
                    attempts=tuple(attempts),
                    paths=paths,
                    selected_unique_ids=selected_ids,
                    actual_attempted=False,
                    existing_run_results_path=None,
                    existing_sources_path=None,
                    existing_manifest_path=None,
                    missing_expected_artifacts=(),
                )
            if not sentinel.is_file():
                attempts.append(
                    subprocess.CompletedProcess(
                        args=deps_command,
                        returncode=2,
                        stdout="",
                        stderr=f"dbt package sentinel missing after self-heal deps: {sentinel}",
                    )
                )
                return DbtExecution(
                    attempts=tuple(attempts),
                    paths=paths,
                    selected_unique_ids=selected_ids,
                    actual_attempted=False,
                    existing_run_results_path=None,
                    existing_sources_path=None,
                    existing_manifest_path=None,
                    missing_expected_artifacts=(),
                )
```

- [ ] **Step 5: Mirror self-heal in Weather executor**

Apply the same import, helpers, and insertion to `domains/weather/weather_ingest/_dbt_execution/executor.py`. Keep helper names identical because modules are domain-local and independent.

- [ ] **Step 6: Add Weather self-heal tests**

In `domains/weather/tests/test_weather_dbt_execution_artifacts.py`, add Weather equivalents of the Traffic tests, using:

```python
pipeline="weather-transform"
selector="ask_seoul_weather_transform_silver"
task_id="dbt_run_silver"
```

Expected command order and sentinel path remain the same:

```python
assert [command[0][1] for command in observed] == ["deps", "ls", "run"]
assert deps_kwargs["env"]["DBT_PACKAGES_INSTALL_PATH"] == execution.paths.packages_path
assert "--target-path" not in deps_command
```

- [ ] **Step 7: Run self-heal test suites**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_dbt_execution_artifacts.py domains\weather\tests\test_weather_dbt_execution_artifacts.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit package self-heal work after approval**

Run only after user approval to commit:

```bash
git add domains/traffic/traffic_ingest/_dbt_execution/executor.py domains/weather/weather_ingest/_dbt_execution/executor.py domains/traffic/tests/test_traffic_dbt_execution_artifacts.py domains/weather/tests/test_weather_dbt_execution_artifacts.py
git commit -m "fix(dbt): self-heal run-local packages before preflight"
```

Expected: commit succeeds; no unrelated files staged.

## Task 6: Same-DAG Traffic Gold Test Cadence and Tier Selectors

**Files:**
- Create: `domains/traffic/traffic_ingest/test_cadence.py`
- Create: `domains/traffic/tests/test_traffic_test_cadence.py`
- Modify: `domains/traffic/traffic_ingest/transform_specs.py:1-92`
- Modify: `domains/traffic/traffic_incident_transform.py:18-400`
- Test: `domains/traffic/tests/test_traffic_transform_contract.py:582-866`
- Test: `domains/traffic/tests/test_traffic_transform_dag.py:70-80`

**Interfaces:**
- Consumes: Airflow `Variable.get/set`, KST current time or logical date, `max_active_runs=1`, `dbt_test_gold` XCom-readable frozen decision.
- Produces: `TrafficTestTier`, frozen `TrafficTestDecision`, `choose_test_decision(...) -> TrafficTestDecision`, `mark_successful_decision(...) -> dict[str, str]`, `select_traffic_test_tier(**context) -> dict[str, str]`, `mark_traffic_test_tier(**context) -> dict[str, str]`, and `DbtPhaseSpec.selector_by_test_tier`.

- [ ] **Step 1: Write failing cadence unit tests**

Create `domains/traffic/tests/test_traffic_test_cadence.py`:

```python
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from traffic_transform_test_support import FakeVariable
from traffic_ingest.test_cadence import (
    TRAFFIC_GOLD_TEST_DAY_KEY,
    TRAFFIC_GOLD_TEST_HOUR_KEY,
    TrafficTestDecision,
    TrafficTestTier,
    choose_test_decision,
    mark_successful_decision,
)


KST = ZoneInfo("Asia/Seoul")


@pytest.fixture(autouse=True)
def reset_variable():
    FakeVariable.reset()


def test_first_run_fails_closed_to_full():
    decision = choose_test_decision(
        variable=FakeVariable,
        now=datetime(2026, 7, 18, 10, 12, tzinfo=KST),
    )
    assert decision == TrafficTestDecision(
        tier=TrafficTestTier.FULL,
        hour_bucket="2026-07-18T10",
        day_bucket="2026-07-18",
    )
    assert decision.as_dict() == {
        "tier": "full",
        "hour_bucket": "2026-07-18T10",
        "day_bucket": "2026-07-18",
    }


def test_same_hour_after_full_success_uses_gate():
    FakeVariable.values = {
        TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
        TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T10",
    }

    assert (
        choose_test_decision(
            variable=FakeVariable,
            now=datetime(2026, 7, 18, 10, 55, tzinfo=KST),
        ).tier
        is TrafficTestTier.GATE
    )


def test_new_hour_after_daily_success_uses_hourly():
    FakeVariable.values = {
        TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
        TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T09",
    }

    assert (
        choose_test_decision(
            variable=FakeVariable,
            now=datetime(2026, 7, 18, 10, 12, tzinfo=KST),
        ).tier
        is TrafficTestTier.HOURLY
    )


def test_new_day_uses_full():
    FakeVariable.values = {
        TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-17",
        TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-17T23",
    }

    assert (
        choose_test_decision(
            variable=FakeVariable,
            now=datetime(2026, 7, 18, 0, 12, tzinfo=KST),
        ).tier
        is TrafficTestTier.FULL
    )


@pytest.mark.parametrize(
    "values",
    [
        {TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18"},
        {TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T10"},
        {
            TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
            TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-17T23",
        },
    ],
)
def test_missing_or_inconsistent_ledger_fails_closed_to_full(values):
    FakeVariable.values = values

    assert (
        choose_test_decision(
            variable=FakeVariable,
            now=datetime(2026, 7, 18, 10, 12, tzinfo=KST),
        ).tier
        is TrafficTestTier.FULL
    )


def test_variable_read_failure_fails_closed_to_full(monkeypatch):
    class BrokenVariable:
        @staticmethod
        def get(*_args, **_kwargs):
            raise RuntimeError("metadata unavailable")

    assert (
        choose_test_decision(
            variable=BrokenVariable,
            now=datetime(2026, 7, 18, 10, 12, tzinfo=KST),
        ).tier
        is TrafficTestTier.FULL
    )


def test_full_success_marks_day_and_hour():
    decision = TrafficTestDecision(
        tier=TrafficTestTier.FULL,
        hour_bucket="2026-07-18T10",
        day_bucket="2026-07-18",
    )

    mark_successful_decision(
        variable=FakeVariable,
        decision=decision,
    )

    assert FakeVariable.values == {
        TRAFFIC_GOLD_TEST_DAY_KEY: "2026-07-18",
        TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T10",
    }


def test_full_success_writes_hour_before_day():
    writes = []

    class OrderedVariable(FakeVariable):
        @classmethod
        def set(cls, key, value):
            writes.append((key, value))
            super().set(key, value)

    decision = TrafficTestDecision(
        tier=TrafficTestTier.FULL,
        hour_bucket="2026-07-18T10",
        day_bucket="2026-07-18",
    )

    mark_successful_decision(variable=OrderedVariable, decision=decision)

    assert writes == [
        (TRAFFIC_GOLD_TEST_HOUR_KEY, "2026-07-18T10"),
        (TRAFFIC_GOLD_TEST_DAY_KEY, "2026-07-18"),
    ]


def test_full_success_day_write_failure_leaves_conservative_hour_only_marker():
    class PartiallyBrokenVariable(FakeVariable):
        @classmethod
        def set(cls, key, value):
            if key == TRAFFIC_GOLD_TEST_DAY_KEY:
                raise RuntimeError("day write failed")
            super().set(key, value)

    decision = TrafficTestDecision(
        tier=TrafficTestTier.FULL,
        hour_bucket="2026-07-18T10",
        day_bucket="2026-07-18",
    )

    with pytest.raises(RuntimeError, match="day write failed"):
        mark_successful_decision(variable=PartiallyBrokenVariable, decision=decision)

    assert FakeVariable.values == {TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T10"}
    assert (
        choose_test_decision(
            variable=FakeVariable,
            now=datetime(2026, 7, 18, 10, 55, tzinfo=KST),
        ).tier
        is TrafficTestTier.FULL
    )


def test_hourly_success_marks_only_frozen_hour():
    decision = TrafficTestDecision(
        tier=TrafficTestTier.HOURLY,
        hour_bucket="2026-07-18T10",
        day_bucket="2026-07-18",
    )

    mark_successful_decision(
        variable=FakeVariable,
        decision=decision,
    )

    assert FakeVariable.values == {TRAFFIC_GOLD_TEST_HOUR_KEY: "2026-07-18T10"}


def test_gate_success_does_not_write_ledger():
    decision = TrafficTestDecision(
        tier=TrafficTestTier.GATE,
        hour_bucket="2026-07-18T10",
        day_bucket="2026-07-18",
    )

    assert mark_successful_decision(variable=FakeVariable, decision=decision) == {}
    assert FakeVariable.values == {}
```

- [ ] **Step 2: Run cadence unit tests to verify failure**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_test_cadence.py -q
```

Expected: FAIL with import error because `traffic_ingest.test_cadence` does not exist.

- [ ] **Step 3: Implement cadence helper module**

Create `domains/traffic/traffic_ingest/test_cadence.py`:

```python
"""Traffic Gold test cadence ledger helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol
from zoneinfo import ZoneInfo


KST = ZoneInfo("Asia/Seoul")
TRAFFIC_GOLD_TEST_HOUR_KEY = "ask_seoul_traffic_gold_test_last_success_hour_kst"
TRAFFIC_GOLD_TEST_DAY_KEY = "ask_seoul_traffic_gold_test_last_success_day_kst"


class TrafficTestTier(str, Enum):
    GATE = "gate"
    HOURLY = "hourly"
    FULL = "full"


@dataclass(frozen=True)
class TrafficTestDecision:
    tier: TrafficTestTier
    hour_bucket: str
    day_bucket: str

    def as_dict(self) -> dict[str, str]:
        return {
            "tier": self.tier.value,
            "hour_bucket": self.hour_bucket,
            "day_bucket": self.day_bucket,
        }


class VariableLike(Protocol):
    @staticmethod
    def get(key: str, default=None): ...

    @staticmethod
    def set(key: str, value: str) -> None: ...


def _kst_buckets(now: datetime | None = None) -> tuple[str, str]:
    resolved = now or datetime.now(tz=KST)
    kst_now = resolved.astimezone(KST)
    return kst_now.strftime("%Y-%m-%dT%H"), kst_now.strftime("%Y-%m-%d")


def choose_test_decision(
    *,
    variable: VariableLike,
    now: datetime | None = None,
) -> TrafficTestDecision:
    hour_bucket, day_bucket = _kst_buckets(now)
    try:
        last_day = variable.get(TRAFFIC_GOLD_TEST_DAY_KEY, default=None)
        last_hour = variable.get(TRAFFIC_GOLD_TEST_HOUR_KEY, default=None)
    except Exception:
        return TrafficTestDecision(TrafficTestTier.FULL, hour_bucket, day_bucket)
    if not last_day or not last_hour:
        return TrafficTestDecision(TrafficTestTier.FULL, hour_bucket, day_bucket)
    if not str(last_hour).startswith(f"{last_day}T"):
        return TrafficTestDecision(TrafficTestTier.FULL, hour_bucket, day_bucket)
    if last_day != day_bucket:
        return TrafficTestDecision(TrafficTestTier.FULL, hour_bucket, day_bucket)
    if last_hour != hour_bucket:
        return TrafficTestDecision(TrafficTestTier.HOURLY, hour_bucket, day_bucket)
    return TrafficTestDecision(TrafficTestTier.GATE, hour_bucket, day_bucket)


def mark_successful_decision(
    *,
    variable: VariableLike,
    decision: TrafficTestDecision,
) -> dict[str, str]:
    written: dict[str, str] = {}
    if decision.tier is TrafficTestTier.FULL:
        variable.set(TRAFFIC_GOLD_TEST_HOUR_KEY, decision.hour_bucket)
        written[TRAFFIC_GOLD_TEST_HOUR_KEY] = decision.hour_bucket
        variable.set(TRAFFIC_GOLD_TEST_DAY_KEY, decision.day_bucket)
        written[TRAFFIC_GOLD_TEST_DAY_KEY] = decision.day_bucket
    elif decision.tier is TrafficTestTier.HOURLY:
        variable.set(TRAFFIC_GOLD_TEST_HOUR_KEY, decision.hour_bucket)
        written[TRAFFIC_GOLD_TEST_HOUR_KEY] = decision.hour_bucket
    return written
```

- [ ] **Step 4: Run cadence helper tests**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_test_cadence.py -q
```

Expected: PASS.

- [ ] **Step 5: Add phase selector mapping contract tests**

In `domains/traffic/tests/test_traffic_transform_contract.py`, update `expected_phase_contracts` so `dbt_test_gold` has static fallback selector `"ask_seoul_traffic_transform_gold_full_tests"`, then add:

```python
def test_gold_test_selector_is_chosen_from_current_run_tier(monkeypatch):
    module = load_transform_module()
    captured = {}

    def execute_dbt_phase(**kwargs):
        captured.update(kwargs)
        return types.SimpleNamespace(
            attempts=[types.SimpleNamespace(returncode=0, stdout="", stderr="")],
            completed=types.SimpleNamespace(returncode=0, stdout="", stderr=""),
            existing_run_results_path=None,
            existing_sources_path=None,
            existing_manifest_path=None,
            missing_expected_artifacts=(),
            selected_unique_ids=tuple(
                f"test.asac.gold_gate_{index}" for index in range(123)
            ),
        )

    monkeypatch.setattr(module.traffic_dbt, "execute_dbt_phase", execute_dbt_phase)
    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=1,
        xcom_pull=lambda *, task_ids, key=None: (
            {
                "tier": module.TrafficTestTier.GATE.value,
                "hour_bucket": "2026-07-18T10",
                "day_bucket": "2026-07-18",
            }
            if task_ids == module.SELECT_TEST_TIER_TASK_ID
            else (
                8738321387624398062
                if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY
                else "snapshot-a"
            )
        ),
    )

    result = module.run_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_traffic_transform_gold_full_tests",
        selector_by_test_tier={
            module.TrafficTestTier.GATE: "ask_seoul_traffic_transform_gold_gate_tests",
            module.TrafficTestTier.HOURLY: "ask_seoul_traffic_transform_gold_hourly_tests",
            module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_gold_full_tests",
        },
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=True,
        fresh_parse=True,
        snapshot_required=True,
        threads=2,
        ti=ti,
        run_id="manual__tier",
        params={"target": "dev"},
    )

    assert result["status"] == "success"
    assert captured["selector"] == "ask_seoul_traffic_transform_gold_gate_tests"
    assert captured["threads"] == 2
    assert len(result["selected_unique_ids"]) == 123
```

Add fail-closed invalid tier and malformed decision tests:

```python
def test_gold_test_selector_fails_closed_for_invalid_tier():
    module = load_transform_module()
    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=1,
        xcom_pull=lambda *, task_ids, key=None: (
            {"tier": "bad-tier", "hour_bucket": "2026-07-18T10", "day_bucket": "2026-07-18"}
            if task_ids == module.SELECT_TEST_TIER_TASK_ID
            else (
                8738321387624398062
                if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY
                else "snapshot-a"
            )
        ),
    )

    with pytest.raises(FakeAirflowFailException, match="invalid traffic test decision"):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_gold_full_tests",
            selector_by_test_tier={
                module.TrafficTestTier.GATE: "ask_seoul_traffic_transform_gold_gate_tests",
                module.TrafficTestTier.HOURLY: "ask_seoul_traffic_transform_gold_hourly_tests",
                module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_gold_full_tests",
            },
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            fresh_parse=True,
            snapshot_required=True,
            threads=2,
            ti=ti,
            run_id="manual__tier",
            params={"target": "dev"},
        )


def test_gold_test_selector_fails_closed_for_malformed_decision():
    module = load_transform_module()
    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=1,
        xcom_pull=lambda *, task_ids, key=None: (
            {"hour_bucket": "2026-07-18T10", "day_bucket": "2026-07-18"}
            if task_ids == module.SELECT_TEST_TIER_TASK_ID
            else (
                8738321387624398062
                if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY
                else "snapshot-a"
            )
        ),
    )

    with pytest.raises(FakeAirflowFailException, match="invalid traffic test decision"):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_gold_full_tests",
            selector_by_test_tier={
                module.TrafficTestTier.GATE: "ask_seoul_traffic_transform_gold_gate_tests",
                module.TrafficTestTier.HOURLY: "ask_seoul_traffic_transform_gold_hourly_tests",
                module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_gold_full_tests",
            },
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            fresh_parse=True,
            snapshot_required=True,
            threads=2,
            ti=ti,
            run_id="manual__tier",
            params={"target": "dev"},
        )
```

Add axes/admin explicit no-op tests. These prove `None` mapping is an intentional success no-op, not a missing selector:

```python
def test_axes_and_admin_tier_noop_returns_success_without_executor(monkeypatch):
    module = load_transform_module()
    calls = []
    monkeypatch.setattr(
        module.traffic_dbt,
        "execute_dbt_phase",
        lambda **kwargs: calls.append(kwargs),
    )
    ti = types.SimpleNamespace(
        task_id="dbt_test_asac_axes_seed_contract",
        try_number=1,
        xcom_pull=lambda *, task_ids, key=None: (
            {
                "tier": module.TrafficTestTier.GATE.value,
                "hour_bucket": "2026-07-18T10",
                "day_bucket": "2026-07-18",
            }
            if task_ids == module.SELECT_TEST_TIER_TASK_ID
            else "snapshot-a"
        ),
    )

    result = module.run_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_traffic_transform_asac_axes_contract",
        selector_by_test_tier={
            module.TrafficTestTier.GATE: None,
            module.TrafficTestTier.HOURLY: None,
            module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_asac_axes_contract",
        },
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=False,
        threads=2,
        ti=ti,
        run_id="manual__tier",
        params={"target": "dev"},
    )

    assert result == {
        "status": "success",
        "skipped": True,
        "skip_reason": "traffic_test_tier_noop",
        "run_results_path": None,
        "sources_path": None,
        "manifest_path": None,
        "selected_unique_ids": [],
    }
    assert calls == []


def test_axes_and_admin_missing_tier_key_fails_instead_of_noop():
    module = load_transform_module()
    ti = types.SimpleNamespace(
        task_id="dbt_test_common_admin_dong_dimension",
        try_number=1,
        xcom_pull=lambda *, task_ids, key=None: (
            {
                "tier": module.TrafficTestTier.HOURLY.value,
                "hour_bucket": "2026-07-18T10",
                "day_bucket": "2026-07-18",
            }
            if task_ids == module.SELECT_TEST_TIER_TASK_ID
            else "snapshot-a"
        ),
    )

    with pytest.raises(FakeAirflowFailException, match="missing dbt selector"):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_common_admin",
            selector_by_test_tier={
                module.TrafficTestTier.GATE: None,
                module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_common_admin",
            },
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=False,
            threads=2,
            ti=ti,
            run_id="manual__tier",
            params={"target": "dev"},
        )
```

- [ ] **Step 6: Run selector tests to verify failure**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_transform_contract.py::test_gold_test_selector_is_chosen_from_current_run_tier domains\traffic\tests\test_traffic_transform_contract.py::test_gold_test_selector_fails_closed_for_invalid_tier domains\traffic\tests\test_traffic_transform_contract.py::test_gold_test_selector_fails_closed_for_malformed_decision domains\traffic\tests\test_traffic_transform_contract.py::test_axes_and_admin_tier_noop_returns_success_without_executor domains\traffic\tests\test_traffic_transform_contract.py::test_axes_and_admin_missing_tier_key_fails_instead_of_noop -q
```

Expected: FAIL because `TrafficTestTier`, `SELECT_TEST_TIER_TASK_ID`, and `selector_by_test_tier` are not wired.

- [ ] **Step 7: Extend Traffic phase spec for tier selector**

In `domains/traffic/traffic_ingest/transform_specs.py`, import:

```python
from traffic_ingest.test_cadence import TrafficTestTier
```

Update dataclass:

```python
    selector_by_test_tier: dict[TrafficTestTier, str | None] | None = None
```

Update axes/admin FULL-only test specs so GATE/HOURLY are explicit success no-op and FULL keeps the existing selector:

```python
    DbtPhaseSpec(
        "dbt_test_common_admin_dong_dimension",
        "test",
        "ask_seoul_traffic_transform_common_admin",
        selector_by_test_tier={
            TrafficTestTier.GATE: None,
            TrafficTestTier.HOURLY: None,
            TrafficTestTier.FULL: "ask_seoul_traffic_transform_common_admin",
        },
    ),
    DbtPhaseSpec(
        "dbt_test_asac_axes_seed_contract",
        "test",
        "ask_seoul_traffic_transform_asac_axes_contract",
        selector_by_test_tier={
            TrafficTestTier.GATE: None,
            TrafficTestTier.HOURLY: None,
            TrafficTestTier.FULL: "ask_seoul_traffic_transform_asac_axes_contract",
        },
    ),
```

Update `dbt_test_gold` spec:

```python
    DbtPhaseSpec(
        "dbt_test_gold",
        "test",
        "ask_seoul_traffic_transform_gold_full_tests",
        silver_persisted=True,
        fresh_parse=True,
        snapshot_required=True,
        pin_critical=True,
        selector_by_test_tier={
            TrafficTestTier.GATE: "ask_seoul_traffic_transform_gold_gate_tests",
            TrafficTestTier.HOURLY: "ask_seoul_traffic_transform_gold_hourly_tests",
            TrafficTestTier.FULL: "ask_seoul_traffic_transform_gold_full_tests",
        },
    ),
```

- [ ] **Step 8: Wire tier tasks without moving existing pre-snapshot semantics**

In `domains/traffic/traffic_incident_transform.py`, add import:

```python
from airflow.sdk import Param, Variable
from traffic_ingest.test_cadence import (
    TrafficTestDecision,
    TrafficTestTier,
    choose_test_decision,
    mark_successful_decision,
)
```

Add constants:

```python
SELECT_TEST_TIER_TASK_ID = "select_traffic_test_tier"
MARK_TEST_TIER_TASK_ID = "mark_traffic_test_tier"
```

Add task callables:

```python
def choose_traffic_test_tier(**context) -> dict[str, str]:
    decision = choose_test_decision(variable=Variable)
    return decision.as_dict()


def mark_traffic_test_tier(**context) -> dict[str, str]:
    ti = context["ti"]
    raw_decision = ti.xcom_pull(task_ids=SELECT_TEST_TIER_TASK_ID)
    if not isinstance(raw_decision, dict):
        raise AirflowFailException(f"invalid traffic test decision: {raw_decision}")
    try:
        decision = TrafficTestDecision(
            tier=TrafficTestTier(str(raw_decision["tier"])),
            hour_bucket=str(raw_decision["hour_bucket"]),
            day_bucket=str(raw_decision["day_bucket"]),
        )
    except (KeyError, ValueError) as exc:
        raise AirflowFailException(f"invalid traffic test decision: {raw_decision}") from exc
    return mark_successful_decision(variable=Variable, decision=decision)
```

Add selector helper:

```python
def _selector_for_test_tier(
    *,
    selector: str | None,
    selector_by_test_tier: dict[TrafficTestTier, str | None] | None,
    ti,
) -> tuple[str | None, bool]:
    if selector_by_test_tier is None:
        return selector, False
    raw_decision = ti.xcom_pull(task_ids=SELECT_TEST_TIER_TASK_ID)
    if not isinstance(raw_decision, dict):
        raise AirflowFailException(f"invalid traffic test decision: {raw_decision}")
    try:
        tier = TrafficTestTier(str(raw_decision["tier"]))
    except (KeyError, ValueError) as exc:
        raise AirflowFailException(f"invalid traffic test decision: {raw_decision}") from exc
    if tier not in selector_by_test_tier:
        raise AirflowFailException(f"missing dbt selector for traffic test tier: {tier.value}")
    selected = selector_by_test_tier[tier]
    if selected is None:
        return None, True
    if not selected.strip():
        raise AirflowFailException(f"empty dbt selector for traffic test tier: {tier.value}")
    return selected, False
```

Update `run_dbt_phase()` signature:

```python
    selector_by_test_tier: dict[TrafficTestTier, str | None] | None = None,
```

Before executor call:

```python
    effective_selector, tier_skipped = _selector_for_test_tier(
        selector=selector,
        selector_by_test_tier=selector_by_test_tier,
        ti=ti,
    )
    if tier_skipped:
        return {
            "status": "success",
            "skipped": True,
            "skip_reason": "traffic_test_tier_noop",
            "run_results_path": None,
            "sources_path": None,
            "manifest_path": None,
            "selected_unique_ids": [],
        }
```

Pass `selector=effective_selector`. In the explicit no-op branch, `traffic_dbt.execute_dbt_phase()` must not be called.

Add `selector_by_test_tier` to `dbt_task()` op_kwargs:

```python
            "selector_by_test_tier": spec.selector_by_test_tier,
```

Create tasks in DAG:

```python
    select_test_tier_task = PythonOperator(
        task_id=SELECT_TEST_TIER_TASK_ID,
        python_callable=choose_traffic_test_tier,
        on_failure_callback=record_traffic_problem,
    )

    mark_test_tier_task = PythonOperator(
        task_id=MARK_TEST_TIER_TASK_ID,
        python_callable=mark_traffic_test_tier,
        on_failure_callback=record_traffic_problem,
    )
```

Preserve pre-snapshot order and insert tier where `dbt_test_gold` can read it:

```python
    transform_tasks = [
        validate_runtime,
        select_test_tier_task,
        *pre_snapshot_tasks,
        resolve_snapshot,
        *pinned_snapshot_tasks,
        mark_test_tier_task,
    ]
```

This keeps the existing `dbt_deps/source/contract/seed/admin` relative order intact, creates the frozen test decision before any long dbt work starts, keeps those pre-snapshot phases before `resolve_snapshot`, and places `mark_traffic_test_tier` only after `dbt_test_gold` succeeds.

- [ ] **Step 9: Add Traffic Airflow Variable fake support**

In `domains/traffic/tests/traffic_transform_test_support.py`, add a minimal fake and expose it on `airflow.sdk`:

```python
class FakeVariable:
    values: dict[str, str] = {}
    fail_get = False
    set_calls: list[tuple[str, str]] = []

    @classmethod
    def reset(cls):
        cls.values = {}
        cls.fail_get = False
        cls.set_calls = []

    @classmethod
    def get(cls, key, default=None, **_kwargs):
        if cls.fail_get:
            raise RuntimeError("metadata unavailable")
        return cls.values.get(key, default)

    @classmethod
    def set(cls, key, value, **_kwargs):
        cls.set_calls.append((key, value))
        cls.values[key] = value
```

Inside `install_airflow_fakes()` after `airflow_sdk.Param = FakeParam`:

```python
    FakeVariable.reset()
    airflow_sdk.Variable = FakeVariable
```

Expected: importing `traffic_incident_transform.py` succeeds after `from airflow.sdk import Param, Variable`.

- [ ] **Step 10: Update task order tests**

In `test_traffic_transform_contract.py::test_traffic_transform_bootstraps_asac_axes_before_silver`, update order:

```python
    expected_task_order = [
        "select_traffic_test_tier",
        "dbt_deps",
        "dbt_source_freshness",
        "dbt_test_traffic_incident_availability",
        "dbt_test_traffic_bronze_source_contract",
        "dbt_seed_asac_axes",
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
        "dbt_test_asac_axes_seed_contract",
        "resolve_traffic_snapshot_run",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
        "mark_traffic_test_tier",
    ]
```

In `test_traffic_transform_dag.py::test_traffic_transform_validates_dev_runtime_before_dbt`, update the downstream assertion:

```python
    assert guard.downstream_task_ids == {
        "select_traffic_test_tier"
    }
    assert module.dag.task_dict["select_traffic_test_tier"].downstream_task_ids == {
        "dbt_deps"
    }
```

In `test_traffic_transform_dag.py::test_traffic_transform_publishes_dbt_run_metrics_after_terminal_test`, update:

```python
    assert module.dag.task_dict["dbt_test_gold"].downstream_task_ids == {
        "mark_traffic_test_tier"
    }
    assert module.dag.task_dict["mark_traffic_test_tier"].downstream_task_ids == {
        "publish_dbt_run_metrics"
    }
```

- [ ] **Step 11: Run cadence and Traffic transform tests**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_test_cadence.py domains\traffic\tests\test_traffic_transform_contract.py domains\traffic\tests\test_traffic_transform_dag.py -q
```

Expected: PASS.

- [ ] **Step 12: Commit cadence work after approval**

Run only after user approval to commit:

```bash
git add domains/traffic/traffic_ingest/test_cadence.py domains/traffic/tests/test_traffic_test_cadence.py domains/traffic/traffic_ingest/transform_specs.py domains/traffic/traffic_incident_transform.py domains/traffic/tests/traffic_transform_test_support.py domains/traffic/tests/test_traffic_transform_contract.py domains/traffic/tests/test_traffic_transform_dag.py
git commit -m "fix(traffic): tier gold tests within transform fence"
```

Expected: commit succeeds; no unrelated files staged.

## Task 7: Full Verification and Dev Canary Handoff

**Files:**
- Modify: `domains/traffic/docs/retrospectives/2026-07-14-traffic-canonical-gold-transform-dev-validation.md` only if the execution owner decides this is the current reusable validation log.
- Modify: `LessonRun.md` in root harness only during runtime validation, not as part of this DAG-only implementation unless explicitly assigned.

**Interfaces:**
- Consumes: Tasks 1-6.
- Produces: local compile/test evidence and dev runtime canary evidence for handoff.

- [ ] **Step 1: Run Python compile**

Run:

```powershell
python -m compileall domains\traffic domains\weather
```

Expected: exits 0; no syntax errors.

- [ ] **Step 2: Run focused Traffic tests**

Run:

```powershell
pytest domains\traffic\tests\test_traffic_run_manifest_module.py domains\traffic\tests\test_traffic_test_cadence.py domains\traffic\tests\test_traffic_transform_contract.py domains\traffic\tests\test_traffic_transform_dag.py domains\traffic\tests\test_traffic_transform_failures.py domains\traffic\tests\test_traffic_dbt_execution_lineage.py domains\traffic\tests\test_traffic_dbt_execution_artifacts.py domains\traffic\tests\test_traffic_snapshot_recovery_dag.py -q
```

Expected: PASS.

- [ ] **Step 3: Run focused Weather tests**

Run:

```powershell
pytest domains\weather\tests\test_weather_transform_dag.py domains\weather\tests\test_weather_transform_execution.py domains\weather\tests\test_weather_dbt_execution.py domains\weather\tests\test_weather_dbt_execution_artifacts.py domains\weather\tests\test_weather_w2_canonical_transform_dag.py domains\weather\tests\test_weather_w2_canonical_transform_execution.py domains\weather\tests\test_weather_w2_observation_recovery.py -q
```

Expected: PASS.

- [ ] **Step 4: Run Ruff if configured**

Run:

```powershell
python -m ruff check domains\traffic domains\weather
```

Expected: PASS. If `ruff` is not installed, record `ruff unavailable in local environment` in handoff evidence and do not mark formatting verified.

- [ ] **Step 5: Run DAG import smoke in local Airflow harness**

From root harness after clean submodule checkout and without printing secrets:

```powershell
docker compose ps
docker compose exec airflow-scheduler airflow dags list | Select-String "traffic_incident_transform|weather_vilage_fcst_transform|weather_w2_canonical_transform"
```

Expected: Airflow services are running and all listed DAG IDs import.

- [ ] **Step 6: Verify Airflow pools before dev canary**

Run in Airflow environment:

```powershell
docker compose exec airflow-scheduler airflow pools list | Select-String "trino_traffic_heavy|trino_weather_heavy"
```

Expected: both pools exist with 1 slot for Stage 1. If absent, create them through the approved root/runtime operation, not in DAG code.

- [ ] **Step 7: Resolve any pre-hotfix active Traffic run before unpause**

Pausing a DAG does not terminate its active DagRun. Airflow 3 also blocks unscheduled downstream task instances of that run with the `Dag Not Paused` dependency. Never resume a pre-hotfix run across the deployment revision boundary because its completed XComs and pending serialized task graph can belong to different revisions.

Inspect active runs and their task boundary while Traffic remains paused:

```powershell
docker compose exec -T airflow-scheduler airflow dags list-runs traffic_incident_transform --state running --output table
$staleRunId = $env:TRAFFIC_STALE_RUN_ID
if (-not [string]::IsNullOrWhiteSpace($staleRunId)) {
  docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run traffic_incident_transform $staleRunId
}
```

If the list is non-empty, set `TRAFFIC_STALE_RUN_ID` to that exact run id, record the task-state table, and mark that pre-hotfix DagRun `failed` in the local Airflow UI while the DAG is still paused. Do not clear or retry its pending tasks. Re-run `airflow dags list-runs ... --state running`; expected result is no active Traffic run. This is an explicit local runtime recovery action, not a code change.

- [ ] **Step 8: Dev canary Stage 1 with a fresh post-hotfix run**

Run Weather once, then unpause Traffic and create one fresh, revision-identifiable manual run:

```powershell
docker compose exec airflow-scheduler airflow dags pause traffic_incident_transform
docker compose exec airflow-scheduler airflow dags trigger weather_vilage_fcst_transform
docker compose exec airflow-scheduler airflow dags unpause traffic_incident_transform
$freshRunId = 'manual__traffic_hotfix__' + (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
docker compose exec airflow-scheduler airflow dags trigger traffic_incident_transform --run-id $freshRunId
```

Expected:
- Weather dbt phases use `trino_weather_heavy`.
- Traffic Bronze/resolver/transform use `trino_traffic_heavy`.
- `dbt_deps` tasks do not wait on either heavy pool.
- Traffic resolver emits one batch manifest MERGE for stale backlog.
- Gold run remains separate from Gold test; `dbt_test_gold` priority remains 10.
- ASAC-DBT #257 selectors exist and select expected Gold counts: GATE 123, HOURLY 143, FULL 173.

- [ ] **Step 9: Record runtime evidence**

Record in the agreed runtime log:

```text
DAG run id:
Traffic resolver duration:
Traffic dbt_test_gold selected tier:
Traffic dbt_test_gold selector:
Traffic dbt_test_gold selected_unique_ids count:
Traffic axes/admin tier no-op status:
Weather transform run id:
Pool waits:
Final Traffic row counts:
Final Weather row counts:
Created/updated R2 objects:
Created/updated Iceberg tables:
Failures or rollback actions:
```

Expected: no secret values appear in logs or markdown.

- [ ] **Step 10: Commit final validation notes after approval**

Run only after user approval to commit docs/notes:

```bash
git add <approved-validation-log-path>
git commit -m "docs: record traffic transform runtime hotfix validation"
```

Expected: commit includes only approved validation documentation.

## Acceptance Criteria

- `TrafficRunManifest.coalesce_many()` emits constant DDL/MERGE round trips for non-empty stale batches and opens no cursor for empty normalized input.
- Existing `coalesce()` caller compatibility remains intact.
- Resolver batches stale Incident run IDs once and still leaves stale/optional Flow as `None`.
- Traffic `dbt_deps` and Weather `dbt_deps` do not occupy heavy Trino pools.
- Traffic and Weather TRINO work use separate domain pools: `trino_traffic_heavy`, `trino_weather_heavy`.
- All non-deps dbt materialization phases receive `threads=2` through every layer and only materialization commands include `--threads`.
- Traffic and Weather non-deps dbt phases self-heal missing run-local package sentinel before parse/ls; failed deps stops the phase before parse/ls/model/test.
- Traffic Gold test tier is selected inside the same DAG run immediately after `validate_dev_runtime`, read through frozen XCom decision, and marked only after `dbt_test_gold` succeeds.
- Existing pre-snapshot Traffic dbt phase relative order remains before resolver: `dbt_deps -> source/contract/seed/admin -> resolve_traffic_snapshot_run`.
- ASAC-DBT #257 selectors `ask_seoul_traffic_transform_gold_gate_tests`, `ask_seoul_traffic_transform_gold_hourly_tests`, `ask_seoul_traffic_transform_gold_full_tests` exist and select expected Gold counts 123/143/173 in dev runtime.
- axes/admin 10 tests are FULL-only; GATE/HOURLY produce explicit success skipped dict without calling `traffic_dbt.execute_dbt_phase()`.
- Gold run/test split and priority fence are preserved.
- Python compile and focused domain pytest suites pass.

## Rollback Notes

- Resolver batch regression: pause Traffic transform and fix batch SQL; do not reintroduce partial per-row stale loop as the long-term rollback.
- Package self-heal regression: disable only self-heal path while keeping explicit `dbt_deps` and run-local package path.
- Domain pool wiring regression: pause affected transform and temporarily restore the previous pool constant only with explicit approval.
- Cadence selector regression: make `dbt_test_gold` use the full selector `ask_seoul_traffic_transform_gold_full_tests` and bypass tier marker reads; do not delete cadence tests.
- Trino OOM under Stage 2: reduce root Trino hard concurrency/query cap; do not change DAG correctness contracts.

## Self-Review

- Spec coverage: resolver batch, domain pools, `LOCAL/TRINO` workload, `threads=2` propagation, Traffic/Weather package self-heal, same-DAG cadence ledger/selectors, optional Flow preservation, and pre-snapshot ordering constraint are all mapped to tasks.
- Placeholder scan: no implementation step uses placeholder-only wording; all code-changing steps include concrete snippets and commands with expected outcomes.
- Type consistency: `TrafficTestTier`, `TrafficTestDecision`, `DbtWorkload`, `selector_by_test_tier`, `threads`, `coalesce_many`, `choose_test_decision`, and `mark_successful_decision` names are consistent across tasks.
