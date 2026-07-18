# Traffic Citydata Iceberg Snapshot Pin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Traffic Gold model과 exact reconciliation test가 하나의 Airflow run에서 동일한 Citydata Iceberg snapshot을 읽도록 고정한다.

**Architecture:** 기존 `resolve_traffic_snapshot_run`이 Incident/Flow Bronze pair를 고정한 직후 외부 crowding table의 최신 Iceberg snapshot ID를 metadata table에서 읽어 XCom과 dbt var로 전달한다. DBT의 Traffic 전용 fail-closed macro가 model과 singular test 모두에 동일한 `FOR VERSION AS OF` relation을 제공하고, Gold row 및 failure record에 snapshot lineage를 남긴다.

**Tech Stack:** Python 3.11, Apache Airflow 3.2.2, pytest, dbt Core 1.10.22, dbt-trino 1.10.2, Trino 482, Iceberg, Docker Compose, GitHub PR

## Global Constraints

- dev만 사용한다. prod/shared schema, full refresh, backfill은 실행하지 않는다.
- 실행·수정 범위는 Weather/Traffic으로 제한하고 production code는 Traffic 소유 경로만 변경한다.
- 외부 Citydata pipeline은 실행·수정·점검하지 않는다. Traffic Gold가 소비하는 `gold_citydata_ppltn_by_time`의 Iceberg metadata와 pinned relation만 읽는다.
- Commerce, Transit, Culture pipeline을 파싱·실행·수정·검증하지 않는다.
- full-history correctness, latest-per-area tie-break, canonical grain, event/ingest time, idempotency를 완화하지 않는다.
- 기존 `trino_heavy` 1-slot priority fence, Asset trigger, dbt task 분리, 즉시 실패 Discord 알림, 09:00 KST reliability schedule을 유지한다.
- `.env`와 secret은 읽거나 출력하지 않는다. runtime command에는 기존 env-file 경로만 전달한다.
- `.omc`, `.omx`, `__pycache__`, `.pytest_cache`, `dbt_packages`, `logs`, `target`을 stage·commit·push하지 않는다.
- 원래 dirty root와 deploy worktree를 수정하지 않는다.
- maintenance, W2, recovery DAG는 pause 상태로 유지하고 실행하지 않는다.
- 새 GitHub issue를 만들지 않는다. ASAC-DAG #419와 ASAC-DBT #117은 닫지 않는다.

## File Map

### ASAC-DAG

- Create: `domains/traffic/traffic_ingest/external_snapshot.py` — 외부 Iceberg snapshot metadata adapter.
- Create: `domains/traffic/tests/test_traffic_external_snapshot.py` — metadata SQL과 fail-closed 단위 계약.
- Modify: `domains/traffic/traffic_incident_transform.py` — resolver XCom, dbt var 요구, success/failure lineage.
- Modify: `domains/traffic/traffic_ingest/transform_dag_support.py` — XCom을 typed dbt var로 변환.
- Modify: `domains/traffic/traffic_dbt_failure.py` — external snapshot failure lineage와 Discord 설명.
- Modify: `domains/traffic/tests/test_traffic_transform_contract.py` — resolver·dbt command RED/GREEN.
- Modify: `domains/traffic/tests/test_traffic_transform_failures.py` — failure record pin 검증.
- Modify: `domains/traffic/tests/test_traffic_dbt_failure.py` — recovery record/notification 계약.

### ASAC-DBT

- Create: `domains/traffic_weather/macros/traffic/traffic_external_snapshot.sql` — pinned source relation macro.
- Modify: `domains/traffic_weather/models/traffic/transform/gold/gold_traffic_incident_x_citydata_crowding_current_hourly.sql` — macro 사용 및 lineage column.
- Modify: `domains/traffic_weather/tests/traffic/transform/gold/assert_gold_traffic_incident_x_citydata_crowding_current_hourly_latest_per_area_reconciles.sql` — 동일 macro 사용.
- Create: `domains/traffic_weather/tests/traffic/transform/gold/assert_gold_traffic_incident_x_citydata_crowding_current_hourly_snapshot_lineage.sql` — pin과 Gold lineage exact test.
- Modify: `domains/traffic_weather/models/traffic/transform/gold/_gold.yml` — lineage column 문서와 not-null contract.
- Modify: `domains/traffic_weather/tests/traffic/test_cross_domain_gold_contract.py` — 정적 source pin 계약.

---

### Task 1: DAGS 외부 Iceberg snapshot metadata adapter

**Files:**
- Create: `domains/traffic/traffic_ingest/external_snapshot.py`
- Create: `domains/traffic/tests/test_traffic_external_snapshot.py`

**Interfaces:**
- Produces: `resolve_citydata_crowding_snapshot_id(cursor_factory, env) -> int`
- Produces: `ExternalSnapshotUnavailableError`
- Consumes: 기존 `trino_cursor()`의 `(cursor, catalog, ask_seoul_schema)` 반환 계약과 `sql_identifier()`.

- [ ] **Step 1: metadata query와 정상 반환 test를 먼저 작성한다**

```python
def test_resolve_citydata_crowding_snapshot_id_reads_latest_iceberg_snapshot():
    cursor = FakeCursor(rows=[(8738321387624398062,)])

    snapshot_id = resolve_citydata_crowding_snapshot_id(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze"),
        env={"SEOUL_CITYDATA_SCHEMA": "seoul_citydata"},
    )

    assert snapshot_id == 8738321387624398062
    assert 'iceberg_dev.seoul_citydata."gold_citydata_ppltn_by_time$snapshots"' in cursor.sql
    assert "ORDER BY committed_at DESC, snapshot_id DESC" in cursor.sql
    assert cursor.sql.rstrip().endswith("LIMIT 1")
```

- [ ] **Step 2: missing row와 invalid ID test를 작성한다**

```python
@pytest.mark.parametrize("row", [None, (None,), (0,), (-1,), ("not-a-number",)])
def test_resolve_citydata_crowding_snapshot_id_fails_closed(row):
    cursor = FakeCursor(rows=[] if row is None else [row])
    with pytest.raises(ExternalSnapshotUnavailableError):
        resolve_citydata_crowding_snapshot_id(
            cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze"),
            env={"SEOUL_CITYDATA_SCHEMA": "seoul_citydata"},
        )
```

- [ ] **Step 3: RED를 확인한다**

Run:

```powershell
python -m pytest domains/traffic/tests/test_traffic_external_snapshot.py -q
```

Expected: import 또는 symbol missing으로 FAIL.

- [ ] **Step 4: 최소 adapter를 구현한다**

```python
class ExternalSnapshotUnavailableError(RuntimeError):
    """An external Iceberg table has no usable snapshot to pin."""


def resolve_citydata_crowding_snapshot_id(
    cursor_factory=trino_cursor,
    env: Mapping[str, str] = os.environ,
) -> int:
    cursor, catalog, _ = cursor_factory()
    schema = sql_identifier(env.get("SEOUL_CITYDATA_SCHEMA", "seoul_citydata"))
    table = sql_identifier("gold_citydata_ppltn_by_time")
    cursor.execute(
        "SELECT snapshot_id "
        f'FROM {catalog}.{schema}."{table}$snapshots" '
        "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
    )
    row = cursor.fetchone()
    try:
        raw_snapshot_id = row[0]
    except (TypeError, IndexError) as exc:
        raise ExternalSnapshotUnavailableError(
            "Citydata crowding Iceberg snapshot is unavailable"
        ) from exc
    if (
        isinstance(raw_snapshot_id, bool)
        or not isinstance(raw_snapshot_id, int)
        or raw_snapshot_id <= 0
    ):
        raise ExternalSnapshotUnavailableError(
            "Citydata crowding Iceberg snapshot ID must be a positive integer"
        )
    return raw_snapshot_id
```

- [ ] **Step 5: GREEN과 전체 Traffic Python 회귀를 확인한다**

```powershell
python -m pytest domains/traffic/tests/test_traffic_external_snapshot.py -q
python -m pytest domains/traffic/tests -q
```

Expected: 모두 PASS.

---

### Task 2: resolver XCom과 pinned dbt var fail-closed 계약

**Files:**
- Modify: `domains/traffic/traffic_incident_transform.py`
- Modify: `domains/traffic/traffic_ingest/transform_dag_support.py`
- Modify: `domains/traffic/tests/test_traffic_transform_contract.py`

**Interfaces:**
- Produces XCom key: `traffic_citydata_crowding_snapshot_id` with `int` value.
- Produces dbt var: `traffic_citydata_crowding_snapshot_id` with the same `int` value.
- Preserves resolver return: `str` Incident Bronze run ID.

- [ ] **Step 1: resolver가 두 pin을 push하는 failing test를 작성한다**

```python
monkeypatch.setattr(
    module,
    "resolve_citydata_crowding_snapshot_id",
    lambda: 8738321387624398062,
)
assert module.resolve_traffic_snapshot_run(ti=ti, triggering_asset_events=events) == "incident-42"
assert (module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY, 8738321387624398062) in pushed
```

- [ ] **Step 2: pinned phase var와 누락 fail-closed test를 작성한다**

```python
assert json.loads(dbt_vars) == {
    "traffic_snapshot_dag_run_id": "snapshot-a",
    "traffic_citydata_crowding_snapshot_id": 8738321387624398062,
}
```

external XCom이 없는 `snapshot_required=True` 호출은
`traffic dbt phase requires Citydata crowding snapshot` 메시지의 `AirflowFailException`이어야 한다.

- [ ] **Step 3: targeted RED를 확인한다**

```powershell
python -m pytest domains/traffic/tests/test_traffic_transform_contract.py -q
```

Expected: 새 XCom/var assertion으로 FAIL.

- [ ] **Step 4: resolver와 변수 전달을 최소 구현한다**

`resolve_traffic_snapshot_run`은 pair 검증 후 external adapter를 호출하고, task instance가 있으면 Flow pin과 external pin을 각각 push한다.
`ExternalSnapshotUnavailableError`는 `AirflowFailException`으로 변환해 최신 source fallback 없이 종료하고, metadata query 자체의 연결 예외는
그대로 전파해 기존 retry를 사용한다.
`dbt_snapshot_variables`는 external XCom을 읽어 양의 `int`만 var에 추가한다. `run_dbt_phase`는
`snapshot_required=True`일 때 Incident pin과 external pin을 모두 요구한다.

- [ ] **Step 5: GREEN을 확인한다**

```powershell
python -m pytest domains/traffic/tests/test_traffic_transform_contract.py domains/traffic/tests/test_traffic_transform_dag.py -q
```

Expected: 29개 이상의 기존 test와 새 test 모두 PASS.

---

### Task 3: DAGS success/failure lineage

**Files:**
- Modify: `domains/traffic/traffic_incident_transform.py`
- Modify: `domains/traffic/traffic_dbt_failure.py`
- Modify: `domains/traffic/tests/test_traffic_transform_failures.py`
- Modify: `domains/traffic/tests/test_traffic_dbt_failure.py`

**Interfaces:**
- Adds success result field: `traffic_citydata_crowding_snapshot_id: int | None`.
- Adds recovery record field with the same name.
- Adds Discord failure description line `**citydata snapshot**`.

- [ ] **Step 1: failing success/failure lineage tests를 작성한다**

성공 `run_dbt_phase` 결과와 contract failure XCom record가 모두 `8738321387624398062`를 보존하는지 검증한다.
`build_failure_notification` 결과에는 다음 문자열이 있어야 한다.

```python
assert "**citydata snapshot**: `8738321387624398062`" in description
```

- [ ] **Step 2: RED를 확인한다**

```powershell
python -m pytest domains/traffic/tests/test_traffic_transform_failures.py domains/traffic/tests/test_traffic_dbt_failure.py -q
```

Expected: missing lineage field로 FAIL.

- [ ] **Step 3: 최소 lineage 구현 후 GREEN을 확인한다**

```powershell
python -m pytest domains/traffic/tests/test_traffic_transform_failures.py domains/traffic/tests/test_traffic_dbt_failure.py -q
python -m pytest domains/traffic/tests -q
```

Expected: 모두 PASS.

---

### Task 4: DBT fail-closed pinned source macro

**Files:**
- Create: `domains/traffic_weather/macros/traffic/traffic_external_snapshot.sql`
- Modify: `domains/traffic_weather/tests/traffic/test_cross_domain_gold_contract.py`

**Interfaces:**
- Produces: `traffic_citydata_crowding_snapshot_id()`.
- Produces: `traffic_citydata_crowding_source_at_snapshot()`.
- Consumes dbt var: `traffic_citydata_crowding_snapshot_id` as positive numeric value.

- [ ] **Step 1: macro 정적 계약 failing test를 작성한다**

```python
assert "var('traffic_citydata_crowding_snapshot_id', none)" in macro_sql
assert "exceptions.raise_compiler_error" in macro_sql
assert "FOR VERSION AS OF" in macro_sql
assert "source('traffic_citydata_gold', 'gold_citydata_ppltn_by_time')" in macro_sql
```

- [ ] **Step 2: RED를 확인한다**

```powershell
python -m pytest domains/traffic_weather/tests/traffic/test_cross_domain_gold_contract.py -q
```

Expected: macro file missing으로 FAIL.

- [ ] **Step 3: parse-safe, execute-time fail-closed macro를 구현한다**

```sql
{% macro traffic_citydata_crowding_snapshot_id() -%}
  {%- set snapshot_id = var('traffic_citydata_crowding_snapshot_id', none) -%}
  {%- if snapshot_id is none and not execute -%}
    {{ return(0) }}
  {%- endif -%}
  {%- if snapshot_id is not number or snapshot_id <= 0 -%}
    {{ exceptions.raise_compiler_error(
      'Traffic Citydata crowding source requires a positive traffic_citydata_crowding_snapshot_id.'
    ) }}
  {%- endif -%}
  {{ return(snapshot_id) }}
{%- endmacro %}

{% macro traffic_citydata_crowding_source_at_snapshot() -%}
  {%- set relation = source('traffic_citydata_gold', 'gold_citydata_ppltn_by_time') -%}
  {%- set snapshot_id = traffic_citydata_crowding_snapshot_id() -%}
  {{ return(relation ~ ' FOR VERSION AS OF ' ~ snapshot_id) }}
{%- endmacro %}
```

- [ ] **Step 4: GREEN을 확인한다**

```powershell
python -m pytest domains/traffic_weather/tests/traffic/test_cross_domain_gold_contract.py -q
```

Expected: PASS.

---

### Task 5: model/test 동일 relation과 Gold row lineage

**Files:**
- Modify: `domains/traffic_weather/models/traffic/transform/gold/gold_traffic_incident_x_citydata_crowding_current_hourly.sql`
- Modify: `domains/traffic_weather/tests/traffic/transform/gold/assert_gold_traffic_incident_x_citydata_crowding_current_hourly_latest_per_area_reconciles.sql`
- Create: `domains/traffic_weather/tests/traffic/transform/gold/assert_gold_traffic_incident_x_citydata_crowding_current_hourly_snapshot_lineage.sql`
- Modify: `domains/traffic_weather/models/traffic/transform/gold/_gold.yml`
- Modify: `domains/traffic_weather/tests/traffic/test_cross_domain_gold_contract.py`

**Interfaces:**
- Produces Gold column: `citydata_crowding_snapshot_id bigint`.
- Model과 latest-per-area test는 `traffic_citydata_crowding_source_at_snapshot()`만 사용한다.

- [ ] **Step 1: model/test/mapping failing assertions를 먼저 작성한다**

```python
assert "traffic_citydata_crowding_source_at_snapshot()" in model_sql
assert "traffic_citydata_crowding_source_at_snapshot()" in reconcile_sql
assert "citydata_crowding_snapshot_id" in model_sql
assert "source('traffic_citydata_gold', 'gold_citydata_ppltn_by_time')" not in model_sql
assert "source('traffic_citydata_gold', 'gold_citydata_ppltn_by_time')" not in reconcile_sql
```

- [ ] **Step 2: RED를 확인한다**

```powershell
python -m pytest domains/traffic_weather/tests/traffic/test_cross_domain_gold_contract.py -q
```

Expected: direct source와 missing lineage 때문에 FAIL.

- [ ] **Step 3: model과 exact test를 macro로 교체하고 lineage를 추가한다**

Gold select에 다음 expression을 추가한다.

```sql
cast({{ traffic_citydata_crowding_snapshot_id() }} as bigint)
    as citydata_crowding_snapshot_id
```

lineage singular test는 다음 계약을 사용한다.

```sql
select product_row_id
from {{ ref('gold_traffic_incident_x_citydata_crowding_current_hourly') }}
where citydata_crowding_snapshot_id is distinct from
      cast({{ traffic_citydata_crowding_snapshot_id() }} as bigint)
```

- [ ] **Step 4: GREEN과 DBT Traffic 정적 회귀를 확인한다**

```powershell
python -m pytest domains/traffic_weather/tests/traffic -q
```

Expected: 모두 PASS.

---

### Task 6: dbt parse/compile와 동일 snapshot SQL 검증

**Files:**
- Generated only: `target/`, `logs/`, `dbt_packages/` — commit 금지.

**Interfaces:**
- Valid pin: `8738321387624398062`.
- Expected compiled fragment: `FOR VERSION AS OF 8738321387624398062` in model과 exact test.

- [ ] **Step 1: 일반 parse가 var 없이 통과하는지 확인한다**

Run in the existing dev dbt runtime with the feature worktree mounted:

```powershell
dbt parse --project-dir /opt/airflow/dbt/domains/traffic_weather --target dev --no-partial-parse
```

Expected: exit 0.

- [ ] **Step 2: 대상 compile이 pin 누락 시 fail-closed하는지 확인한다**

```powershell
dbt compile --project-dir /opt/airflow/dbt/domains/traffic_weather --target dev --no-partial-parse --select gold_traffic_incident_x_citydata_crowding_current_hourly
```

Expected: positive `traffic_citydata_crowding_snapshot_id` compiler error.

- [ ] **Step 3: 유효 pin으로 model과 singular test를 compile한다**

```powershell
dbt compile --project-dir /opt/airflow/dbt/domains/traffic_weather --target dev --no-partial-parse --vars '{"traffic_snapshot_dag_run_id":"scheduled__2026-07-17T23:55:00+00:00","traffic_citydata_crowding_snapshot_id":8738321387624398062}' --select gold_traffic_incident_x_citydata_crowding_current_hourly assert_gold_traffic_incident_x_citydata_crowding_current_hourly_latest_per_area_reconciles
```

Expected: exit 0, 두 compiled SQL 모두 동일한 `FOR VERSION AS OF 8738321387624398062` 포함.

- [ ] **Step 4: generated artifact가 stage되지 않았는지 확인한다**

```powershell
git status --short
git diff --check
```

Expected: production/test/doc 변경만 표시되고 generated directory는 ignored 또는 untracked 상태로 제외.

---

### Task 7: branch verification, review, PR, merge

**Files:**
- Review all changed files in both worktrees.

- [ ] **Step 1: DAGS 검증**

```powershell
python -m pytest domains/traffic/tests -q
python -m compileall -q domains/traffic
ruff check domains/traffic
git diff --check
```

Expected: all tests PASS, compile/ruff/diff-check exit 0.

- [ ] **Step 2: DBT 검증**

```powershell
python -m pytest domains/traffic_weather/tests/traffic -q
git diff --check
```

Expected: all tests PASS, diff-check exit 0.

- [ ] **Step 3: `.omc`와 generated artifact exclusion을 확인한다**

```powershell
git status --short
git diff --cached --name-only
```

Expected: `.omc`, `.omx`, `__pycache__`, `.pytest_cache`, `dbt_packages`, `logs`, `target` 없음.

- [ ] **Step 4: 각 repo를 의도적으로 commit·push한다**

Commit messages:

```text
fix(traffic): pin external crowding snapshot
fix(traffic): read crowding Gold from pinned snapshot
```

- [ ] **Step 5: 공용 PR template로 dev 대상 PR 두 개를 만들고 본문 UTF-8을 확인한다**

새 issue나 `Closes #419`, `Closes #117`를 사용하지 않는다. DAGS PR에 DBT PR을, DBT PR에 DAGS PR을 cross-link하고
변경 요약, RED/GREEN, parse/compile, dev impact를 기록한다.

- [ ] **Step 6: checks와 review를 확인한 뒤 DAGS → DBT 순서로 merge한다**

Expected: 두 PR `MERGED`; #419와 #117은 계속 `OPEN`.

---

### Task 8: clean dev 재배포와 Traffic transform 재활성화

**Files:**
- Runtime record only: `C:/Users/Dell3571/Desktop/Projects/ask-seoul-sample-worktrees/deploy-root-dev-20260717/LessonRun.md`.

- [ ] **Step 1: clean deploy root의 submodule을 두 `origin/dev` merge SHA로 맞춘다**

```powershell
git -C dags fetch origin dev
git -C dags checkout --detach origin/dev
git -C dbt fetch origin dev
git -C dbt checkout --detach origin/dev
```

- [ ] **Step 2: dev compose를 직접 재배포한다**

```powershell
docker compose up -d --build
docker compose ps
```

`scripts/deploy.sh`는 사용하지 않는다.

- [ ] **Step 3: parser와 pause guard를 확인한다**

- fresh DagBag import error 0.
- parsed file은 Weather/Traffic allowlist만 포함.
- maintenance, W2, recovery, backfill은 paused.
- Traffic Landing/Incident Bronze/Flow Bronze, Weather Bronze/transform, 두 reliability DAG는 기존 상태 유지.

- [ ] **Step 4: `traffic_incident_transform`만 unpause하고 frozen run을 계속 진행시킨다**

resolver 이전에 멈춘 기존 run은 새 코드로 external snapshot을 고정한 뒤 Silver/Gold를 진행해야 한다. 새 backfill이나 manual full refresh는 만들지 않는다.

- [ ] **Step 5: dev run의 exact 증거를 확인한다**

- resolver XCom에 Incident/Flow/external snapshot pin 존재.
- dbt source freshness success.
- Silver run/test success.
- Gold run/test success.
- latest-per-area reconciliation 0행.
- snapshot lineage singular test 0행.
- Gold 전체 test 171개 이상 PASS.
- Traffic Bronze/audit 최근 중복 0.
- 실패가 없으면 즉시 Discord failure 알림 없음.

- [ ] **Step 6: `LessonRun.md`에 run ID, task 시간·상태, snapshot ID, row/test count, 영향 table을 기록한다**

- [ ] **Step 7: 24시간 heartbeat를 계속 관찰한다**

Weather/Traffic raw/Bronze/transform, `trino_heavy`, source freshness, Traffic 중복, 다음 09:00 KST reliability를 확인한다.
새 실패나 장기 적체만 즉시 알리고, 24회 완료 후 최종 종합과 heartbeat 종료를 수행한다.
