# Weather·Traffic Pipeline Reliability v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Weather와 Traffic의 기존 일일 Bronze reliability를 전체 정규 pipeline lifecycle·24시간 SLO·7일 추세를 보여 주는 도메인별 Pipeline Reliability v2 카드로 확장한다.

**Architecture:** Trino/R2 정본 수집, Marquez control-plane 요약, Discord/history delivery를 세 Airflow task로 분리한다. Airflow ORM은 사용하지 않고, Marquez 장애는 `UNKNOWN/WARN`, 데이터 정본 또는 최신 필수 stage 실패는 `FAIL`로 판정한다.

**Tech Stack:** Python 3.11/3.12, Apache Airflow 3, Trino 482, Cloudflare R2, Marquez/OpenLineage HTTP API, pytest

## Global Constraints

- 변경은 `domains/weather/**`, `domains/traffic/**`로 제한한다.
- dev만 사용하고 prod/shared schema를 조회·수정하지 않는다.
- `.env`, secret, token, webhook URL을 읽거나 출력하지 않는다.
- 기존 DAG ID, `0 9 * * *`, `catchup=False`, `max_active_runs=1`을 유지한다.
- 15분 reliability report를 만들지 않는다.
- 공통 `problem_failure_callback`의 즉시 실패 Discord 알림을 유지한다.
- Airflow metadata ORM/DB 직접 조회를 도입하지 않는다.
- Asset-triggered DAG에 expected 1:1 run count를 요구하지 않는다.
- DBT 모델/selector, canonical grain, event/ingest time, idempotency를 변경하지 않는다.
- maintenance, W2, recovery, recollect, backfill을 이 plan에서 실행하지 않는다.
- production code는 반드시 실패하는 test를 먼저 확인한 뒤 작성한다.
- `.omc`, `.omx`, `__pycache__`, `.pytest_cache`를 생성·stage·push하지 않는다.

---

## File Structure

- Create: `domains/traffic/traffic_ingest/reliability/lineage.py`
  - exact Marquez API client와 Traffic stage summary
- Create: `domains/weather/weather_ingest/reliability/lineage.py`
  - exact Marquez API client와 Weather stage summary
- Create: `domains/traffic/traffic_ingest/reliability/history.py`
  - compact Traffic daily snapshot R2 store/read
- Create: `domains/weather/weather_ingest/reliability/history.py`
  - compact Weather daily snapshot R2 store/read
- Modify: `domains/*/*_ingest/reliability/config.py`
  - exact job allowlist, stale limits, Marquez/history 설정
- Modify: `domains/*/*_ingest/reliability/trino_repository.py`
  - 기존 bounded data-plane summary를 유지하고 필요한 manifest stage summary를 추가
- Modify: `domains/*/*_ingest/reliability/report.py`
  - data-plane/control-plane/history 최종 합성
- Modify: `domains/*/*_ingest/reliability/discord.py`
  - status-driven structured embed payload
- Modify: `domains/*/*_reliability_report.py`
  - collect → compose → deliver 3-task DAG와 daily delivery state
- Modify: `domains/*/tests/test_*_reliability_*.py`
  - 기존 계약 migration과 v2 behavior
- Create: `domains/traffic/tests/test_traffic_reliability_lineage.py`
- Create: `domains/weather/tests/test_weather_reliability_lineage.py`
- Create: `domains/traffic/tests/test_traffic_reliability_history.py`
- Create: `domains/weather/tests/test_weather_reliability_history.py`
- Modify: `domains/traffic/README.md`, `domains/weather/README.md`

---

### Task 1: Marquez run summary 계약

**Files:**
- Create: `domains/traffic/tests/test_traffic_reliability_lineage.py`
- Create: `domains/weather/tests/test_weather_reliability_lineage.py`
- Create: `domains/traffic/traffic_ingest/reliability/lineage.py`
- Create: `domains/weather/weather_ingest/reliability/lineage.py`
- Modify: both domain `reliability/config.py`

**Interfaces:**
- Consumes: Marquez `GET /api/v1/namespaces/{namespace}/jobs/{job}/runs?limit=500`
- Produces: `StagePolicy`, `summarize_stage_runs()`, `collect_pipeline_stages()`

- [x] **Step 1: stage 상태 RED test 작성**

각 도메인 test에서 실제 Marquez run response shape를 완전한 dict fixture로 만든다.

```python
def test_latest_success_with_recovered_failure_is_warn():
    summary = summarize_stage_runs(
        policy=StagePolicy("gold", "Gold", "traffic_gold_transform", 180),
        runs=[failed_run("2026-07-19T00:10:00Z"), completed_run("2026-07-19T01:00:00Z", 120_000)],
        detected_at=datetime.fromisoformat("2026-07-19T02:00:00+00:00"),
        lookback_hours=24,
    )
    assert summary["status"] == "WARN"
    assert summary["failed"] == 1
    assert summary["latest_state"] == "COMPLETED"
```

stale RUNNING, latest FAILED, fresh RUNNING+recent success, no observation, malformed response,
p50/p95 duration cases를 각각 독립 test로 작성한다.

- [x] **Step 2: RED 확인**

Run:

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
python -m pytest -q -p no:cacheprovider domains/traffic/tests/test_traffic_reliability_lineage.py domains/weather/tests/test_weather_reliability_lineage.py
```

Expected: import 또는 missing symbol로 FAIL.

- [x] **Step 3: 최소 구현**

두 domain module은 서로 import하지 않고 같은 public contract를 소유한다.

```python
@dataclass(frozen=True)
class StagePolicy:
    key: str
    label: str
    job_name: str
    stale_after_minutes: int
    required: bool = True

def summarize_stage_runs(*, policy: StagePolicy, runs: list[dict[str, Any]], detected_at: datetime, lookback_hours: int) -> dict[str, Any]: ...

def collect_pipeline_stages(*, policies: tuple[StagePolicy, ...], detected_at: datetime, lookback_hours: int, fetch_json: Callable[[str], dict[str, Any]] | None = None) -> dict[str, Any]: ...
```

API URL과 namespace/job path는 config의 exact allowlist에서만 만들고 `urllib.parse.quote(...,
safe="")`를 사용한다. exception message는 버리고 `error_type`만 반환한다.

- [x] **Step 4: GREEN·domain regression 확인**

Run Task 1 tests, then both existing reliability test groups. Expected: PASS.

- [x] **Step 5: commit**

```powershell
git add domains/traffic domains/weather
git commit -m "feat(reliability): add pipeline lineage summaries"
```

---

### Task 2: compact 7-day history 계약

**Files:**
- Create: `domains/traffic/tests/test_traffic_reliability_history.py`
- Create: `domains/weather/tests/test_weather_reliability_history.py`
- Create: both domain `reliability/history.py`

**Interfaces:**
- Consumes: final report dict, KST report date, R2 object client
- Produces: `compact_history_snapshot()`, `history_object_key()`, `load_recent_history()`, `write_history_snapshot()`

- [x] **Step 1: RED tests 작성**

```python
def test_history_key_is_domain_and_kst_date_scoped():
    assert history_object_key(date(2026, 7, 20)) == (
        "reliability/date=2026-07-20/domain=traffic/pipeline-reliability-v2.json"
    )

def test_compact_snapshot_excludes_errors_and_run_payloads():
    snapshot = compact_history_snapshot(report_with_error_and_stage())
    assert "error" not in json.dumps(snapshot)
    assert set(snapshot) == {"version", "domain", "report_date", "detected_at", "status", "stages", "source", "bottleneck"}
```

7 exact GET, missing key, malformed JSON, wrong domain/version, write failure type-only logging을 test한다.

- [x] **Step 2: RED 확인**

Run both new history test files. Expected: missing module/symbol FAIL.

- [x] **Step 3: 최소 구현**

R2 client는 `common.storage.r2_env`를 사용하되 endpoint/key/token 값을 log·payload에 넣지
않는다. `load_recent_history`는 list operation 없이 날짜별 exact key만 읽는다.

- [x] **Step 4: GREEN 확인 후 commit**

Run history + lineage suites. Expected: PASS.

```powershell
git add domains/traffic domains/weather
git commit -m "feat(reliability): persist compact daily trends"
```

---

### Task 3: Traffic 전체 pipeline report 합성

**Files:**
- Modify: `domains/traffic/tests/traffic_reliability_test_support.py`
- Modify: `domains/traffic/tests/test_traffic_reliability_trino_repository.py`
- Modify: `domains/traffic/tests/test_traffic_reliability_report_composition.py`
- Modify: `domains/traffic/traffic_ingest/reliability/trino_repository.py`
- Modify: `domains/traffic/traffic_ingest/reliability/report.py`
- Modify: `domains/traffic/traffic_ingest/reliability/config.py`

**Interfaces:**
- Consumes: existing landing ledger, pending receipts, Incident/Flow manifest, Traffic audit, stage summaries, history
- Produces: `collect_traffic_data_plane()`, `compose_traffic_pipeline_report()`

- [x] **Step 1: RED tests 작성**

다음을 독립 test로 고정한다.

- Incident/Flow manifest를 모두 포함한다.
- data-plane FAIL은 Marquez PASS보다 우선한다.
- Marquez unavailable + data PASS는 WARN이다.
- 최신 stage FAIL/stale은 FAIL이다.
- 과거 실패 뒤 최신 성공은 WARN이다.
- Asset stage count를 landing expected count와 비교하지 않는다.
- bottleneck은 관측된 stage p95 중 최대이며 `pool_wait`라고 이름 붙이지 않는다.

- [x] **Step 2: RED 확인**

Run Traffic repository/composition tests. Expected: missing v2 key/function assertion FAIL.

- [x] **Step 3: 최소 구현**

```python
def collect_traffic_data_plane(cursor=None, detected_at: datetime | None = None) -> dict[str, Any]: ...

def compose_traffic_pipeline_report(*, data_plane: dict[str, Any], stages: dict[str, Any], history: list[dict[str, Any]], detected_at: datetime) -> dict[str, Any]: ...
```

기존 `build_traffic_reliability_report()`는 compatibility facade로 두고 위 두 함수를 순서대로
호출한다. report name은 `traffic_pipeline_reliability_v2`로 올리되 기존 top-level data key는
한 release 동안 유지한다.

- [x] **Step 4: GREEN 확인 후 commit**

Run all Traffic reliability tests. Expected: PASS.

---

### Task 4: Weather 전체 pipeline report 합성

**Files:**
- Modify: `domains/weather/tests/weather_reliability_test_support.py`
- Modify: `domains/weather/tests/test_weather_reliability_trino_repository.py`
- Modify: `domains/weather/tests/test_weather_reliability_report_composition.py`
- Modify: `domains/weather/weather_ingest/reliability/trino_repository.py`
- Modify: `domains/weather/weather_ingest/reliability/report.py`
- Modify: `domains/weather/weather_ingest/reliability/config.py`

**Interfaces:**
- Consumes: Weather Bronze/manifest, transform/source-freshness/maintenance stage summary, history
- Produces: `collect_weather_data_plane()`, `compose_weather_pipeline_report()`

- [x] **Step 1: RED tests 작성**

Traffic과 같은 precedence를 적용하되 Weather KMA base/grid/raw page coverage를 보존한다.
maintenance 미관측은 `UNKNOWN` informational, 실제 최신 maintenance FAILED는 FAIL로 test한다.

- [x] **Step 2: RED 확인**

Run Weather repository/composition tests. Expected: v2 assertion FAIL.

- [x] **Step 3: 최소 구현**

기존 `build_weather_reliability_report()` compatibility facade와 기존 Weather dict field를
유지하면서 pipeline/stages/trend/bottleneck을 추가한다.

- [x] **Step 4: GREEN 확인 후 commit**

Run all Weather reliability tests. Expected: PASS.

---

### Task 5: 구조화된 Discord 카드

**Files:**
- Modify: `domains/traffic/tests/test_traffic_reliability_discord.py`
- Modify: `domains/weather/tests/test_weather_reliability_discord.py`
- Modify: both domain `reliability/discord.py`

**Interfaces:**
- Consumes: v2 report dict
- Produces: `build_*_discord_payload(report) -> dict`, `send_discord_report(report, webhook_url=None) -> bool`

- [x] **Step 1: RED tests 작성**

PASS/WARN/FAIL color, five named fields, 7-day icons, bottleneck label, field/total limits, Korean
UTF-8, webhook redaction을 test한다. formatter는 report status로 색상을 결정해야 한다.

- [x] **Step 2: RED 확인**

Run both Discord suites. Expected: missing payload API FAIL.

- [x] **Step 3: 최소 구현**

기존 `format_*_discord_message()`는 readable text compatibility facade로 유지한다. transport는
구조화 payload를 JSON UTF-8로 보내며 response status >=400은 False를 반환한다.

- [x] **Step 4: GREEN 확인 후 commit**

Run both Discord suites. Expected: PASS.

---

### Task 6: Airflow 3-task DAG와 daily delivery migration

**Files:**
- Modify: `domains/traffic/tests/test_traffic_reliability_dag.py`
- Modify: `domains/weather/tests/test_weather_reliability_dag.py`
- Modify: both top-level reliability DAG files

**Interfaces:**
- Produces task graph: `collect_data_plane >> compose_pipeline_reliability >> deliver_pipeline_reliability`

- [x] **Step 1: RED DAG contract tests 작성**

각 DAG에서 정확히 세 task, domain heavy pool은 collect task에만 적용, 09:00 schedule,
`max_active_runs=1`, 모든 task failure callback, lineage enable, daily idempotency를 assert한다.

- [x] **Step 2: RED 확인**

Run both DAG tests. Expected: 기존 single-task topology 때문에 FAIL.

- [x] **Step 3: 최소 DAG 구현**

collect task는 data-plane dict, compose task는 XCom data와 Marquez/history를 합친 report,
deliver task는 history write와 Discord 전송·fingerprint persistence 결과를 반환한다.
delivery fingerprint contract는 `pipeline-reliability-daily-v2`이며 성공 전 claim을 금지한다.

- [x] **Step 4: GREEN·failure alert regression 확인**

Run DAG, architecture, failure-alert contract suites. Expected: PASS.

- [x] **Step 5: commit**

```powershell
git add domains/traffic domains/weather
git commit -m "feat(reliability): report end-to-end pipeline health"
```

---

### Task 7: 문서·전체 검증·dev smoke

**Files:**
- Modify: `domains/traffic/README.md`
- Modify: `domains/weather/README.md`

- [x] **Step 1: README 계약 갱신**

Bronze-only 설명을 Pipeline Reliability v2, 09:00 daily, immediate failure callback,
Trino/R2/Marquez source, 7-day observed trend로 교체한다.

- [x] **Step 2: 정적·전체 unit 검증**

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
python -m pytest -q -p no:cacheprovider domains/weather/tests domains/traffic/tests
python -m compileall -q domains/weather domains/traffic
python -m ruff check --no-cache domains/weather domains/traffic
git diff --check
git status --short
```

Expected: tests PASS, compile PASS, ruff 신규 오류 0, diff check PASS, cache artifact 0.

- [ ] **Step 3: container integration smoke**

feature ref를 clean dev harness에 mount하고 실제 Discord 전송은 stub한다. 실제 dev Trino/R2와
Marquez를 읽어 두 report가 JSON-safe v2 dict를 만들고 secret/error string을 포함하지 않는지
확인한다. 정규 Traffic/Weather run과 heavy pool이 idle이 아닐 때는 smoke를 실행하지 않는다.

- [ ] **Step 4: PR 전 검증 보고**

DAG ID/schedule, report status, stage 수, trend observed days, query/pool behavior, 변경 파일,
테스트 수를 기록한다. `.omc`, `.omx`, `__pycache__`, `.pytest_cache`가 없음을 다시 확인한다.

- [ ] **Step 5: PR/merge/redeploy**

사용자 승인 범위에 따라 dev PR을 생성하고 CI를 확인한다. merge 뒤 clean runtime mount를 exact
`origin/dev`로 갱신해 직접 compose 배포한다. root submodule pointer PR은 만들지 않는다.

- [ ] **Step 6: 운영 관찰**

다음 09:00 KST의 Weather/Traffic 카드, 즉시 실패 callback, pipeline stage convergence를
24시간 관찰한다. 이 기간 manual recovery/recollect/backfill/W2는 paused를 유지한다.

---

## Self-Review

- Spec coverage: 데이터 정본, Marquez 보조 관측, 3-task DAG, 7-day history, Discord,
  daily idempotency, 즉시 실패 알림, dev/domain 경계가 Task 1~7에 각각 연결된다.
- 문서 내 미정 항목이나 후속 구현 표시는 남기지 않는다.
- Type consistency: `StagePolicy`, `collect_pipeline_stages`, `collect_*_data_plane`,
  `compose_*_pipeline_report`, history와 Discord API 이름을 전 task에서 동일하게 사용한다.
