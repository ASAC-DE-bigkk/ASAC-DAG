# culture SLO 마트 구현 계획 (#257 + DBT#110)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** run_report(수집 성적표 JSON)와 Airflow `dag_run` 상태를 메달리온으로 흘려 "자정 수집 가용률·적재 추세·초록 위장 run"을 SQL로 답하게 하는 culture SLO 마트를 만든다.

**Architecture:** 신규 `culture_slo` DAG(05:00 KST)가 ① R2 `_reports/` 스캔→`bronze_culture_run_report` ② Airflow 메타DB `dag_run` 스캔→`bronze_culture_dag_runs`(14일 윈도우 멱등)를 적재하고 `dbt build --select tag:slo`를 실행한다. dbt는 silver 3 + gold 1(`gold_culture_slo_daily`)로 변환하며 `slo_passed = raw AND expected>0` 교정으로 "초록 위장"을 사후 판정한다. 본류(`culture_bronze`/`culture_transform`) 수정은 transform의 `--exclude tag:slo` 한 줄뿐.

**Tech Stack:** Airflow 3.2.2(pendulum KST, `airflow.sdk`), Python(boto3 R2 Sink, SQLAlchemy `create_session`), Trino HTTP INSERT, dbt-trino on Iceberg, pytest(호스트).

## Global Constraints

- 자기 도메인만: `domains/culture/`(DAG 레포)·`domains/culture/`(DBT 레포)만 수정. `common/`·타 도메인 읽기 전용.
- API 키·시크릿 값을 코드/로그/커밋에 절대 노출 금지. 로더는 `build_r2_sink`/`build_warehouse`가 처리하는 기존 env 경로만 사용.
- 모든 `_at` 컬럼 KST(#48 표준). SLO 모델은 **공간축 면제**(boxoffice 선례) — `culture_quality_status`/dong_map 미적용.
- silver/gold **도메인 중립**(§6): `domain` 컬럼 포함, metric 이름에 culture 접두어 금지(테이블명만 스키마 관례 유지). population 복사 채택 대비.
- gold `gold_culture_slo_daily`는 **contract enforced**(전 컬럼 `data_type`), 비율은 `decimal(38,18)`(double 아님). snake_case.
- 로더 = **순수 함수 + 도메인 파라미터화**(§6.2, `_shared` 승격 대비). dag_run 스캐너는 전 도메인 스캔 가능하게 짜되 v1은 culture 4 DAG만 적재.
- `bronze_culture_run_report`는 warehouse 엔진 디스패치 재사용(기본 trino). `bronze_culture_dag_runs`는 **v1 trino 엔진 고정**(14일 delete+insert 멱등, pyiceberg delete는 v2).
- 커밋 마지막 줄: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. PR body 마지막: `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. **PR 셀프 머지 금지**(사용자 머지).
- 라이브 검증(dbt build·컨테이너)·머지는 **게이트 체인 후**로 이연. 이 계획의 완료 = 코드 + 호스트 테스트 green + 파싱 스모크(가능 범위).
- 작업 위치: DAG 태스크 = worktree `…/scratchpad/wt-dag-257`(브랜치 `feat/257-culture-slo`), DBT 태스크 = worktree `C:\Users\Dell3571\ask-seoul\.wt\dbt110`(브랜치 `feat/110-culture-slo-mart`). 도는 컨테이너(sample/dags·sample/dbt)는 **건드리지 않는다**.

---

## 파일 구조 (Files map)

**ASAC-DAG (worktree wt-dag-257):**
- Create `domains/culture/culture_ingest/slo/__init__.py` — 패키지.
- Create `domains/culture/culture_ingest/slo/loader.py` — 순수 함수(리포트 스캔·파싱, dag_run 행 빌드) + 트리노 IO 래퍼. #257 핵심 로직.
- Create `domains/culture/culture_slo.py` — 2태스크 DAG.
- Modify `domains/culture/culture_transform.py:86,90` — `--exclude`에 `tag:slo` 추가.
- Create `domains/culture/tests/test_slo_loader.py` — 순수 함수 유닛(FakeSink/Fake trino).
- Create `domains/culture/tests/test_slo_schedule.py` — 스케줄 regex 회귀.
- Create `domains/culture/tests/test_transform_dbt_selection.py` — transform `--exclude tag:slo` 토큰 검증(traffic 선례 축약판). *(weather/traffic엔 있으나 culture엔 없음 — 신규)*
- Modify `domains/culture/docs/change-log.md` — 항목 추가.
- (이 계획 문서: `domains/culture/docs/design/2026-07-16-culture-slo-mart-plan.md`)

**ASAC-DBT (worktree .wt/dbt110):**
- Modify `domains/culture/models/sources.yml` — `culture_bronze.tables`에 2건 추가.
- Create `domains/culture/models/silver/silver_culture_slo_run.sql`
- Create `domains/culture/models/silver/silver_culture_slo_dataset.sql`
- Create `domains/culture/models/silver/silver_culture_dag_run.sql`
- Create `domains/culture/models/gold/gold_culture_slo_daily.sql`
- Modify `domains/culture/models/silver/_culture_silver__models.yml` — silver 3 등록(+`tags:['slo']`·grain 테스트).
- Modify `domains/culture/models/gold/_culture_gold__models.yml` — gold 1 등록(+contract·`tags:['slo']`).
- Create `domains/culture/tests/assert_culture_slo_dataset_grain_unique.sql` — 복합 그레인(run×dataset) 단일 테스트.
- Create `domains/culture/tests/assert_culture_slo_regression_77.sql` — 7/7 회귀(라이브 게이트, 게이트 후 활성).
- Modify `domains/culture/docs/` 해당 문서 or README — 대표 가용률 쿼리 동봉.

**소스 스키마 참조(run_report JSON, DAG map §3):** top-level = `domain, layer, load_date, ingest_ts, run_id, coverage{expected,landed,skipped,failed,coverage_pct}, total_rows, total_iceberg_rows, load_failed, freshness{max_age_hours}, violation_count, violations[], failed_datasets[], slo_passed, datasets[]`. `datasets[]` 요소 = `name, source, endpoint, prefix, pages, rows, bytes, error, checks{...}, iceberg_rows, duration_sec, finished_ts`. R2 키 = `raw/culture/_reports/load_date=<ld>/ingest_ts=<ts>/run_report.json`.

---

## Part A — ASAC-DAG (#257) · worktree `wt-dag-257`

호스트 테스트 실행 전제: `cd domains/culture && python -m pytest <경로> -q` (culture `tests/conftest.py`가 `_CULTURE`+`_DAGS_ROOT`를 sys.path에 삽입).

### Task A1: SLO 리포트 스캔 순수 함수

**Files:**
- Create: `domains/culture/culture_ingest/slo/__init__.py` (빈 파일)
- Create: `domains/culture/culture_ingest/slo/loader.py`
- Test: `domains/culture/tests/test_slo_loader.py`

**Interfaces:**
- Consumes: `culture_ingest.common.landing.Sink`(`.list(prefix)->list[str]`, `.get(key)->bytes`), `culture_ingest.common.config.LANDING_ROOT`(="raw/culture").
- Produces:
  - `REPORTS_PREFIX = f"{LANDING_ROOT}/_reports/"`
  - `report_ingest_ts(key: str) -> str | None` — 키에서 `ingest_ts=([0-9TZ]+)` 추출(선례 `source/ingest.py:472` 정규식 재사용).
  - `scan_new_reports(sink, already_loaded: set[str]) -> list[tuple[str, dict]]` — `_reports/` 나열→`run_report.json` 필터→`already_loaded`에 없는 ingest_ts만→`json.loads(sink.get(key))`. 반환 `[(raw_object_key, report_dict), ...]`, ingest_ts 오름차순(사전식=시간순).
  - `report_records(reports: list[tuple[str,dict]]) -> list[tuple[str,int,dict]]` — `[(key, 0, report) for key, report in reports]` (warehouse.load 계약 = `(raw_object_key, page_no, record)`).

- [ ] **Step 1: 실패 테스트** — `tests/test_slo_loader.py`

```python
from culture_ingest.slo import loader

class FakeSink:
    def __init__(self, objects): self._o = objects            # {key: bytes}
    def list(self, prefix): return [k for k in self._o if k.startswith(prefix)]
    def get(self, key): return self._o[key]

def _key(ts): return f"raw/culture/_reports/load_date=2026-07-07/ingest_ts={ts}/run_report.json"

def test_report_ingest_ts_extracts():
    assert loader.report_ingest_ts(_key("20260707T000000Z")) == "20260707T000000Z"
    assert loader.report_ingest_ts("raw/culture/foo.json") is None

def test_scan_skips_loaded_and_sorts():
    objs = {
        _key("20260707T000000Z"): b'{"ingest_ts":"20260707T000000Z","run_id":"a"}',
        _key("20260706T000000Z"): b'{"ingest_ts":"20260706T000000Z","run_id":"b"}',
        "raw/culture/_reports/load_date=2026-07-07/ingest_ts=20260707T000000Z/other.json": b'{}',
    }
    got = loader.scan_new_reports(FakeSink(objs), already_loaded={"20260706T000000Z"})
    assert [r["run_id"] for _, r in got] == ["a"]              # 로드된 것 스킵 + run_report.json만

def test_report_records_shape():
    recs = loader.report_records([("k1", {"run_id": "a"})])
    assert recs == [("k1", 0, {"run_id": "a"})]
```

- [ ] **Step 2: 실패 확인** — `python -m pytest tests/test_slo_loader.py -q` → FAIL(`ModuleNotFoundError: culture_ingest.slo`).

- [ ] **Step 3: 구현** — `culture_ingest/slo/__init__.py`(빈 파일) + `culture_ingest/slo/loader.py`:

```python
"""culture SLO 로더 — 순수 함수(스캔·파싱·행빌드) + 트리노 IO 래퍼.

설계: domains/culture/docs/design/2026-07-07-culture-slo-mart.md §3/§4.
순수 함수는 호스트 pytest 로 검증, 트리노 IO 는 컨테이너 라이브(게이트 후).
"""
from __future__ import annotations

import json
import re

from culture_ingest.common.config import LANDING_ROOT

REPORTS_PREFIX = f"{LANDING_ROOT}/_reports/"
_INGEST_TS_RE = re.compile(r"ingest_ts=([0-9TZ]+)")   # source/ingest.py:472 와 동일 규약


def report_ingest_ts(key: str) -> str | None:
    m = _INGEST_TS_RE.search(key)
    return m.group(1) if m else None


def scan_new_reports(sink, already_loaded: set[str]) -> list[tuple[str, dict]]:
    """R2 _reports/ 에서 아직 안 실린 run_report.json 만 (키, dict) 로 — ingest_ts 오름차순."""
    keys = [k for k in sink.list(REPORTS_PREFIX) if k.endswith("run_report.json")]
    fresh = []
    for key in keys:
        ts = report_ingest_ts(key)
        if ts is None or ts in already_loaded:
            continue
        fresh.append((ts, key, json.loads(sink.get(key))))
    fresh.sort(key=lambda t: t[0])                     # ingest_ts UTC → 사전식=시간순
    return [(key, report) for _ts, key, report in fresh]


def report_records(reports: list[tuple[str, dict]]) -> list[tuple[str, int, dict]]:
    """warehouse.load 계약: (raw_object_key, page_no, record). 리포트 1건=1행이라 page_no=0."""
    return [(key, 0, report) for key, report in reports]
```

- [ ] **Step 4: 통과 확인** — `python -m pytest tests/test_slo_loader.py -q` → PASS(3개).

- [ ] **Step 5: 커밋**

```bash
git add domains/culture/culture_ingest/slo/__init__.py domains/culture/culture_ingest/slo/loader.py domains/culture/tests/test_slo_loader.py
git commit -m "feat(culture): SLO 리포트 스캔 순수 함수 (#257)"
```

### Task A2: `bronze_culture_dag_runs` 행 빌더 + 트리노 IO 래퍼

**Files:**
- Modify: `domains/culture/culture_ingest/slo/loader.py`
- Test: `domains/culture/tests/test_slo_loader.py` (추가)

**Interfaces:**
- Produces:
  - `CULTURE_SLO_DAG_IDS = ("culture_bronze", "culture_transform", "culture_maintenance", "culture_facility_refresh")` (설계 §3, dag_run 스캔 대상 4개).
  - `dag_run_row(dag_run, *, domain: str) -> dict` — Airflow `DagRun` ORM 객체 1개를 타입드 dict로. 키: `domain, dag_id, run_id, state, run_type, start_at, end_at, duration_sec, load_date`. `start_at/end_at`은 KST ISO(UTC+9), `duration_sec = (end-start).total_seconds()` 또는 None, `load_date`=start의 KST date(문자열 `YYYY-MM-DD`).
  - `run_report_ingest_ts_query(catalog: str, schema: str = "culture") -> str` — `SELECT DISTINCT ingest_ts FROM {catalog}.{schema}.bronze_culture_run_report` (이미 적재된 ingest_ts 집합 조회 SQL).
  - `dag_runs_window_delete_sql(catalog, schema, load_date_from: str) -> str` / `dag_runs_insert_sql(...)` — 14일 윈도우 delete + values insert. *(트리노 실행은 A3 DAG 태스크가 담당; 여기선 SQL/행 빌더까지 순수 검증)*

- [ ] **Step 1: 실패 테스트** (추가)

```python
import datetime as dt
from culture_ingest.slo import loader

class FakeDagRun:
    def __init__(self, dag_id, run_id, state, run_type, start, end):
        self.dag_id, self.run_id, self.state, self.run_type = dag_id, run_id, state, run_type
        self.start_date, self.end_date = start, end

def test_dag_run_row_typed_kst():
    r = FakeDagRun("culture_bronze", "scheduled__2026-07-07T18:00:00+00:00", "success",
                   "scheduled",
                   dt.datetime(2026, 7, 7, 18, 0, tzinfo=dt.timezone.utc),
                   dt.datetime(2026, 7, 7, 18, 5, tzinfo=dt.timezone.utc))
    row = loader.dag_run_row(r, domain="culture")
    assert row["dag_id"] == "culture_bronze"
    assert row["duration_sec"] == 300.0
    assert row["load_date"] == "2026-07-08"          # 18:00Z +9h = 07-08 03:00 KST
    assert row["start_at"].startswith("2026-07-08T03:00:00")
    assert row["domain"] == "culture"

def test_dag_run_row_running_has_no_end():
    r = FakeDagRun("culture_transform", "manual__x", "running", "manual",
                   dt.datetime(2026, 7, 7, 0, 0, tzinfo=dt.timezone.utc), None)
    row = loader.dag_run_row(r, domain="culture")
    assert row["end_at"] is None and row["duration_sec"] is None

def test_slo_dag_ids_are_four_culture_dags():
    assert loader.CULTURE_SLO_DAG_IDS == (
        "culture_bronze", "culture_transform", "culture_maintenance", "culture_facility_refresh")
```

- [ ] **Step 2: 실패 확인** — `python -m pytest tests/test_slo_loader.py -q` → FAIL(`AttributeError: dag_run_row`).

- [ ] **Step 3: 구현** — `loader.py`에 추가:

```python
import datetime as _dt

CULTURE_SLO_DAG_IDS = (
    "culture_bronze", "culture_transform", "culture_maintenance", "culture_facility_refresh",
)
_KST = _dt.timezone(_dt.timedelta(hours=9))


def _to_kst_iso(value):
    if value is None:
        return None
    return value.astimezone(_KST).isoformat()


def dag_run_row(dag_run, *, domain: str) -> dict:
    """Airflow DagRun ORM → 타입드 dict(KST _at). 도메인 파라미터화(§6.2 _shared 대비)."""
    start = getattr(dag_run, "start_date", None)
    end = getattr(dag_run, "end_date", None)
    duration = (end - start).total_seconds() if (start and end) else None
    load_date = start.astimezone(_KST).date().isoformat() if start else None
    return {
        "domain": domain,
        "dag_id": dag_run.dag_id,
        "run_id": dag_run.run_id,
        "state": getattr(dag_run, "state", None),
        "run_type": getattr(dag_run, "run_type", None),
        "start_at": _to_kst_iso(start),
        "end_at": _to_kst_iso(end),
        "duration_sec": duration,
        "load_date": load_date,
    }
```

- [ ] **Step 4: 통과 확인** — `python -m pytest tests/test_slo_loader.py -q` → PASS(6개).

- [ ] **Step 5: 커밋**

```bash
git add domains/culture/culture_ingest/slo/loader.py domains/culture/tests/test_slo_loader.py
git commit -m "feat(culture): dag_run 타입드 행 빌더 + SLO DAG 목록 (#257)"
```

> **구현 노트(트리노 IO, A3에서 배선):** 실제 트리노 읽기/쓰기는 `culture_ingest.source.ingest.build_warehouse(target, engine="trino")`가 주는 `BronzeWarehouse`의 커넥션(`warehouse.py`)을 재사용한다. `bronze_culture_run_report`는 `warehouse.load(ds, ctx, records)`로 적재(합성 `Dataset(name="culture_run_report", source="culture", endpoint="_reports", row_tag="")` — `warehouse.load`는 `.name/.source/.endpoint`만 읽음, 표는 `bronze_culture_run_report`로 생성됨). `bronze_culture_dag_runs`는 record_json 11열 형태가 아니라 타입드라 별도 CREATE/INSERT(트리노). 이 IO 함수들은 컨테이너 라이브(게이트 후)에서만 검증 — 호스트 유닛은 순수 부분만.

### Task A3: `culture_slo` DAG (2태스크) + 스케줄 테스트

**Files:**
- Create: `domains/culture/culture_slo.py`
- Test: `domains/culture/tests/test_slo_schedule.py`

**Interfaces:**
- Consumes: `culture_ingest.slo.loader`(A1·A2), `culture_ingest.source.ingest.build_warehouse`/`build_r2_sink`, `common.errors.airflow.problem_failure_callback`.
- Produces: 모듈 전역 `dag`(dag_id="culture_slo"), 태스크 `load_slo_bronze`(Python) `>>` `dbt_slo`(Bash `dbt build --select tag:slo`).

- [ ] **Step 1: 실패 테스트** — `tests/test_slo_schedule.py` (소스 regex 회귀, `test_bronze_schedule.py` 템플릿):

```python
import pathlib, re

_SLO = pathlib.Path(__file__).resolve().parents[1] / "culture_slo.py"

def test_slo_dag_schedule_is_5am_kst():
    text = _SLO.read_text(encoding="utf-8")
    assert re.search(r'schedule\s*=\s*"0 5 \* \* \*"', text)

def test_slo_dbt_task_selects_tag_slo():
    text = _SLO.read_text(encoding="utf-8")
    assert "build --select tag:slo" in text
```

- [ ] **Step 2: 실패 확인** — `python -m pytest tests/test_slo_schedule.py -q` → FAIL(파일 없음).

- [ ] **Step 3: 구현** — `culture_slo.py` (프롤로그·`_dbt` 헬퍼는 `culture_transform.py`·`culture_maintenance.py`에서 verbatim 복사):

```python
"""culture_slo — run_report·dag_runs → SLO bronze 적재 + dbt tag:slo 빌드 (05:00 KST).

설계 §3: 본류(bronze 03:00 → transform ~04:00) 뒤·facility_refresh(05:30) 앞 슬롯.
Asset outlet 없음(스케줄 구동) — 본류 무수정 원칙(§3).
"""
import os
import shlex
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)
from common.errors.airflow import problem_failure_callback  # noqa: E402
from culture_ingest.slo.loader import (  # noqa: E402
    CULTURE_SLO_DAG_IDS, dag_run_row, report_records, scan_new_reports,
)

KST = "Asia/Seoul"
record_culture_problem = problem_failure_callback(domain="culture")

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/culture"
DEFAULT_PARAMS = {"target": "dev"}


def _dbt(args: str) -> str:                                    # culture_transform.py:53-61 동일
    project = shlex.quote(DBT_PROJECT)
    return ("set -euo pipefail\n"
            f"cd {project}\n"
            f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
            f"{shlex.quote(DBT_BIN)} {args} --target {{{{ params.target }}}} --no-use-colors")


def _load_slo_bronze(**context):
    """R2 리포트 + Airflow dag_run 을 SLO bronze 2표에 적재(멱등)."""
    from culture_ingest.slo.io import load_run_reports, load_dag_runs  # A2 IO 래퍼
    target = context["params"]["target"]
    n_reports = load_run_reports(target=target)
    n_dag_runs = load_dag_runs(target=target, dag_ids=CULTURE_SLO_DAG_IDS, window_days=14)
    print(f"slo bronze: run_reports+{n_reports}, dag_runs={n_dag_runs}")


with DAG(
    dag_id="culture_slo",
    description="run_report·dag_runs → SLO bronze 적재 + dbt tag:slo (05:00 KST)",
    start_date=pendulum.datetime(2026, 7, 16, tz=KST),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["culture", "slo", "observability"],
) as dag:
    load_slo_bronze = PythonOperator(
        task_id="load_slo_bronze",
        python_callable=_load_slo_bronze,
        on_failure_callback=record_culture_problem,
    )
    dbt_slo = BashOperator(
        task_id="dbt_slo",
        bash_command=_dbt("build --select tag:slo"),
        on_failure_callback=record_culture_problem,
    )
    load_slo_bronze >> dbt_slo
```

> **A2 IO 모듈 분리:** 트리노 실행 로직(`load_run_reports`/`load_dag_runs`)은 `culture_ingest/slo/io.py`에 두고, A2의 순수 함수(`scan_new_reports`/`dag_run_row`)를 조합한다. `load_run_reports`: `sink=build_r2_sink(target)`; 기존 ingest_ts 집합 = `warehouse` 커넥션으로 `SELECT DISTINCT ingest_ts FROM ...bronze_culture_run_report`(표 없으면 빈 집합); `scan_new_reports(sink, loaded)` → `report_records(...)` → `warehouse.load(synthetic_ds, ctx, records)`. `load_dag_runs`: `from airflow.utils.session import create_session; from airflow.models.dagrun import DagRun`; `session.query(DagRun).filter(DagRun.dag_id.in_(dag_ids))` 14일 윈도우 → `[dag_run_row(r, domain="culture") for r in runs]` → 트리노 14일 윈도우 delete+insert. 이 모듈은 Airflow/트리노 의존이라 호스트 유닛 없이 컨테이너 라이브(게이트 후) 검증 — 전역 배선은 `test_dag_global_wiring`이 커버.

- [ ] **Step 4: 통과 확인** — `python -m pytest tests/test_slo_schedule.py tests/test_dag_global_wiring.py -q` → PASS(스케줄 2 + 전역배선 파라미터 케이스에 `culture_slo.py` 자동 포함).

- [ ] **Step 5: 커밋**

```bash
git add domains/culture/culture_slo.py domains/culture/culture_ingest/slo/io.py domains/culture/tests/test_slo_schedule.py
git commit -m "feat(culture): culture_slo DAG + IO 래퍼 (#257)"
```

### Task A4: `culture_transform` 에서 SLO 모델 제외 (본류 한 줄)

**Files:**
- Modify: `domains/culture/culture_transform.py:86,90`
- Test: `domains/culture/tests/test_transform_dbt_selection.py`

**Interfaces:** Consumes 없음. Produces 없음(본류 빌드가 미존재 SLO 소스를 빌드하다 깨지는 것 방지 — SLO 모델 소유권은 culture_slo).

- [ ] **Step 1: 실패 테스트** — `tests/test_transform_dbt_selection.py`:

```python
import pathlib, re

_TF = pathlib.Path(__file__).resolve().parents[1] / "culture_transform.py"

def test_transform_run_excludes_tag_slo():
    text = _TF.read_text(encoding="utf-8")
    assert re.search(r'run --exclude package:asac_axes tag:slo', text)

def test_transform_test_excludes_tag_slo():
    text = _TF.read_text(encoding="utf-8")
    assert re.search(r'test --exclude package:asac_axes tag:slo', text)
```

- [ ] **Step 2: 실패 확인** — `python -m pytest tests/test_transform_dbt_selection.py -q` → FAIL.

- [ ] **Step 3: 구현** — `culture_transform.py`:
  - L86 `_dbt("run --exclude package:asac_axes")` → `_dbt("run --exclude package:asac_axes tag:slo")`
  - L90 `_dbt("test --exclude package:asac_axes")` → `_dbt("test --exclude package:asac_axes tag:slo")`

- [ ] **Step 4: 통과 확인** — `python -m pytest tests/test_transform_dbt_selection.py -q` → PASS(2개).

- [ ] **Step 5: 커밋**

```bash
git add domains/culture/culture_transform.py domains/culture/tests/test_transform_dbt_selection.py
git commit -m "feat(culture): transform 에서 tag:slo 제외 (#257)"
```

### Task A5: docs 스윕 + 전체 호스트 테스트

**Files:** Modify `domains/culture/docs/change-log.md`.

- [ ] **Step 1:** change-log에 항목 추가(날짜 2026-07-16, "#257 culture_slo DAG + SLO bronze 로더 — run_report/dag_runs 적재, transform tag:slo 제외. 라이브 검증·머지는 게이트 후").
- [ ] **Step 2: 전체 호스트 테스트** — `cd domains/culture && python -m pytest -q` → 기존 전량 + 신규(test_slo_loader 6·test_slo_schedule 2·test_transform_dbt_selection 2) PASS, 실패 0. `test_dag_global_wiring`가 `culture_slo.py` 배선 커버 확인.
- [ ] **Step 3: 커밋**

```bash
git add domains/culture/docs/change-log.md
git commit -m "docs(culture): SLO 마트 로더 change-log (#257)"
```

---

## Part B — ASAC-DBT (DBT#110) · worktree `.wt/dbt110`

> **검증 게이트:** dbt parse/compile/build 는 컨테이너 dev 타깃 필요(호스트 실행 불가). 이 파트의 코드 완료 후 검증은 **게이트 체인 후** 컨테이너에서 `dbt build --select tag:slo`. 코드 시점 자체 점검 = SQL 육안 리뷰 + (가능 시)컨테이너 `dbt parse`. 각 태스크의 "테스트"는 모델+스키마+계약의 **정합성 리뷰**이며, 라이브 실측(7/7 회귀)은 최종 게이트.

### Task B1: SLO bronze 2건 sources.yml 등록

**Files:** Modify `domains/culture/models/sources.yml`.

- [ ] **Step 1:** `culture_bronze.tables` 아래에 추가 (`bronze_kopis_boxoffice` 엔트리 형태 참조):

```yaml
      - name: bronze_culture_run_report
        description: culture_slo 로더 적재 — run_report.json 원본(record_json). silver 가 json_extract 로 파싱.
        columns:
          - name: record_json
            description: run_report 전문(JSON 문자열)
          - name: ingest_ts
            description: 적재 실행 식별 타임스탬프(UTC)
          - name: load_date
            description: 적재 파티션 일자(KST)
      - name: bronze_culture_dag_runs
        description: culture_slo 로더 적재 — Airflow dag_run 타입드 스냅샷(14일 윈도우 멱등).
        loaded_at_field: start_at
        freshness:
          warn_after: {count: 30, period: hour}
          error_after: {count: 48, period: hour}
        columns:
          - name: domain
          - name: dag_id
          - name: run_id
          - name: state
          - name: run_type
          - name: start_at
          - name: end_at
          - name: duration_sec
          - name: load_date
```

- [ ] **Step 2: 리뷰** — 두 소스가 그룹 `culture_bronze`(`database/schema` = target) 아래 정확히 들어갔는지, `bronze_culture_run_report`가 record_json 관례(freshness 그룹 기본 상속)인지 확인.
- [ ] **Step 3: 커밋** `git commit -m "feat(culture): SLO bronze 2건 sources 등록 (#110)"`

### Task B2: `silver_culture_slo_run` (run 그레인)

**Files:** Create `domains/culture/models/silver/silver_culture_slo_run.sql`; Modify `_culture_silver__models.yml`.

**Interfaces:** Produces run 그레인 컬럼 — `domain, run_id, load_date, event_at(KST), ingest_ts, expected, landed, skipped, failed, coverage_pct, total_rows, total_iceberg_rows, load_failed, violation_count, slo_passed_raw, slo_passed, run_kind, report_object_key` + `culture_lineage` 계보.

- [ ] **Step 1:** 모델 SQL (`silver_culture_boxoffice.sql` 템플릿 — 스냅샷 그레인·공간축 면제·quality_status 없음). `record_json`을 `json_extract_scalar`로 파싱, `slo_passed = raw AND expected>0` 교정:

```sql
{{ config(tags=['slo']) }}
with bronze as (
    select * from {{ source('culture_bronze', 'bronze_culture_run_report') }}
),
typed as (
    select
        json_extract_scalar(record_json, '$.domain')                       as domain,
        json_extract_scalar(record_json, '$.run_id')                       as run_id,
        try(cast(json_extract_scalar(record_json, '$.load_date') as date)) as load_date,
        json_extract_scalar(record_json, '$.ingest_ts')                    as ingest_ts,
        try(cast(json_extract_scalar(record_json, '$.coverage.expected') as integer))     as expected,
        try(cast(json_extract_scalar(record_json, '$.coverage.landed') as integer))       as landed,
        try(cast(json_extract_scalar(record_json, '$.coverage.skipped') as integer))      as skipped,
        try(cast(json_extract_scalar(record_json, '$.coverage.failed') as integer))       as failed,
        try(cast(json_extract_scalar(record_json, '$.coverage.coverage_pct') as double))  as coverage_pct,
        try(cast(json_extract_scalar(record_json, '$.total_rows') as bigint))             as total_rows,
        try(cast(json_extract_scalar(record_json, '$.total_iceberg_rows') as bigint))     as total_iceberg_rows,
        coalesce(try(cast(json_extract_scalar(record_json, '$.load_failed') as boolean)), false) as load_failed,
        try(cast(json_extract_scalar(record_json, '$.violation_count') as integer))       as violation_count,
        coalesce(try(cast(json_extract_scalar(record_json, '$.slo_passed') as boolean)), false)  as slo_passed_raw,
        raw_object_key                                                     as report_object_key,
        {{ culture_lineage('culture') }}
    from bronze
),
final as (
    select
        domain, run_id, load_date,
        cast(load_date as timestamp(6))                                    as event_at,
        ingest_ts, expected, landed, skipped, failed, coverage_pct,
        total_rows, total_iceberg_rows, load_failed, violation_count,
        slo_passed_raw,
        (slo_passed_raw and coalesce(expected, 0) > 0)                     as slo_passed,
        case
            when run_id like 'scheduled__%' then 'scheduled'
            when run_id like 'backfill%'    then 'backfill'
            else 'manual'
        end                                                               as run_kind,
        report_object_key,
        source_system, dag_run_id, raw_object_key, collected_at, ingested_at
    from typed
),
deduped as (
    select *, row_number() over (
        partition by run_id order by {{ culture_dedup_order() }}
    ) as rn
    from final
)
select * from deduped where rn = 1
```

> **주의:** `culture_lineage('culture')`가 `load_date/ingest_ts/collected_at/ingested_at/raw_object_key/source_system/dag_run_id`를 emit하므로 `typed`에서 이미 뽑은 `load_date/ingest_ts/raw_object_key`와 **이름 충돌**한다. 구현 시 `culture_lineage` 산출과 겹치는 컬럼은 typed에서 alias를 바꾸거나(예: `report_object_key`만 유지) 매크로 산출을 우선. 실제 `culture_lineage` 컬럼셋을 매크로(`macros/culture_axes.sql:10-17`)에서 확인 후 중복 제거할 것. `culture_dedup_order()`는 `load_date desc, ingest_ts desc, raw_object_key desc`.

- [ ] **Step 2:** `_culture_silver__models.yml`에 등록:

```yaml
  - name: silver_culture_slo_run
    description: run 그레인 SLO — coverage 4분해 + slo_passed 교정(raw AND expected>0). 도메인 중립.
    config:
      tags: ['slo']
    columns:
      - name: run_id
        tests: [not_null, unique]
      - name: domain
        tests: [not_null]
      - name: slo_passed
        tests: [not_null]
      - name: run_kind
        tests:
          - accepted_values:
              arguments: {values: [scheduled, manual, backfill]}
```

- [ ] **Step 3: 리뷰** — grain(run_id) unique, `slo_passed` 교정식, `run_kind` 분류, KST `event_at`, `culture_lineage` 중복 제거 확인.
- [ ] **Step 4: 커밋** `git commit -m "feat(culture): silver_culture_slo_run — slo_passed 교정 (#110)"`

### Task B3: `silver_culture_slo_dataset` (run×dataset UNNEST)

**Files:** Create `domains/culture/models/silver/silver_culture_slo_dataset.sql`; Modify `_culture_silver__models.yml`.

**Interfaces:** Produces run×dataset 그레인 — `domain, run_id, load_date, dataset_name, source, endpoint, rows, pages, bytes, duration_sec, finished_at(KST), error, dataset_passed` + 계보.

- [ ] **Step 1:** UNNEST 모델 (citydata `silver_citydata_cmrcl_rsb.sql:46` 관용구 차용). `datasets[]`의 `finished_ts`→`finished_at` 매핑:

```sql
{{ config(tags=['slo']) }}
with bronze as (
    select * from {{ source('culture_bronze', 'bronze_culture_run_report') }}
),
exploded as (
    select
        json_extract_scalar(b.record_json, '$.domain')  as domain,
        json_extract_scalar(b.record_json, '$.run_id')  as run_id,
        try(cast(json_extract_scalar(b.record_json, '$.load_date') as date)) as load_date,
        b.raw_object_key,
        ds
    from bronze b
    cross join unnest(
        cast(json_extract(b.record_json, '$.datasets') as array(json))
    ) as t(ds)
),
typed as (
    select
        domain, run_id, load_date, raw_object_key,
        json_extract_scalar(ds, '$.name')     as dataset_name,
        json_extract_scalar(ds, '$.source')   as source,
        json_extract_scalar(ds, '$.endpoint') as endpoint,
        try(cast(json_extract_scalar(ds, '$.rows') as bigint))         as rows,
        try(cast(json_extract_scalar(ds, '$.pages') as integer))       as pages,
        try(cast(json_extract_scalar(ds, '$.bytes') as bigint))        as bytes,
        try(cast(json_extract_scalar(ds, '$.duration_sec') as double)) as duration_sec,
        json_extract_scalar(ds, '$.finished_ts')                       as finished_ts_raw,
        nullif(json_extract_scalar(ds, '$.error'), '')                 as error,
        coalesce(try(cast(json_extract_scalar(ds, '$.checks.passed') as boolean)), false) as dataset_passed
    from exploded
)
select
    domain, run_id, load_date, dataset_name, source, endpoint,
    rows, pages, bytes, duration_sec,
    {{ asac_axes.kst_at('finished_ts_raw') }} as finished_at,
    error, dataset_passed
from typed
```

> `finished_ts`는 `YYYYMMDDTHHMMSSZ` 형태로 추정 — `asac_axes.kst_at`이 `yyyyMMddHHmmss`/하이픈 문자열을 자동 감지. 라이브에서 실제 포맷 확인 후 `kst_at`이 NULL이면 `date_parse(finished_ts_raw,'%Y%m%dT%H%i%sZ')` + `utc_to_kst`로 교체.

- [ ] **Step 2:** `_culture_silver__models.yml` 등록(`config: tags:['slo']`, 복합 그레인이라 컬럼 unique 대신 Task B7 단일 테스트).
- [ ] **Step 3: 리뷰** — UNNEST 캐스팅, `finished_ts→finished_at`, 병목 추이 컬럼(duration_sec) 확인.
- [ ] **Step 4: 커밋** `git commit -m "feat(culture): silver_culture_slo_dataset — datasets UNNEST (#110)"`

### Task B4: `silver_culture_dag_run` (DAG run 그레인)

**Files:** Create `domains/culture/models/silver/silver_culture_dag_run.sql`; Modify `_culture_silver__models.yml`.

**Interfaces:** Produces — `domain, dag_id, run_id, state, run_type, is_scheduled, started_at(KST), ended_at(KST), duration_sec, load_date`. 소스 `bronze_culture_dag_runs`는 이미 타입드+KST ISO 문자열이라 캐스팅만.

- [ ] **Step 1:** 모델 SQL:

```sql
{{ config(tags=['slo']) }}
with bronze as (
    select * from {{ source('culture_bronze', 'bronze_culture_dag_runs') }}
)
select
    domain, dag_id, run_id, state, run_type,
    (run_type = 'scheduled')                          as is_scheduled,
    try(cast(start_at as timestamp(6)))               as started_at,
    try(cast(end_at as timestamp(6)))                 as ended_at,
    try(cast(duration_sec as double))                 as duration_sec,
    try(cast(load_date as date))                      as load_date
from bronze
```

> `start_at/end_at`은 로더가 이미 KST ISO(`_to_kst_iso`)로 적재 → `cast(... as timestamp(6))`만. dedup 불필요(로더가 14일 윈도우 delete+insert 멱등).

- [ ] **Step 2:** `_culture_silver__models.yml` 등록 — grain `(dag_id, run_id)` 복합이라 Task B7 단일 테스트, `config: tags:['slo']`.
- [ ] **Step 3: 리뷰** — is_scheduled 파생, KST 캐스팅.
- [ ] **Step 4: 커밋** `git commit -m "feat(culture): silver_culture_dag_run (#110)"`

### Task B5: `gold_culture_slo_daily` (날짜 그레인 + 계약)

**Files:** Create `domains/culture/models/gold/gold_culture_slo_daily.sql`; Modify `_culture_gold__models.yml`.

**Interfaces:** Produces 날짜 1행 — `domain, event_date, scheduled_slo_passed, eod_slo_passed, best_coverage_pct, failed_dataset_count, violation_count, total_rows, ingest_duration_min, transform_runs, transform_all_success, maintenance_ran, green_disguise_runs`.

- [ ] **Step 1:** gold SQL (`gold_culture_location_daily.sql` 집계 패턴). scheduled/eod 분모 둘 다, green_disguise 교차검증:

```sql
{{ config(tags=['slo']) }}
with runs as (
    select * from {{ ref('silver_culture_slo_run') }}
),
dag_runs as (
    select * from {{ ref('silver_culture_dag_run') }}
),
run_daily as (
    select
        domain, load_date as event_date,
        -- 자정 스케줄런 기준 SLO(그 날 scheduled run 이 모두 통과)
        bool_and(case when run_kind = 'scheduled' then slo_passed end)      as scheduled_slo_passed,
        -- 일 최종(그 날 아무 run 이든 하나라도 통과 = 복구 성공 인정)
        bool_or(slo_passed)                                                 as eod_slo_passed,
        max(coverage_pct)                                                   as best_coverage_pct,
        sum(failed)                                                         as failed_dataset_count,
        sum(violation_count)                                                as violation_count,
        max(total_rows)                                                     as total_rows,
        -- 초록 위장: raw 통과인데 expected=0 (Airflow success ∧ 전멸)
        sum(case when slo_passed_raw and coalesce(expected, 0) = 0 then 1 else 0 end) as green_disguise_runs
    from runs
    group by domain, load_date
),
dag_daily as (
    select
        domain,
        cast(started_at as date) as event_date,
        count(case when dag_id = 'culture_transform' then 1 end)                              as transform_runs,
        bool_and(case when dag_id = 'culture_transform' then state = 'success' end)           as transform_all_success,
        bool_or(dag_id = 'culture_maintenance' and state = 'success')                         as maintenance_ran,
        sum(case when dag_id = 'culture_bronze' then duration_sec else 0 end) / 60.0          as ingest_duration_min
    from dag_runs
    group by domain, cast(started_at as date)
)
select
    r.domain,
    r.event_date,
    r.scheduled_slo_passed,
    r.eod_slo_passed,
    cast(r.best_coverage_pct as decimal(38,18))          as best_coverage_pct,
    r.failed_dataset_count,
    r.violation_count,
    r.total_rows,
    cast(coalesce(d.ingest_duration_min, 0) as decimal(38,18)) as ingest_duration_min,
    coalesce(d.transform_runs, 0)                        as transform_runs,
    coalesce(d.transform_all_success, false)             as transform_all_success,
    coalesce(d.maintenance_ran, false)                   as maintenance_ran,
    r.green_disguise_runs
from run_daily r
left join dag_daily d on r.domain = d.domain and r.event_date = d.event_date
```

- [ ] **Step 2:** `_culture_gold__models.yml`에 **contract enforced** 등록(전 컬럼 `data_type`, 비율 `decimal(38,18)`):

```yaml
  - name: gold_culture_slo_daily
    description: 날짜 1행 SLO — scheduled/eod 가용·green_disguise·적재추세. 도메인 중립.
    config:
      contract: {enforced: true}
      tags: ['slo']
    columns:
      - name: domain
        data_type: varchar
        tests: [not_null]
      - name: event_date
        data_type: date
        tests: [not_null, unique]        # 도메인 단일이라 날짜 unique (population 채택 시 domain+date 복합으로 확장)
      - name: scheduled_slo_passed
        data_type: boolean
      - name: eod_slo_passed
        data_type: boolean
      - name: best_coverage_pct
        data_type: decimal(38,18)
      - name: failed_dataset_count
        data_type: bigint
      - name: violation_count
        data_type: bigint
      - name: total_rows
        data_type: bigint
      - name: ingest_duration_min
        data_type: decimal(38,18)
      - name: transform_runs
        data_type: bigint
      - name: transform_all_success
        data_type: boolean
      - name: maintenance_ran
        data_type: boolean
      - name: green_disguise_runs
        data_type: bigint
```

> **계약 타입 주의:** `sum(...)`은 bigint, `max(coverage_pct double)`은 double이라 `cast(... as decimal(38,18))` 명시 필요(위 SQL에 반영). `bool_and/bool_or`는 boolean. 라이브에서 `dbt build`가 타입 불일치를 run 단계에서 차단하므로 실측 후 미세조정.

- [ ] **Step 3: 리뷰** — scheduled(`bool_and` on scheduled)·eod(`bool_or`)·green_disguise(`raw ∧ expected=0`) 산식이 7/7 회귀(scheduled=false·eod=true·green=1)를 만족하는지 논리 검증. 계약 data_type 정합.
- [ ] **Step 4: 커밋** `git commit -m "feat(culture): gold_culture_slo_daily + contract (#110)"`

### Task B6: tag:slo 배선 + 대표 쿼리 문서

**Files:** 확인용(전 모델 `tags:['slo']` 이미 각 태스크에서 부여) + Create 대표 쿼리 문서.

- [ ] **Step 1:** 4개 모델 전부 `{{ config(tags=['slo']) }}` 또는 yml `config: tags:['slo']` 확인. `dbt ls --select tag:slo`가 4개(silver 3+gold 1) 반환하는지 (컨테이너, 게이트 후).
- [ ] **Step 2:** 대표 가용률 window 쿼리를 `domains/culture/docs/`에 문서화(주간 99.5% 등):

```sql
-- 최근 7일 자정 수집 가용률
select
    count_if(scheduled_slo_passed) * 1.0 / count(*) as scheduled_availability_7d,
    count_if(eod_slo_passed) * 1.0 / count(*)       as eod_availability_7d
from {{ ref('gold_culture_slo_daily') }}
where event_date >= current_date - interval '7' day;
```

- [ ] **Step 3: 커밋** `git commit -m "docs(culture): SLO 대표 가용률 쿼리 (#110)"`

### Task B7: 복합 그레인 단일 테스트 + 7/7 회귀 테스트

**Files:** Create `tests/assert_culture_slo_dataset_grain_unique.sql`, `tests/assert_culture_dag_run_grain_unique.sql`, `tests/assert_culture_slo_regression_77.sql`.

- [ ] **Step 1:** 복합 그레인 단일 테스트(culture 관례 `group by ... having count(*)>1`):

```sql
-- assert_culture_slo_dataset_grain_unique.sql
select run_id, dataset_name, count(*) as n
from {{ ref('silver_culture_slo_dataset') }}
group by run_id, dataset_name
having count(*) > 1
```
```sql
-- assert_culture_dag_run_grain_unique.sql
select dag_id, run_id, count(*) as n
from {{ ref('silver_culture_dag_run') }}
group by dag_id, run_id
having count(*) > 1
```

- [ ] **Step 2:** 7/7 회귀(라이브 게이트 — bronze 채워진 뒤 활성):

```sql
-- assert_culture_slo_regression_77.sql — 7/7 자정 전멸이 사후 판정되는지
select event_date
from {{ ref('gold_culture_slo_daily') }}
where event_date = date '2026-07-07'
  and not (scheduled_slo_passed = false and eod_slo_passed = true and green_disguise_runs >= 1)
```

> 이 테스트는 bronze에 7/7 리포트가 실려야 통과 판정 가능 — **게이트 후 로더 첫 실행(전량 백필) 뒤** 활성. 그 전엔 데이터 부재로 0행(통과)이나, AC는 라이브에서 확인.

- [ ] **Step 3: 리뷰** — 세 단일 테스트 tag 부여(`{{ config(tags=['slo']) }}` 상단) 확인.
- [ ] **Step 4: 커밋** `git commit -m "test(culture): SLO 그레인 + 7/7 회귀 테스트 (#110)"`

---

## 게이트 후(라이브) 검증 체크리스트 — 이 계획 밖, 별도 세션

1. 게이트 체인(compose build → up -d → dags/dbt dev pull → 파싱 스모크).
2. `culture_slo` 수동 트리거 → `bronze_culture_run_report`(전량 백필)·`bronze_culture_dag_runs`(14일) 적재 확인 + 재실행 멱등.
3. 컨테이너 `dbt build --select tag:slo` → silver 3·gold 1 PASS, 계약 위반 0.
4. **7/7 회귀 AC**: `gold_culture_slo_daily` where event_date=7/7 → scheduled=false ∧ eod=true ∧ green_disguise=1.
5. 두 레포 dev 복귀·worktree 정리. PR 2건(사용자 머지).

## Self-Review (계획 검토)

- **스펙 커버리지:** #257 체크리스트 5항목 → A1~A5 매핑. #110 체크리스트 6항목 → B1~B7 매핑. 공통 AC(전량 백필·멱등·7/7 회귀·호스트 테스트·도메인 경계) → A1/A5·B7·게이트 체크리스트. ✅
- **미해결(라이브 의존):** `culture_lineage` 컬럼 중복(B2 주의), `finished_ts` 실제 포맷(B3 주의), 계약 data_type 미세조정(B5 주의) — 셋 다 코드에 주의 명시 + 게이트 검증에서 확정. 순수 로직은 호스트 테스트로 고정.
- **타입 일관성:** `dag_run_row` 키(A2) ↔ `bronze_culture_dag_runs` 컬럼(B1) ↔ `silver_culture_dag_run`(B4) 이름 일치(domain/dag_id/run_id/state/run_type/start_at/end_at/duration_sec/load_date). `report_records` 튜플 계약(A1) ↔ `warehouse.load` 계약 일치. ✅
