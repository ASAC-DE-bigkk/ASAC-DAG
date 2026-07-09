# #206 facility 상세 주간 크롤 분리 — 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** KOPIS 시설 상세를 자정 일배치에서 분리해 주간 전수 크롤로 전환 — 좌표·좌석수 커버리지 11.9%→100%, 자정 KOPIS 호출 -200/일.

**Architecture:** ①`Dataset.refresh` 필드 + 순수 선택 함수로 자정런에서 weekly 데이터셋 제외 ②`TriggerDagRunOperator` 배선 전용 주간 DAG ③`load_baselines`를 다중 리포트 병합으로 바꿔 부분 run이 볼륨 HWM을 못 가리게. 설계 문서: [2026-07-08-culture-facility-weekly-refresh.md](2026-07-08-culture-facility-weekly-refresh.md)

**Tech Stack:** Python 3.12(호스트 테스트)/3.11(컨테이너), Airflow 3.2.2 standard provider, pytest.

## Global Constraints

- 자기 도메인만 수정: `domains/culture/` 밖 금지 (`common/` 포함 금지)
- API 키·웹훅 값 echo/print/커밋 금지
- 커밋 마지막 줄 `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
- PR body 마지막 `🤖 Generated with [Claude Code](https://claude.com/claude-code)`, base=dev, **셀프 머지 금지**
- 테스트는 호스트에서 `cd domains/culture && python -m pytest tests -q` (airflow 미설치 — DAG 모듈을 import하는 테스트 금지, `culture_ingest.*`만)
- 컨테이너는 워킹트리 마운트 — 라이브 스모크 후 **dags 레포를 dev로 복귀**(오늘 밤 자정런은 dev 코드로 돌아야 함)
- 브랜치: `feat/206-culture-facility-weekly-refresh` (생성됨, 설계 문서 커밋 있음)

---

### Task 1: `Dataset.refresh` 필드 + `plan_dataset_names` 선택 함수

`_plan`의 인라인 필터를 순수 함수로 추출해 airflow 없이 테스트 가능하게 하고, weekly 제외 규칙을 넣는다. 주간 트리거가 쓸 conf도 상수로 여기 두어 계약을 테스트로 고정한다.

**Files:**
- Modify: `domains/culture/culture_ingest/source/datasets.py` (Dataset 필드 + 함수 + 상수)
- Modify: `domains/culture/culture_bronze.py:119-129` (_plan 배선), `:51` (import)
- Test: `domains/culture/tests/test_plan_selection.py` (신규)

**Interfaces:**
- Produces: `plan_dataset_names(wanted: list[str] | None, *, include_detail: bool) -> list[str]`, `WEEKLY_FACILITY_REFRESH_CONF: dict` (keys: datasets/max_detail/include_detail), `Dataset.refresh: str`("daily"|"weekly")

- [ ] **Step 1: 실패하는 테스트 작성** — `domains/culture/tests/test_plan_selection.py` 신규:

```python
"""#206 — 자정/주간 크롤 분리: plan 데이터셋 선택 로직."""
from __future__ import annotations

from culture_ingest.source.datasets import (
    BY_NAME,
    WEEKLY_FACILITY_REFRESH_CONF,
    plan_dataset_names,
)


def test_scheduled_run_excludes_weekly_datasets():
    names = plan_dataset_names([], include_detail=True)
    assert "kopis_facility_detail" not in names  # weekly(정적 dim)는 자정런 제외
    assert "kopis_performance_detail" in names   # 공연 상세(fact)는 유지
    assert "kopis_facility" in names             # 시설 목록은 유지


def test_explicit_selection_includes_weekly():
    names = plan_dataset_names(
        ["kopis_facility", "kopis_facility_detail"], include_detail=True
    )
    assert names == ["kopis_facility", "kopis_facility_detail"]  # 목록이 detail 앞


def test_include_detail_false_still_excludes_details():
    names = plan_dataset_names([], include_detail=False)
    assert all(BY_NAME[n].kind != "kopis_detail" for n in names)


def test_details_sorted_last_for_id_reuse():
    names = plan_dataset_names([], include_detail=True)
    kinds = [BY_NAME[n].kind for n in names]
    tail = kinds[kinds.index("kopis_detail"):] if "kopis_detail" in kinds else []
    assert all(k == "kopis_detail" for k in tail)  # detail 은 전부 뒤(#146 id 재사용)


def test_weekly_refresh_conf_contract():
    assert all(n in BY_NAME for n in WEEKLY_FACILITY_REFRESH_CONF["datasets"])
    assert "kopis_facility_detail" in WEEKLY_FACILITY_REFRESH_CONF["datasets"]
    assert WEEKLY_FACILITY_REFRESH_CONF["max_detail"] >= 1700  # 시설 1,686 + 여유
    assert WEEKLY_FACILITY_REFRESH_CONF["include_detail"] is True
```

- [ ] **Step 2: 실패 확인**

Run: `cd "C:\Users\Dell3571\ask-seoul\sample\dags\domains\culture" && python -m pytest tests/test_plan_selection.py -q`
Expected: FAIL — `ImportError: cannot import name 'WEEKLY_FACILITY_REFRESH_CONF'`

- [ ] **Step 3: 구현** — `datasets.py`:

(a) `Dataset`에 필드 추가 (`volume_drop_threshold` 아래):

```python
    # 크롤 주기(#206): "daily"=자정 일배치, "weekly"=주간 refresh 전용(자정런 제외).
    # 정적 dim(시설 상세)은 매일 재크롤이 낭비 + 자정 KOPIS 400(#201) 압력이라 분리.
    refresh: str = "daily"
```

(b) `kopis_facility_detail` Dataset에 `refresh="weekly",` 한 줄 추가 (`key_fields=("mt10id", "fcltynm"),` 다음)

(c) 파일 끝(`select` 함수 뒤)에 추가:

```python
def plan_dataset_names(wanted: list[str] | None, *, include_detail: bool) -> list[str]:
    """DAG plan 용 적재 대상 이름 선택.

    상세(kopis_detail)는 마지막으로 정렬(#146) — 목록이 먼저 랜딩될 확률을 높여
    detail 의 "랜딩된 raw 에서 id 재사용" 경로를 살린다. ``wanted`` 가 비면 스케줄
    run — refresh="weekly" 데이터셋(#206 시설 상세)은 제외한다. 주간 트리거·수동
    run 은 이름을 명시하므로 그대로 포함된다.
    """
    chosen = set(wanted or [])
    return [
        ds.name
        for ds in sorted(enabled_datasets(), key=lambda d: d.kind == "kopis_detail")
        if (include_detail or ds.kind != "kopis_detail")
        and (ds.name in chosen if chosen else ds.refresh == "daily")
    ]


# 주간 facility refresh(#206) 트리거 conf — culture_facility_refresh DAG 가 사용.
# 목록을 같이 태우는 이유: detail 이 같은 run 에 랜딩된 목록에서 id 재사용(#146)
# + 신규 시설이 목록→상세 같은 주기에 편입. max_detail 은 시설 1,686 + 여유.
WEEKLY_FACILITY_REFRESH_CONF = {
    "datasets": ["kopis_facility", "kopis_facility_detail"],
    "max_detail": 2000,
    "include_detail": True,
}
```

(d) `culture_bronze.py` — import 교체(`:51`):

```python
from culture_ingest.source.datasets import plan_dataset_names  # noqa: E402
```

(e) `_plan`의 필터 블록(`:119-129`)을 교체:

```python
    # 데이터셋 선택은 datasets.plan_dataset_names 로 위임(#206) — include_detail,
    # datasets 파라미터, weekly 제외, detail 후순위 정렬(#146)을 한 곳에서 결정.
    include_detail = bool(params["include_detail"])
    names = plan_dataset_names(params.get("datasets") or [], include_detail=include_detail)
```

- [ ] **Step 4: 통과 확인**

Run: `cd "C:\Users\Dell3571\ask-seoul\sample\dags\domains\culture" && python -m pytest tests/test_plan_selection.py -q`
Expected: 5 passed

- [ ] **Step 5: 커밋**

```bash
git add domains/culture/culture_ingest/source/datasets.py domains/culture/culture_bronze.py domains/culture/tests/test_plan_selection.py
git commit -m "feat(culture): #206 Dataset.refresh 주기 분류 — 자정런에서 facility 상세 제외

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `load_baselines` 다중 리포트 병합

부분 run(주간 refresh·백필) 리포트가 최신이어도 다음 자정런의 볼륨 HWM이 전 데이터셋에 유지되도록, "최신 1건"을 "최신순 최대 5건 병합"으로 바꾼다.

**Files:**
- Modify: `domains/culture/culture_ingest/source/ingest.py:358-386` (`load_baselines`)
- Test: `domains/culture/tests/test_truncation_guard.py` (기존 파일에 2케이스 추가)

**Interfaces:**
- Consumes/Produces: `load_baselines(sink, root, *, before_ingest_ts) -> dict[str, int]` — 시그니처 불변(호출측 무수정). 기존 테스트 2케이스는 병합의 특수 케이스라 그대로 통과해야 함.

- [ ] **Step 1: 실패하는 테스트 작성** — `test_truncation_guard.py` 끝에 추가:

```python
def test_load_baselines_merges_partial_reports(tmp_path):
    """주간 facility run(부분 리포트)이 최신이어도 자정런 HWM 이 전 데이터셋 유지(#206)."""
    sink = LocalSink(str(tmp_path))
    root = "raw/culture"
    _write_report(sink, root, "2026-07-05", "20260704T150000Z",
                  [{"name": "seoul_cultural_event", "rows": 19371, "error": ""},
                   {"name": "kopis_facility_detail", "rows": 200, "error": ""}])
    _write_report(sink, root, "2026-07-05", "20260705T083000Z",  # 주간 부분 run
                  [{"name": "kopis_facility", "rows": 1686, "error": ""},
                   {"name": "kopis_facility_detail", "rows": 1686, "error": ""}])
    baselines = load_baselines(sink, root, before_ingest_ts="20260705T150000Z")
    assert baselines.get("kopis_facility_detail") == 1686  # 최신 리포트 우선
    assert baselines.get("seoul_cultural_event") == 19371  # 과거 리포트에서 보충


def test_load_baselines_merge_skips_error_and_caps_scan(tmp_path):
    sink = LocalSink(str(tmp_path))
    root = "raw/culture"
    # 스캔 상한(5건) 밖의 옛 리포트에만 있는 데이터셋은 병합되지 않는다
    _write_report(sink, root, "2026-06-30", "20260629T150000Z",
                  [{"name": "kopis_boxoffice", "rows": 50, "error": ""}])
    for i in range(1, 6):  # 최신 5건에는 boxoffice 없음
        _write_report(sink, root, f"2026-07-0{i}", f"2026070{i}T150000Z",
                      [{"name": "seoul_cultural_event", "rows": 19000 + i, "error": ""},
                       {"name": "kopis_festival", "rows": 0, "error": "HTTPError: ..."}])
    baselines = load_baselines(sink, root, before_ingest_ts="20260706T000000Z")
    assert baselines.get("seoul_cultural_event") == 19005  # 데이터셋별 최신값
    assert "kopis_festival" not in baselines               # error 제외 유지
    assert "kopis_boxoffice" not in baselines              # 상한 밖은 안 읽음
```

- [ ] **Step 2: 실패 확인**

Run: `cd "C:\Users\Dell3571\ask-seoul\sample\dags\domains\culture" && python -m pytest tests/test_truncation_guard.py -q`
Expected: 기존 통과 + 신규 2건 FAIL (`seoul_cultural_event` 미보충 / `kopis_boxoffice` 병합됨)

- [ ] **Step 3: 구현** — `ingest.py`의 `load_baselines` 전체 교체:

```python
_BASELINE_SCAN_REPORTS = 5  # 부분 run(주간 refresh·백필)이 껴도 이 안에 전체 run 이 있도록


def load_baselines(sink, root: str, *, before_ingest_ts: str) -> dict[str, int]:
    """직전 run_report 들에서 {dataset: rows} 볼륨 HWM 을 읽는다(#147, 병합 #206).

    ``before_ingest_ts`` 이전(=이번 실행보다 과거) 리포트를 최신순으로 최대
    ``_BASELINE_SCAN_REPORTS`` 건 훑어 데이터셋별 가장 최근 rows 를 채운다 —
    부분 run(주간 facility refresh 등) 리포트가 최신이어도 나머지 데이터셋
    기준선이 과거 전체 run 에서 보충된다. 리포트 경로의 ingest_ts 는 UTC
    문자열이라 사전순 = 시간순. error 가 있던 데이터셋은 제외(실패 런의 부분
    rows 로 기준선을 끌어내리지 않기 위해). 리포트가 없거나 읽기 실패 시 {} —
    볼륨 검사가 조용히 생략될 뿐 수집 자체는 막지 않는다(fail-open).
    """
    try:
        keys = sink.list(f"{root}/_reports/")
        report_keys = [k for k in keys if k.endswith("run_report.json")]
        candidates = []
        for k in report_keys:
            m = re.search(r"ingest_ts=([0-9TZ]+)", k)
            if m and m.group(1) < before_ingest_ts:
                candidates.append((m.group(1), k))
        merged: dict[str, int] = {}
        for _, key in sorted(candidates, reverse=True)[:_BASELINE_SCAN_REPORTS]:
            report = json.loads(sink.get(key))
            for s in report.get("datasets", []):
                if s.get("rows") and not s.get("error") and s["name"] not in merged:
                    merged[s["name"]] = int(s["rows"])
        return merged
    except Exception as exc:  # noqa: BLE001 -- baseline 은 보조 신호, 수집을 막지 않는다
        print(f"[baselines] 직전 리포트 조회 실패(볼륨 검사 생략): {type(exc).__name__}")
        return {}
```

- [ ] **Step 4: 통과 확인** (기존 2케이스 포함 회귀)

Run: `cd "C:\Users\Dell3571\ask-seoul\sample\dags\domains\culture" && python -m pytest tests/test_truncation_guard.py -q`
Expected: 전부 passed (기존 `test_load_baselines_picks_latest_before_current` 포함)

- [ ] **Step 5: 커밋**

```bash
git add domains/culture/culture_ingest/source/ingest.py domains/culture/tests/test_truncation_guard.py
git commit -m "fix(culture): #206 볼륨 HWM 베이스라인 다중 리포트 병합 — 부분 run 오염 방지

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: 주간 refresh DAG `culture_facility_refresh`

배선 전용 DAG — 일요일 05:30 KST에 culture_bronze를 facility 전수 conf로 트리거. 호스트에 airflow가 없어 유닛테스트 불가 → conf 계약은 Task 1의 `test_weekly_refresh_conf_contract`가 고정하고, DAG 자체는 Task 5의 컨테이너 파싱 스모크로 검증.

**Files:**
- Create: `domains/culture/culture_facility_refresh.py`

**Interfaces:**
- Consumes: `WEEKLY_FACILITY_REFRESH_CONF` (Task 1), `problem_failure_callback` (common — 사용만, 수정 아님)

- [ ] **Step 1: DAG 파일 작성** — 전체 내용:

```python
"""Airflow DAG: culture 시설 상세 주간 refresh (#206).

kopis_facility_detail 은 SCD2 정적 dim(freshness SLA 8일)이라 자정 일배치에서
분리(datasets.refresh="weekly")하고, 이 DAG 가 매주 일요일 05:30 KST 에
culture_bronze 를 시설 목록+상세 전수(max_detail=2000)로 트리거한다.

  - 자정이 아닌 시각: KOPIS 자정 간헐 400(#201) 창 회피, 자정 호출 -200/일
  - 목록을 같이 태움: detail 이 같은 run 에 랜딩된 목록에서 id 재사용(#146),
    신규 시설이 목록→상세 같은 주기에 편입
  - 리포트·SLO·볼륨 HWM·Discord·에러 콜백은 culture_bronze 것을 그대로 재사용
    (베이스라인은 다중 리포트 병합이라 이 부분 run 이 자정런 HWM 을 안 가림)

파라미터 (트리거 시 덮어쓰기 가능):
  target   "dev" | "prod"   (기본 dev — culture_bronze 로 전달)
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# 이 파일의 디렉토리(domains/culture)를 sys.path에 넣어 `culture_ingest.*`를 import.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402

from culture_ingest.source.datasets import WEEKLY_FACILITY_REFRESH_CONF  # noqa: E402

KST = "Asia/Seoul"

record_culture_problem = problem_failure_callback(domain="culture")

with DAG(
    dag_id="culture_facility_refresh",
    description="Weekly full crawl of KOPIS facility detail via culture_bronze trigger (#206).",
    start_date=pendulum.datetime(2026, 6, 1, tz=KST),
    schedule="30 5 * * 0",  # 매주 일요일 05:30 KST — 자정 400 창(#201)·maintenance(04:30)와 시차
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    params={"target": "dev"},
    tags=["ingestion", "culture", "kopis", "weekly"],
) as dag:
    trigger_full_crawl = TriggerDagRunOperator(
        task_id="trigger_full_crawl",
        trigger_dag_id="culture_bronze",
        # conf 는 culture_bronze 의 params 를 run 단위로 덮어쓴다(수동 트리거와 동일 경로).
        conf={**WEEKLY_FACILITY_REFRESH_CONF, "target": "{{ params.target }}"},
        on_failure_callback=record_culture_problem,
    )
```

- [ ] **Step 2: 호스트에서 문법만 확인** (airflow import는 컨테이너에서)

Run: `cd "C:\Users\Dell3571\ask-seoul\sample\dags\domains\culture" && python -c "import ast; ast.parse(open('culture_facility_refresh.py', encoding='utf-8').read()); print('syntax ok')"`
Expected: `syntax ok`

- [ ] **Step 3: 커밋**

```bash
git add domains/culture/culture_facility_refresh.py
git commit -m "feat(culture): #206 culture_facility_refresh 주간 DAG — 시설 전수 크롤 트리거

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: 문서 스윕 + change-log

**Files:**
- Modify: `domains/culture/culture_bronze.py:13-21` (모듈 docstring 파라미터 표)
- Modify: `domains/culture/docs/operations.md:37` 부근 (파라미터 표 + 주간 refresh 절)
- Modify: `domains/culture/docs/sources.md:49` 부근 (kopis_detail 설명)
- Modify: `domains/culture/change-log.md` (항목 추가)

- [ ] **Step 1: culture_bronze.py docstring** — `max_detail` 행 아래 내용을 반영:

```
  datasets        적재할 데이터셋 슬러그; 빈 값 -> 활성 전체 중 daily 만
                  (kopis_facility_detail 은 refresh="weekly" — culture_facility_refresh 가
                  일요일 05:30 KST 에 전수 크롤, #206)
```

- [ ] **Step 2: operations.md** — 파라미터 표의 `max_detail` 행에 "(공연 상세용 — 시설 상세는 주간 DAG가 2000으로 오버라이드)" 주석 추가, "상세(detail)" 절에 아래 추가:

```markdown
- **시설 상세 주간 분리(#206)**: `kopis_facility_detail`은 자정 일배치에서 제외
  (`refresh="weekly"`). `culture_facility_refresh`(일 05:30 KST)가 목록+상세를
  `max_detail=2000`으로 전수 크롤한다. 수동 전수 크롤:
  `airflow dags trigger culture_bronze --conf '{"datasets": ["kopis_facility", "kopis_facility_detail"], "max_detail": 2000}'`
```

- [ ] **Step 3: sources.md** — 상세(`kopis_detail`) 절에 한 줄 추가: "시설 상세는 주간 크롤(#206) — 자정 일배치는 공연 상세만 200캡으로 돈다."

- [ ] **Step 4: change-log.md** — 맨 위에 항목 추가:

```markdown
## 2026-07-08 — #206 facility 상세 주간 크롤 분리
- `Dataset.refresh`("daily"/"weekly") 추가, `kopis_facility_detail`을 weekly로 — 자정런 제외(KOPIS -200콜/일, #201 압력↓)
- `culture_facility_refresh` DAG 신규(일 05:30 KST) — culture_bronze를 목록+상세 전수(max_detail=2000)로 트리거
- `load_baselines` 다중 리포트 병합(최신 5건) — 부분 run이 자정런 볼륨 HWM을 가리던 결함 수정
- 설계: docs/design/2026-07-08-culture-facility-weekly-refresh.md
```

- [ ] **Step 5: 커밋**

```bash
git add domains/culture/culture_bronze.py domains/culture/docs/operations.md domains/culture/docs/sources.md domains/culture/change-log.md
git commit -m "docs(culture): #206 주간 refresh 운영 문서·change-log

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: 전체 테스트 + 컨테이너 파싱 스모크 + PR

- [ ] **Step 1: 전체 테스트**

Run: `cd "C:\Users\Dell3571\ask-seoul\sample\dags\domains\culture" && python -m pytest tests -q`
Expected: 전부 passed (직전 기준 76개 + 신규 7개)

- [ ] **Step 2: 컨테이너 파싱 스모크** (워킹트리 마운트 = 현 브랜치가 파싱됨)

```bash
MSYS_NO_PATHCONV=1 docker exec elt-infra-airflow-dag-processor-1 bash -c \
  "airflow dags list-import-errors && airflow dags list | grep culture"
```
Expected: import error 0건, `culture_facility_refresh` 목록에 등장

- [ ] **Step 3: 브랜치 푸시 + PR 생성** (base dev, 셀프 머지 금지 — 머지는 7/9 아침 사용자)

```bash
git push -u origin feat/206-culture-facility-weekly-refresh
gh pr create --repo ASAC-DE-bigkk/ASAC-DAG --base dev \
  --title "feat(culture): #206 시설 상세 주간 크롤 분리 — 커버리지 11.9%→100%, 자정 KOPIS -200콜" \
  --body-file <PR body 파일>
```
PR body: 설계 문서 요약(문제/①②③/전수 크롤 운영 절차/테스트·스모크 결과/롤백) + `Closes #206` + 푸터.

- [ ] **Step 4: dags 레포 dev 복귀** (오늘 밤 자정런은 dev 코드로)

```bash
cd "C:\Users\Dell3571\ask-seoul\sample\dags" && git checkout dev
MSYS_NO_PATHCONV=1 docker exec elt-infra-airflow-dag-processor-1 bash -c "airflow dags list-import-errors"
```
Expected: dev 체크아웃 + import error 0건 재확인

---

## 운영 런북 (7/9 아침 — 코드 아님, 순서 고정)

1. 자정런(7/8→7/9) 클린 확인 — #187 첫 실전 밤
2. PR 머지(사용자): #200, #204, 이 PR
3. 전수 크롤 수동 트리거 (머지 후 — 병합 베이스라인 코드가 배포된 뒤):
   `airflow dags trigger culture_bronze --conf '{"datasets": ["kopis_facility", "kopis_facility_detail"], "max_detail": 2000}'`
4. bronze Asset → culture_transform 자동 실행 → silver_culture_facility 커버리지 실측:
   좌표 not null 카운트가 200 → ~1,686 (AC: 11.9% → 100%)
5. 7/12(일) 05:30 첫 주간 자동 run 관찰 + 7/13(월) 자정런 볼륨 HWM 정상(병합) 확인
