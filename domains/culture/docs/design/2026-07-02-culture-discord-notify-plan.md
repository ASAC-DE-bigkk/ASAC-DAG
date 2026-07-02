# culture raw 적재 Discord 알림 — 구현 플랜

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** culture raw 적재(@daily) 완료 시 Discord 채널로 데이터셋별 세분화 일일 리포트(성공+실패)를 보낸다.

**Architecture:** `report` 태스크가 만든 `run_report` 를 순수 함수 `build_report_payload` 로 Discord 임베드로 포맷하고, `notifier_from_env()` 가 고른 Notifier 로 전송한다. env `CULTURE_DISCORD_WEBHOOK_URL` 없으면 `NoopNotifier`(전송 안 함) → 코드만으로 안전 머지. per-dataset 완료시각/소요는 `DatasetResult` 에 신규 캡처.

**Tech Stack:** Python 3.12, Airflow 3, `requests`, pytest 9 (로컬 실행). 관련 설계: [2026-07-02-culture-discord-notify.md](2026-07-02-culture-discord-notify.md), 이슈 [#68](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/68).

**브랜치:** `feat/68-culture-discord-notify` (dev 기준, 이미 생성됨).

---

## 파일 구조

| 파일 | 역할 | 작업 |
|---|---|---|
| `culture_ingest/common/landing.py` | `DatasetResult` 에 타이밍 필드 | 수정 |
| `culture_ingest/source/ingest.py` | `ingest_dataset` 에서 타이밍 캡처 | 수정 |
| `culture_ingest/common/notify.py` | Notifier/Discord/포맷/env 팩토리 | 생성 |
| `culture_bronze_ingest.py` | `_report` 에 알림 호출 와이어링 | 수정 |
| `tests/conftest.py` | `domains/culture` 를 sys.path 에 | 생성 |
| `tests/test_ingest_timing.py` | 타이밍 캡처 테스트 | 생성 |
| `tests/test_notify.py` | notify/포맷 테스트 | 생성 |
| `.airflowignore` | `tests/`·`docs/` 제외 | 수정 |

**테스트 실행 (로컬):**
```
cd /c/Users/Dell3571/ask-seoul/sample/dags/domains/culture && PYTHONPATH="$PWD" python -m pytest tests/ -v
```

---

## Task 0: 테스트 부트스트랩

**Files:**
- Create: `tests/conftest.py`

- [ ] **Step 1: conftest 작성** (commerce 패턴 차용 — `domains/culture` 를 sys.path 에 올려 `culture_ingest.*` import 가능하게)

```python
import os
import sys

# tests/ 의 부모 = domains/culture. 여기를 sys.path 에 올려 culture_ingest 패키지 import.
_CULTURE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _CULTURE)
```

- [ ] **Step 2: 커밋**

```bash
git -C /c/Users/Dell3571/ask-seoul/sample/dags add domains/culture/tests/conftest.py
git -C /c/Users/Dell3571/ask-seoul/sample/dags commit -m "test(culture): pytest 부트스트랩 conftest (#68)"
```

---

## Task 1: DatasetResult 타이밍 필드

**Files:**
- Modify: `culture_ingest/common/landing.py` (`DatasetResult` dataclass + `summary()`)
- Test: `tests/test_ingest_timing.py`

- [ ] **Step 1: 실패 테스트 작성**

```python
from culture_ingest.common.landing import DatasetResult


def test_dataset_result_summary_has_timing_fields():
    r = DatasetResult(name="kopis_performance", source="kopis", endpoint="pblprfr", prefix="p")
    r.duration_sec = 4.1
    r.finished_ts = "20260701T000012Z"
    s = r.summary()
    assert s["duration_sec"] == 4.1
    assert s["finished_ts"] == "20260701T000012Z"
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `cd /c/Users/Dell3571/ask-seoul/sample/dags/domains/culture && PYTHONPATH="$PWD" python -m pytest tests/test_ingest_timing.py -v`
Expected: FAIL — `AttributeError` (또는 KeyError: 'duration_sec')

- [ ] **Step 3: DatasetResult 에 필드 추가** — `iceberg_rows` 필드 바로 아래(현재 라인 88 부근)에 2줄 추가:

```python
    iceberg_rows: int = 0  # bronze Iceberg 테이블에 적재된 행 수 (write_iceberg 시)
    duration_sec: float = 0.0  # 이 데이터셋 적재 소요(초)
    finished_ts: str = ""  # 이 데이터셋 적재 완료 시각 (UTC YYYYMMDDTHHMMSSZ)
```

- [ ] **Step 4: summary() 에 키 추가** — `"iceberg_rows": self.iceberg_rows,` 다음 줄에:

```python
            "iceberg_rows": self.iceberg_rows,
            "duration_sec": self.duration_sec,
            "finished_ts": self.finished_ts,
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `cd /c/Users/Dell3571/ask-seoul/sample/dags/domains/culture && PYTHONPATH="$PWD" python -m pytest tests/test_ingest_timing.py -v`
Expected: PASS

- [ ] **Step 6: 커밋**

```bash
git -C /c/Users/Dell3571/ask-seoul/sample/dags add domains/culture/culture_ingest/common/landing.py domains/culture/tests/test_ingest_timing.py
git -C /c/Users/Dell3571/ask-seoul/sample/dags commit -m "feat(culture): DatasetResult 에 per-dataset 타이밍 필드 (#68)"
```

---

## Task 2: ingest_dataset 타이밍 캡처

**Files:**
- Modify: `culture_ingest/source/ingest.py` (`ingest_dataset` — import 추가 + try/finally)
- Test: `tests/test_ingest_timing.py` (테스트 추가)

`ingest_dataset` 에는 이른 `return result`(skipped·unknown kind·detail 과반 실패)가 여러 개 있다. `finally` 는 return·예외 모두에서 실행되므로 **모든 경로에서** 타이밍을 남긴다.

- [ ] **Step 1: 실패 테스트 추가** (`tests/test_ingest_timing.py` 하단에)

```python
from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import Landing, LocalSink
from culture_ingest.source.ingest import ingest_dataset, IngestOptions, Clients
from culture_ingest.source.datasets import BY_NAME
import re


def _run(tmp_path, name, include_detail=False):
    ds = BY_NAME[name]
    ctx = RunContext(load_date="2026-07-01", ingest_ts="20260701T000000Z", run_id="t")
    landing = Landing(LocalSink(str(tmp_path)), "bronze/culture", ctx)
    opts = IngestOptions(include_detail=include_detail, max_detail=5)

    class DeadKopis:
        def list_pages(self, *a, **k): return []
        def list_ids(self, *a, **k): return []
        def fetch_once(self, *a, **k): raise RuntimeError("no net")
        def detail(self, *a, **k): raise RuntimeError("no net")

    clients = Clients(kopis=DeadKopis(), seoul=None)
    return ingest_dataset(ds, clients, landing, opts, warehouse=None)


def test_timing_set_on_skipped_path(tmp_path):
    # kopis_detail + include_detail=False -> 이른 return(skipped) 경로에서도 타이밍 남음
    r = _run(tmp_path, "kopis_performance_detail", include_detail=False)
    assert "skipped" in r.error
    assert r.finished_ts and re.match(r"^\d{8}T\d{6}Z$", r.finished_ts)
    assert r.duration_sec >= 0.0
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `... python -m pytest tests/test_ingest_timing.py::test_timing_set_on_skipped_path -v`
Expected: FAIL — `r.finished_ts` 빈 문자열(assert 실패)

- [ ] **Step 3: import 추가** — `ingest.py` 상단 import 블록(현재 `import json` 부근)에:

```python
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
```

- [ ] **Step 4: ingest_dataset 에 타이밍 캡처** — 함수 본문에서 `result = DatasetResult(...)` 다음 줄에 `t0` 를 두고, 기존 최상위 `try: ... except Exception as exc: result.error = ...` 에 `finally` 를 추가한다. 최종 형태(발췌):

```python
    result = DatasetResult(name=ds.name, source=ds.source, endpoint=ds.endpoint, prefix=prefix)
    t0 = time.monotonic()
    sample_body: bytes | None = None
    ...
    try:
        ...  # (기존 본문 그대로 — 이른 return 들도 그대로 둔다)
    except Exception as exc:  # noqa: BLE001
        result.error = f"{type(exc).__name__}: {exc}"
    finally:
        result.duration_sec = round(time.monotonic() - t0, 1)
        result.finished_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return result
```

- [ ] **Step 5: 테스트 통과 확인 (전체 timing 테스트)**

Run: `... python -m pytest tests/test_ingest_timing.py -v`
Expected: PASS (2 passed)

- [ ] **Step 6: 커밋**

```bash
git -C /c/Users/Dell3571/ask-seoul/sample/dags add domains/culture/culture_ingest/source/ingest.py domains/culture/tests/test_ingest_timing.py
git -C /c/Users/Dell3571/ask-seoul/sample/dags commit -m "feat(culture): ingest_dataset per-dataset 타이밍 캡처(finally) (#68)"
```

---

## Task 3: notify.py — 인터페이스 + 전송 + env 팩토리

**Files:**
- Create: `culture_ingest/common/notify.py`
- Test: `tests/test_notify.py`

- [ ] **Step 1: 실패 테스트 작성**

```python
from culture_ingest.common import notify


def test_notifier_from_env_noop_without_url():
    assert isinstance(notify.notifier_from_env({}), notify.NoopNotifier)


def test_notifier_from_env_discord_with_url():
    n = notify.notifier_from_env({"CULTURE_DISCORD_WEBHOOK_URL": "https://x/y"})
    assert isinstance(n, notify.DiscordWebhookNotifier)


def test_noop_send_does_not_raise():
    notify.NoopNotifier().send({"embeds": [{"title": "t"}]})


def test_discord_send_posts_payload(monkeypatch):
    calls = []
    monkeypatch.setattr(notify.requests, "post",
                        lambda url, json, timeout: calls.append((url, json, timeout)))
    notify.DiscordWebhookNotifier("https://hook").send({"embeds": [{"title": "t"}]})
    assert calls and calls[0][0] == "https://hook"


def test_discord_send_swallows_failure(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("down")
    monkeypatch.setattr(notify.requests, "post", boom)
    notify.DiscordWebhookNotifier("https://hook").send({"embeds": []})  # 예외 없어야 함
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `... python -m pytest tests/test_notify.py -v`
Expected: FAIL — `ModuleNotFoundError: culture_ingest.common.notify`

- [ ] **Step 3: notify.py 생성 (인터페이스 부분)**

```python
"""culture raw 적재 완료 Discord 알림.

`report` 태스크의 run_report 를 Discord 임베드로 포맷(build_report_payload)하고
webhook 으로 전송(DiscordWebhookNotifier)한다. env CULTURE_DISCORD_WEBHOOK_URL 이 없으면
NoopNotifier(전송 안 함) 라 코드만으로 안전하게 머지된다.

시크릿(웹훅 URL)은 메시지·로그에 절대 넣지 않는다. 전송 실패는 삼켜 파이프라인을 막지 않는다.
"""
from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

from culture_ingest.source.datasets import BY_NAME

log = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
WEBHOOK_ENV = "CULTURE_DISCORD_WEBHOOK_URL"
COLOR_PASS = 3066993   # 0x2ECC71
COLOR_FAIL = 15158332  # 0xE74C3C


class Notifier(ABC):
    @abstractmethod
    def send(self, payload: dict) -> None:
        """Discord webhook payload 1건 전송."""


class NoopNotifier(Notifier):
    def send(self, payload: dict) -> None:
        title = ((payload.get("embeds") or [{}])[0]).get("title", "")
        log.info("[notify:noop] %s (전송 비활성 — URL 미설정)", title)


class DiscordWebhookNotifier(Notifier):
    def __init__(self, url: str, timeout: float = 10.0):
        self._url = url
        self._timeout = timeout

    def send(self, payload: dict) -> None:
        try:
            requests.post(self._url, json=payload, timeout=self._timeout)
        except Exception:  # noqa: BLE001 -- best-effort. URL 로그 금지.
            log.exception("[notify] Discord 전송 실패(무시)")


def notifier_from_env(env: dict | None = None) -> Notifier:
    env = os.environ if env is None else env
    url = (env.get(WEBHOOK_ENV) or "").strip()
    return DiscordWebhookNotifier(url) if url else NoopNotifier()
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `... python -m pytest tests/test_notify.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: 커밋**

```bash
git -C /c/Users/Dell3571/ask-seoul/sample/dags add domains/culture/culture_ingest/common/notify.py domains/culture/tests/test_notify.py
git -C /c/Users/Dell3571/ask-seoul/sample/dags commit -m "feat(culture): notify.py Notifier/Discord/env 팩토리 (#68)"
```

---

## Task 4: build_report_payload — run_report → 임베드

**Files:**
- Modify: `culture_ingest/common/notify.py` (포맷 함수 추가)
- Test: `tests/test_notify.py` (테스트 추가)

- [ ] **Step 1: 실패 테스트 추가** (`tests/test_notify.py` 하단)

```python
def _sample_report(pass_case=True):
    ds_ok = {"name": "kopis_performance", "rows": 1204, "pages": 13, "error": "",
             "checks": {"passed": True, "violations": []}, "iceberg_rows": 0,
             "duration_sec": 4.1, "finished_ts": "20260701T000012Z"}
    if pass_case:
        return {"load_date": "2026-07-02", "ingest_ts": "20260701T000007Z",
                "run_id": "scheduled__x", "slo_passed": True,
                "coverage": {"expected": 1, "landed": 1, "skipped": 0, "failed": 0, "coverage_pct": 100.0},
                "total_rows": 1204, "total_iceberg_rows": 0,
                "freshness": {"max_age_hours": 0.3}, "violations": [], "failed_datasets": [],
                "datasets": [ds_ok]}
    ds_fail = {"name": "seoul_sejong", "rows": 0, "pages": 0,
               "error": "RuntimeError: HTTP 500", "checks": {}, "iceberg_rows": 0,
               "duration_sec": 0.0, "finished_ts": ""}
    return {"load_date": "2026-07-02", "ingest_ts": "20260701T000007Z",
            "run_id": "scheduled__x", "slo_passed": False,
            "coverage": {"expected": 2, "landed": 1, "skipped": 0, "failed": 1, "coverage_pct": 50.0},
            "total_rows": 1204, "total_iceberg_rows": 0,
            "freshness": {"max_age_hours": 0.3},
            "violations": [], "failed_datasets": [{"dataset": "seoul_sejong", "error": "RuntimeError: HTTP 500"}],
            "datasets": [ds_ok, ds_fail]}


def test_payload_pass_case_is_green_and_raw_titled():
    p = notify.build_report_payload(_sample_report(True))
    emb = p["embeds"][0]
    assert emb["color"] == notify.COLOR_PASS
    assert "raw 적재 리포트" in emb["title"]
    assert "공연목록" in emb["description"]        # 한국어 서비스명
    assert "kopis_performance" in emb["description"]  # slug
    assert "❌" not in emb["description"]           # 이슈 줄 없음


def test_payload_fail_case_is_red_with_issue_lines():
    p = notify.build_report_payload(_sample_report(False))
    emb = p["embeds"][0]
    assert emb["color"] == notify.COLOR_FAIL
    assert "❌ FAIL" in emb["description"]
    assert "seoul_sejong" in emb["description"]
    assert len(emb["description"]) <= 4096


def test_payload_hides_iceberg_when_zero():
    p = notify.build_report_payload(_sample_report(True))
    assert "Iceberg" not in p["embeds"][0]["description"]
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `... python -m pytest tests/test_notify.py -v`
Expected: FAIL — `AttributeError: module ... has no attribute 'build_report_payload'`

- [ ] **Step 3: notify.py 에 포맷 함수 추가** (파일 하단에 붙임)

```python
def _kst_hms(ingest_ts: str) -> str:
    try:
        dt = datetime.strptime(ingest_ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        return dt.astimezone(KST).strftime("%H:%M:%S")
    except (ValueError, TypeError):
        return "--"


def _dur_str(seconds: float) -> str:
    total = int(seconds)
    m, s = divmod(total, 60)
    return f"{m}m{s:02d}s" if m else f"{s}s"


def _run_duration(ingest_ts: str, finish_tss: list[str]) -> str:
    if not finish_tss:
        return "--"
    try:
        t0 = datetime.strptime(ingest_ts, "%Y%m%dT%H%M%SZ")
        t1 = datetime.strptime(max(finish_tss), "%Y%m%dT%H%M%SZ")
        return _dur_str((t1 - t0).total_seconds())
    except (ValueError, TypeError):
        return "--"


def _title_of(name: str) -> str:
    return BY_NAME[name].title if name in BY_NAME else name


def _fmt_int(v) -> str:
    return "--" if v in (None, "") else f"{int(v):,}"


def _status(s: dict) -> str:
    err = s.get("error") or ""
    if err and "skipped" not in err:
        return "FAIL"
    if "skipped" in err:
        return "skip"
    checks = s.get("checks") or {}
    ib = s.get("iceberg_rows", 0)
    mismatch = bool(ib) and ib != s.get("rows", 0)
    if checks.get("passed") is False or mismatch:
        return "WARN"
    return "ok"


def _table(datasets: list[dict]) -> str:
    lines = [f"{'st':<4}  {'rows':>6}  {'pages':>6}  {'done':<8}  {'sec':>5}  dataset"]
    for s in datasets:
        done = _kst_hms(s["finished_ts"]) if s.get("finished_ts") else "--"
        dur = s.get("duration_sec") or 0
        sec = f"{float(dur):.1f}" if dur else "--"
        lines.append(
            f"{_status(s):<4}  {_fmt_int(s.get('rows')):>6}  {_fmt_int(s.get('pages')):>6}  "
            f"{done:<8}  {sec:>5}  {_title_of(s['name'])} · {s['name']}"
        )
    return "\n".join(lines)


def _issue_lines(report: dict, datasets: list[dict]) -> list[str]:
    out = []
    for f in report.get("failed_datasets", []):
        out.append(f"❌ FAIL — {_title_of(f['dataset'])}({f['dataset']}): {str(f.get('error',''))[:200]}")
    for v in report.get("violations", []):
        out.append(f"⚠️ 위반 — {_title_of(v['dataset'])}({v['dataset']}): {v['violation']}")
    for s in datasets:
        ib = s.get("iceberg_rows", 0)
        if ib and ib != s.get("rows", 0):
            out.append(f"⚠️ Iceberg 불일치 — {_title_of(s['name'])}({s['name']}): "
                       f"raw {_fmt_int(s.get('rows'))} ≠ iceberg {_fmt_int(ib)}")
    return out


def build_report_payload(report: dict) -> dict:
    datasets = report.get("datasets", [])
    cov = report.get("coverage", {})
    passed = report.get("slo_passed", False)

    start = _kst_hms(report.get("ingest_ts", ""))
    finish_tss = [s["finished_ts"] for s in datasets if s.get("finished_ts")]
    finish = _kst_hms(max(finish_tss)) if finish_tss else "--"
    dur = _run_duration(report.get("ingest_ts", ""), finish_tss)

    line1 = f"수집 {start} → 완료 {finish} KST · 소요 {dur}"
    parts = [
        f"커버리지 {cov.get('landed', 0)}/{cov.get('expected', 0)}",
        f"landed {cov.get('landed', 0)}",
        f"skipped {cov.get('skipped', 0)}",
        f"failed {cov.get('failed', 0)}",
        f"records {int(report.get('total_rows', 0)):,}",
    ]
    ib_total = report.get("total_iceberg_rows", 0)
    if ib_total:
        parts.append(f"Iceberg {int(ib_total):,}")
    fresh = (report.get("freshness") or {}).get("max_age_hours")
    if fresh is not None:
        parts.append(f"freshness {fresh}h")

    desc = f"{line1}\n{' · '.join(parts)}\n```\n{_table(datasets)}\n```"
    issues = _issue_lines(report, datasets)
    if issues:
        desc += "\n" + "\n".join(issues)

    return {
        "embeds": [{
            "title": f"culture raw 적재 리포트 · {report.get('load_date', '')} (KST)",
            "color": COLOR_PASS if passed else COLOR_FAIL,
            "description": desc[:4096],
            "footer": {"text": f"run_id={report.get('run_id', '')} · @daily"},
        }]
    }
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `... python -m pytest tests/test_notify.py -v`
Expected: PASS (8 passed)

- [ ] **Step 5: 커밋**

```bash
git -C /c/Users/Dell3571/ask-seoul/sample/dags add domains/culture/culture_ingest/common/notify.py domains/culture/tests/test_notify.py
git -C /c/Users/Dell3571/ask-seoul/sample/dags commit -m "feat(culture): build_report_payload run_report->Discord 임베드 (#68)"
```

---

## Task 5: _report 태스크에 알림 와이어링

**Files:**
- Modify: `culture_bronze_ingest.py` (import + `_report` 안 호출)

- [ ] **Step 1: import 추가** — DAG 상단 `from culture_ingest.source.ingest import (...)` 블록 아래(라인 44 부근)에:

```python
from culture_ingest.common.notify import build_report_payload, notifier_from_env  # noqa: E402
```

- [ ] **Step 2: _report 에 알림 호출** — `write_run_report` try/except 블록(현재 라인 202-206) **다음**, `fail_on_violation` 게이트(라인 208) **앞**에 삽입:

```python
    # Discord 완료 알림(best-effort) — URL 없으면 no-op. 알림 실패는 삼킨다(파이프라인 보호).
    try:
        notifier_from_env().send(build_report_payload(report))
    except Exception as exc:  # noqa: BLE001
        print(f"[culture raw] discord 알림 실패(무시): {type(exc).__name__}")
```

- [ ] **Step 3: 파싱/스모크 검증** — DAG 파일이 import 에러 없이 로드되고 알림 경로(Noop)가 도는지 확인:

Run:
```
cd /c/Users/Dell3571/ask-seoul/sample/dags/domains/culture && PYTHONPATH="$PWD" python -c "
import culture_ingest.common.notify as n
rep={'load_date':'2026-07-02','ingest_ts':'20260701T000007Z','run_id':'r','slo_passed':True,'coverage':{'expected':1,'landed':1,'skipped':0,'failed':0,'coverage_pct':100.0},'total_rows':1204,'total_iceberg_rows':0,'freshness':{'max_age_hours':0.3},'violations':[],'failed_datasets':[],'datasets':[{'name':'kopis_performance','rows':1204,'pages':13,'error':'','checks':{'passed':True,'violations':[]},'iceberg_rows':0,'duration_sec':4.1,'finished_ts':'20260701T000012Z'}]}
n.notifier_from_env({}).send(n.build_report_payload(rep))
print('noop path OK')
"
```
Expected: `noop path OK` (예외 없음)

- [ ] **Step 4: 커밋**

```bash
git -C /c/Users/Dell3571/ask-seoul/sample/dags add domains/culture/culture_bronze_ingest.py
git -C /c/Users/Dell3571/ask-seoul/sample/dags commit -m "feat(culture): report 태스크에서 Discord 완료 알림 전송 (#68)"
```

---

## Task 6: .airflowignore + 전체 검증 + PR

**Files:**
- Modify: `.airflowignore`

- [ ] **Step 1: .airflowignore 에 tests/·docs/ 추가** — 파일 끝(regexp 구문)에:

```
tests/
docs/
```

- [ ] **Step 2: 전체 테스트** 

Run: `cd /c/Users/Dell3571/ask-seoul/sample/dags/domains/culture && PYTHONPATH="$PWD" python -m pytest tests/ -v`
Expected: PASS (전체 통과: timing 2 + notify 8 = 10)

- [ ] **Step 3: 컨테이너 DAG 파싱 확인** (알림 코드가 스케줄러 파싱을 깨지 않는지)

Run: `docker exec elt-infra-airflow-scheduler-1 python -c "import sys; sys.path.insert(0,'/opt/airflow/dags/domains/culture'); import culture_bronze_ingest; print('DAG import OK')"`
Expected: `DAG import OK`

- [ ] **Step 4: 커밋 + push + PR**

```bash
git -C /c/Users/Dell3571/ask-seoul/sample/dags add domains/culture/.airflowignore
git -C /c/Users/Dell3571/ask-seoul/sample/dags commit -m "chore(culture): .airflowignore 에 tests/·docs/ 제외 (#68)"
git -C /c/Users/Dell3571/ask-seoul/sample/dags push -u origin feat/68-culture-discord-notify
gh pr create --repo ASAC-DE-bigkk/ASAC-DAG --base dev --head feat/68-culture-discord-notify \
  --title "feat(culture): raw 적재 완료 Discord 일일 리포트 알림 (#68)" \
  --body "Closes #68. URL 미설정 시 no-op(안전 머지). 설계: docs/design/2026-07-02-culture-discord-notify.md"
```

---

## Self-Review 체크

- [ ] 스펙 §4 구조(Notifier/Noop/Discord/build_report_payload/notifier_from_env) → Task 3·4 ✔
- [ ] 스펙 §5 per-dataset 타이밍 → Task 1·2 ✔
- [ ] 스펙 §6 메시지(raw 제목·표·상태규칙·조건부 Iceberg·이슈줄) → Task 4 ✔
- [ ] 스펙 §6 전송 위치(report) → Task 5 ✔
- [ ] 스펙 §7 시크릿 미노출 → notify.py(URL 로그 금지, error 200자 절단) ✔
- [ ] 스펙 §8 best-effort → DiscordWebhookNotifier.send + _report try/except ✔
- [ ] 스펙 §9 테스트 → Task 1·2·3·4 ✔
- [ ] 멘토 게이트(`sample/.env` URL)는 코드 PR 밖 — 문서·PR 본문에 명시 ✔
