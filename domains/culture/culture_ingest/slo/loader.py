"""culture SLO 로더 — 순수 함수(스캔·파싱·행빌드).

설계: domains/culture/docs/design/2026-07-07-culture-slo-mart.md §3/§4.
순수 함수는 호스트 pytest 로 검증, 트리노/Airflow IO 는 io.py 에서 컨테이너 라이브(게이트 후).
"""
from __future__ import annotations

import datetime as _dt
import json
import re

from culture_ingest.source.config import LANDING_ROOT

REPORTS_PREFIX = f"{LANDING_ROOT}/_reports/"
_INGEST_TS_RE = re.compile(r"ingest_ts=([0-9TZ]+)")  # source/ingest.py:472 와 동일 규약

# dag_run 스캔 대상 — 설계 §3 (초안 3개에서 #206 facility_refresh 추가).
CULTURE_SLO_DAG_IDS = (
    "culture_bronze",
    "culture_transform",
    "culture_maintenance",
    "culture_facility_refresh",
)

_KST = _dt.timezone(_dt.timedelta(hours=9))


def report_ingest_ts(key: str) -> str | None:
    """R2 키에서 ingest_ts 파티션 값 추출 (없으면 None)."""
    m = _INGEST_TS_RE.search(key)
    return m.group(1) if m else None


def scan_new_reports(sink, already_loaded: set[str]) -> list[tuple[str, dict]]:
    """R2 _reports/ 에서 아직 안 실린 run_report.json 만 (키, dict) 로 — ingest_ts 오름차순.

    ingest_ts 는 UTC 라 사전식 정렬 == 시간순. already_loaded 에 있는 ingest_ts 는 스킵(멱등).
    """
    keys = [k for k in sink.list(REPORTS_PREFIX) if k.endswith("run_report.json")]
    fresh: list[tuple[str, str, dict]] = []
    for key in keys:
        ts = report_ingest_ts(key)
        if ts is None or ts in already_loaded:
            continue
        fresh.append((ts, key, json.loads(sink.get(key))))
    fresh.sort(key=lambda t: t[0])
    return [(key, report) for _ts, key, report in fresh]


def report_records(reports: list[tuple[str, dict]]) -> list[tuple[str, int, dict]]:
    """warehouse.load 계약: (raw_object_key, page_no, record). 리포트 1건=1행이라 page_no=0."""
    return [(key, 0, report) for key, report in reports]


def _to_kst_iso(value):
    if value is None:
        return None
    return value.astimezone(_KST).isoformat()


def dag_run_row(dag_run, *, domain: str) -> dict:
    """Airflow DagRun ORM → 타입드 dict(KST _at). 도메인 파라미터화(§6.2 _shared 승격 대비)."""
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
