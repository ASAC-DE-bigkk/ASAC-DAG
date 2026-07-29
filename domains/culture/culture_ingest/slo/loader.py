"""culture SLO 로더 — 순수 함수(스캔·파싱·행빌드).

설계: domains/culture/docs/design/2026-07-07-culture-slo-mart.md §3/§4.
순수 함수는 호스트 pytest 로 검증, 트리노/Airflow IO 는 io.py 에서 컨테이너 라이브(게이트 후).
"""
from __future__ import annotations

import datetime as _dt
import json
import re

from culture_ingest.source.config import LEGACY_REPORTS_PREFIX, OPS_REPORTS_ROOT

# 신·구 양쪽을 스캔한다(#60 약속 ②) — 리포트 목적지가 raw 안에서 ops/reports 로 옮겨졌고,
# 옛 62건은 "기존 객체 이동 0건" 원칙에 따라 제자리에 남는다. 한쪽만 보면 전환 지점에서
# SLO 이력이 끊긴다. 순서는 신규 우선(같은 ingest_ts 면 새 경로 키가 실린다).
REPORTS_PREFIX = f"{OPS_REPORTS_ROOT}/"
SCAN_PREFIXES = (REPORTS_PREFIX, LEGACY_REPORTS_PREFIX)
_INGEST_TS_RE = re.compile(r"ingest_ts=([0-9TZ]+)")  # source/ingest.py 와 동일 규약

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
    """신·구 리포트 존에서 아직 안 실린 run_report.json 만 (키, dict) 로 — ingest_ts 오름차순.

    ingest_ts 는 UTC 라 사전식 정렬 == 시간순. already_loaded 에 있는 ingest_ts 는 스킵(멱등).

    **배치 안 중복 제거가 따로 필요하다** — already_loaded 는 이미 적재된 것만 막는다.
    같은 리포트가 두 존에 동시에 존재하는 상황(prod 승격 중 복사된 61건이 그렇다)에서
    한 배치가 같은 ingest_ts 를 두 번 담으면 bronze 에 중복 행이 생긴다. 먼저 본 쪽
    (= 신규 존)을 남긴다.
    """
    seen: set[str] = set()
    fresh: list[tuple[str, str, dict]] = []
    for prefix in SCAN_PREFIXES:
        for key in sink.list(prefix):
            if not key.endswith("run_report.json"):
                continue
            ts = report_ingest_ts(key)
            if ts is None or ts in already_loaded or ts in seen:
                continue
            seen.add(ts)
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
