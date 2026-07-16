"""공용 run-metadata 테이블 (``ops.run_metadata``) — 실행 1건 = 1행 append.

파편화된 도메인별 run 기록(weather ``bronze_collection_run_manifest`` · ``common/runmetrics``
R2 JSON · ``errors`` Problem JSON · citydata run_report)을 **하나의 조회 가능한 canonical
Iceberg 테이블**로 모으기 위한 씨앗(citydata 파일럿). SLO(적시성·완전성·성공률) 집계와
데일리 digest 의 소스.

왜 append 인가: 매 실행이 **자기 행 1개만 추가**하므로 R2 delete+insert 비원자성(이중삽입)
문제가 원천적으로 없다. 재시도는 ``try_number`` 로 구분돼 안 겹친다. → dedup 불필요.

grain = (dag_id, task_id, run_id, try_number). 태스크 1건 = 1행.
best-effort: 기록 실패는 태스크를 실패시키지 않는다(경고 로그만) — 기존 sink 규약과 동일.

컬럼 = weather manifest(완전성) + runmetrics(타이밍) 합집합 + 명시적 ``scheduled_at``.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

LOGGER = logging.getLogger(__name__)

OPS_SCHEMA = "ops"
OPS_TABLE = "run_metadata"
KST = ZoneInfo("Asia/Seoul")

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ── SQL 리터럴 헬퍼 (인젝션 방지·NULL 처리) ──────────────────────────────────────
def _ident(value: str) -> str:
    if not _IDENT_RE.match(value):
        raise ValueError(f"unsafe SQL identifier: {value}")
    return value


def _s(value: object) -> str:
    """문자열 리터럴. None/빈값 → NULL."""
    if value is None:
        return "NULL"
    text = str(value).strip()
    return "NULL" if text == "" else "'" + text.replace("'", "''") + "'"


def _i(value: object) -> str:
    if value is None or value == "":
        return "NULL"
    return str(int(value))


def _f(value: object) -> str:
    if value is None or value == "":
        return "NULL"
    return repr(float(value))


def _ts(dt: datetime | None) -> str:
    """UTC timestamp(6) 리터럴. None → NULL. tz 없으면 UTC 로 간주(runmetrics 규약)."""
    if dt is None:
        return "NULL"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    u = dt.astimezone(timezone.utc)
    return "TIMESTAMP '" + u.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"


def _catalog(target: str) -> str:
    """dev → iceberg_dev, prod → iceberg (build_trino_settings 규약과 동일)."""
    dev = (target or "dev").lower() != "prod"
    return (
        os.environ.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
        if dev
        else os.environ.get("TRINO_ICEBERG_CATALOG", "iceberg")
    )


def _connect(catalog: str):
    """자체 최소 Trino 연결(도메인 헬퍼 의존 없음 — common 은 도메인을 import 하지 않는다)."""
    import trino.dbapi

    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=_ident(catalog),
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )


@dataclass
class RunRow:
    """run-metadata 1행. 신원 + 적시성 + 성공률 + 완전성."""

    # 신원(dim)
    domain: str
    layer: str            # bronze | silver | gold | transform
    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    target: str           # dev | prod
    # 적시성
    scheduled_at: datetime | None   # 예정 시각(data_interval_end)
    started_at: datetime | None
    ended_at: datetime | None
    # 성공률
    status: str           # success | failed | skipped
    failure_reason: str | None = None
    error_id: str | None = None     # errors Problem 문서 링크(2단계, 현재 옵션)
    # 완전성
    expected_rows: int | None = None
    actual_rows: int | None = None
    expected_raw_objects: int | None = None
    actual_raw_objects: int | None = None


_DDL_COLUMNS = """
    domain varchar,
    layer varchar,
    dag_id varchar,
    task_id varchar,
    run_id varchar,
    try_number integer,
    target varchar,
    scheduled_at timestamp(6),
    started_at timestamp(6),
    ended_at timestamp(6),
    duration_s double,
    status varchar,
    failure_reason varchar,
    error_id varchar,
    expected_rows bigint,
    actual_rows bigint,
    expected_raw_objects integer,
    actual_raw_objects integer,
    recorded_at timestamp(6),
    dt varchar
""".strip()

_INSERT_COLUMNS = (
    "domain", "layer", "dag_id", "task_id", "run_id", "try_number", "target",
    "scheduled_at", "started_at", "ended_at", "duration_s",
    "status", "failure_reason", "error_id",
    "expected_rows", "actual_rows", "expected_raw_objects", "actual_raw_objects",
    "recorded_at", "dt",
)


def _qualified(catalog: str) -> str:
    return f"{_ident(catalog)}.{_ident(OPS_SCHEMA)}.{_ident(OPS_TABLE)}"


def ensure_table(cur, catalog: str) -> str:
    """ops 스키마 + run_metadata 테이블 멱등 생성. dt(KST 날짜)로 파티션."""
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(catalog)}.{_ident(OPS_SCHEMA)}")
    except Exception as exc:  # noqa: BLE001 -- 동시 생성 레이스만 무시
        if "already exists" not in str(exc).lower():
            raise
    table = _qualified(catalog)
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {table} (\n{_DDL_COLUMNS}\n)\n"
        "WITH (format = 'PARQUET', partitioning = ARRAY['dt'])"
    )
    return table


def append_run_row(row: RunRow, *, target: str = "dev") -> bool:
    """run 1건을 append. best-effort — 실패해도 예외를 삼키고 False 반환(태스크 안 죽임)."""
    try:
        catalog = _catalog(target)
        conn = _connect(catalog)
        cur = conn.cursor()
        table = ensure_table(cur, catalog)

        # 파생값: 소요시간, dt(KST 날짜), recorded_at
        duration_s = None
        if row.started_at and row.ended_at:
            duration_s = (row.ended_at - row.started_at).total_seconds()
        anchor = row.started_at or row.scheduled_at or datetime.now(timezone.utc)
        if anchor.tzinfo is None:
            anchor = anchor.replace(tzinfo=timezone.utc)
        dt = anchor.astimezone(KST).strftime("%Y-%m-%d")

        values = ", ".join([
            _s(row.domain), _s(row.layer), _s(row.dag_id), _s(row.task_id),
            _s(row.run_id), _i(row.try_number), _s(row.target),
            _ts(row.scheduled_at), _ts(row.started_at), _ts(row.ended_at), _f(duration_s),
            _s(row.status), _s(row.failure_reason), _s(row.error_id),
            _i(row.expected_rows), _i(row.actual_rows),
            _i(row.expected_raw_objects), _i(row.actual_raw_objects),
            _ts(datetime.now(timezone.utc)), _s(dt),
        ])
        cur.execute(f"INSERT INTO {table} ({', '.join(_INSERT_COLUMNS)}) VALUES ({values})")
        cur.fetchall()
        return True
    except Exception as exc:  # noqa: BLE001 -- 기록 실패가 태스크 판정을 가리지 않게
        LOGGER.warning("[ops.run_metadata] append 실패(무시): %s", exc)
        return False


def daily_summary(domain: str, dt: str, *, target: str = "dev") -> dict:
    """한 도메인의 dt(KST 날짜) SLO 집계 — 성공률·완전성·적시성. 데일리 digest 소스."""
    catalog = _catalog(target)
    cur = _connect(catalog).cursor()
    t = _qualified(catalog)
    cur.execute(
        f"""
        select
          count(*) as total,
          sum(if(status = 'success', 1, 0)) as ok,
          sum(if(status = 'failed', 1, 0)) as failed,
          count(distinct run_id) as runs,
          avg(if(layer = 'bronze' and expected_raw_objects > 0,
                 100.0 * actual_raw_objects / expected_raw_objects, null)) as cov_pct,
          avg(if(task_id = 'load_bronze', duration_s, null)) as bronze_avg_s,
          max(if(task_id = 'load_bronze', duration_s, null)) as bronze_max_s
        from {t}
        where domain = {_s(domain)} and dt = {_s(dt)}
        """
    )
    total, ok, failed, runs, cov, b_avg, b_max = cur.fetchone()
    cur.execute(
        f"select task_id, count(*) c from {t} "
        f"where domain = {_s(domain)} and dt = {_s(dt)} and status = 'failed' "
        f"group by 1 order by 2 desc limit 5"
    )
    top_failures = [(r[0], r[1]) for r in cur.fetchall()]
    return {
        "dt": dt, "domain": domain,
        "total": total or 0, "ok": ok or 0, "failed": failed or 0, "runs": runs or 0,
        "success_pct": round(100.0 * (ok or 0) / total, 1) if total else None,
        "coverage_pct": round(float(cov), 1) if cov is not None else None,
        "bronze_avg_s": round(float(b_avg), 1) if b_avg is not None else None,
        "bronze_max_s": round(float(b_max), 1) if b_max is not None else None,
        "top_failures": top_failures,
    }
