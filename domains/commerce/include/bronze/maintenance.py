"""Iceberg 테이블 유지보수 — 소파일 병합(optimize)·스냅샷 만료(expire)·orphan 제거 + 자원 캡처 (#226).

증분 적재는 커밋마다 스냅샷을 만들어(bronze 실측 300+) metadata/과거파일/orphan 이 쌓인다('더미파일').
표준 유지보수 3종으로 정리하고, 쿼리별 Trino stats(elapsed/cpu/peak memory)를 캡처해 리포트에 남긴다.

**재개 표준(#·resumability)**: 단위 = (테이블, op). 각 op 는 **멱등**이라 중단 후 재실행해도 안전하다
— 성공분을 다시 실행해도 무해하고 실패분만 다음 실행이 이어받는다. 리포트는 (테이블, op) 단위로
성공/실패/skip 과 자원을 남긴다. (GPU 는 Trino 미사용 → N/A.)
"""
from __future__ import annotations

import logging

from bronze.warehouse import _connect, _qualified

log = logging.getLogger(__name__)

# 유지보수 대상 = commerce Iceberg 테이블(bronze + silver). 없는 테이블은 skip.
# bronze_collection_run_manifest 포함 필수 — 이 발행 게이트 테이블은 write_manifest 가 종·run 단위
# delete-then-insert 로 커밋을 계속 쌓으므로, 유지보수에서 빠지면 스냅샷/메타데이터가 무한 축적돼
# R2 Data Catalog 메타데이터 불일치(metadata not found)를 유발한다(실측 원인).
DEFAULT_TABLES = (
    "bronze_localdata_license",
    "bronze_collection_run_manifest",
    "silver_license_history",
    "silver_license_current",
    "silver_license_detail_health",
)
_SAFE = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


def _safe_table(t: str) -> str:
    if not t or any(c not in _SAFE for c in t):
        raise ValueError(f"안전하지 않은 테이블명: {t!r}")
    return t


def _snapshot_count(cur, qschema: str, t: str) -> int | None:
    try:
        cur.execute(f'select count(*) from {qschema}."{t}$snapshots"')  # security: allow-sql — t=_safe_table
        return int(cur.fetchall()[0][0])
    except Exception:                                   # 테이블/메타 없음
        return None


def run_table_maintenance(tables: tuple[str, ...] = DEFAULT_TABLES, *, expire_days: int = 7) -> list[dict]:
    """대상 테이블에 optimize/expire_snapshots/remove_orphan_files 실행. (테이블,op) 단위 결과 리스트 반환.

    각 원소: {table, op, status(ok|failed|skipped|info), elapsed_ms, cpu_ms, peak_mem_bytes, ...}.
    멱등 — 중단 시 재실행으로 이어받기 안전.
    """
    catalog, schema, qschema = _qualified()
    ops = [
        ("optimize", "ALTER TABLE {qt} EXECUTE optimize"),
        ("expire_snapshots",
         "ALTER TABLE {qt} EXECUTE expire_snapshots(retention_threshold => '{d}d')"),
        ("remove_orphan_files",
         "ALTER TABLE {qt} EXECUTE remove_orphan_files(retention_threshold => '{d}d')"),
    ]
    conn = _connect(catalog, schema)
    results: list[dict] = []
    try:
        cur = conn.cursor()
        for raw_t in tables:
            t = _safe_table(raw_t)
            qt = f"{qschema}.{t}"
            before = _snapshot_count(cur, qschema, t)
            if before is None:
                results.append({"table": t, "op": "-", "status": "skipped", "reason": "테이블 없음"})
                continue
            for op, tmpl in ops:
                sql = tmpl.format(qt=qt, d=int(expire_days))
                try:
                    cur.execute(sql)                    # security: allow-sql — qt from _qualified()+_safe_table
                    cur.fetchall()
                    s = cur.stats or {}
                    results.append({"table": t, "op": op, "status": "ok",
                                    "elapsed_ms": s.get("elapsedTimeMillis"),
                                    "cpu_ms": s.get("cpuTimeMillis"),
                                    "peak_mem_bytes": s.get("peakMemoryBytes")})
                    log.info("maintenance %s.%s ok elapsed=%sms cpu=%sms peakMem=%sB", t, op,
                             s.get("elapsedTimeMillis"), s.get("cpuTimeMillis"), s.get("peakMemoryBytes"))
                except Exception as exc:                # noqa: BLE001 — op 단위 격리(다음 op/table 계속)
                    results.append({"table": t, "op": op, "status": "failed", "error": str(exc)[:150]})
                    log.warning("maintenance %s.%s 실패: %s", t, op, type(exc).__name__)
            after = _snapshot_count(cur, qschema, t)
            results.append({"table": t, "op": "snapshots", "status": "info",
                            "before": before, "after": after})
    finally:
        conn.close()
    log.info("maintenance 완료: %d 결과", len(results))
    return results
