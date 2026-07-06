"""Iceberg 테이블 유지보수: 작은 파일 압축 + 옛 스냅샷/고아 파일 정리.

5분 주기로 쌓이는 스냅샷·데이터파일이 R2 저장을 부풀리므로, 주기적으로
``optimize``(작은 파일 병합) → ``expire_snapshots``(옛 버전 정리) →
``remove_orphan_files``(커밋 실패 찌꺼기)를 돌린다.

데이터(현재 스냅샷의 행)는 건드리지 않고 **죽은 파일/옛 버전만** 제거한다.
현재 테이블 = 누적된 전체 데이터이므로 이 정리로 손실되지 않는다.

⚠ 짧은 retention을 쓰려면 Trino 카탈로그의 ``iceberg.expire-snapshots.min-retention``·
``iceberg.remove-orphan-files.min-retention`` 하한 이상이어야 한다(기본 7d).
"""

from __future__ import annotations

from ..common.trino import build_trino_settings, connect, sql_identifier

# 유지보수 대상 테이블(도메인 스키마 내). 참조 seed는 정적이라 제외.
MAINTAINED_TABLES: tuple[str, ...] = (
    "bronze_seoul_ppltn",
    "silver_seoul_ppltn",
    "gold_seoul_ppltn_by_time",
)


def run_maintenance(
    target: str = "dev",
    *,
    tables: tuple[str, ...] = MAINTAINED_TABLES,
    retention: str = "3d",
) -> dict[str, str]:
    """대상 테이블에 optimize + expire_snapshots + remove_orphan_files 실행.

    반환: {테이블명: 'ok' | 'error: ...'} — 한 테이블 실패해도 나머지는 계속.
    """
    s = build_trino_settings(target)
    cur = connect(s).cursor()
    results: dict[str, str] = {}
    for tbl in tables:
        t = f"{sql_identifier(s.catalog)}.{sql_identifier(s.schema)}.{sql_identifier(tbl)}"
        try:
            for op in (
                "optimize",
                f"expire_snapshots(retention_threshold => '{retention}')",
                f"remove_orphan_files(retention_threshold => '{retention}')",
            ):
                cur.execute(f"ALTER TABLE {t} EXECUTE {op}")
                cur.fetchall()  # 일부 procedure는 통계 행을 반환하므로 소비
            results[tbl] = "ok"
        except Exception as exc:  # noqa: BLE001 -- 테이블별 격리, 배치는 계속
            results[tbl] = f"error: {type(exc).__name__}: {exc}"
    return results
