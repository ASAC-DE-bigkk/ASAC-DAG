"""보존 정책(#369 4단계) 순수 로직 테스트 — cutoff·만료 판정·대상 테이블·SQL.

핵심 계약: 마스터(주간 스냅샷)는 어떤 경로로도 삭제 대상에 들어가지 않는다.
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_TRANSIT = Path(__file__).resolve().parents[1]
_DAGS = Path(__file__).resolve().parents[3]
for p in (str(_DAGS), str(_TRANSIT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from seoul_transit import maintenance  # noqa: E402

KST = timezone(timedelta(hours=9))
_NOW = datetime(2026, 7, 16, 12, 0, 0, tzinfo=KST)


def test_cutoff_keeps_recent_n_days():
    # 보존 3일 = 오늘(16) 포함 14·15·16 유지 → cutoff 는 07-14 (미만 삭제)
    assert maintenance.cutoff_load_date(3, now=_NOW) == "2026-07-14"


def test_is_expired_key_by_load_date():
    cutoff = "2026-07-14"
    old = "raw/transit/seoul_bus/bus_position/load_date=2026-07-13/ingest_ts=x/page-0001.xml"
    keep = "raw/transit/seoul_bus/bus_position/load_date=2026-07-14/ingest_ts=x/page-0001.xml"
    no_date = "raw/transit/seoul_bus/bus_position/notes.txt"
    assert maintenance.is_expired_key(old, cutoff)
    assert not maintenance.is_expired_key(keep, cutoff)
    assert not maintenance.is_expired_key(no_date, cutoff)  # load_date 없으면 불가침


def test_realtime_datasets_only_no_masters():
    # 마스터 dataset 이 소스 매핑·테이블 목록 어디에도 없어야 한다
    for name in maintenance.SOURCE_BY_DATASET:
        assert "master" not in name
    for table in maintenance.bronze_tables():
        assert "master" not in table
    assert "bronze_bus_position" in maintenance.bronze_tables()
    assert maintenance.raw_prefix("parking") == "raw/transit/seoul_parking/parking/"


def test_stale_pending_detection():
    cutoff = maintenance.stale_pending_ingest_cutoff(3, now=_NOW)  # 20260714T000000Z
    old = "state/transit/loader_pending/parking/20260713T090001Z__run.json"
    fresh = "state/transit/loader_pending/parking/20260715T090001Z__run.json"
    assert maintenance.is_stale_pending(old, cutoff)
    assert not maintenance.is_stale_pending(fresh, cutoff)


def test_purge_sql_statements():
    stmts = maintenance.purge_sql("cat.transit.bronze_parking", retention_days=3)
    assert stmts[0].startswith("DELETE FROM cat.transit.bronze_parking")
    assert "interval '3' day" in stmts[0]
    assert "EXECUTE optimize" in stmts[1]
    # 스냅샷/고아파일은 Trino min-retention(7d) 제약으로 7d 고정
    assert "expire_snapshots(retention_threshold => '7d')" in stmts[2]
    assert "remove_orphan_files(retention_threshold => '7d')" in stmts[3]
