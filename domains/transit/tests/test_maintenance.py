"""보존 정책(#369 — 주 단위 월~일 KST) 순수 로직 테스트 — 주 경계·만료 판정·SQL.

핵심 계약:
- 주 경계 = 이번 주 월요일 00:00 KST (UTC 환산 = 일요일 15:00Z) 미만 삭제.
- 마스터(주간 스냅샷)는 어떤 경로로도 삭제 대상에 들어가지 않는다.
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_TRANSIT = Path(__file__).resolve().parents[1]
_DAGS = Path(__file__).resolve().parents[3]
for p in (str(_DAGS), str(_TRANSIT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from seoul_transit import maintenance  # noqa: E402

KST = timezone(timedelta(hours=9))
# 2026-07-16 = 목요일 (그 주 월요일 = 07-13)
_THU = datetime(2026, 7, 16, 12, 0, 0, tzinfo=KST)


def test_week_cutoff_is_monday_midnight_kst_in_utc():
    cut = maintenance.week_cutoff(_THU)
    # 월 07-13 00:00 KST == 일 07-12 15:00 UTC
    assert cut == datetime(2026, 7, 12, 15, 0, 0, tzinfo=timezone.utc)
    assert maintenance.cutoff_ingest_ts(_THU) == "20260712T150000Z"
    assert maintenance.cutoff_bronze_ts(_THU) == "2026-07-12 15:00:00"


def test_week_cutoff_monday_morning_purges_last_week():
    # 월요일 00:30 KST — 새 주 시작 직후: 직전 일요일 밤 데이터가 즉시 만료된다
    monday = datetime(2026, 7, 20, 0, 30, 0, tzinfo=KST)
    cut = maintenance.cutoff_ingest_ts(monday)
    assert cut == "20260719T150000Z"
    sunday_night = "raw/transit/seoul_bus/bus_position/load_date=2026-07-19/ingest_ts=20260719T145900Z/bundle.jsonl"
    this_week = "raw/transit/seoul_bus/bus_position/load_date=2026-07-19/ingest_ts=20260719T150100Z/bundle.jsonl"
    assert maintenance.is_expired_key(sunday_night, cut)
    assert not maintenance.is_expired_key(this_week, cut)


def test_is_expired_key_by_ingest_ts_and_untouchables():
    cut = maintenance.cutoff_ingest_ts(_THU)  # 20260712T150000Z
    old = "raw/transit/seoul_subway/subway_arrival/load_date=2026-07-10/ingest_ts=20260710T090001Z/page-0001.json"
    keep = "raw/transit/seoul_subway/subway_arrival/load_date=2026-07-14/ingest_ts=20260714T090001Z/page-0001.json"
    no_ts = "raw/transit/seoul_subway/subway_arrival/notes.txt"
    assert maintenance.is_expired_key(old, cut)
    assert not maintenance.is_expired_key(keep, cut)
    assert not maintenance.is_expired_key(no_ts, cut)  # ingest_ts 없으면 불가침


def test_stale_pending_uses_same_week_boundary():
    cut = maintenance.cutoff_ingest_ts(_THU)
    old_marker = "ops/control/state/transit/loader_pending/parking/20260711T090001Z__run.json"
    fresh_marker = "ops/control/state/transit/loader_pending/parking/20260715T090001Z__run.json"
    assert maintenance.is_stale_pending(old_marker, cut)
    assert not maintenance.is_stale_pending(fresh_marker, cut)


def test_realtime_datasets_only_no_masters():
    # 마스터 dataset 이 소스 매핑·테이블 목록 어디에도 없어야 한다
    for name in maintenance.SOURCE_BY_DATASET:
        assert "master" not in name
    for table in maintenance.bronze_tables():
        assert "master" not in table
    assert "bronze_bus_position" in maintenance.bronze_tables()
    assert maintenance.raw_prefix("parking") == "raw/transit/seoul_parking/parking/"


def test_purge_sql_statements():
    stmts = maintenance.purge_sql("cat.transit.bronze_parking", "2026-07-12 15:00:00")
    assert stmts[0] == ("DELETE FROM cat.transit.bronze_parking "
                        "WHERE ingested_at < TIMESTAMP '2026-07-12 15:00:00'")
    assert "EXECUTE optimize" in stmts[1]
    # 스냅샷/고아파일은 Trino min-retention(7d) 제약으로 7d 고정
    assert "expire_snapshots(retention_threshold => '7d')" in stmts[2]
    assert "remove_orphan_files(retention_threshold => '7d')" in stmts[3]


def test_purge_sql_rejects_malformed_cutoff():
    with pytest.raises(ValueError):
        maintenance.purge_sql("cat.transit.bronze_parking", "2026-07-12' OR 1=1 --")

# ── purge 선행 게이트 (#443 — 아카이브가 삭제 구간을 소비했는가) ─────────────────
def test_archive_watermark_is_week_boundary_in_kst_wallclock():
    # gold 의 bucket_at 은 KST 벽시계 계약(#48)이라 워터마크도 KST 로 낸다.
    # _THU(07-16 목) 기준 이번 주 월요일 = 07-13 00:00 KST.
    assert maintenance.archive_watermark_required(_THU) == "2026-07-13 00:00:00"


def test_archive_caught_up_requires_reaching_boundary():
    # 경계 이상이면 통과 — 아카이브가 지울 구간을 이미 집계했다는 뜻.
    assert maintenance.is_archive_caught_up("2026-07-13 00:00:00", _THU) is True
    assert maintenance.is_archive_caught_up("2026-07-16 11:45:00", _THU) is True
    # 경계 미만이면 차단 — 지금 지우면 그 구간이 어디에도 안 남는다.
    assert maintenance.is_archive_caught_up("2026-07-12 23:45:00", _THU) is False


def test_archive_caught_up_blocks_when_archive_empty_or_missing():
    # 빈 아카이브(개시 직후·변환 미수행)에서 purge 가 돌면 원본만 사라진다.
    assert maintenance.is_archive_caught_up(None, _THU) is False
    assert maintenance.is_archive_caught_up("", _THU) is False


def test_archive_caught_up_tolerates_fractional_seconds():
    # Trino 의 timestamp(6) 문자열은 소수 초를 달고 나온다 — 앞 19자만 비교한다.
    assert maintenance.is_archive_caught_up("2026-07-13 00:00:00.000000", _THU) is True
    assert maintenance.is_archive_caught_up("2026-07-12 23:59:59.999999", _THU) is False
