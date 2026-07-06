"""#157 — culture Iceberg 유지보수: 대상 테이블 레지스트리 정합 + storage_cleanup 판정 로직.

storage_cleanup 은 population #156 의 적응 — 삭제/보존 판정을 순수 함수(_classify)로
분리해 네트워크 없이 검증한다. 보존 규칙(안전장치)이 핵심:
  * 최근 파일(cutoff 이후 수정) → 무조건 보존 (진행 중 커밋 보호)
  * 살아있는 디렉터리의 참조 metadata·데이터 파일 → 보존
  * 살아있는 디렉터리의 "미참조 옛 metadata.json" 만 → old_metadata
  * 죽은(카탈로그에 없는) 디렉터리 → orphan_dir
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from culture_ingest.common.maintenance import MAINTAINED_TABLES, _META_PATH_RE, _classify
from culture_ingest.source.datasets import ALL_DATASETS

CUTOFF = datetime(2026, 7, 6, 0, 0, tzinfo=timezone.utc)
OLD = CUTOFF - timedelta(hours=1)     # cutoff 이전 = 정리 후보
RECENT = CUTOFF + timedelta(hours=1)  # cutoff 이후 = 보호

LIVE = {"table-live-uuid"}
KEEP = {"00042-current.metadata.json"}

PREFIX = "__r2_data_catalog/019f165c-2b06-75e2-9269-aa9d0dee1cd8"


# ── 대상 테이블 = 레지스트리 정합 (드리프트 그물) ─────────────────────────────

def test_maintained_tables_cover_all_bronze():
    """datasets 레지스트리의 12개 전부 bronze_<name> 으로 포함돼야 한다."""
    for ds in ALL_DATASETS:
        assert f"bronze_{ds.name}" in MAINTAINED_TABLES, f"bronze_{ds.name} 누락"


def test_maintained_tables_include_future_silver_gold():
    """재설계 산출물(silver 9·gold 3)을 선등록 — 없는 동안은 skipped, 생기면 자동 편입."""
    assert "silver_culture_event" in MAINTAINED_TABLES
    assert "silver_culture_facility" in MAINTAINED_TABLES
    assert "gold_culture_location_daily" in MAINTAINED_TABLES
    silver = [t for t in MAINTAINED_TABLES if t.startswith("silver_")]
    gold = [t for t in MAINTAINED_TABLES if t.startswith("gold_")]
    assert len(silver) == 9 and len(gold) == 3


# ── _classify 판정 ────────────────────────────────────────────────────────────

def test_recent_file_always_protected():
    """cutoff 이후 수정 파일은 죽은 디렉터리라도 보존 (진행 중 커밋 보호)."""
    key = f"{PREFIX}/dead-dir/metadata/00001.metadata.json"
    assert _classify(key, RECENT, CUTOFF, LIVE, KEEP) is None


def test_dead_directory_is_orphan():
    key = f"{PREFIX}/dead-dir/data/part-0001.parquet"
    assert _classify(key, OLD, CUTOFF, LIVE, KEEP) == "orphan_dir"


def test_unreferenced_old_metadata_in_live_dir():
    key = f"{PREFIX}/table-live-uuid/metadata/00001-old.metadata.json"
    assert _classify(key, OLD, CUTOFF, LIVE, KEEP) == "old_metadata"


def test_referenced_metadata_kept():
    key = f"{PREFIX}/table-live-uuid/metadata/00042-current.metadata.json"
    assert _classify(key, OLD, CUTOFF, LIVE, KEEP) is None


def test_data_files_in_live_dir_kept():
    """살아있는 디렉터리의 데이터/스냅샷 파일은 여기서 안 지운다 — expire_snapshots 소관."""
    key = f"{PREFIX}/table-live-uuid/data/part-0001.parquet"
    assert _classify(key, OLD, CUTOFF, LIVE, KEEP) is None
    snap = f"{PREFIX}/table-live-uuid/metadata/snap-123.avro"
    assert _classify(snap, OLD, CUTOFF, LIVE, KEEP) is None


def test_short_keys_ignored():
    assert _classify("__r2_data_catalog/uuid-only", OLD, CUTOFF, LIVE, KEEP) is None


# ── metadata 경로 정규식 ──────────────────────────────────────────────────────

def test_meta_path_regex_groups():
    path = f"s3://seoul-dev/{PREFIX}/table-live-uuid/metadata/00042-current.metadata.json"
    m = _META_PATH_RE.match(path)
    assert m is not None
    assert m.group("prefix") == PREFIX
    assert m.group("dir") == "table-live-uuid"
    assert m.group("name") == "00042-current.metadata.json"
