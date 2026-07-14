"""silver 상태 R2 파일 규약(commerce_core.silver_state) + 마커 기반 스킵/분류 로직 테스트.

- silver_state: 워터마크·마커 스냅샷 파일 round-trip (LocalStorage — 파일만, RDB 없음).
- silver.chunked_run.classify_unmarked: '신규 run 도착'(Cosmos 몫)과 '빌드 미완'(seed 재빌드)
  구분 — 신규 run 만으로 기적재 이력 전체를 재빌드하지 않는다(기적재분 재적재 차단).
"""
from __future__ import annotations

from datetime import datetime

from common.storage import LocalStorage

from commerce_core import silver_state


def _storage(tmp_path):
    return LocalStorage(str(tmp_path))


# ── silver_state 파일 round-trip ─────────────────────────────────────────────
def test_watermark_roundtrip(tmp_path):
    st = _storage(tmp_path)
    assert silver_state.read_watermark(st, "p") is None          # 부재 → None(fail-open)
    silver_state.write_watermark(st, "p", max_collected_at="2026-07-13T02:37:14", marker_rows=3)
    doc = silver_state.read_watermark(st, "p")
    assert doc["max_collected_at"] == "2026-07-13T02:37:14"
    assert doc["marker_rows"] == 3
    assert "updated_at" in doc


def test_watermark_none_collected_at(tmp_path):
    st = _storage(tmp_path)
    silver_state.write_watermark(st, "", max_collected_at=None, marker_rows=0)
    assert silver_state.read_watermark(st, "")["max_collected_at"] is None


def test_marker_snapshot_roundtrip_dedup_sorted(tmp_path):
    st = _storage(tmp_path)
    assert silver_state.read_marker_snapshot(st, "p") == []      # 부재 → 빈 목록
    markers = [("b", "2026-07-02_000000_000"), ("a", "2026-07-01_000000_000"),
               ("b", "2026-07-02_000000_000")]                   # 중복 포함
    silver_state.write_marker_snapshot(st, "p", markers)
    got = silver_state.read_marker_snapshot(st, "p")
    assert got == [("a", "2026-07-01_000000_000"), ("b", "2026-07-02_000000_000")]


def test_state_layer_isolated_from_bronze(tmp_path):
    # bronze(commerce_bronze_state)와 다른 레이어 — 키 충돌 없음.
    assert silver_state.watermark_key("p").startswith("p/commerce_silver_state/")
    assert silver_state.markers_key("").startswith("commerce_silver_state/")


# ── seed 분류 (classify_unmarked) — 신규 run 은 재빌드가 아니라 Cosmos 몫 ──────
def _classify(*args):
    from silver.chunked_run import classify_unmarked

    return classify_unmarked(*args)


def test_new_runs_go_to_cosmos_not_rebuild():
    # 기존 DONE 이 있는 dataset 에 미빌드 신규 run 만 도착 → 재빌드 금지(기적재 재적재 차단).
    unmarked = {("food", "2026-07-13_000002_396")}
    built = set()                                   # 신규 run 은 아직 history 에 없음
    done = {"food"}                                 # 과거 run 은 DONE
    rebuild, cosmos = _classify(unmarked, built, done)
    assert rebuild == [] and cosmos == ["food"]


def test_interrupted_build_rebuilds():
    # 미마킹 run 이 history 에 행을 남김(마킹 전 중단) → 재빌드.
    unmarked = {("food", "2026-07-13_000002_396")}
    built = {("food", "2026-07-13_000002_396")}
    rebuild, cosmos = _classify(unmarked, built, {"food"})
    assert rebuild == ["food"] and cosmos == []


def test_no_done_marker_rebuilds():
    # DONE 전무(cold-start/언마크 후 중단) → 전 run 재처리 필요 → 재빌드.
    unmarked = {("food", "r1"), ("food", "r2")}
    rebuild, cosmos = _classify(unmarked, set(), set())
    assert rebuild == ["food"] and cosmos == []


def test_mixed_datasets_split():
    unmarked = {("a", "r9"), ("b", "r9"), ("c", "r1")}
    built = {("b", "r9")}                           # b 만 빌드 후 미마킹(중단)
    done = {"a", "b"}                               # c 는 DONE 전무
    rebuild, cosmos = _classify(unmarked, built, done)
    assert rebuild == ["b", "c"] and cosmos == ["a"]


def test_fully_marked_is_noop():
    rebuild, cosmos = _classify(set(), set(), {"a", "b"})
    assert rebuild == [] and cosmos == []
