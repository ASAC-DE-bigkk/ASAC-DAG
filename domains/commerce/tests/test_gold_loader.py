"""gold loader — force_full(트리거 전량 재적재) 분기 계약.

commerce_load_gold_refresh 가 쓰는 force_full=True 가:
- no_new_silver 조기 스킵을 **건너뛰고**(기적재만 있어도 재적재),
- _load_chunked 에 force_full=True 를 전파해 마커 무관 전량 재적재하는지 검증.
기본(force_full=False)은 기존 조기 스킵 동작을 그대로 유지하는지도 고정한다.
DB/Trino 왕복은 페이크로 격리(순수 분기 로직만).
"""
from __future__ import annotations

from datetime import datetime

import pytest

from gold import loader


class _FakeCur:
    def __init__(self, fetchone_val=None):
        self._fetchone = fetchone_val

    def execute(self, *a, **k):
        return None

    def fetchone(self):
        return self._fetchone


class _FakeTrinoConn:
    def __init__(self, hi):
        self._hi = hi

    def cursor(self):
        return _FakeCur(fetchone_val=(self._hi,))

    def close(self):
        pass


class _FakePgConn:
    def close(self):
        pass


_HI = datetime(2026, 7, 13, 2, 37, 14)
_DETAILS = [{"object": "commerce_food_detail", "kind": "detail_single",
             "members": ["bakery"], "payload": ["A"]}]
_DMAP = {"bakery": {"entity_type": "food", "detail_table": "commerce_food_detail"}}
_EXPECTED = ["commerce_business_entity_history", "commerce_business_entity", "commerce_food_detail"]


def _patch_common(monkeypatch, markers):
    monkeypatch.setattr(loader.pg, "connect", lambda: _FakePgConn())
    monkeypatch.setattr(loader, "read_markers", lambda pg: dict(markers))
    monkeypatch.setattr(loader, "_trino", lambda: (_FakeTrinoConn(_HI), "iceberg_dev.commerce"))
    monkeypatch.setattr(loader, "ensure_objects", lambda pg, details: None)


def test_force_full_bypasses_no_new_silver_skip(monkeypatch):
    # 전 객체 마커가 hi 이상 → 평상시라면 no_new_silver 로 스킵되는 상태.
    markers = {o: _HI for o in _EXPECTED}
    _patch_common(monkeypatch, markers)
    monkeypatch.setattr(loader, "_read_silver_watermark", lambda: _HI)
    seen = {}

    def _fake_chunked(tconn, qschema, pgconn, details, dmap, hi, *, force_full=False):
        seen["force_full"] = force_full
        return {"loaded": {"commerce_food_detail": 3}, "hi": str(hi),
                "mode": "full_reload" if force_full else "chunked"}

    monkeypatch.setattr(loader, "_load_chunked", _fake_chunked)
    out = loader.run_load(_DETAILS, _DMAP, force_full=True)
    assert seen["force_full"] is True          # force_full 이 청크 경로로 전파
    assert out["mode"] == "full_reload"
    assert "skipped" not in out                 # 조기 스킵 안 함


def test_default_still_skips_when_no_new(monkeypatch):
    # 동일 상태에서 force_full=False(기본)면 기존대로 조기 스킵(회귀 방지).
    markers = {o: _HI for o in _EXPECTED}
    _patch_common(monkeypatch, markers)
    monkeypatch.setattr(loader, "_read_silver_watermark", lambda: _HI)
    monkeypatch.setattr(loader, "_load_chunked",
                        lambda *a, **k: pytest.fail("스킵돼야 하는데 청크 진입"))
    out = loader.run_load(_DETAILS, _DMAP)
    assert out.get("skipped") == "no_new_silver"


def test_force_full_reloads_even_with_full_markers(monkeypatch):
    # 마커가 전부 있어도(정상 증분 경로 조건) force_full 이면 청크 전량 경로로 간다.
    markers = {o: _HI for o in _EXPECTED}
    _patch_common(monkeypatch, markers)
    monkeypatch.setattr(loader, "_read_silver_watermark", lambda: None)  # 워터마크 무관
    seen = {}
    monkeypatch.setattr(loader, "_load_chunked",
                        lambda *a, force_full=False, **k: seen.setdefault("ff", force_full) or {"mode": "full_reload"})
    loader.run_load(_DETAILS, _DMAP, force_full=True)
    assert seen["ff"] is True
