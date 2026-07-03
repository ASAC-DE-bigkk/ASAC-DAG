"""bronze.load_state — 워터마크·pending·receipt(파일 기반, RDB 없음) 단위테스트.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_load_state.py -q
"""
import json

from bronze import load_state as ls


class _FakeStorage:
    def __init__(self):
        self.data: dict[str, bytes] = {}

    def exists(self, k):
        return k in self.data

    def read_bytes(self, k):
        return self.data[k]

    def write_bytes(self, k, b):
        self.data[k] = b

    def read_json(self, k):
        return json.loads(self.data[k].decode("utf-8"))

    def write_json(self, k, obj):
        self.data[k] = json.dumps(obj, ensure_ascii=False).encode("utf-8")


def test_watermark_roundtrip_and_absence():
    st = _FakeStorage()
    assert not ls.has_watermark(st, "")           # 없으면 전체 재적재 신호
    assert ls.read_watermark(st, "") == {}
    ls.write_watermark(st, "", {"bakery": "2026-07-03_010101_001"})
    assert ls.has_watermark(st, "")
    assert ls.read_watermark(st, "")["bakery"] == "2026-07-03_010101_001"


def test_advance_watermark_monotonic():
    wm = {}
    ls.advance_watermark(wm, "bakery", "2026-07-01_010101_001")
    ls.advance_watermark(wm, "bakery", "2026-07-03_010101_001")
    ls.advance_watermark(wm, "bakery", "2026-07-02_010101_001")   # 과거 → 무시
    assert wm["bakery"] == "2026-07-03_010101_001"


def test_days_between():
    assert ls.days_between("2026-07-01", "2026-07-04") == 3
    assert ls.days_between("bad", "2026-07-04") >= 10**5           # 오류 → 즉시 만료 취급


def test_reconcile_pending_add_resolve_expire():
    pending = [
        {"date": "2026-07-01", "short": "old", "first_seen": "2026-07-01"},   # 3일 경과 → 폐기
        {"date": "2026-07-03", "short": "keep", "first_seen": "2026-07-03"},  # 유지
        {"date": "2026-07-03", "short": "done", "first_seen": "2026-07-03"},  # resolved → 제거
    ]
    kept, expired = ls.reconcile_pending(
        pending, resolved={("2026-07-03", "done")},
        new_incomplete={("2026-07-04", "new")}, today="2026-07-04")
    keys = {(p["date"], p["short"]) for p in kept}
    assert ("2026-07-03", "keep") in keys
    assert ("2026-07-04", "new") in keys                          # 신규 incomplete 추가
    assert ("2026-07-03", "done") not in keys                     # resolved 제거
    assert [(e["date"], e["short"]) for e in expired] == [("2026-07-01", "old")]


def test_retry_dates_within_lookback():
    pending = [
        {"date": "2026-07-04", "short": "a", "first_seen": "2026-07-04"},   # 0일 → 포함
        {"date": "2026-07-02", "short": "b", "first_seen": "2026-07-02"},   # 2일 → 포함
        {"date": "2026-07-01", "short": "c", "first_seen": "2026-07-01"},   # 3일 → 제외
    ]
    assert ls.retry_dates(pending, today="2026-07-04") == {"2026-07-04", "2026-07-02"}


def test_write_receipt_isolated_path():
    st = _FakeStorage()
    ls.write_receipt(st, "", {"load_date": "2026-07-03", "bronze_run_id": "R", "short": "bakery",
                              "rows_loaded": 5})
    (key,) = list(st.data)
    assert key.startswith("commerce_bronze_state/receipts/2026-07-03/")   # raw 밖·commerce_ prefix
    assert key.endswith("R__bakery.json")
