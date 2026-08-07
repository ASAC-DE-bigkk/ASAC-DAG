"""스키마 관문(#732 재발 방지) — 키셋 변경의 감지·격리·전파 차단 계약.

08-04 사고의 재발 조건을 그대로 재현해 고정한다: 원천이 별칭표 밖의 키 변화를 보내면
①정렬·diff·증분이 돌지 않고(diff-target 무변경) ②원문이 격리되고 ③마커가 incomplete
(비게시 — 브론즈 적재가 소비하지 않아 실버·골드로 전파 불가) ④경보가 나간다.
별칭표가 아는 개명(v2 12종)은 정본화가 흡수하므로 오탐하지 않아야 한다.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from bronze import bronze_tasks, incremental, schema_guard
from commerce_core.schemas import Dataset


def _dataset(short="mail_order_sale", fmt="v2", canonical="v1"):
    return Dataset(oa_id="OA-1", name_ko="테스트", short=short, category="industry",
                   schedule="daily", service_name="SVC", fmt=fmt, canonical_fmt=canonical)


def _page(rows: list[dict]) -> bytes:
    return json.dumps({"SVC": {"list_total_count": len(rows),
                               "RESULT": {"CODE": "INFO-000", "MESSAGE": "정상"},
                               "row": rows}}).encode()


class _Storage:
    def __init__(self):
        self.data: dict[str, bytes] = {}

    def write_json(self, key, value):
        self.data[key] = json.dumps(value, ensure_ascii=False).encode()

    def read_json(self, key):
        return json.loads(self.data[key]) if key in self.data else None

    def write_bytes(self, key, body):
        self.data[key] = body

    def read_bytes(self, key):
        return self.data[key]

    def exists(self, key):
        return key in self.data

    def delete(self, key):
        self.data.pop(key, None)

    def list_keys(self, prefix):
        return [k for k in self.data if k.startswith(prefix)]


# ── 판정 단위 ────────────────────────────────────────────────────────────────

def test_no_baseline_is_bootstrap_not_change():
    st = _Storage()
    out = schema_guard.check(st, prefix="", dataset=_dataset(), incoming_keys={"MGTNO", "X"})
    assert out["status"] == schema_guard.BOOTSTRAP


def test_same_keys_pass_and_diff_keys_flag_both_directions():
    st = _Storage()
    ds = _dataset()
    schema_guard.record_baseline(st, prefix="", dataset=ds, keys={"MGTNO", "X"}, run_id="r1")
    assert schema_guard.check(st, prefix="", dataset=ds,
                              incoming_keys={"MGTNO", "X"})["status"] == schema_guard.OK
    out = schema_guard.check(st, prefix="", dataset=ds, incoming_keys={"MGTNO", "NEW"})
    assert out["status"] == schema_guard.CHANGED
    assert out["added"] == ["NEW"] and out["removed"] == ["X"]


def test_known_v2_rename_is_absorbed_by_canonicalization():
    """별칭표가 아는 개명(표준+도메인 오버레이)은 오탐하지 않는다 — 08-04 12종의 정상 경로."""
    ds = _dataset(short="animal_sale")
    v1_keys = schema_guard.sample_canonical_keys(
        ds, [[{"MGTNO": "1", "SITEAREA": "84", "RGTMBDSNO": "2"}]])
    v2_keys = schema_guard.sample_canonical_keys(
        ds, [[{"MNG_NO": "1", "LCTN_AREA": "84", "RGHT_MNBD_SN": "2"}]])
    assert v1_keys == v2_keys == {"MGTNO", "SITEAREA", "RGTMBDSNO"}


# ── _write_bronze 통합 — 격리·전파 차단 ─────────────────────────────────────────

def _write(st, ds, rows, monkeypatch, store_calls):
    def fake_store(_storage, **kwargs):
        store_calls.append(kwargs)
        list(kwargs["rows"])
        return {"mode": "diff", "key": "k", "count": len(rows), "increment_count": 1,
                "increment_key": "inc.jsonl", "target_key": "diff.jsonl"}

    monkeypatch.setattr(incremental, "find_diff_target", lambda *_a, **_k: (None, None))
    monkeypatch.setattr(incremental, "incremental_store", fake_store)
    return bronze_tasks._write_bronze(
        st, prefix="", bronze_run_id="2026-08-08_000000_001", dataset=ds,
        raw_pages=[_page(rows)], page_metas=[{"page": 1}],
        base={"observed_date": "2026-08-08", "run_id": "dag", "bronze_run_id": "run"},
        status="ok", rows_total=len(rows), list_total_count=len(rows), complete=True,
        schema_version="v1", base_url="https://example.invalid", started_at="t")


def test_unknown_key_change_quarantines_and_blocks_propagation(monkeypatch):
    st = _Storage()
    ds = _dataset(short="animal_sale")
    schema_guard.record_baseline(st, prefix="", dataset=ds,
                                 keys={"MGTNO", "SITEAREA"}, run_id="r0")
    alerts = []
    monkeypatch.setattr(bronze_tasks, "notify_quality_event",
                        lambda **kw: alerts.append(kw))
    store_calls = []
    rows = [{"MNG_NO": "1", "LCTN_AREA": "84", "TOTALLY_NEW_KEY": "x"}]

    out = _write(st, ds, rows, monkeypatch, store_calls)

    assert out["status"] == schema_guard.STATUS_QUARANTINED
    assert store_calls == [], "격리 시 정렬·diff·증분이 돌면 안 된다"
    qkey = schema_guard.quarantine_key(prefix="", run_id="2026-08-08_000000_001",
                                       short="animal_sale")
    dumped = [json.loads(line) for line in st.read_bytes(qkey).decode().splitlines()]
    assert dumped == rows, "격리 원문은 정본화 전 키 그대로 보존한다"
    marker = next(v for k, v in st.data.items() if k.endswith("animal_sale.incomplete"))
    marker = json.loads(marker)
    assert marker["status"] == schema_guard.STATUS_QUARANTINED
    assert marker["schema_guard"]["verdict"] == schema_guard.CHANGED
    assert "TOTALLY_NEW_KEY" in marker["schema_guard"]["added"]
    assert len(alerts) == 1 and alerts[0]["level"] == "error"
    completed = [k for k in st.data if k.endswith("animal_sale.completed")]
    assert not completed, "격리 run 이 completed 마커를 남기면 브론즈가 적재해 버린다"


def test_matching_keys_store_and_advance_baseline(monkeypatch):
    st = _Storage()
    ds = _dataset(short="animal_sale")
    schema_guard.record_baseline(st, prefix="", dataset=ds,
                                 keys={"MGTNO", "SITEAREA", "RGTMBDSNO"}, run_id="r0")
    store_calls = []
    rows = [{"MNG_NO": "1", "LCTN_AREA": "84", "RGHT_MNBD_SN": "2"}]   # v2 → 별칭 흡수

    out = _write(st, ds, rows, monkeypatch, store_calls)

    assert out["status"] == "ok" and len(store_calls) == 1
    doc = schema_guard.read_baseline(st, prefix="", short="animal_sale")
    assert doc["bronze_run_id"] == "2026-08-08_000000_001", "성공 적재 후 기준선이 전진해야 한다"


def test_first_run_bootstraps_baseline(monkeypatch):
    st = _Storage()
    ds = _dataset()
    store_calls = []
    out = _write(st, ds, [{"MNG_NO": "1", "BZSTAT_SE_NM": "판매"}], monkeypatch, store_calls)

    assert out["status"] == "ok" and len(store_calls) == 1
    doc = schema_guard.read_baseline(st, prefix="", short="mail_order_sale")
    assert set(doc["keys"]) == {"MGTNO", "UPTAENM"}   # 정본화 이후 키가 기준선이다
