"""bronze.load_plan — raw run 목록 + 상태 → 적재 계획 해석 단위테스트.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_load_plan.py -q
"""
import json

from bronze import load_plan
from common import paths


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

    def list_keys(self, prefix):
        return sorted(k for k in self.data if k.startswith(prefix))


def _seed(st, run_id, entries):
    """entries: {short: {"status": "completed"|"incomplete", "file": bool,
                         "mode": str, "count": int}}."""
    for short, e in entries.items():
        status = e["status"]
        st.write_json(paths.bronze_marker_key(run_id=run_id, short=short, status=status),
                      {"increment_mode": e.get("mode", "changed"),
                       "increment_count": e.get("count", 0),
                       "observed_date": run_id[:10], "run_id": f"af_{run_id}",
                       "collected_at": f"{run_id[:10]}T00:00:00+00:00"})
        if e.get("file"):
            st.write_bytes(paths.bronze_object_key(run_id=run_id, short=short),
                           b'{"MGTNO":"1"}\n')


def _plan(st, datasets, watermark=None, pending=None, today="2026-07-05", max_dates=None):
    return load_plan.resolve_load_plan(
        st, prefix="", datasets=datasets, watermark=watermark or {},
        pending=pending or [], today=today, max_dates=max_dates)


def test_fresh_full_then_incremental_engine_choice():
    st = _FakeStorage()
    _seed(st, "2026-07-03_010000_001", {"bakery": {"status": "completed", "file": True,
                                                    "mode": "first", "count": 100}})
    _seed(st, "2026-07-04_010000_001", {"bakery": {"status": "completed", "file": True,
                                                    "mode": "changed", "count": 3}})
    plan = _plan(st, ["bakery"])
    assert plan["no_watermark"] is True
    engines = [(u["run_id"][:10], u["engine"]) for u in plan["units"]]
    assert engines == [("2026-07-03", "pyiceberg"), ("2026-07-04", "trino")]
    assert plan["resolved_runs"]["bakery"] == ["2026-07-03_010000_001", "2026-07-04_010000_001"]


def test_identical_run_resolved_without_unit():
    st = _FakeStorage()
    _seed(st, "2026-07-04_010000_001", {"bakery": {"status": "completed", "file": False}})  # identical
    plan = _plan(st, ["bakery"])
    assert plan["units"] == []                                   # 적재할 파일 없음
    assert plan["resolved_runs"]["bakery"] == ["2026-07-04_010000_001"]   # 그래도 전진 대상


def test_incomplete_becomes_pending_not_resolved():
    st = _FakeStorage()
    _seed(st, "2026-07-04_010000_001", {"bakery": {"status": "incomplete"}})
    plan = _plan(st, ["bakery"], today="2026-07-05")
    assert plan["units"] == []
    assert "bakery" not in plan["resolved_runs"]
    assert {(p["date"], p["short"]) for p in plan["pending_keep"]} == {("2026-07-04", "bakery")}


def test_watermark_skips_already_loaded():
    st = _FakeStorage()
    _seed(st, "2026-07-03_010000_001", {"bakery": {"status": "completed", "file": True, "count": 1}})
    _seed(st, "2026-07-05_010000_001", {"bakery": {"status": "completed", "file": True, "count": 2}})
    plan = _plan(st, ["bakery"], watermark={"bakery": "2026-07-03_010000_001"})
    assert [u["run_id"] for u in plan["units"]] == ["2026-07-05_010000_001"]   # wm 이후만


def test_max_dates_bounds_backfill():
    st = _FakeStorage()
    for d in ("2026-07-01", "2026-07-02", "2026-07-03"):
        _seed(st, f"{d}_010000_001", {"bakery": {"status": "completed", "file": True, "count": 1}})
    plan = _plan(st, ["bakery"], max_dates=2)
    assert sorted({u["run_id"][:10] for u in plan["units"]}) == ["2026-07-01", "2026-07-02"]


def test_commit_watermark_stops_before_failure():
    resolved = {"bakery": ["2026-07-01_010000_001", "2026-07-02_010000_001",
                           "2026-07-03_010000_001"]}
    failed = {("bakery", "2026-07-02_010000_001")}                # 2일차 적재 실패
    wm = load_plan.commit_watermark({}, resolved_runs=resolved, failed_run_ids=failed)
    assert wm["bakery"] == "2026-07-01_010000_001"               # 실패 직전까지만 전진
