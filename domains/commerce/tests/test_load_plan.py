"""bronze.load_plan — raw run + 상태 → 적재 계획 해석 단위테스트.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_load_plan.py -q

동작: 워터마크 이후 완료 run 을 시간순 전부 적재. legacy(page-NDJSON)도 유닛(engine=pyiceberg).
identical(파일없음)은 적재 없이 전진. changed→Trino, 그 외(legacy/first)→PyIceberg.
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


def _seed_run(st, run_id, entries):
    """entries: {short: {"status","file","mode","count","legacy","rows_total","service"}}."""
    for short, e in entries.items():
        marker = {"observed_date": run_id[:10], "run_id": f"af_{run_id}",
                  "collected_at": f"{run_id[:10]}T00:00:00+00:00",
                  "source_name": e.get("service", "LOCALDATA_072404"),
                  "rows_total": e.get("rows_total", 0)}
        if not e.get("legacy"):
            marker["increment_mode"] = e.get("mode", "changed")
            marker["increment_count"] = e.get("count", 0)
        st.write_json(paths.bronze_marker_key(run_id=run_id, short=short,
                                              status=e["status"]), marker)
        if e.get("file"):
            st.write_bytes(paths.bronze_object_key(run_id=run_id, short=short), b'{"MGTNO":"1"}\n')


def _plan(st, datasets, watermark=None, pending=None, today="2026-07-05", max_dates=None):
    return load_plan.resolve_load_plan(
        st, prefix="", datasets=datasets, watermark=watermark or {},
        pending=pending or [], today=today, max_dates=max_dates)


def test_fresh_loads_all_runs_legacy_and_changed():
    st = _FakeStorage()
    _seed_run(st, "2026-06-30_010000_001", {"bakery": {"status": "completed", "file": True,
                                                       "legacy": True, "rows_total": 500}})
    _seed_run(st, "2026-07-04_010000_001", {"bakery": {"status": "completed", "file": True,
                                                       "mode": "changed", "count": 7}})
    _seed_run(st, "2026-07-05_000001_709", {"bakery": {"status": "completed", "file": False,
                                                       "mode": "identical"}})
    plan = _plan(st, ["bakery"])
    assert plan["no_watermark"] is True
    u = {x["run_id"][:10]: x for x in plan["units"]}
    assert u["2026-06-30"]["engine"] == "pyiceberg" and u["2026-06-30"]["legacy"] is True
    assert u["2026-06-30"]["increment_count"] == 500          # legacy: rows_total
    assert u["2026-06-30"]["service_name"] == "LOCALDATA_072404"
    assert u["2026-07-04"]["engine"] == "trino" and u["2026-07-04"]["increment_count"] == 7
    assert "2026-07-05" not in u                              # identical → 유닛 없음
    assert plan["resolved_runs"]["bakery"][-1] == "2026-07-05_000001_709"   # identical 도 전진


def test_watermark_skips_already_loaded():
    st = _FakeStorage()
    _seed_run(st, "2026-07-03_010000_001", {"bakery": {"status": "completed", "file": True,
                                                       "mode": "changed", "count": 1}})
    _seed_run(st, "2026-07-05_010000_001", {"bakery": {"status": "completed", "file": True,
                                                       "mode": "changed", "count": 2}})
    plan = _plan(st, ["bakery"], watermark={"bakery": "2026-07-03_010000_001"})
    assert [u["run_id"] for u in plan["units"]] == ["2026-07-05_010000_001"]


def test_identical_resolved_without_unit():
    st = _FakeStorage()
    _seed_run(st, "2026-07-05_010000_001", {"bakery": {"status": "completed", "file": False,
                                                       "mode": "identical"}})
    plan = _plan(st, ["bakery"])
    assert plan["units"] == []
    assert plan["resolved_runs"]["bakery"] == ["2026-07-05_010000_001"]


def test_incomplete_becomes_pending():
    st = _FakeStorage()
    _seed_run(st, "2026-07-05_010000_001", {"bakery": {"status": "incomplete"}})
    plan = _plan(st, ["bakery"], today="2026-07-05")
    assert plan["units"] == [] and "bakery" not in plan["resolved_runs"]
    assert {(p["date"], p["short"]) for p in plan["pending_keep"]} == {("2026-07-05", "bakery")}


def test_max_dates_bounds_backfill():
    st = _FakeStorage()
    for d in ("2026-07-01", "2026-07-02", "2026-07-03"):
        _seed_run(st, f"{d}_010000_001", {"bakery": {"status": "completed", "file": True,
                                                     "mode": "changed", "count": 1}})
    plan = _plan(st, ["bakery"], max_dates=2)
    assert sorted({u["run_id"][:10] for u in plan["units"]}) == ["2026-07-01", "2026-07-02"]


def test_commit_watermark_stops_before_failure():
    resolved = {"bakery": ["2026-07-01_010000_001", "2026-07-02_010000_001",
                           "2026-07-03_010000_001"]}
    failed = {("bakery", "2026-07-02_010000_001")}
    wm = load_plan.commit_watermark({}, resolved_runs=resolved, failed_run_ids=failed)
    assert wm["bakery"] == "2026-07-01_010000_001"
