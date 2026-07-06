"""bronze.load_plan — raw run + 상태 → 적재 계획 해석 단위테스트.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_load_plan.py -q

동작: 워터마크 이후 완료 run 을 시간순 전부 적재. **엔진 = 첫 파일이냐(순서)**:
테이블 비었으면(워터마크 없음) 데이터셋의 **첫 파일 = 전체 재적재(PyIceberg)**, 이후 = 증분(Trino).
identical(파일없음)은 적재 없이 전진.
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
    """entries: {short: {"status","file","mode","count","legacy","rows_total"}}.
    file=True → 증분 파일 존재. legacy=True → 마커에 increment_mode 없음."""
    for short, e in entries.items():
        marker = {"observed_date": run_id[:10], "run_id": f"af_{run_id}",
                  "collected_at": f"{run_id[:10]}T00:00:00+00:00",
                  "source_name": e.get("service", "LOCALDATA_072404"),
                  "rows_total": e.get("rows_total", 999999)}
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


def test_first_file_pyiceberg_rest_trino():
    st = _FakeStorage()
    # 워터마크 없음: 첫 파일(06-30)=전체 재적재 PyIceberg, 이후(07-01)=증분 Trino
    _seed_run(st, "2026-06-30_010000_001", {"gr": {"status": "completed", "file": True,
                                                    "legacy": True}})
    _seed_run(st, "2026-07-01_010000_001", {"gr": {"status": "completed", "file": True,
                                                    "legacy": True}})
    plan = _plan(st, ["gr"])
    got = {u["run_id"][:10]: (u["engine"], u["is_base"]) for u in plan["units"]}
    assert got == {"2026-06-30": ("pyiceberg", True), "2026-07-01": ("trino", False)}


def test_base_ignores_rows_total_for_count():
    st = _FakeStorage()
    _seed_run(st, "2026-06-30_010000_001", {"gr": {"status": "completed", "file": True,
                                                    "legacy": True, "rows_total": 534748}})
    (u,) = _plan(st, ["gr"])["units"]
    assert u["is_base"] and u["increment_count"] is None      # 발행판정은 rows>0


def test_with_watermark_all_increments_trino():
    st = _FakeStorage()
    _seed_run(st, "2026-07-05_010000_001", {"gr": {"status": "completed", "file": True,
                                                   "mode": "changed", "count": 7}})
    plan = _plan(st, ["gr"], watermark={"gr": "2026-07-04_000000_000"})
    (u,) = plan["units"]
    assert u["engine"] == "trino" and u["is_base"] is False and u["increment_count"] == 7


def test_watermark_skips_already_loaded():
    st = _FakeStorage()
    _seed_run(st, "2026-07-03_010000_001", {"gr": {"status": "completed", "file": True, "count": 1}})
    _seed_run(st, "2026-07-05_010000_001", {"gr": {"status": "completed", "file": True, "count": 2}})
    plan = _plan(st, ["gr"], watermark={"gr": "2026-07-03_010000_001"})
    assert [u["run_id"] for u in plan["units"]] == ["2026-07-05_010000_001"]


def test_identical_first_file_skipped_base_goes_to_next_file():
    st = _FakeStorage()
    # 첫 run 이 identical(파일없음)이면 base 는 다음 '파일 있는' run 에 배정
    _seed_run(st, "2026-06-30_010000_001", {"gr": {"status": "completed", "file": False,
                                                   "mode": "identical"}})
    _seed_run(st, "2026-07-01_010000_001", {"gr": {"status": "completed", "file": True,
                                                   "legacy": True}})
    plan = _plan(st, ["gr"])
    assert [(u["run_id"][:10], u["engine"], u["is_base"]) for u in plan["units"]] \
        == [("2026-07-01", "pyiceberg", True)]
    assert plan["resolved_runs"]["gr"] == ["2026-06-30_010000_001", "2026-07-01_010000_001"]


def test_incomplete_becomes_pending():
    st = _FakeStorage()
    _seed_run(st, "2026-07-05_010000_001", {"gr": {"status": "incomplete"}})
    plan = _plan(st, ["gr"], today="2026-07-05")
    assert plan["units"] == [] and "gr" not in plan["resolved_runs"]
    assert {(p["date"], p["short"]) for p in plan["pending_keep"]} == {("2026-07-05", "gr")}


def test_max_dates_bounds_backfill():
    st = _FakeStorage()
    for d in ("2026-07-01", "2026-07-02", "2026-07-03"):
        _seed_run(st, f"{d}_010000_001", {"gr": {"status": "completed", "file": True, "count": 1}})
    plan = _plan(st, ["gr"], max_dates=2)
    assert sorted({u["run_id"][:10] for u in plan["units"]}) == ["2026-07-01", "2026-07-02"]


def test_commit_watermark_stops_before_failure():
    resolved = {"gr": ["2026-07-01_010000_001", "2026-07-02_010000_001", "2026-07-03_010000_001"]}
    failed = {("gr", "2026-07-02_010000_001")}
    wm = load_plan.commit_watermark({}, resolved_runs=resolved, failed_run_ids=failed)
    assert wm["gr"] == "2026-07-01_010000_001"
