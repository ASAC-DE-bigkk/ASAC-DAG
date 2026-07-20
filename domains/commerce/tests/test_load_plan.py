"""bronze.load_plan — raw run + 상태 → 적재 계획 해석 단위테스트.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_load_plan.py -q

동작: 워터마크 이후 완료 run 을 시간순 전부 적재. **엔진 = 첫 파일이냐(순서)**:
테이블 비었으면(워터마크 없음) 데이터셋의 **첫 파일 = 전체 재적재(PyIceberg)**, 이후 = 증분(Trino).
identical(파일없음)은 적재 없이 전진.
"""
import json

from bronze import load_plan
from commerce_core import paths


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


def _plan(st, datasets, watermark=None, pending=None, today="2026-07-05", lookback_days=None):
    return load_plan.resolve_load_plan(
        st, prefix="", datasets=datasets, watermark=watermark or {},
        pending=pending or [], today=today, lookback_days=lookback_days)


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


def test_lookback_window_keeps_recent_days_only():
    # #223: 최근 lookback_days 창(today-N ~ today)만 적재 — 오래된 날짜 제외(이른-N-날짜였던 구버전과 반대).
    st = _FakeStorage()
    for d in ("2026-07-01", "2026-07-02", "2026-07-05"):
        _seed_run(st, f"{d}_010000_001", {"gr": {"status": "completed", "file": True, "count": 1}})
    # today=07-05, lookback=2 → cutoff=07-03 → 07-05 만 포함(07-01/02 제외)
    plan = _plan(st, ["gr"], today="2026-07-05", lookback_days=2)
    assert sorted({u["run_id"][:10] for u in plan["units"]}) == ["2026-07-05"]


def test_new_dataset_latest_run_loads_within_window():
    # #223 회귀: 신규 데이터셋(wm 없음)의 데이터가 최신 run 에만 있어도 최근 창에 들어 base 적재된다.
    # (구버전 이른-N-날짜였다면 07-05 가 창 밖으로 밀려 영구 배제됐을 케이스.)
    st = _FakeStorage()
    for d in ("2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04"):
        _seed_run(st, f"{d}_010000_001", {"old": {"status": "completed", "file": True, "count": 1}})
    _seed_run(st, "2026-07-05_010000_001", {"new": {"status": "completed", "file": True, "legacy": True}})
    plan = _plan(st, ["new"], today="2026-07-05", lookback_days=3)
    assert [u["run_id"][:10] for u in plan["units"]] == ["2026-07-05"]
    assert plan["units"][0]["is_base"] is True


def test_commit_watermark_stops_before_failure():
    resolved = {"gr": ["2026-07-01_010000_001", "2026-07-02_010000_001", "2026-07-03_010000_001"]}
    failed = {("gr", "2026-07-02_010000_001")}
    wm = load_plan.commit_watermark({}, resolved_runs=resolved, failed_run_ids=failed)
    assert wm["gr"] == "2026-07-01_010000_001"


def test_first_load_identical_outside_base_window_does_not_advance_watermark():
    # 2026-07-20 실측 스킵버그 회귀: first_load 인데 base 파일 run 이 lookback 창 밖이고
    # 창 안은 identical 뿐이면 — 워터마크를 전진시키면 base 가 영구 skip 된다.
    # 가드 후: resolved_runs 에 못 들어가 wm 미전진 → 다음 무제한 run 이 base 를 재계획.
    st = _FakeStorage()
    _seed_run(st, "2026-07-01_010000_001", {"gr": {"status": "completed", "file": True, "count": 5}})
    for d in ("2026-07-04", "2026-07-05"):
        _seed_run(st, f"{d}_010000_001", {"gr": {"status": "completed", "file": False}})
    # 창=2일 → base(07-01) 제외, identical(07-04/05)만 후보
    plan = _plan(st, ["gr"], today="2026-07-05", lookback_days=2)
    assert plan["units"] == []                       # 적재할 것 없음(base 는 창 밖)
    assert "gr" not in plan["resolved_runs"]        # 워터마크 전진 금지(핵심)
    # 무제한 재계획 → base 정상 배정
    plan2 = _plan(st, ["gr"], today="2026-07-05", lookback_days=None)
    assert [u["run_id"][:10] for u in plan2["units"]] == ["2026-07-01"]
    assert plan2["units"][0]["is_base"] is True
    # base 적재 후(wm 존재)에는 identical 전진이 정상 동작(기존 계약 불변)
    plan3 = _plan(st, ["gr"], watermark={"gr": "2026-07-01_010000_001"},
                  today="2026-07-05", lookback_days=None)
    assert plan3["units"] == []
    assert plan3["resolved_runs"]["gr"] == ["2026-07-04_010000_001", "2026-07-05_010000_001"]
