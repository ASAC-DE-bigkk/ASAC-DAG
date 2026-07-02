"""feat/59 재수집 규칙 — 동일자 성공분 제외 · KST 일자변경 가드 · 한 파일 정리 단위테스트.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_recollect_rules.py -q
"""
from bronze import markers as M
from common import paths


class _FS:
    """dict 스토리지(list_keys/exists/delete) — 마커/객체 키 시뮬."""
    def __init__(self):
        self.data: dict[str, bytes] = {}

    def write_bytes(self, k, b): self.data[k] = b
    def read_bytes(self, k): return self.data[k]
    def exists(self, k): return k in self.data
    def list_keys(self, prefix): return sorted(k for k in self.data if k.startswith(prefix))
    def delete(self, k): self.data.pop(k, None)


def _completed(fs, rid, short):
    fs.write_bytes(paths.bronze_marker_key(run_id=rid, short=short, status=paths.MARKER_COMPLETED), b"{}")
    fs.write_bytes(paths.bronze_object_key(run_id=rid, short=short), b"data")


def _incomplete(fs, rid, short):
    fs.write_bytes(paths.bronze_marker_key(run_id=rid, short=short, status=paths.MARKER_INCOMPLETE), b"{}")
    fs.write_bytes(paths.bronze_object_key(run_id=rid, short=short), b"partial")


def test_run_date():
    assert M.run_date("2026-07-02_120000_000") == "2026-07-02"


def test_plan_excludes_same_day_completed():
    fs = _FS()
    _completed(fs, "2026-07-02_090000_000", "a")     # 오늘 a 성공
    _incomplete(fs, "2026-07-02_090000_000", "b")    # b 실패
    plan = M.plan_excluding_same_day_completed(fs, "", "2026-07-02", ["a", "b", "c"])
    assert plan == ["b", "c"]                         # 이미 성공한 a 제외(동일자 수동 재실행)


def test_plan_prev_day_completed_not_excluded():
    fs = _FS()
    _completed(fs, "2026-07-01_090000_000", "a")     # 어제 성공은 오늘 대상에서 제외 안 함
    plan = M.plan_excluding_same_day_completed(fs, "", "2026-07-02", ["a", "b"])
    assert plan == ["a", "b"]


def test_recollect_same_day_vs_date_changed():
    fs = _FS()
    _completed(fs, "2026-07-02_090000_000", "a")
    _incomplete(fs, "2026-07-02_090000_000", "b")
    assert M.recollect_targets_same_day(fs, "", ["a", "b"], "2026-07-02") == ["b"]   # 오늘 → 재수집
    assert M.recollect_targets_same_day(fs, "", ["a", "b"], "2026-07-03") == []      # 날짜 변경 → 재수집 안 함


def test_cleanup_incomplete_keeps_success_run():
    fs = _FS()
    _incomplete(fs, "2026-07-02_090000_000", "b")    # 실패 run
    _completed(fs, "2026-07-02_120000_000", "b")     # 재수집 성공 run
    removed = M.cleanup_incomplete(fs, "", "b", keep_run_id="2026-07-02_120000_000")
    assert len(removed) == 2                          # 실패 run 의 마커+파일 삭제
    assert not fs.exists(paths.bronze_marker_key(run_id="2026-07-02_090000_000", short="b",
                                                 status=paths.MARKER_INCOMPLETE))
    assert fs.exists(paths.bronze_object_key(run_id="2026-07-02_120000_000", short="b"))  # 성공본 유지


def test_cleanup_incomplete_does_not_touch_other_days():
    fs = _FS()
    _incomplete(fs, "2026-07-01_090000_000", "b")    # 어제 실패(다른 일자) — 건드리면 안 됨
    _completed(fs, "2026-07-02_120000_000", "b")     # 오늘 성공
    removed = M.cleanup_incomplete(fs, "", "b", keep_run_id="2026-07-02_120000_000")
    assert removed == []                              # 다른 일자는 정리 안 함
    assert fs.exists(paths.bronze_marker_key(run_id="2026-07-01_090000_000", short="b",
                                             status=paths.MARKER_INCOMPLETE))
