"""bronze.incremental — 외부 병합 정렬 · 검증키 · 스트리밍 diff 단위테스트.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_incremental.py -q
"""
import os

from bronze import incremental as inc


class _FakeStorage:
    """메모리 dict 스토리지 — orchestration 오프라인 테스트용(발견/이동 포함)."""
    def __init__(self):
        self.data: dict[str, bytes] = {}

    def exists(self, k):
        return k in self.data

    def read_bytes(self, k):
        return self.data[k]

    def write_bytes(self, k, b):
        self.data[k] = b

    def list_keys(self, prefix):
        return sorted(k for k in self.data if k.startswith(prefix))

    def delete(self, k):
        self.data.pop(k, None)

    def copy(self, src, dst):
        self.data[dst] = self.data[src]


def _row(mgtno, updatedt, name="x"):
    return {"MGTNO": mgtno, "UPDATEDT": updatedt, "BPLCNM": name}


def test_updatedt_num():
    assert inc.updatedt_num({"UPDATEDT": "2026-05-11 22:33:12"}) == 20260511223312
    assert inc.updatedt_num({"UPDATEDT": ""}) == 0
    assert inc.updatedt_num({}) == 0


def test_sort_key_desc_by_updatedt():
    a = _row("1", "2026-01-01 00:00:00")
    b = _row("2", "2026-06-01 00:00:00")
    assert inc.sort_key(b) < inc.sort_key(a)   # b 가 더 최신 → desc 에서 앞


def test_external_merge_sort_multichunk(tmp_path):
    rows = [_row(str(i), f"2026-01-{(i % 28) + 1:02d} 00:00:00") for i in range(10)]
    # chunk_rows=2 로 여러 청크 강제 → 병합 정확성 검증
    out = list(inc.external_merge_sort(iter(rows), tmp_dir=str(tmp_path), chunk_rows=2))
    assert len(out) == len(rows)
    keys = [inc.sort_key(r) for r in out]
    assert keys == sorted(keys)                # 전역 정렬(UPDATEDT desc)
    # 임시 sortrun 파일 정리 확인
    assert not list(tmp_path.glob("*.sortrun.jsonl"))


def test_verification_key_order_sensitive():
    r1, r2 = _row("1", "2026-02-02 00:00:00"), _row("2", "2026-01-01 00:00:00")
    k_ab, n = inc.verification_key([r1, r2])
    k_ba, _ = inc.verification_key([r2, r1])
    assert n == 2 and k_ab != k_ba             # 순서 민감
    assert inc.verification_key([r1, r2])[0] == k_ab   # 결정적


def test_diff_identical_is_empty():
    prev = [_row("1", "2026-02-01 00:00:00"), _row("2", "2026-01-01 00:00:00")]
    today = [dict(r) for r in prev]
    assert list(inc.diff_new_rows(today, prev)) == []


def test_diff_new_head_only():
    prev = [_row("2", "2026-01-02 00:00:00"), _row("1", "2026-01-01 00:00:00")]
    new = _row("9", "2026-06-01 00:00:00")     # 최신 → 정렬 시 최상단
    today = sorted([new] + [dict(r) for r in prev], key=inc.sort_key)
    out = list(inc.diff_new_rows(today, prev))
    assert out == [new]                        # 신규 1건만


def test_diff_changed_row():
    prev = [_row("1", "2026-01-01 00:00:00", "old")]
    # 같은 업장(mgtno=1)이 갱신 → UPDATEDT 최신 + 내용 변경
    changed = _row("1", "2026-06-01 00:00:00", "new")
    today = sorted([changed], key=inc.sort_key)
    out = list(inc.diff_new_rows(today, prev))
    assert out == [changed]                    # 변경분 방출(전날 old 는 키가 달라 tail 로 남음)


def test_diff_prev_only_rows_skipped():
    # 전날에만 있던 행(오늘 없음) 은 신규 판정에 영향 없음
    prev = [_row("2", "2026-05-01 00:00:00"), _row("1", "2026-01-01 00:00:00")]
    today = [_row("1", "2026-01-01 00:00:00")]
    assert list(inc.diff_new_rows(today, prev)) == []


def test_diff_early_exit_on_aligned_match():
    """같은 정보가 정렬 위치에서 일치하는 순간 비교 중단(이하 소비 안 함)."""
    prev = [_row("2", "2026-01-02 00:00:00"), _row("1", "2026-01-01 00:00:00")]
    new = _row("9", "2026-06-01 00:00:00")
    today = sorted([new] + [dict(r) for r in prev], key=inc.sort_key)
    consumed = []

    def prev_gen():
        for r in prev:
            consumed.append(r["MGTNO"])
            yield r

    out = list(inc.diff_new_rows(today, prev_gen(), stop_on_aligned_match=True))
    assert out == [new]                # 신규분 동일
    assert consumed == ["2"]           # 첫 일치(MGTNO=2)에서 중단 — 나머지(1) 미소비


def test_find_diff_target_picks_latest_dated():
    st = _FakeStorage()
    st.data["_diff_target/x.jsonl"] = b"legacy"                    # 구형(무날짜) — 최저 순위
    st.data["_diff_target/x.2026-07-01.jsonl"] = b"a"
    st.data["_diff_target/x.2026-07-01.key"] = b"k"
    st.data["_diff_target/x.2026-07-02.jsonl"] = b"b"              # 최신(keyfile 없음)
    t, k = inc.find_diff_target(st, dir_prefix="_diff_target/x.")
    assert t == "_diff_target/x.2026-07-02.jsonl" and k is None
    st.delete("_diff_target/x.2026-07-02.jsonl")
    t, k = inc.find_diff_target(st, dir_prefix="_diff_target/x.")
    assert t == "_diff_target/x.2026-07-01.jsonl" and k == "_diff_target/x.2026-07-01.key"


# ── 파일 브리지(save 증분 + diff-target 롤링 모델) ──────────────────────────────
def test_sort_rows_to_file_and_read(tmp_path):
    rows = [_row("1", "2026-01-01 00:00:00"), _row("2", "2026-06-01 00:00:00")]
    p = str(tmp_path / "s.jsonl")
    key, n = inc.sort_rows_to_file(iter(rows), dest_path=p, tmp_dir=str(tmp_path))
    out = list(inc.read_rows(p))
    assert n == 2 and out[0]["MGTNO"] == "2"          # 최신(2026-06) 먼저(desc)
    assert key == inc.verification_key(out)[0]         # 파일 키 == 정렬본 검증키


def test_build_increment_first(tmp_path):
    rows = [_row("1", "2026-01-01 00:00:00"), _row("2", "2026-02-01 00:00:00")]
    sp, ip = str(tmp_path / "today.jsonl"), str(tmp_path / "inc.jsonl")
    res = inc.build_increment(rows, tmp_dir=str(tmp_path), today_sorted_path=sp, increment_path=ip)
    assert res["mode"] == "first" and res["count"] == 2 and res["increment_count"] == 2
    assert len(list(inc.read_rows(ip))) == 2 and len(list(inc.read_rows(sp))) == 2


def test_build_increment_identical(tmp_path):
    rows = [_row("1", "2026-01-01 00:00:00"), _row("2", "2026-02-01 00:00:00")]
    prev = str(tmp_path / "prev.jsonl")
    pkey, _ = inc.sort_rows_to_file(rows, dest_path=prev, tmp_dir=str(tmp_path))
    sp, ip = str(tmp_path / "today.jsonl"), str(tmp_path / "inc.jsonl")
    res = inc.build_increment([dict(r) for r in rows], tmp_dir=str(tmp_path), today_sorted_path=sp,
                              increment_path=ip, prev_target_path=prev, prev_key=pkey)
    assert res["mode"] == "identical" and res["increment_count"] == 0


def test_build_increment_changed(tmp_path):
    prev = str(tmp_path / "prev.jsonl")
    pkey, _ = inc.sort_rows_to_file([_row("1", "2026-01-01 00:00:00", "old")],
                                    dest_path=prev, tmp_dir=str(tmp_path))
    today = [_row("1", "2026-01-01 00:00:00", "old"), _row("9", "2026-06-01 00:00:00", "new")]
    sp, ip = str(tmp_path / "today.jsonl"), str(tmp_path / "inc.jsonl")
    res = inc.build_increment(today, tmp_dir=str(tmp_path), today_sorted_path=sp,
                              increment_path=ip, prev_target_path=prev, prev_key=pkey)
    assert res["mode"] == "changed" and res["increment_count"] == 1
    assert list(inc.read_rows(ip))[0]["MGTNO"] == "9"   # 신규분만


# ── orchestration(랜딩 → 비교 → 증분 → diff 이동): first → identical → changed ──
def _store(st, tmp_path, run, date, rows):
    d = tmp_path / run
    d.mkdir()
    prev_t, prev_k = inc.find_diff_target(st, dir_prefix="_diff_target/tour.")
    return inc.incremental_store(
        st, rows=rows, tmp_dir=str(d),
        landing_key=f"{run}/_full/tour.jsonl", increment_key=f"{run}/tour.jsonl",
        target_key=f"_diff_target/tour.{date}.jsonl",
        target_key_file=f"_diff_target/tour.{date}.key",
        prev_target_key=prev_t, prev_target_keyfile=prev_k)


def test_incremental_store_lifecycle(tmp_path):
    st = _FakeStorage()
    day1 = [_row("1", "2026-01-01 00:00:00"), _row("2", "2026-02-01 00:00:00")]

    r1 = _store(st, tmp_path, "run1", "2026-07-01", [dict(r) for r in day1])   # 첫 수집
    assert r1["mode"] == "first" and r1["increment_key"] == "run1/tour.jsonl"
    assert st.exists("run1/tour.jsonl")                          # save = full(첫 수집)
    assert st.exists("_diff_target/tour.2026-07-01.jsonl")       # diff = 수집일 태깅 정렬 full
    assert st.exists("_diff_target/tour.2026-07-01.key")
    assert not st.exists("run1/_full/tour.jsonl")                # landing 은 이동 후 삭제(완료)

    r2 = _store(st, tmp_path, "run2", "2026-07-02", [dict(r) for r in day1])   # 동일
    assert r2["mode"] == "identical" and r2["increment_key"] is None
    assert not st.exists("run2/tour.jsonl")                      # 증분 없음
    assert st.exists("_diff_target/tour.2026-07-02.jsonl")       # 날짜는 오늘로 롤링(완료 표시)
    assert not st.exists("_diff_target/tour.2026-07-01.jsonl")   # 구 날짜 diff 삭제
    assert not st.exists("run2/_full/tour.jsonl")

    day3 = [dict(r) for r in day1] + [_row("9", "2026-06-01 00:00:00")]        # 변경
    r3 = _store(st, tmp_path, "run3", "2026-07-03", day3)
    assert r3["mode"] == "changed" and r3["increment_count"] == 1
    assert st.exists("run3/tour.jsonl")                          # 신규분만 저장
    body = st.read_bytes("_diff_target/tour.2026-07-03.jsonl").decode()
    assert len([l for l in body.splitlines() if l.strip()]) == 3  # diff = 오늘 full(3행)로 교체
    assert not st.exists("_diff_target/tour.2026-07-02.jsonl")


def test_incremental_store_same_date_rerun_keeps_target(tmp_path):
    """같은 수집일 재실행(동일자 재수집): prev == target 이어도 diff 파일이 지워지지 않아야."""
    st = _FakeStorage()
    rows = [_row("1", "2026-01-01 00:00:00")]
    _store(st, tmp_path, "run1", "2026-07-01", [dict(r) for r in rows])
    r2 = _store(st, tmp_path, "run2", "2026-07-01", [dict(r) for r in rows])   # 같은 날짜 재실행
    assert r2["mode"] == "identical"
    assert st.exists("_diff_target/tour.2026-07-01.jsonl")       # 자기 자신 삭제 금지 가드
    assert st.exists("_diff_target/tour.2026-07-01.key")


def test_step0_seed_then_identical(tmp_path):
    """step0 시드 후, 같은 내용의 첫 수집은 identical(증분 없음) 이어야."""
    st = _FakeStorage()
    tk, tkf = "_diff_target/x.2026-07-01.jsonl", "_diff_target/x.2026-07-01.key"
    rows = [_row("1", "2026-01-01 00:00:00"), _row("2", "2026-03-01 00:00:00")]
    d0 = tmp_path / "seed"; d0.mkdir()
    seed = inc.seed_diff_target(st, target_key=tk, target_key_file=tkf,
                                rows=[dict(r) for r in rows], tmp_dir=str(d0))
    assert st.exists(tk) and st.exists(tkf)
    prev_t, prev_k = inc.find_diff_target(st, dir_prefix="_diff_target/x.")
    assert prev_t == tk and prev_k == tkf
    d1 = tmp_path / "run1"; d1.mkdir()
    res = inc.incremental_store(
        st, rows=[dict(r) for r in rows], tmp_dir=str(d1),
        landing_key="run1/_full/x.jsonl", increment_key="run1/x.jsonl",
        target_key="_diff_target/x.2026-07-02.jsonl",
        target_key_file="_diff_target/x.2026-07-02.key",
        prev_target_key=prev_t, prev_target_keyfile=prev_k)
    assert res["mode"] == "identical" and res["key"] == seed["key"]   # 시드와 동일 → 증분 없음
    assert not st.exists(tk)                                          # 시드본은 오늘 날짜로 롤링됨
