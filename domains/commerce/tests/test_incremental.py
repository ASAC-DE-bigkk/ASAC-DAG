"""bronze.incremental — 외부 병합 정렬 · 검증키 · 스트리밍 diff 단위테스트.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_incremental.py -q
"""
from bronze import incremental as inc


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
