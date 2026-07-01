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
