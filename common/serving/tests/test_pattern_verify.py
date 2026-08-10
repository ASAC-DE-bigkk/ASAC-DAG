# common/serving/pattern_verify.py 단위 — export 시점 검증 스탬프 (Serving#217)
from common.serving.pattern_verify import resolve_params, verify_and_stamp


def _row(pid, sql, **kw):
    r = {"pattern_id": pid, "sql": sql, "question_ko": "", "axes": "", "insight_sample_ko": ""}
    r.update(kw)
    return r


def test_resolve_params_from_comment():
    sql = "-- :gu='강남구', :n=10\nSELECT * FROM t WHERE gu = :gu ORDER BY x LIMIT :n"
    sub, resolved, unresolved = resolve_params(sql)
    assert unresolved == []
    assert resolved["gu"] == "'강남구'" and resolved["n"] == "10"
    assert ":gu" not in sub and ":n" not in sub and "'강남구'" in sub


def test_resolve_params_unresolved_when_no_example():
    _, _, unresolved = resolve_params("SELECT * FROM t WHERE g = :gu")
    assert "gu" in unresolved


def test_verify_stamps_only_unverified_and_on_success():
    now = "2026-08-10T00:00:00Z"
    rows = [
        _row("draft", "-- :n=5\nSELECT a FROM t LIMIT :n"),                       # 미검증 → 스탬프
        _row("already", "SELECT 1", verified_at="2026-01-01T00:00:00Z", verified_rows=9),  # 검증됨 → 무접촉
    ]
    calls = []
    rep = verify_and_stamp(rows, run_sql=lambda s: (calls.append(s), [{"a": 1}, {"a": 2}])[1],
                           publication_id="pub1", now_iso=now)
    assert rep["verified"] == ["draft"]
    d = rows[0]
    assert d["verified_at"] == now and d["verified_rows"] == 2 and d["verified_publication_id"] == "pub1"
    # 이미 검증된 건 그대로 · 실행도 안 함
    assert rows[1]["verified_at"] == "2026-01-01T00:00:00Z" and rows[1]["verified_rows"] == 9
    assert all(":n" not in c for c in calls)   # 예시값으로 치환돼 실행됨


def test_verify_skips_unresolved_params():
    rows = [_row("p", "SELECT a FROM t WHERE g = :gu")]   # 예시값 없음
    rep = verify_and_stamp(rows, run_sql=lambda s: [{"a": 1}], publication_id="pub1")
    assert rep["verified"] == [] and rows[0].get("verified_at") is None
    assert rep["skipped"] and "미해결" in rep["skipped"][0][1]


def test_verify_skips_zero_rows_unless_allow_empty():
    rows = [_row("p", "-- :n=5\nSELECT a FROM t LIMIT :n")]
    rep = verify_and_stamp(rows, run_sql=lambda s: [], publication_id="pub1")
    assert rep["verified"] == [] and rows[0].get("verified_at") is None
    # allow_empty=True 면 0행도 검증
    rows2 = [_row("p", "-- :n=5\nSELECT a FROM t LIMIT :n", allow_empty=True)]
    rep2 = verify_and_stamp(rows2, run_sql=lambda s: [], publication_id="pub1")
    assert rep2["verified"] == ["p"] and rows2[0]["verified_rows"] == 0


def test_verify_records_execution_failure_without_stamping():
    def boom(_sql):
        raise RuntimeError("no such column")
    rows = [_row("p", "-- :n=5\nSELECT bad FROM t LIMIT :n")]
    rep = verify_and_stamp(rows, run_sql=boom, publication_id="pub1")
    assert rep["failed"] and rep["failed"][0][0] == "p"
    assert rows[0].get("verified_at") is None      # 실패는 미검증으로 남긴다(안전망)


def test_verify_skips_non_select():
    rows = [_row("p", "-- :n=5\nDELETE FROM t")]
    rep = verify_and_stamp(rows, run_sql=lambda s: [{"a": 1}], publication_id="pub1")
    assert rep["verified"] == [] and rows[0].get("verified_at") is None


def test_verify_needs_publication_id():
    rows = [_row("p", "-- :n=5\nSELECT a FROM t LIMIT :n")]
    rep = verify_and_stamp(rows, run_sql=lambda s: [{"a": 1}], publication_id="")
    assert rep["skipped"] and rows[0].get("verified_at") is None
