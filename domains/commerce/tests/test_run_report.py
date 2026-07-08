"""run_report — 대분류(보건/문화/산업/환경)>중분류>소분류(API) 리포트 단위테스트.

    PYTHONPATH=dags/domains/commerce/include:dags pytest dags/domains/commerce/tests/test_run_report.py -q
"""
import re

from commerce_core import registry, run_report

_DS = registry.all_datasets()


def _shorts(cat, k=1):
    xs = [d.short for d in _DS if d.category == cat]
    assert len(xs) >= k, f"{cat} 데이터셋 부족"
    return xs[:k]


def _s(short, status, new=0, total=0, error=None):
    d = {"short": short, "status": status, "new": new, "total": total}
    if error:
        d["error"] = error
    return d


def _build(results, **kw):
    kw.setdefault("stage", "collect")
    return run_report.build_run_report(dag_id="commerce_collect_raw", run_id="r1",
                                       observed_date="2026-07-08", results=results, **kw)


def test_major_is_health_or_own():
    # food/livestock/... → 보건, culture/industry/environment → 자기 자신
    assert run_report._major("food") == "health" and run_report._major("lodging") == "health"
    for m in ("culture", "industry", "environment"):
        assert run_report._major(m) == m


def test_rollup_and_hierarchy():
    r = [_s(_shorts("food")[0], "ok", 100, 1000),          # 보건 > 식품
         _s(_shorts("culture")[0], "ok", 50, 500),         # 문화 > sub_category
         _s(_shorts("industry")[0], "ok", 30, 300),        # 산업
         _s(_shorts("environment")[0], "ok", 10, 100)]     # 환경
    rep = _build(r)
    d = rep["description"]
    assert rep["counts"]["new"] == 190
    assert set(rep["counts"]["majors"]) <= {"health", "culture", "industry", "environment", "etc"}
    # 대분류 통계 표: 4 대분류 + 합계
    assert "보건(health)" in d and "문화(culture)" in d and "산업(industry)" in d and "환경(environment)" in d
    assert "합계(total)" in d
    # 중분류: 보건 하위는 category(식품), API 상세 존재
    assert "식품(food)" in d and "API별 신규" in d


def test_alignment_ascii_grid():
    d = _build([_s(_shorts("food")[0], "ok", 1500, 1), _s(_shorts("environment")[0], "ok", 12, 1)],
               scope_shorts=_shorts("food") + _shorts("environment") + _shorts("culture"))["description"]
    block = re.search(r"```\n(.*?)\n```", d, re.S).group(1)
    rows = [x for x in block.splitlines() if x.strip()]
    pl = {len(re.match(r"^[\x00-\x7f]*", x).group(0)) for x in rows}
    assert len(pl) == 1, f"격자 정렬 깨짐: {pl}"
    assert rows[0].lstrip().startswith("OK")


def test_zero_no_change_summary():
    two = _shorts("food", 2)
    rep = _build([_s(two[0], "ok", 5, 100), _s(two[1], "ok", 0, 100)])   # 하나는 신규 0
    assert "변경내역 없음" in rep["description"] and "보건 하위 1개" in rep["description"]


def test_missing_and_color():
    rep = _build([_s(_shorts("food")[0], "ok", 7, 100)],
                 scope_shorts=_shorts("food", 1) + _shorts("culture", 2))
    assert rep["counts"]["missing"] == 2 and rep["color"] == run_report.COLOR_WARN
    assert "⛔ 미수집" in rep["description"]


def test_stage_labels():
    b = _build([_s(_shorts("food")[0], "ok", 16620, 16620)], stage="bronze_load")
    assert "(bronze 적재)" in b["title"] and "신규 16,620건" in b["description"]
    s = _build([_s(_shorts("food")[0], "ok", 999, 999)], stage="silver",
               count_label="현재", show_total=False)
    assert "(silver 변환)" in s["title"] and "현재 999건" in s["description"] and "전체수집" not in s["description"]


def test_send_no_webhook_is_safe(monkeypatch):
    monkeypatch.delenv("COMMERCE_DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    counts = run_report.send_run_report(dag_id="d", run_id="r", observed_date="d",
                                        stage="collect", results=[_s(_shorts("food")[0], "ok", 1, 1)])
    assert counts["total"] == 1 and counts["new"] == 1
