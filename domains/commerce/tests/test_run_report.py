"""run_report — DAG 단위 완료 리포트(신규건수 중심 · category 롤업 · API단위 · 정렬표) 단위테스트.

    PYTHONPATH=dags/domains/commerce/include:dags pytest dags/domains/commerce/tests/test_run_report.py -q
"""
from commerce_core import run_report


def _s(short, status, new=0, total=0, error=None):
    d = {"short": short, "status": status, "new": new, "total": total}
    if error:
        d["error"] = error
    return d


def _build(results, stage="collect", **kw):
    return run_report.build_run_report(dag_id="commerce_collect_raw", run_id="r1",
                                       observed_date="2026-07-08", stage=stage, results=results, **kw)


def test_counts_new_and_categories():
    results = [
        _s("general_restaurant", "ok", 100, 45000),   # food 신규 100
        _s("bakery", "ok", 0, 30000),                 # food 신규 0(정상)
        _s("clinic", "partial", 0, 100),              # health_medical 경고
        _s("hospital", "failed", 0, 0, error="ERROR-500: 서버 오류입니다\ntoken=LEAK"),
    ]
    rep = _build(results)
    c = rep["counts"]
    assert (c["total"], c["ok"], c["warning"], c["error"], c["missing"], c["new"]) == (4, 2, 1, 1, 0, 100)
    assert rep["color"] == run_report.COLOR_FAIL
    d = rep["description"]
    assert "신규 100건" in d and "전체수집" in d           # 신규 우선 + 전체 병기
    assert "```" in d and "대분류별" in d and "합계" in d   # 정렬 코드블록 + 완전집계 합계행
    assert "식품" in d and "의료" in d
    assert "❌ 실패" in d and "hospital" in d and "서버 오류" in d and "token=LEAK" not in d  # 시크릿/2번째줄 미노출
    assert "⚠️ 경고" in d and "clinic" in d
    assert "API별" in d and "general_restaurant" in d      # API 단위 신규 지표


def test_scope_reconcile_missing():
    # scope 3종 중 결과 1종 → 나머지 2종은 미수집(missing) 으로 집계
    rep = _build([_s("general_restaurant", "ok", 7, 100)],
                 scope_shorts=["general_restaurant", "bakery", "clinic"])
    c = rep["counts"]
    assert (c["total"], c["ok"], c["missing"]) == (3, 1, 2)
    assert rep["color"] == run_report.COLOR_WARN            # 미수집 있으면 최소 경고색
    assert "⛔ 미수집" in rep["description"] and "bakery" in rep["description"]


def test_alignment_uses_display_width_padding():
    # 이름 길이가 달라도 코드블록 안에서 열이 밀리지 않도록 폭 계산 패딩
    assert run_report._dw("위생·미용") == run_report._dw("aaaaaaaaa")   # CJK 2폭 → 9
    assert run_report._pad("식품", 8).startswith("식품") and run_report._dw(run_report._pad("식품", 8)) == 8


def test_all_ok_green_and_zero_new_hidden_per_api():
    rep = _build([_s("general_restaurant", "ok", 0, 10), _s("bakery", "ok", 0, 5)])
    assert rep["color"] == run_report.COLOR_OK and rep["counts"]["new"] == 0
    assert "❌ 실패" not in rep["description"]
    # 신규 0(정상)은 API별 표에 개별 표기하지 않음(롤업엔 포함) → API별 섹션 자체가 생략될 수 있음
    assert "합계" in rep["description"]


def test_bronze_and_silver_labels():
    b = _build([_s("bakery", "ok", 16620, 16620)], stage="bronze_load")
    assert "(bronze 적재)" in b["title"] and "신규 16,620건" in b["description"]
    s = _build([_s("bakery", "ok", 999, 999)], stage="silver", count_label="현재", show_total=False)
    assert "(silver 변환)" in s["title"] and "현재 999건" in s["description"] and "전체수집" not in s["description"]


def test_send_no_webhook_is_safe(monkeypatch):
    monkeypatch.delenv("COMMERCE_DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    counts = run_report.send_run_report(dag_id="d", run_id="r", observed_date="d",
                                        stage="collect", results=[_s("bakery", "ok", 1, 1)])
    assert counts["total"] == 1 and counts["new"] == 1
