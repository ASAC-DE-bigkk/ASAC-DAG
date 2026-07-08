"""run_report — DAG 단위 완료 리포트(성공/경고/실패 · category 한글 · API단위) 단위테스트.

    PYTHONPATH=dags/domains/commerce/include:dags pytest dags/domains/commerce/tests/test_run_report.py -q
"""
from commerce_core import run_report


def _s(short, status, rows=0, total=0, error=None):
    d = {"short": short, "status": status, "rows_total": rows, "list_total_count": total}
    if error:
        d["error"] = error
    return d


def _build(results, stage="collect"):
    return run_report.build_run_report(dag_id="commerce_collect_raw", run_id="r1",
                                       observed_date="2026-07-08", stage=stage, results=results)


def test_counts_levels_categories():
    results = [
        _s("general_restaurant", "ok", 100, 100),   # food 성공
        _s("bakery", "ok", 0, 0),                    # food 0건 성공
        _s("clinic", "partial", 50, 100),            # health_medical 경고
        _s("hospital", "failed", 0, error="ERROR-500: 서버 오류입니다"),  # health_medical 에러
    ]
    rep = _build(results)
    c = rep["counts"]
    assert (c["total"], c["ok"], c["warning"], c["error"], c["rows"], c["empty"]) == (4, 2, 1, 1, 150, 1)
    assert rep["color"] == run_report.COLOR_FAIL              # 실패 있으면 빨강
    desc = rep["description"]
    assert "❌ 실패" in desc and "hospital" in desc and "서버 오류" in desc
    assert "⚠️ 경고" in desc and "clinic" in desc and "50/100" in desc.replace(",", "")
    assert "0건" in desc and "bakery" in desc                 # 0건도 표기
    assert "식품" in desc and "의료" in desc                 # category 한글
    assert "식품: ✅2" in desc and "의료: ✅0 ⚠️1 ❌1" in desc
    assert "(수집)" in rep["title"]                          # 스테이지 라벨


def test_all_ok_is_green_no_error_section():
    rep = _build([_s("general_restaurant", "ok", 10, 10), _s("bakery", "ok", 5, 5)])
    assert rep["color"] == run_report.COLOR_OK and rep["counts"]["error"] == 0
    assert "❌ 실패" not in rep["description"]


def test_warn_only_is_yellow():
    assert _build([_s("clinic", "partial", 1, 2)])["color"] == run_report.COLOR_WARN


def test_stage_and_alt_row_keys():
    # bronze 적재 스테이지: rows_loaded / is_publishable → status 로 정규화해 넘긴 형태
    results = [{"short": "bakery", "status": "ok", "rows_loaded": 16620},
               {"short": "clinic", "status": "failed", "error": "commit 충돌"}]
    rep = _build(results, stage="bronze_load")
    assert "(bronze 적재)" in rep["title"] and rep["counts"]["rows"] == 16620
    assert rep["color"] == run_report.COLOR_FAIL


def test_send_no_webhook_is_safe(monkeypatch):
    monkeypatch.delenv("COMMERCE_DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    counts = run_report.send_run_report(dag_id="d", run_id="r", observed_date="d",
                                        stage="collect", results=[_s("bakery", "ok", 1, 1)])
    assert counts["total"] == 1 and counts["ok"] == 1        # webhook 없어도 예외 없이 counts 반환
