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


def _s(short, status, new=0, total=0, error=None, task=None):
    d = {"short": short, "status": status, "new": new, "total": total}
    if error:
        d["error"] = error
    if task:
        d["task"] = task
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
    # 단계 식별 마커(작은 텍스트 + 들여쓰기): 대분류=볼드 · 중분류=• · 소분류=◦
    assert run_report.MARK_MID in d and run_report.MARK_API in d and run_report._INDENT in d


def test_fail_names_task_and_error_before_success_with_gap():
    r = [_s(_shorts("food")[0], "failed", 0, 0, error="ERROR-500 서버 오류", task="ingest_one"),
         _s(_shorts("culture")[0], "ok", 100, 100)]
    d = _build(r)["description"]
    assert "❌ 실패" in d and "@ingest_one" in d and "서버 오류" in d   # 실패 task 명시
    assert "✅ API별" in d
    assert d.index("❌ 실패") < d.index("✅ API별")                    # 순서: 에러 → 성공
    assert "\n\n" in d                                            # 그룹 간 빈 줄 간격


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
               count_label="신규", show_total=False)   # silver 도 신규 지표(PROJECT.md §2, #66 후속)
    assert "(silver 변환)" in s["title"] and "신규 999건" in s["description"] and "전체수집" not in s["description"]


def test_send_no_webhook_is_safe(monkeypatch):
    monkeypatch.delenv("COMMERCE_DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    counts = run_report.send_run_report(dag_id="d", run_id="r", observed_date="d",
                                        stage="collect", results=[_s(_shorts("food")[0], "ok", 1, 1)])
    assert counts["total"] == 1 and counts["new"] == 1


def test_nonzero_never_omitted_and_paginates(monkeypatch):
    # 전 152종 신규(new>0) — 전건 보존(생략 없음), 길면 여러 임베드로 분할 전송.
    results = [_s(d.short, "ok", 100 + i, 1000) for i, d in enumerate(_DS)]
    rep = _build(results)
    assert "생략" not in rep["description"]                       # 신규>0 은 생략 금지
    sent = []
    monkeypatch.setattr(run_report, "send_embed",
                        lambda title, desc, **kw: sent.append((title, desc)) or True)
    run_report.send_run_report(dag_id="d", run_id="r", observed_date="x",
                               stage="collect", results=results)
    assert len(sent) >= 2                                        # 분할 전송(페이지네이션)
    assert all("(1/" in t or "/" in t for t, _ in sent)         # 제목에 (i/N)
    joined = "\n".join(d for _, d in sent)
    for d in _DS:                                               # 전 API 가 어느 페이지엔가 존재
        assert d.short in joined, d.short
    assert all(len(d) <= run_report._MAX_DESC for _, d in sent)  # 각 페이지 한도 이하


def test_zero_count_may_be_omitted():
    # 미수집(⛔, 0건)은 요약(…외) 허용 — 0건은 임의 생략 가능
    culture = [d.short for d in _DS if d.category == "culture"][:25]
    assert len(culture) >= 22
    rep = _build([_s(culture[0], "ok", 5, 10)], scope_shorts=culture)
    assert "⛔ 미수집" in rep["description"] and "…외" in rep["description"]


def test_elapsed_in_head():
    d = _build([_s(_shorts("food")[0], "ok", 1, 1)], elapsed_seconds=125)["description"]
    assert "⏱" in d and "2m 5s" in d


def test_paginate_keeps_code_fence_balanced():
    text = "a\n```\ntbl\n```\n" + "\n".join(f"line{i}" for i in range(300))
    pages = run_report._paginate(text, 200)
    assert len(pages) > 1
    for p in pages:
        assert p.count("```") % 2 == 0                          # 각 페이지 코드펜스 균형


def test_empty_results_zero_summary():
    # 적재 대상 0건(재실행 등) — 세부 없이 '신규 0건 · 0종' 요약 임베드가 성립(0건 리포트 계약).
    rep = _build([], stage="bronze_load")
    d = rep["description"]
    assert rep["counts"]["total"] == 0 and rep["counts"]["new"] == 0
    assert rep["color"] == run_report.COLOR_OK                  # 0건은 정상(초록)
    assert "0종" in rep["title"] and "신규 0건" in d
    assert "❌ 실패" not in d and "⛔ 미수집" not in d           # 세부 섹션 없음(요약만)


def test_send_empty_results_with_extra_section():
    # bronze finalize 0건 경로 — extra_sections(적재 대상 없음)와 함께 전송돼도 counts 반환.
    counts = run_report.send_run_report(
        dag_id="commerce_load_bronze", run_id="r0", observed_date="2026-07-13",
        stage="bronze_load", results=[],
        extra_sections=["**적재 대상 없음(0건)** — 워터마크 이후 신규 증분 run 없음"])
    assert counts["total"] == 0 and counts["new"] == 0
