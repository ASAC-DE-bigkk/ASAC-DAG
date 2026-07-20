"""gold.report(집계 전용, #70) — 집계 테이블 현황 렌더·실패색 계약.

원형(entity/detail) 리포트는 silver(quality_tasks)로 이동 — 여기는 gold 집계만.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_gold_report.py -q
"""
import common.discord as discord
from gold import report


def _capture(monkeypatch, counts):
    pages = []
    monkeypatch.setattr(discord, "send_embed",
                        lambda title, body, **kw: pages.append((title, body, kw)))
    monkeypatch.setattr(report, "agg_counts", lambda: dict(counts))
    return pages


def test_gold_report_ok_renders_agg_rows(monkeypatch):
    pages = _capture(monkeypatch, {"gold_license_dong_summary": 417})
    out = report.send_gold_report(elapsed_seconds=12.0)
    assert out["status"] == "ok" and out["tables"]["gold_license_dong_summary"] == 417
    title, body, _ = pages[0]
    assert "1/1 OK" in title and "✅" in title
    assert "gold_license_dong_summary" in body and "417" in body and "⏱" in body


def test_gold_report_failure_color_when_table_missing(monkeypatch):
    pages = _capture(monkeypatch, {"gold_license_dong_summary": -1})
    out = report.send_gold_report()
    assert out["status"] == "failed"
    title, body, kw = pages[0]
    assert "0/1 OK" in title and "❌" in title
    assert "미빌드/실패" in body


def test_gold_report_survives_send_failure(monkeypatch):
    monkeypatch.setattr(report, "agg_counts", lambda: {"gold_license_dong_summary": 1})
    monkeypatch.setattr(discord, "send_embed",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("net down")))
    out = report.send_gold_report()                    # 전송 실패가 태스크를 못 죽임
    assert out["status"] == "ok"


def test_send_gold_report_accepts_refresh_context(monkeypatch):
    """전량 재구축 경로가 넘기는 catalog/load 인자를 수용해야 한다 — 2026-07-20 회귀.

    버그: commerce_load_gold_refresh.report_gold 가 send_gold_report(catalog=, load=, ...)
    로 호출하는데 시그니처에 없어 TypeError → refresh DAG 가 끝까지 성공한 적이 없었다.
    """
    from gold import report

    sent = {}
    monkeypatch.setattr(report, "agg_counts", lambda: {"gold_a": 10, "gold_b": 20})
    import common.discord as _d
    monkeypatch.setattr(_d, "send_embed",
                        lambda title, body, **k: sent.update(title=title, body=body))

    out = report.send_gold_report(
        elapsed_seconds=12.5,
        catalog={"version": "v3", "specs": 78},
        load={"loaded": {"silver_x_detail": 100, "silver_y_detail": 50}})

    assert out["status"] == "ok"
    assert "전량 재구축" in sent["body"], "refresh 컨텍스트가 리포트에 반영돼야 한다"
    assert "v3" in sent["body"] and "78" in sent["body"]
    assert "commerce_load_gold_refresh" in sent["title"]
    # 정기 gold 경로(컨텍스트 없음)는 기존 동작 유지
    report.send_gold_report(elapsed_seconds=1.0)
    assert "commerce_load_gold (gold 집계)" in sent["title"]
