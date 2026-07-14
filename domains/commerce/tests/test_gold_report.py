"""gold.report — 신규 버전 적재 vs 차원 스냅샷 분리 표기(#4 정확성 재검증).

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_gold_report.py -q
"""
import common.discord as discord
from gold import report


def _capture(monkeypatch):
    pages = []
    monkeypatch.setattr(discord, "send_embed",
                        lambda title, body, **kw: pages.append((title, body)))
    return pages


def test_gold_report_separates_new_versions_from_dim_snapshot(monkeypatch):
    pages = _capture(monkeypatch)
    catalog = {"version": "abcdef123456", "clusters": 3, "singles": 5, "datasets": 152, "drift": False}
    load = {"hi": "2026-07-14 05:00:00", "loaded": {
        "commerce_business_entity_history": 5,
        "commerce_business_entity": 4,
        "commerce_pharmacy_detail": 3,
        "commerce_dim_region": 15000,          # 전량 스냅샷 — 신규 아님
        "commerce_dim_dataset": 152,           # 전량 스냅샷
        "commerce_dim_business_status": 40,    # 전량 스냅샷
    }}

    counts = report.send_gold_report(catalog=catalog, load=load, elapsed_seconds=12.0)

    # 신규 버전(entity/history/detail) = 5+4+3 = 12, 차원 스냅샷 = 15000+152+40 = 15192
    assert counts["new_version_rows"] == 12
    assert counts["dim_snapshot_rows"] == 15192
    assert counts["loaded_rows"] == 12 + 15192

    title, body = pages[0]
    assert "신규 버전 12행" in title            # 헤드라인은 '신규'만 = 15,000행 오인 방지
    assert "차원 스냅샷" in body                 # dim 은 별도 섹션(전량 갱신 명시)
    assert "전량 갱신" in body
    # dim 이 '공통(신규 버전)' 섹션에 섞이지 않는다
    common_section = body.split("차원 스냅샷")[0]
    assert "commerce_dim_region" not in common_section


def test_gold_report_skipped_no_new_silver(monkeypatch):
    pages = _capture(monkeypatch)
    catalog = {"version": "v1", "clusters": 0, "singles": 0, "datasets": 152, "drift": False}
    load = {"loaded": {}, "hi": "2026-07-14 05:00:00", "skipped": "no_new_silver"}

    counts = report.send_gold_report(catalog=catalog, load=load)

    assert counts["new_version_rows"] == 0 and counts["dim_snapshot_rows"] == 0
    body = pages[0][1]
    assert "신규 버전 적재" in body and "0행" in body
    assert "조기 스킵" in body or "신규 없음" in body
