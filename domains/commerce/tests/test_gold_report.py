"""gold.report(Iceberg) — 코어(dbt) 현황 + detail 신규 분리 표기·실패 경로.

서빙 레이어 개편(PROJECT.md §4): gold = Iceberg(코어 dbt + 카탈로그 구동 detail).
구 Postgres 계약(dim 스냅샷 분리·조기 스킵 표기)은 경로와 함께 폐기 — 새 계약을 고정한다.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_gold_report.py -q
"""
import common.discord as discord
from gold import report


def _capture(monkeypatch):
    pages = []
    monkeypatch.setattr(discord, "send_embed",
                        lambda title, body, **kw: pages.append((title, body)))
    # 코어 현황은 Trino 조회 — 테스트에서는 고정값으로 격리.
    monkeypatch.setattr(report, "core_counts",
                        lambda: {"gold_license_entity": 2_900_000,
                                 "gold_license_entity_history": 2_950_000,
                                 "gold_license_dong_summary": 426})
    return pages


def test_gold_report_core_status_and_detail_news(monkeypatch):
    pages = _capture(monkeypatch)
    catalog = {"version": "abcdef123456", "clusters": 3, "singles": 5, "datasets": 152, "drift": False}
    load = {"loaded": {"commerce_pharmacy_detail": 3, "commerce_food_detail": 120,
                       "commerce_gas_detail": 0}}

    counts = report.send_gold_report(catalog=catalog, load=load, elapsed_seconds=12.0)

    assert counts["status"] == "ok" and counts["loaded_rows"] == 123 and counts["objects"] == 3
    title, body = pages[0]
    assert "Iceberg" in title and "123" in title
    # 코어(dbt) 현황과 detail 신규가 분리 표기 — 코어 카운트는 detail 신규 합계에 안 섞인다.
    assert "코어(dbt) 현황" in body and "entity" in body and "426" in body
    assert "상세(detail) 신규" in body and "commerce_food_detail" in body
    assert "적재 0행" in body and "1객체" in body            # gas 0행 요약(정상)
    assert "카탈로그" in body and "abcdef12" in body


def test_gold_report_failure_when_xcom_missing(monkeypatch):
    pages = []
    monkeypatch.setattr(discord, "send_embed",
                        lambda title, body, **kw: pages.append((title, body)))
    counts = report.send_gold_report(catalog=None, load=None)
    assert counts["status"] == "failed"
    title, body = pages[0]
    assert "실패" in title
    assert "build_catalog" in body and "load_details" in body


def test_gold_report_survives_core_count_error(monkeypatch):
    pages = []
    monkeypatch.setattr(discord, "send_embed",
                        lambda title, body, **kw: pages.append((title, body)))
    monkeypatch.setattr(report, "core_counts",
                        lambda: (_ for _ in ()).throw(RuntimeError("trino down")))
    catalog = {"version": "v1", "clusters": 0, "singles": 0, "datasets": 152, "drift": False}
    counts = report.send_gold_report(catalog=catalog, load={"loaded": {"commerce_x_detail": 1}})
    assert counts["status"] == "ok"                           # 코어 조회 실패가 리포트를 못 죽임
    assert pages and "코어(dbt) 현황" not in pages[0][1]
