"""일반 제품 증거 게시 — d1_catalog_sources/d1_product_quality 에 commerce 가 비지 않게 (#434).

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_product_evidence.py -q

운영 실측(2026-08-06): 두 증거 테이블에 commerce 행이 **0** 이었다(전체 83·35행은 전부 타 도메인).
원인의 절반은 dbt 선언 부재(1/21종)였고, 나머지 절반이 여기다 — **export 가 flow_monthly 1종만
증거를 게시하도록 배선돼 있었다**(`PUBLIC_EVIDENCE_PRODUCT_ID` 단일 상수). 나머지 21개 제품은
품질을 실측할 재료(행·PK·신선도)를 손에 쥐고도 버렸다.
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from gold import serving_export as se

ROWS = [("2026-07", "opened", "a", "b", "11", 5)] * 100_000   # flow_monthly 밴드 안
COLS = ["ym", "event_type", "major", "category", "gu_code", "cnt"]


# ── 빌더 단위 ────────────────────────────────────────────────────────────────
def test_pk_duplicates_and_nulls_are_counted(monkeypatch):
    monkeypatch.setattr(se, "log_event", lambda *a, **k: None)
    rows = [("2026-07", "opened", 1), ("2026-07", "opened", 2),   # 중복 PK
            ("2026-07", None, 3),                                  # NULL PK
            ("2026-06", "closed", 4)]
    q = se._generic_quality_evidence(
        {"primary_key": ["ym", "event_type"]}, ["ym", "event_type", "cnt"], rows,
        product_id="commerce_x", d1_row_count=4, measured_at="t")
    assert q["source_row_count"] == 4 and q["d1_row_count"] == 4
    assert q["duplicate_primary_key_count"] == 1
    assert q["null_primary_key_count"] == 1
    assert q["serving_status"] == "published"
    assert q["coverage"] is None                  # coverage 선언 없는 제품은 재지 않는다


def test_unmeasurable_pk_warns_instead_of_folding_to_zero(monkeypatch):
    """모른다 ≠ 0 (§19.3) — PK 를 못 재면 경보를 남긴다."""
    seen: list = []
    monkeypatch.setattr(se, "log_event", lambda name, **kw: seen.append((name, kw)))
    q = se._generic_quality_evidence(
        {"primary_key": ["missing_col"]}, ["ym"], [("2026-07",)],
        product_id="commerce_x", d1_row_count=1, measured_at="t")
    assert [n for n, _ in seen] == ["serve.evidence_pk_unmeasurable"]
    assert seen[0][1]["missing_in_projection"] == ["missing_col"]
    assert q["duplicate_primary_key_count"] == 0 and q["null_primary_key_count"] == 0


def test_freshness_uses_declared_event_time(monkeypatch):
    monkeypatch.setattr(se, "log_event", lambda *a, **k: None)
    q = se._generic_quality_evidence(
        {"primary_key": ["ym"], "event_time": "ym", "freshness_slo_minutes": 2880},
        ["ym"], [("2026-06",), ("2026-07",)],
        product_id="commerce_x", d1_row_count=2, measured_at="t")
    assert q["freshness_as_of"] == "2026-07"
    assert q["freshness_slo_minutes"] == 2880


# ── export 배선 — 일반 제품도 증거 게시 대상이다 ─────────────────────────────
@pytest.fixture
def export(monkeypatch):
    """flow_monthly 1종을 **일반 제품 경로**로 통과시킨다(PUBLIC 상수를 딴 데로 돌림)."""
    module = types.ModuleType("bronze.warehouse")

    class _Cursor:
        description = [(c, "varchar") for c in COLS]

        def execute(self, sql):
            pass

        def fetchall(self):
            return ROWS

    module._connect = lambda *a, **k: types.SimpleNamespace(
        cursor=lambda: _Cursor(), close=lambda: None)
    module._qualified = lambda: ("iceberg", "commerce", "iceberg.commerce")
    monkeypatch.setitem(sys.modules, "bronze.warehouse", module)
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "t")
    monkeypatch.setenv("SERVING_CLOUDFLARE_ACCOUNT_ID", "0" * 32)
    monkeypatch.setenv("SERVING_D1_DATABASE_ID", "00000000-0000-0000-0000-000000000000")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "test_run__evidence")

    captured: list[tuple] = []
    monkeypatch.setattr(se, "_d1", lambda sql, token: [])
    monkeypatch.setattr(se, "_load_serving_meta", lambda: {
        "gold_license_flow_monthly": {"description": "", "serving": {
            "primary_key": ["ym", "event_type", "major", "category", "gu_code"],
            "event_time": "ym",
            "source_evidence": [{"source_id": "s1", "source_url": "u", "license": "l",
                                 "license_url": "lu", "redistribution": "r",
                                 "attribution": "a", "rights_checked_at": "2026-08-04"}],
        }, "tests": [], "columns": {}}})
    monkeypatch.setattr(se, "_glossary_rows", lambda *a, **k: [])
    monkeypatch.setattr(se, "_read_publish_state", lambda token: {})
    monkeypatch.setattr(se, "_d1_row_count", lambda table, token: 100_000)
    monkeypatch.setattr(se, "_upsert_publish_state", lambda *a, **k: None)
    monkeypatch.setattr(se, "_write_serve_state", lambda *a, **k: None)
    monkeypatch.setattr(se, "_report", lambda *a, **k: None)
    monkeypatch.setattr(se, "_check_contract_drift", lambda meta: None)
    monkeypatch.setattr(se, "_load_public_evidence_contract",
                        lambda: types.SimpleNamespace(model_name="__none__"))
    monkeypatch.setattr(se, "_measure_source_relation_coverage",
                        lambda *a, **k: (None, None))
    monkeypatch.setattr(se, "_publish_public_evidence",
                        lambda token, rows: captured.extend(rows))
    monkeypatch.setattr(se, "PUBLIC_EVIDENCE_PRODUCT_ID", "__elsewhere__")
    monkeypatch.setattr(se, "SERVING_SPEC", (se.SERVING_SPEC[0],))
    assert se.SERVING_SPEC[0].d1_table == "d1_flow_monthly"
    return se, captured


def test_generic_product_publishes_sources_and_quality(export):
    se_, captured = export
    assert se_.export_to_d1()["status"] == "ok"
    assert len(captured) == 1
    product_id, publication_id, sources, quality = captured[0]
    assert product_id == "commerce_flow_monthly" and publication_id
    assert sources and sources[0]["source_id"] == "s1"       # dbt 선언이 그대로 게시된다
    assert quality["source_row_count"] == 100_000
    assert quality["d1_row_count"] == 100_000
    assert quality["duplicate_primary_key_count"] == 99_999  # 동일행 10만 개 → PK 실측이 진짜다
    assert quality["serving_status"] == "published"
    assert quality["freshness_as_of"] == "2026-07"


def test_undeclared_sources_publish_quality_only(export, monkeypatch):
    """source_evidence 미선언 제품은 sources=None — 공용 클라이언트가 기존 행을 보존한다."""
    se_, captured = export
    monkeypatch.setattr(se_, "_load_serving_meta", lambda: {})
    se_.export_to_d1()
    product_id, _pub, sources, quality = captured[-1]
    assert product_id == "commerce_flow_monthly"
    assert sources is None
    assert quality["source_row_count"] == 100_000
