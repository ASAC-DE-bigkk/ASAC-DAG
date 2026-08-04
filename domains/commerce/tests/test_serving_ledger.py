"""공유 `_publication_ledger` 기록 — commerce 게시 증거가 공용 자리에 남는지 (#668).

타 도메인(공용 Publisher)은 원장에 남는데 commerce 자체 export 는 안 남아, 게시 증거가
`_catalog`/`d1_publish_state` 에만 흩어져 있었다. 세 가지를 고정한다:

- **published** — 새 내용 게시 시 원장 1행(공용 어휘·컬럼 정본 그대로).
- **skipped_retained** — 밴드 게이트 스킵 시 원장 1행 + LKG(직전 게시 유지) 사유.
- **무변경 스킵은 기록하지 않는다** — serving publication_id 를 재사용하므로(#601) 매 run 넣으면
  원장 PK(publication_id) 와 충돌한다. 그 증거는 `d1_publish_state.checked_at` 전진이 맡는다.
"""
from __future__ import annotations

import re
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

ROWS = [("2026-07", "opened", "a", "b", "11", 5)] * 100_000  # flow_monthly 밴드(90k~300k) 안


def _fake_warehouse(rows):
    module = types.ModuleType("bronze.warehouse")

    class _Cursor:
        description = [("ym", "varchar"), ("event_type", "varchar"), ("major", "varchar"),
                       ("category", "varchar"), ("gu_code", "varchar"), ("cnt", "bigint")]

        def execute(self, sql):
            pass

        def fetchall(self):
            return rows

    module._connect = lambda *a, **k: types.SimpleNamespace(
        cursor=lambda: _Cursor(), close=lambda: None)
    module._qualified = lambda: ("iceberg", "commerce", "iceberg.commerce")
    return module


@pytest.fixture
def export(monkeypatch):
    """flow_monthly 1종으로 좁힌 export_to_d1 — D1 호출을 전부 가로챈다."""
    monkeypatch.setitem(sys.modules, "bronze.warehouse", _fake_warehouse(ROWS))
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "t")
    monkeypatch.setenv("SERVING_CLOUDFLARE_ACCOUNT_ID", "0" * 32)
    monkeypatch.setenv("SERVING_D1_DATABASE_ID", "00000000-0000-0000-0000-000000000000")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "test_run__ledger")
    from gold import serving_export as se

    calls: list[str] = []
    state: dict[str, dict] = {}

    def _capture_state(state_rows, token):
        for table, fingerprint, n, publication_id, written, checked in state_rows:
            state[table] = {"payload_hash": fingerprint, "row_count": n,
                            "publication_id": publication_id, "written_at": written}

    monkeypatch.setattr(se, "_d1", lambda sql, token: calls.append(sql) or [])
    monkeypatch.setattr(se, "_load_serving_meta", lambda: {})
    monkeypatch.setattr(se, "_glossary_rows", lambda *a, **k: [])
    monkeypatch.setattr(se, "_read_publish_state", lambda token: dict(state))
    monkeypatch.setattr(se, "_d1_row_count", lambda table, token: state.get(table, {}).get("row_count", 0))
    monkeypatch.setattr(se, "_upsert_publish_state", _capture_state)
    monkeypatch.setattr(se, "_write_serve_state", lambda *a, **k: None)
    monkeypatch.setattr(se, "_report", lambda *a, **k: None)
    monkeypatch.setattr(se, "_check_contract_drift", lambda meta: None)
    monkeypatch.setattr(
        se,
        "_load_public_evidence_contract",
        lambda: types.SimpleNamespace(model_name="gold_license_flow_monthly"),
    )
    monkeypatch.setattr(se, "_public_quality_evidence", lambda *a, **k: {})
    monkeypatch.setattr(se, "_measure_source_relation_coverage", lambda *a, **k: (147, {"status": "passed"}))
    monkeypatch.setattr(se, "_publish_public_evidence", lambda *a, **k: None)
    monkeypatch.setattr(se, "SERVING_SPEC", (se.SERVING_SPEC[0],))
    assert se.SERVING_SPEC[0].d1_table == "d1_flow_monthly"
    return se, calls, state


def _ledger_inserts(calls):
    return [c for c in calls if "_publication_ledger" in c and "INSERT INTO" in c]


def test_publish_appends_one_ledger_row_with_the_catalog_publication_id(export):
    se, calls, state = export
    result = se.export_to_d1()
    assert result["status"] == "ok"
    rows = _ledger_inserts(calls)
    assert len(rows) == 1
    row = rows[0]
    assert "'published'" in row and "'completed'" in row
    assert "gold_license_flow_monthly" in row and "commerce_flow_monthly" in row
    assert "test_run__ledger" in row
    # 원장의 publication_id 는 _catalog 가 갖게 될 그 발급분과 동일해야 한다(동일 게시 증거).
    assert state["d1_flow_monthly"]["publication_id"] in row
    # 스키마는 공용 정본 DDL 을 동봉해 부트스트랩한다(DROP 없음).
    assert "CREATE TABLE IF NOT EXISTS _publication_ledger" in row
    assert "DROP" not in row.upper()


def test_band_skip_appends_skipped_retained_with_lkg_reason(export, monkeypatch):
    se, calls, _state = export
    monkeypatch.setitem(sys.modules, "bronze.warehouse", _fake_warehouse(ROWS[:10]))
    result = se.export_to_d1()
    assert result["status"] == "stale"
    rows = _ledger_inserts(calls)
    assert len(rows) == 1
    assert "'skipped_retained'" in rows[0] and "'gate'" in rows[0]
    assert "직전 게시 유지" in rows[0]           # LKG 보존이 사유로 남는다
    assert re.search(r"band 10 outside \[90000, 300000\]", rows[0])


def test_unchanged_skip_appends_nothing(export):
    """무변경 run 은 publication_id 를 재사용하므로 원장에 넣으면 PK 충돌 — 넣지 않는다."""
    se, calls, _state = export
    se.export_to_d1()                            # 1회차: 게시(지문 상태 축적)
    first = len(_ledger_inserts(calls))
    se.export_to_d1()                            # 2회차: 같은 payload → 무변경 스킵
    assert len(_ledger_inserts(calls)) == first  # 원장 행이 늘지 않는다
