from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from gold import serving_export as se


ROWS = [
    ("2026-01", "opened", "health", "clinic", "11680", 2),
    ("2026-02", "opened", "health", "clinic", "11680", 3),
]
COLUMNS = [
    ("ym", "varchar"),
    ("event_type", "varchar"),
    ("major", "varchar"),
    ("category", "varchar"),
    ("gu_code", "varchar"),
    ("cnt", "bigint"),
]


class _Cursor:
    def __init__(self):
        self.description = []
        self._rows = []
        self.statements: list[str] = []

    def execute(self, sql):
        self.statements.append(sql)
        if sql.startswith("SELECT COUNT(DISTINCT"):
            self.description = [("_col0", "bigint")]
            self._rows = [(147,)]
        elif "gold_license_flow_monthly" in sql:
            self.description = COLUMNS
            self._rows = ROWS
        else:
            self.description = []
            self._rows = []

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0]


def _contract():
    return SimpleNamespace(
        product_id="commerce_flow_monthly",
        model_name="gold_license_flow_monthly",
        primary_key=("ym", "event_type", "major", "category", "gu_code"),
        public_projection=tuple(name for name, _type in COLUMNS),
        event_time="ym",
        freshness_slo_minutes=89_280,
        projection_schema_version="1.0.0",
        projection_schema_hash="projection-hash",
        quality_coverage={
            "field": "dataset",
            "expected_distinct_count": 152,
            "minimum_ratio": 0.95,
            "measurement_scope": "source_relation",
        },
        source_evidence=({"source_id": "seoul_open_data"},),
    )


def test_flow_monthly_publishes_public_evidence_with_active_publication(monkeypatch):
    cursor = _Cursor()
    warehouse = types.ModuleType("bronze.warehouse")
    warehouse._qualified = lambda: ("iceberg_dev", "commerce", "iceberg_dev.commerce")
    warehouse._connect = lambda *_args: SimpleNamespace(cursor=lambda: cursor, close=lambda: None)
    monkeypatch.setitem(sys.modules, "bronze.warehouse", warehouse)
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "test-token")

    spec = se.Serve(
        "gold_license_flow_monthly",
        "d1_flow_monthly",
        "d1_rollup",
        se.SERVING_SPEC[0].select,
        (1, 100),
    )
    contract = _contract()
    evidence_calls: list[tuple] = []
    state: dict[str, tuple] = {}

    monkeypatch.setattr(se, "SERVING_SPEC", (spec,))
    monkeypatch.setattr(se, "_load_public_evidence_contract", lambda: contract)
    monkeypatch.setattr(se, "_load_serving_meta", lambda: {
        "gold_license_flow_monthly": {
            "description": "monthly flow",
            "tests": [],
            "columns": {},
            "serving": {
                "event_time": "ym",
                "public_primary_key": list(contract.primary_key),
            },
        }
    })
    monkeypatch.setattr(se, "_check_contract_drift", lambda _meta: None)
    monkeypatch.setattr(se, "_catalog_schema", lambda: ((), ""))
    monkeypatch.setattr(se, "_ensure_shared_tables", lambda *_args: None)
    monkeypatch.setattr(se, "_read_publish_state", lambda _token: {})
    monkeypatch.setattr(se, "_upsert_publish_state", lambda rows, _token: state.update({row[0]: row for row in rows}))
    monkeypatch.setattr(se, "_d1", lambda *_args: [])
    monkeypatch.setattr(se, "_insert_rows", lambda *_args: None)
    monkeypatch.setattr(se, "_d1_row_count", lambda _table, _token: 2)
    monkeypatch.setattr(se, "_append_ledger", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(se, "_upsert_catalog", lambda *_args: None)
    monkeypatch.setattr(se, "_upsert_meta", lambda *_args: None)
    monkeypatch.setattr(se, "_publish_handoff", lambda *_args, **_kw: None)
    monkeypatch.setattr(se, "_glossary_rows", lambda *_args: [])
    monkeypatch.setattr(se, "_publish_public_evidence", lambda token, rows: evidence_calls.append((token, rows)))
    monkeypatch.setattr(se, "_write_serve_state", lambda *_args: None)
    monkeypatch.setattr(se, "_report", lambda *_args: None)

    result = se.export_to_d1()

    assert result["status"] == "ok"
    assert any(statement.startswith('SELECT COUNT(DISTINCT "dataset")') for statement in cursor.statements)
    assert len(evidence_calls) == 1
    token, evidence_rows = evidence_calls[0]
    assert token == "test-token"
    # 증거 게시 확장(#434) — 행 모양은 (product_id, publication_id, sources, quality)로 통일됐다.
    product_id, publication_id, sources, quality = evidence_rows[0]
    assert product_id == contract.product_id
    assert sources is contract.source_evidence
    assert publication_id == state["d1_flow_monthly"][3]
    assert quality["source_row_count"] == quality["d1_row_count"] == 2
    assert quality["duplicate_primary_key_count"] == quality["null_primary_key_count"] == 0
    assert quality["freshness_as_of"] == "2026-02"
    assert quality["coverage"]["observed_distinct_count"] == 147
    assert quality["coverage"]["status"] == "passed"
