"""Regression coverage for the common Publisher's D1 catalog boundary."""

from __future__ import annotations

import sqlite3
from typing import Any

import pytest

from common.serving.d1_client import (
    CATALOG_COLUMNS,
    MAX_API_BATCH_BYTES,
    MAX_SQL_STATEMENT_BYTES,
    MAX_STATEMENTS_PER_API_BATCH,
    HttpD1Client,
    build_insert_statements,
    group_api_batches,
)


class LegacyCatalogClient(HttpD1Client):
    """D1 HTTP seam with the pre-v1.1 eight-column catalog schema."""

    def __init__(self) -> None:
        super().__init__(api_url="https://example.invalid", token="test-token")
        self.columns = [
            "name", "description", "serving_tier", "tests",
            "time_axis", "columns", "row_count", "exported_at",
        ]
        self.queries: list[str] = []

    def _query(self, sql: str) -> list[dict[str, Any]]:
        self.queries.append(sql)
        if sql == "PRAGMA table_info(_catalog);":
            return [{"name": name} for name in self.columns]
        if sql.startswith("ALTER TABLE _catalog ADD COLUMN "):
            self.columns.append(sql.split('"')[1])
        return []

    def _query_batch(self, statements: list[str]) -> list[list[dict[str, Any]]]:
        return [self._query(statement) for statement in statements]


class SqliteCatalogClient(HttpD1Client):
    """Real SQLite seam for preserving fields outside the v1.1 catalog model."""

    def __init__(self) -> None:
        super().__init__(api_url="https://example.invalid", token="test-token")
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.queries: list[str] = []

    def _query(self, sql: str) -> list[dict[str, Any]]:
        self.queries.append(sql)
        if ";" in sql.rstrip(";"):
            self.connection.executescript(sql)
            self.connection.commit()
            return []
        cursor = self.connection.execute(sql)
        self.connection.commit()
        return [dict(row) for row in cursor.fetchall()] if cursor.description else []

    def _query_batch(self, statements: list[str]) -> list[list[dict[str, Any]]]:
        return [self._query(statement) for statement in statements]


def _catalog_row() -> dict[str, Any]:
    return {
        "name": "gold_weather_place_current_outlook",
        "product_id": "weather_place_current_outlook",
        "external": True,
        "description": "현재 장소 예보",
        "product_question": "지금 이 장소의 예보는?",
        "tests": "not_null(product_row_id)",
        "time_axis": "forecast_at",
        "columns": "[]",
        "row_count": 427,
        "serving_status": "published",
        "publication_id": "publication-1",
        "source_run_id": "run-1",
        "published_bytes": 1024,
        "freshness": "2026-07-28T10:00:00",
        "exported_at": "2026-07-28T10:01:00",
    }


def test_catalog_upsert_migrates_legacy_catalog_and_names_v11_columns():
    """Catch a positional insert that cannot write the v1.1 15-field catalog row."""
    d1 = LegacyCatalogClient()

    d1.upsert_catalog([_catalog_row()])

    assert set(CATALOG_COLUMNS).issubset(d1.columns)
    insert = next(query for query in d1.queries if query.startswith("INSERT INTO _catalog"))
    assert '("name", "product_id", "external", "description", "product_question"' in insert
    assert 'ON CONFLICT("name") DO UPDATE SET' in insert
    assert '"serving_tier"' not in insert


def test_catalog_upsert_does_not_alter_an_already_migrated_schema():
    """Catch a retry that attempts to add an existing v1.1 catalog column again."""
    d1 = LegacyCatalogClient()

    d1.upsert_catalog([_catalog_row()])
    alter_count = sum(query.startswith("ALTER TABLE _catalog ADD COLUMN") for query in d1.queries)
    assert alter_count == 8
    d1.upsert_catalog([_catalog_row()])

    assert sum(query.startswith("ALTER TABLE _catalog ADD COLUMN") for query in d1.queries) == alter_count


def test_catalog_upsert_preserves_legacy_serving_tier_on_existing_row():
    d1 = SqliteCatalogClient()
    d1._query(
        "CREATE TABLE _catalog (name TEXT PRIMARY KEY, description TEXT, serving_tier TEXT, "
        "tests TEXT, time_axis TEXT, columns TEXT, row_count INTEGER, exported_at TEXT);"
    )
    d1._query(
        "INSERT INTO _catalog (name, description, serving_tier) "
        "VALUES ('gold_weather_place_current_outlook', 'legacy description', 'public');"
    )

    d1.upsert_catalog([_catalog_row()])

    row = d1._query(
        "SELECT product_id, description, serving_tier FROM _catalog "
        "WHERE name = 'gold_weather_place_current_outlook';"
    )[0]
    assert row == {
        "product_id": "weather_place_current_outlook",
        "description": _catalog_row()["description"],
        "serving_tier": "public",
    }


def test_serving_table_enforces_contract_primary_key_and_reports_readback_counts():
    d1 = SqliteCatalogClient()
    table = "gold_weather_place_current_outlook"
    columns = [("product_row_id", "varchar"), ("forecast_at", "timestamp")]

    assert hasattr(d1, "primary_key_stats")
    d1.ensure_table(table, columns, ("product_row_id",))
    d1.insert_rows(table, columns, [{"product_row_id": "row-1", "forecast_at": "old"}], replace=False)
    d1.insert_rows(table, columns, [{"product_row_id": "row-1", "forecast_at": "new"}], replace=True)

    assert any('CREATE UNIQUE INDEX IF NOT EXISTS "gold_weather_place_current_outlook__pk_uq"' in query for query in d1.queries)
    assert d1.primary_key_stats(table, ("product_row_id",)) == (1, 1, 0)


def test_repeated_snapshot_swaps_keep_a_physical_primary_key_constraint():
    d1 = SqliteCatalogClient()
    table = "gold_weather_place_current_outlook"
    columns = [("product_row_id", "varchar"), ("forecast_at", "timestamp")]

    d1.replace_table(table, columns, [{"product_row_id": "row-1", "forecast_at": "first"}], ("product_row_id",))
    d1.replace_table(table, columns, [{"product_row_id": "row-1", "forecast_at": "second"}], ("product_row_id",))

    assert any(row["unique"] for row in d1._query(f'PRAGMA index_list("{table}");'))
    with pytest.raises(sqlite3.IntegrityError):
        d1._query(
            'INSERT INTO "gold_weather_place_current_outlook" ("product_row_id", "forecast_at") '
            "VALUES ('row-1', 'duplicate');"
        )


def test_snapshot_replacement_requires_a_contract_primary_key():
    d1 = SqliteCatalogClient()

    with pytest.raises(ValueError, match="primary_key is required"):
        d1.replace_table(
            "gold_weather_place_current_outlook",
            [("product_row_id", "varchar")],
            [{"product_row_id": "row-1"}],
            (),
        )


def test_insert_statements_pack_rows_by_rendered_utf8_sql_bytes():
    rows = [{"product_row_id": f"row-{index}", "label": "용신동" * 300} for index in range(120)]

    statements = build_insert_statements(
        "gold_weather_place_risk_window",
        [("product_row_id", "varchar"), ("label", "varchar")],
        rows,
        replace=False,
    )

    assert len(statements) > 1
    assert all(len(statement.encode("utf-8")) <= MAX_SQL_STATEMENT_BYTES for statement in statements)
    assert sum(statement.count("('row-") for statement in statements) == len(rows)


def test_insert_statements_reject_one_row_that_exceeds_sql_budget_before_http():
    with pytest.raises(ValueError, match=str(MAX_SQL_STATEMENT_BYTES)):
        build_insert_statements(
            "gold_weather_place_risk_window",
            [("product_row_id", "varchar"), ("label", "varchar")],
            [{"product_row_id": "oversized", "label": "용" * 30_000}],
            replace=False,
        )


def test_group_api_batches_limits_statement_count_and_total_utf8_body_bytes():
    statement = "INSERT INTO risk (label) VALUES ('" + ("a" * 60_000) + "');"

    batches = group_api_batches([statement] * 9)

    assert len(batches) == 3
    assert all(len(batch) <= MAX_STATEMENTS_PER_API_BATCH for batch in batches)
    assert all(
        sum(len(sql.encode("utf-8")) for sql in batch) <= MAX_API_BATCH_BYTES
        for batch in batches
    )


def test_query_batch_sends_one_cloudflare_batch_request(monkeypatch):
    d1 = HttpD1Client(api_url="https://example.invalid", token="test-token")
    sent: list[dict[str, Any]] = []

    def fake_request(body: dict[str, Any]) -> dict[str, Any]:
        sent.append(body)
        return {
            "success": True,
            "result": [
                {"success": True, "results": [{"id": 1}]},
                {"success": True, "results": [{"id": 2}]},
            ],
        }

    monkeypatch.setattr(d1, "_request", fake_request, raising=False)

    assert d1._query_batch(["SELECT 1;", "SELECT 2;"]) == [[{"id": 1}], [{"id": 2}]]
    assert sent == [{"batch": [{"sql": "SELECT 1;"}, {"sql": "SELECT 2;"}]}]


def test_query_rejects_a_failed_statement_in_a_multi_statement_response(monkeypatch):
    d1 = HttpD1Client(api_url="https://example.invalid", token="test-token")
    monkeypatch.setattr(
        d1,
        "_request",
        lambda body: {
            "success": True,
            "result": [
                {"success": True, "results": []},
                {"success": False, "errors": [{"message": "rename failed"}], "results": []},
            ],
        },
    )

    with pytest.raises(RuntimeError, match="D1 API"):
        d1._query('DROP TABLE IF EXISTS "gold_traffic"; ALTER TABLE "gold_traffic__staging" RENAME TO "gold_traffic";')


def test_snapshot_restore_reactivates_previous_table_after_post_promotion_failure():
    d1 = SqliteCatalogClient()
    table = "gold_weather_place_current_outlook"
    columns = [("product_row_id", "varchar"), ("forecast_at", "timestamp")]

    d1.replace_table(table, columns, [{"product_row_id": "old", "forecast_at": "old"}], ("product_row_id",))
    d1.replace_table(table, columns, [{"product_row_id": "new", "forecast_at": "new"}], ("product_row_id",))
    d1.restore_replaced_table(table)

    assert d1._query(f'SELECT product_row_id FROM "{table}";') == [{"product_row_id": "old"}]
    assert d1._query(f"SELECT name FROM sqlite_master WHERE name = '{table}__previous';") == []


def test_publication_ledger_is_append_only_and_records_publication_stage():
    d1 = SqliteCatalogClient()
    record = {
        "publication_id": "p-1",
        "product_id": "weather_place_risk_window",
        "model_name": "gold_weather_place_risk_window",
        "source_run_id": "run-1",
        "attempted_at": "2026-07-29T00:00:00+00:00",
        "outcome": "published",
        "stage": "completed",
        "source_row_count": 304878,
        "published_row_count": 304878,
        "d1_row_count": 304878,
        "api_smoke_status": "not_evaluated",
        "rollback_status": "not_needed",
        "reason": "ok",
    }

    d1.append_publication_ledger(record)

    assert d1._query("SELECT publication_id, outcome, stage FROM _publication_ledger;") == [
        {"publication_id": "p-1", "outcome": "published", "stage": "completed"}
    ]
    with pytest.raises(sqlite3.IntegrityError):
        d1.append_publication_ledger(record)
