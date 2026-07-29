"""Regression coverage for the common Publisher's D1 catalog boundary."""

from __future__ import annotations

import sqlite3
from typing import Any

from common.serving.d1_client import CATALOG_COLUMNS, HttpD1Client


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


class SqliteCatalogClient(HttpD1Client):
    """Real SQLite seam for preserving fields outside the v1.1 catalog model."""

    def __init__(self) -> None:
        super().__init__(api_url="https://example.invalid", token="test-token")
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row

    def _query(self, sql: str) -> list[dict[str, Any]]:
        cursor = self.connection.execute(sql)
        self.connection.commit()
        return [dict(row) for row in cursor.fetchall()] if cursor.description else []


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
