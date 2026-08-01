from __future__ import annotations

from typing import Any

import pytest

from common.serving.contract import ServingContract
from common.serving.runtime import HttpSmokeTester, TrinoSourceReader


def test_missing_api_base_url_is_not_evaluated_smoke():
    assert HttpSmokeTester("").check("gold_weather_place_current_outlook") == "not_evaluated"


class FakeCursor:
    def __init__(self, show_columns_rows: list[tuple[str, str]], select_rows: list[tuple[Any, ...]]) -> None:
        self.show_columns_rows = show_columns_rows
        self.select_rows = select_rows
        self.statements: list[str] = []
        self.description: list[tuple[str]] = []
        self._pending: str | None = None

    def execute(self, sql: str) -> None:
        self.statements.append(sql)
        if sql.startswith("SHOW COLUMNS"):
            self._pending = "show"
            self.description = []
            return
        self._pending = "select"
        selected = sql.removeprefix("SELECT ").split(" FROM ", 1)[0]
        if selected == "*":
            self.description = [(name,) for name, _type in self.show_columns_rows]
            return
        self.description = [(part.strip().strip('"'),) for part in selected.split(",")]

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._pending == "show":
            return self.show_columns_rows
        return self.select_rows


def _contract(**overrides: Any) -> ServingContract:
    base: dict[str, Any] = {
        "product_id": "weather_place_current_outlook",
        "model_name": "gold_weather_place_current_outlook",
        "enabled": True,
        "external": True,
        "publication_mode": "snapshot",
        "zero_policy": "fail",
        "primary_key": ("product_row_id",),
        "event_time": "forecast_at",
        "public_projection": ("product_row_id", "place_id", "forecast_at"),
        "projection_schema_version": "1.0.0",
        "projection_schema_hash": "hash",
    }
    base.update(overrides)
    return ServingContract(**base)


def test_opted_in_snapshot_read_uses_exact_quoted_projection_and_columns():
    cursor = FakeCursor(
        [
            ("forecast_at", "timestamp"),
            ("place_id", "varchar"),
            ("extra_internal", "varchar"),
            ("product_row_id", "varchar"),
        ],
        [("row-1", "place-1", "2026-07-30 00:00:00")],
    )
    reader = TrinoSourceReader(cursor, "iceberg_dev", "weather")

    plan = reader.read(_contract(), last_good_max=None)

    assert cursor.statements == [
        "SHOW COLUMNS FROM iceberg_dev.weather.gold_weather_place_current_outlook",
        'SELECT "product_row_id","place_id","forecast_at" FROM iceberg_dev.weather.gold_weather_place_current_outlook',
    ]
    assert plan.columns == [("product_row_id", "varchar"), ("place_id", "varchar"), ("forecast_at", "timestamp")]
    assert plan.rows == [
        {"product_row_id": "row-1", "place_id": "place-1", "forecast_at": "2026-07-30 00:00:00"}
    ]


def test_opted_in_append_full_and_incremental_reads_use_projection_and_preserve_delete_window():
    full_cursor = FakeCursor(
        [("area_cd", "varchar"), ("event_at", "timestamp"), ("base_n", "integer"), ("raw_key", "varchar")],
        [("a", "2026-07-30 00:00:00", 40)],
    )
    contract = _contract(
        product_id="citydata_ppltn_dow_hour",
        model_name="gold_citydata_ppltn_dow_hour",
        publication_mode="append",
        primary_key=("area_cd", "event_at"),
        event_time="event_at",
        reliability={"sample_count_field": "base_n"},
        public_projection=("area_cd", "event_at", "base_n"),
    )

    full_plan = TrinoSourceReader(full_cursor, "iceberg_dev", "citydata").read(contract, last_good_max=None)

    assert full_cursor.statements[-1] == (
        'SELECT "area_cd","event_at","base_n" FROM iceberg_dev.citydata.gold_citydata_ppltn_dow_hour'
    )
    assert full_plan.columns == [("area_cd", "varchar"), ("event_at", "timestamp"), ("base_n", "integer")]

    incremental_cursor = FakeCursor(
        [("area_cd", "varchar"), ("event_at", "timestamp"), ("base_n", "integer"), ("raw_key", "varchar")],
        [("a", "2026-07-30 01:00:00", 41)],
    )
    incremental_plan = TrinoSourceReader(incremental_cursor, "iceberg_dev", "citydata").read(
        contract,
        last_good_max="2026-07-30 03:10:00",
    )

    assert incremental_cursor.statements[-1] == (
        'SELECT "area_cd","event_at","base_n" FROM iceberg_dev.citydata.gold_citydata_ppltn_dow_hour '
        'WHERE "event_at" >= timestamp \'2026-07-30 01:00:00\''
    )
    assert incremental_plan.delete_column == "event_at"
    assert incremental_plan.delete_literal == "'2026-07-30 01:00:00'"


def test_projection_missing_physical_column_fails_before_data_select():
    cursor = FakeCursor([("product_row_id", "varchar"), ("forecast_at", "timestamp")], [])
    reader = TrinoSourceReader(cursor, "iceberg_dev", "weather")

    with pytest.raises(ValueError, match="missing projected columns"):
        reader.read(_contract(), last_good_max=None)

    assert cursor.statements == ["SHOW COLUMNS FROM iceberg_dev.weather.gold_weather_place_current_outlook"]


@pytest.mark.parametrize(
    "overrides,error",
    [
        ({"primary_key": ("product_row_id", "place_id"), "public_projection": ("product_row_id", "forecast_at")}, "primary_key"),
        ({"event_time": "forecast_at", "public_projection": ("product_row_id", "place_id")}, "event_time"),
        (
            {"reliability": {"sample_count_field": "base_n"}, "public_projection": ("product_row_id", "forecast_at")},
            "sample_count_field",
        ),
    ],
)
def test_projection_missing_required_contract_field_fails_before_data_select(overrides, error):
    cursor = FakeCursor(
        [("product_row_id", "varchar"), ("place_id", "varchar"), ("forecast_at", "timestamp"), ("base_n", "integer")],
        [],
    )
    reader = TrinoSourceReader(cursor, "iceberg_dev", "weather")

    with pytest.raises(ValueError, match=error):
        reader.read(_contract(**overrides), last_good_max=None)

    assert cursor.statements == ["SHOW COLUMNS FROM iceberg_dev.weather.gold_weather_place_current_outlook"]


def test_legacy_contract_preserves_select_star_read_plan_behavior():
    cursor = FakeCursor(
        [("product_row_id", "varchar"), ("place_id", "varchar"), ("raw_key", "varchar")],
        [("row-1", "place-1", "raw-1")],
    )
    reader = TrinoSourceReader(cursor, "iceberg_dev", "weather")

    plan = reader.read(_contract(public_projection=None), last_good_max=None)

    assert cursor.statements == [
        "SHOW COLUMNS FROM iceberg_dev.weather.gold_weather_place_current_outlook",
        "SELECT * FROM iceberg_dev.weather.gold_weather_place_current_outlook",
    ]
    assert plan.columns == [("product_row_id", "varchar"), ("place_id", "varchar"), ("raw_key", "varchar")]
    assert plan.rows == [{"product_row_id": "row-1", "place_id": "place-1", "raw_key": "raw-1"}]
