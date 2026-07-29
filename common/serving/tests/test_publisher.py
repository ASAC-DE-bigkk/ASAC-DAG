"""Behavioral oracle for the common D1 Publisher.

Exercises the full Publication pipeline (gate → write → verify → _catalog → smoke)
against in-memory fakes: no Trino, no Cloudflare, no Airflow, no prod D1.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from common.serving.contract import ServingContract, load_contracts
from common.serving.d1_client import Column
from common.serving.gate import STATUS_DEGRADED, STATUS_PUBLISHED, STATUS_SKIPPED
from common.serving.publisher import PublicationError, ReadPlan, publish

FIXTURES = Path(__file__).parent / "fixtures"
COLUMNS: list[Column] = [("product_row_id", "varchar"), ("place_id", "varchar"), ("forecast_at", "timestamp")]


# ── fakes ───────────────────────────────────────────────────────────────────────────

class FakeD1:
    def __init__(self) -> None:
        self.tables: dict[str, list[dict[str, Any]]] = {}
        self.primary_keys: dict[str, tuple[str, ...]] = {}
        self.columns_by_table: dict[str, list[Column]] = {}
        self.catalog: dict[str, dict[str, Any]] = {}
        self.ledger: list[dict[str, Any]] = []
        self.previous_tables: dict[str, list[dict[str, Any]]] = {}
        self.replace_calls = 0

    def table_row_count(self, name: str) -> int:
        return len(self.tables.get(name, []))

    def primary_key_stats(self, name: str, primary_key) -> tuple[int, int, int]:
        rows = self.tables.get(name, [])
        values = [tuple(row.get(column) for column in primary_key) for row in rows]
        return len(rows), len(set(values)), sum(1 for value in values if any(part is None for part in value))

    def table_max(self, name: str, column: str) -> Any | None:
        values = [r.get(column) for r in self.tables.get(name, []) if r.get(column) is not None]
        return max(values) if values else None

    def catalog_row(self, name: str) -> dict[str, Any] | None:
        return dict(self.catalog[name]) if name in self.catalog else None

    def ensure_table(self, name: str, columns, primary_key) -> None:
        self.tables.setdefault(name, [])
        self.primary_keys[name] = tuple(primary_key)
        self.columns_by_table.setdefault(name, list(columns))

    def replace_table(self, name: str, columns, rows, primary_key) -> None:
        self.replace_calls += 1
        if name in self.tables:
            self.previous_tables[name] = [dict(row) for row in self.tables[name]]
        self.primary_keys[name] = tuple(primary_key)
        self.columns_by_table[name] = list(columns)
        self.tables[name] = [dict(r) for r in rows]  # atomic swap semantics

    def restore_replaced_table(self, name: str) -> None:
        if name in self.previous_tables:
            self.tables[name] = self.previous_tables.pop(name)
        else:
            self.tables.pop(name, None)

    def finalize_replaced_table(self, name: str) -> None:
        self.previous_tables.pop(name, None)

    def delete_where_gte(self, name: str, column: str, trino_literal: str) -> None:
        cutoff = trino_literal.strip().strip("'")
        self.tables[name] = [r for r in self.tables.get(name, []) if str(r.get(column)) < cutoff]

    def insert_rows(self, name: str, columns, rows, *, replace: bool) -> None:
        table = self.tables.setdefault(name, [])
        if not replace:
            table.extend(dict(row) for row in rows)
            return
        primary_key = self.primary_keys[name]
        positions = {tuple(row.get(column) for column in primary_key): index for index, row in enumerate(table)}
        for row in rows:
            key = tuple(row.get(column) for column in primary_key)
            if key in positions:
                table[positions[key]] = dict(row)
            else:
                positions[key] = len(table)
                table.append(dict(row))

    def upsert_catalog(self, catalog_rows) -> None:
        for row in catalog_rows:
            self.catalog[row["name"]] = dict(row)

    def delete_catalog_row(self, name: str) -> None:
        self.catalog.pop(name, None)

    def append_publication_ledger(self, record: dict[str, Any]) -> None:
        self.ledger.append(dict(record))

    def catalog_domain_count(self, model_names: set[str]) -> int:
        return sum(1 for n in model_names if n in self.catalog)


class ForgetfulCatalogD1(FakeD1):
    """Writes tables but 'forgets' to register _catalog — reproduces the #477 bug."""

    def upsert_catalog(self, catalog_rows) -> None:  # noqa: D401 - intentional no-op
        pass


class ExplodingCatalogD1(FakeD1):
    """Fails after snapshot promotion, before a new catalog value is committed."""

    def upsert_catalog(self, catalog_rows) -> None:
        raise RuntimeError("simulated catalog write failure")


class FakeSource:
    def __init__(self, plans: dict[str, ReadPlan]) -> None:
        self.plans = plans
        self.seen_last_good_max: dict[str, Any] = {}

    def read(self, contract: ServingContract, last_good_max: Any | None) -> ReadPlan:
        self.seen_last_good_max[contract.model_name] = last_good_max
        return self.plans[contract.model_name]


class FakeSmoke:
    def __init__(self, status: str = "passed") -> None:
        self.status = status
        self.checked: list[str] = []

    def check(self, model_name: str) -> str:
        self.checked.append(model_name)
        return self.status


def _contract(**overrides: Any) -> ServingContract:
    base: dict[str, Any] = dict(
        product_id="weather_place_current_outlook",
        model_name="gold_weather_place_current_outlook",
        enabled=True,
        external=True,
        publication_mode="snapshot",
        zero_policy="fail",
        primary_key=("product_row_id",),
        event_time="forecast_at",
        description="d",
        product_question="q",
    )
    base.update(overrides)
    return ServingContract(**base)


def _rows(n: int) -> list[dict[str, Any]]:
    return [{"product_row_id": f"r{i}", "place_id": "p", "forecast_at": f"2026-07-2{i}T00:00:00"} for i in range(n)]


# ── tests ─────────────────────────────────────────────────────────────────────────

def test_snapshot_publish_success_records_metadata():
    contract = _contract()
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})
    smoke = FakeSmoke(status="passed")

    report = publish([contract], source, d1, smoke, source_run_id="run-1")

    assert report.ok
    assert d1.table_row_count(contract.model_name) == 3
    rec = report.records[0]
    assert rec.serving_status == STATUS_PUBLISHED
    assert rec.source_run_id == "run-1"
    assert rec.publication_id and rec.published_row_count == 3
    assert rec.d1_row_count == 3
    assert rec.distinct_primary_key_count == 3
    assert rec.null_primary_key_count == 0
    assert rec.api_smoke_status == "passed"
    assert rec.published_bytes > 0 and rec.freshness == "2026-07-22T00:00:00"
    cat = d1.catalog_row(contract.model_name)
    assert cat["product_id"] == "weather_place_current_outlook" and cat["serving_status"] == STATUS_PUBLISHED
    assert smoke.checked == [contract.model_name]  # external => smoke ran


def test_zero_rows_retain_last_good_keeps_previous():
    contract = _contract(zero_policy="retain_last_good")
    d1 = FakeD1()
    d1.tables[contract.model_name] = _rows(5)
    d1.catalog[contract.model_name] = {"name": contract.model_name, "row_count": 5, "serving_status": STATUS_PUBLISHED}
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=[])})

    report = publish([contract], source, d1, FakeSmoke(), source_run_id="run-2")

    assert report.ok
    assert report.records[0].serving_status == STATUS_SKIPPED
    assert d1.table_row_count(contract.model_name) == 5  # untouched
    assert d1.catalog[contract.model_name]["row_count"] == 5  # not overwritten


def test_zero_rows_fail_policy_raises_and_protects_table():
    contract = _contract(zero_policy="fail")
    d1 = FakeD1()
    d1.tables[contract.model_name] = _rows(4)
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=[])})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(), source_run_id="run-3")
    assert d1.table_row_count(contract.model_name) == 4  # last-known-good intact
    assert any("zero_policy=fail" in f for f in excinfo.value.report.failures)


def test_duplicate_snapshot_source_primary_key_fails_before_replacing_last_good():
    contract = _contract()
    d1 = FakeD1()
    d1.tables[contract.model_name] = _rows(1)
    duplicate_rows = [
        {"product_row_id": "same", "place_id": "p", "forecast_at": "2026-07-22T00:00:00"},
        {"product_row_id": "same", "place_id": "p", "forecast_at": "2026-07-22T01:00:00"},
    ]
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=duplicate_rows)})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(), source_run_id="duplicate-snapshot")

    assert d1.table_row_count(contract.model_name) == 1
    assert any("source primary key" in failure for failure in excinfo.value.report.failures)


def test_repeated_upsert_replaces_the_existing_primary_key_row():
    contract = _contract(publication_mode="upsert")
    d1 = FakeD1()
    first = ReadPlan(columns=COLUMNS, rows=[{"product_row_id": "same", "place_id": "p", "forecast_at": "old"}])
    second = ReadPlan(columns=COLUMNS, rows=[{"product_row_id": "same", "place_id": "p", "forecast_at": "new"}])

    publish([contract], FakeSource({contract.model_name: first}), d1, FakeSmoke(), source_run_id="upsert-1")
    report = publish([contract], FakeSource({contract.model_name: second}), d1, FakeSmoke(), source_run_id="upsert-2")

    assert d1.table_row_count(contract.model_name) == 1
    assert d1.tables[contract.model_name][0]["forecast_at"] == "new"
    assert report.records[0].d1_row_count == 1
    assert d1.replace_calls == 0


def test_exact_set_upsert_replaces_the_full_source_set_and_schema():
    contract = _contract(publication_mode="upsert", upsert_strategy="exact_set")
    d1 = FakeD1()
    d1.tables[contract.model_name] = [
        {"product_row_id": "stale", "place_id": "old", "forecast_at": "old"},
    ]
    current_columns = COLUMNS + [("new_metric", "integer")]
    current_rows = [
        {"product_row_id": "current", "place_id": "new", "forecast_at": "now", "new_metric": 1},
    ]

    report = publish(
        [contract],
        FakeSource({contract.model_name: ReadPlan(current_columns, current_rows)}),
        d1,
        FakeSmoke(),
        source_run_id="upsert-exact-set",
    )

    assert report.ok
    assert d1.tables[contract.model_name] == current_rows
    assert d1.columns_by_table[contract.model_name] == current_columns
    assert report.records[0].d1_row_count == 1


def test_partial_truncation_retains_last_good():
    contract = _contract(zero_policy="retain_last_good", partial_min_ratio=0.8)
    d1 = FakeD1()
    d1.tables[contract.model_name] = _rows(100)
    d1.catalog[contract.model_name] = {"name": contract.model_name, "row_count": 100}
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(50))})  # 50 < 100*0.8

    report = publish([contract], source, d1, FakeSmoke(), source_run_id="run-4")
    assert report.records[0].serving_status == STATUS_SKIPPED
    assert d1.table_row_count(contract.model_name) == 100


def test_reliability_suppress_row_filters_and_degrades():
    contract = _contract(
        external=False,
        reliability={"sample_count_field": "base_n", "minimum_sample_count": 30, "insufficient_sample_policy": "suppress_row"},
    )
    d1 = FakeD1()
    rows = [
        {"product_row_id": "a", "base_n": 50},
        {"product_row_id": "b", "base_n": 10},  # suppressed
        {"product_row_id": "c", "base_n": 40},
    ]
    source = FakeSource({contract.model_name: ReadPlan(columns=[("product_row_id", "varchar"), ("base_n", "integer")], rows=rows)})

    report = publish([contract], source, d1, FakeSmoke(), source_run_id="run-5")
    assert report.records[0].serving_status == STATUS_DEGRADED
    assert d1.table_row_count(contract.model_name) == 2  # under-sampled row dropped


def test_catalog_self_check_detects_missing_registration():
    contract = _contract()
    d1 = ForgetfulCatalogD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(), source_run_id="run-6")
    assert any("자기검증" in f for f in excinfo.value.report.failures)
    assert d1.table_row_count(contract.model_name) == 0  # failed first publication leaves no partial snapshot


def test_snapshot_catalog_registration_failure_restores_last_good_and_records_ledger():
    contract = _contract()
    d1 = ExplodingCatalogD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    with pytest.raises(PublicationError):
        publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="catalog-failure")

    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert d1.ledger[-1]["outcome"] == "failed"
    assert d1.ledger[-1]["rollback_status"] == "restored"


def test_exact_set_upsert_catalog_failure_restores_last_good_and_records_ledger():
    contract = _contract(publication_mode="upsert", upsert_strategy="exact_set")
    d1 = ExplodingCatalogD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    with pytest.raises(PublicationError):
        publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="upsert-catalog-failure")

    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert d1.ledger[-1]["outcome"] == "failed"
    assert d1.ledger[-1]["rollback_status"] == "restored"


def test_smoke_failure_on_external_product_raises():
    contract = _contract()
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(2))})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(status="failed"), source_run_id="run-7")
    assert any("smoke" in f for f in excinfo.value.report.failures)


def test_snapshot_smoke_failure_restores_last_good_catalog_and_records_ledger():
    contract = _contract()
    d1 = FakeD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    with pytest.raises(PublicationError):
        publish([contract], source, d1, FakeSmoke(status="failed"), source_run_id="smoke-failure")

    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert d1.ledger[-1]["outcome"] == "failed"
    assert d1.ledger[-1]["rollback_status"] == "restored"


def test_not_evaluated_smoke_is_recorded_without_failing_d1_publication():
    contract = _contract()
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(2))})

    report = publish([contract], source, d1, FakeSmoke(status="not_evaluated"), source_run_id="run-no-api")

    assert report.ok
    assert report.records[0].api_smoke_status == "not_evaluated"


def test_append_uses_last_good_max_and_windows():
    contract = _contract(
        model_name="gold_citydata_ppltn_hourly",
        product_id="citydata_ppltn_hourly",
        external=False,
        publication_mode="append",
        zero_policy="retain_last_good",
        event_time="event_at",
        primary_key=("area_cd", "event_at"),
    )
    d1 = FakeD1()
    d1.tables[contract.model_name] = [
        {"area_cd": "a", "event_at": "2026-07-01 00:00:00"},
        {"area_cd": "a", "event_at": "2026-07-02 00:00:00"},
    ]
    d1.catalog[contract.model_name] = {"name": contract.model_name, "row_count": 2}
    plan = ReadPlan(
        columns=[("area_cd", "varchar"), ("event_at", "timestamp")],
        rows=[
            {"area_cd": "a", "event_at": "2026-07-02 00:00:00"},
            {"area_cd": "a", "event_at": "2026-07-03 00:00:00"},
        ],
        delete_column="event_at",
        delete_literal="'2026-07-02 00:00:00'",
    )
    source = FakeSource({contract.model_name: plan})

    report = publish([contract], source, d1, FakeSmoke(), source_run_id="run-8")
    assert report.ok
    assert source.seen_last_good_max[contract.model_name] == "2026-07-02 00:00:00"  # publisher fetched + passed it
    assert d1.table_row_count(contract.model_name) == 3  # 07-01 kept, window re-inserted


def test_load_contracts_filters_enabled_and_product_ids():
    manifest = FIXTURES / "manifest.json"
    all_enabled = load_contracts(manifest)
    assert [c.model_name for c in all_enabled] == [
        "gold_citydata_ppltn_dow_hour",
        "gold_weather_place_current_outlook",
    ]  # sorted, internal disabled dropped

    one = load_contracts(manifest, ["weather_place_current_outlook"])
    assert len(one) == 1
    contract = one[0]
    assert contract.external is True and contract.publication_mode == "snapshot"
    assert contract.primary_key == ("product_row_id",)
    assert contract.tests == ("not_null(product_row_id)",)  # gate label collected from manifest
