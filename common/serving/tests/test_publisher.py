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
        self.catalog: dict[str, dict[str, Any]] = {}

    def table_row_count(self, name: str) -> int:
        return len(self.tables.get(name, []))

    def table_max(self, name: str, column: str) -> Any | None:
        values = [r.get(column) for r in self.tables.get(name, []) if r.get(column) is not None]
        return max(values) if values else None

    def catalog_row(self, name: str) -> dict[str, Any] | None:
        return dict(self.catalog[name]) if name in self.catalog else None

    def ensure_table(self, name: str, columns) -> None:
        self.tables.setdefault(name, [])

    def replace_table(self, name: str, columns, rows) -> None:
        self.tables[name] = [dict(r) for r in rows]  # atomic swap semantics

    def delete_where_gte(self, name: str, column: str, trino_literal: str) -> None:
        cutoff = trino_literal.strip().strip("'")
        self.tables[name] = [r for r in self.tables.get(name, []) if str(r.get(column)) < cutoff]

    def insert_rows(self, name: str, columns, rows, *, replace: bool) -> None:
        self.tables.setdefault(name, []).extend(dict(r) for r in rows)

    def upsert_catalog(self, catalog_rows) -> None:
        for row in catalog_rows:
            self.catalog[row["name"]] = dict(row)

    def catalog_domain_count(self, model_names: set[str]) -> int:
        return sum(1 for n in model_names if n in self.catalog)


class ForgetfulCatalogD1(FakeD1):
    """Writes tables but 'forgets' to register _catalog — reproduces the #477 bug."""

    def upsert_catalog(self, catalog_rows) -> None:  # noqa: D401 - intentional no-op
        pass


class FakeSource:
    def __init__(self, plans: dict[str, ReadPlan]) -> None:
        self.plans = plans
        self.seen_last_good_max: dict[str, Any] = {}

    def read(self, contract: ServingContract, last_good_max: Any | None) -> ReadPlan:
        self.seen_last_good_max[contract.model_name] = last_good_max
        return self.plans[contract.model_name]


class FakeSmoke:
    def __init__(self, ok: bool = True) -> None:
        self.ok = ok
        self.checked: list[str] = []

    def check(self, model_name: str) -> bool:
        self.checked.append(model_name)
        return self.ok


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
    smoke = FakeSmoke(ok=True)

    report = publish([contract], source, d1, smoke, source_run_id="run-1")

    assert report.ok
    assert d1.table_row_count(contract.model_name) == 3
    rec = report.records[0]
    assert rec.serving_status == STATUS_PUBLISHED
    assert rec.source_run_id == "run-1"
    assert rec.publication_id and rec.published_row_count == 3
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
    assert d1.table_row_count(contract.model_name) == 3  # data written, but registration missing => fail


def test_smoke_failure_on_external_product_raises():
    contract = _contract()
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(2))})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(ok=False), source_run_id="run-7")
    assert any("smoke" in f for f in excinfo.value.report.failures)


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
    d1.tables[contract.model_name] = [{"event_at": "2026-07-01 00:00:00"}, {"event_at": "2026-07-02 00:00:00"}]
    d1.catalog[contract.model_name] = {"name": contract.model_name, "row_count": 2}
    plan = ReadPlan(
        columns=[("event_at", "timestamp")],
        rows=[{"event_at": "2026-07-02 00:00:00"}, {"event_at": "2026-07-03 00:00:00"}],
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
