"""Behavioral oracle for the common D1 Publisher.

Exercises the full Publication pipeline (gate → write → verify → _catalog → smoke)
against in-memory fakes: no Trino, no Cloudflare, no Airflow, no prod D1.
"""

from __future__ import annotations

import json
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
        self.read_table_rows_calls: list[tuple[str, list[Column], tuple[str, ...]]] = []
        self.product_meta: dict[str, dict[str, Any]] = {}  # 핸드오프 메타(#638) 게시 기록
        self.product_evidence: dict[str, dict[str, Any]] = {}  # V1 source/quality evidence (#678)

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

    def read_table_rows(self, name, ordered_columns, primary_key):
        self.read_table_rows_calls.append((name, list(ordered_columns), tuple(primary_key)))
        rows = sorted(
            self.tables.get(name, []),
            key=lambda row: tuple(row.get(column) for column in primary_key),
        )
        return [
            {column: row.get(column) for column, _type in ordered_columns}
            for row in rows
        ]

    def publish_product_meta(self, product_id, publication_id, columns_rows, ext_rows, pattern_rows) -> None:
        self.product_meta[product_id] = {
            "publication_id": publication_id,
            "columns": [dict(row) for row in columns_rows],
            "ext": [dict(row) for row in ext_rows],
            "patterns": [dict(row) for row in pattern_rows],
        }

    def publish_product_evidence(self, product_id, publication_id, sources, quality) -> None:
        self.product_evidence[product_id] = {
            "publication_id": publication_id,
            "sources": None if sources is None else [dict(source) for source in sources],
            "quality": dict(quality),
        }


class ForgetfulCatalogD1(FakeD1):
    """Writes tables but 'forgets' to register _catalog — reproduces the #477 bug."""

    def upsert_catalog(self, catalog_rows) -> None:  # noqa: D401 - intentional no-op
        pass


class ExplodingCatalogD1(FakeD1):
    """Fails after snapshot promotion, before a new catalog value is committed."""

    def upsert_catalog(self, catalog_rows) -> None:
        raise RuntimeError("simulated catalog write failure")


class ExplodingMetaD1(FakeD1):
    """Fails in the handoff-meta step (#638), after the catalog upsert committed."""

    def publish_product_meta(self, *args, **kwargs) -> None:
        raise RuntimeError("simulated product meta write failure")


class ExplodingEvidenceD1(FakeD1):
    """Fails after catalog/meta publication to exercise the #678 rollback boundary."""

    def publish_product_evidence(self, *args, **kwargs) -> None:
        raise RuntimeError("simulated product evidence write failure")


class CorruptingReadBackD1(FakeD1):
    def read_table_rows(self, name, ordered_columns, primary_key):
        rows = super().read_table_rows(name, ordered_columns, primary_key)
        rows[0]["place_id"] = "corrupted"
        return rows


class ExplodingReadBackD1(FakeD1):
    def read_table_rows(self, name, ordered_columns, primary_key):
        super().read_table_rows(name, ordered_columns, primary_key)
        raise RuntimeError("simulated D1 read-back failure")


class ExplodingPrimaryKeyStatsD1(FakeD1):
    def primary_key_stats(self, name: str, primary_key) -> tuple[int, int, int]:
        if name in self.previous_tables:
            raise RuntimeError("simulated activated read-back failure")
        return super().primary_key_stats(name, primary_key)


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


def _projected_contract(**overrides: Any) -> ServingContract:
    return _contract(
        public_projection=("product_row_id", "place_id", "forecast_at"),
        projection_schema_hash="projection-hash-1",
        **overrides,
    )


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


def test_opted_in_snapshot_records_matching_source_and_d1_content_hashes():
    contract = _projected_contract()
    d1 = FakeD1()
    rows = list(reversed(_rows(3)))  # source order must not matter
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=rows)})

    report = publish(
        [contract],
        source,
        d1,
        FakeSmoke(status="passed"),
        source_run_id="content-ok",
        verify_content_parity=True,
    )

    record = report.records[0]
    assert record.serving_status == STATUS_PUBLISHED
    assert record.projection_schema_hash == "projection-hash-1"
    assert record.source_content_hash
    assert record.source_content_hash == record.d1_content_hash
    assert d1.read_table_rows_calls == [(contract.model_name, COLUMNS, ("product_row_id",))]


def test_opted_in_content_mismatch_restores_lkg_before_smoke_catalog_or_meta():
    contract = _projected_contract()
    d1 = CorruptingReadBackD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    smoke = FakeSmoke(status="passed")

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
            d1,
            smoke,
            source_run_id="content-mismatch",
            verify_content_parity=True,
        )

    record = excinfo.value.report.records[0]
    assert record.stage == "content_parity"
    assert record.rollback_status == "restored"
    assert record.source_content_hash and record.d1_content_hash
    assert record.source_content_hash != record.d1_content_hash
    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert d1.product_meta == {}
    assert smoke.checked == []
    assert d1.ledger[-1]["stage"] == "content_parity"


def test_opted_in_d1_row_read_exception_after_activation_restores_lkg():
    contract = _projected_contract()
    d1 = ExplodingReadBackD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
            d1,
            FakeSmoke(status="passed"),
            source_run_id="content-read-failure",
            verify_content_parity=True,
        )

    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert excinfo.value.report.records[0].stage == "content_parity"
    assert any("content parity" in failure for failure in excinfo.value.report.failures)


@pytest.mark.parametrize(
    "contract,error",
    [
        (_contract(projection_schema_hash="projection-hash-1"), "public_projection"),
        (_contract(public_projection=("product_row_id", "place_id", "forecast_at")), "projection_schema_hash"),
        (
            _contract(
                public_projection=("product_row_id", "place_id", "forecast_at"),
                projection_schema_hash="projection-hash-1",
                primary_key=(),
            ),
            "primary_key",
        ),
    ],
)
def test_content_parity_requires_projection_hash_and_primary_key_before_physical_write(contract, error):
    d1 = FakeD1()

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(1))}),
            d1,
            FakeSmoke(status="passed"),
            source_run_id="missing-projection-hash",
            verify_content_parity=True,
        )

    assert d1.replace_calls == 0
    assert d1.read_table_rows_calls == []
    assert excinfo.value.report.records[0].stage == "content_contract"
    assert error in excinfo.value.report.records[0].reason


def test_default_legacy_publication_does_not_read_full_d1_content_rows():
    contract = _contract()
    d1 = FakeD1()

    report = publish(
        [contract],
        FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(1))}),
        d1,
        FakeSmoke(status="passed"),
        source_run_id="legacy-no-content-read",
    )

    assert report.ok
    assert d1.read_table_rows_calls == []
    assert report.records[0].source_content_hash is None
    assert report.records[0].d1_content_hash is None


def test_snapshot_catalog_carries_static_contract_and_runtime_publication_id():
    contract = _contract(
        public_gold={
            "quality": {"coverage_explanation": "부분 커버리지입니다."},
            "time": {"canonical_timezone": "Asia/Seoul"},
        },
        mcp_projection={
            "operation": {"id": "weather.get_current_outlook"},
            "question_examples": ["가", "나", "다"],
        },
    )
    d1 = FakeD1()
    source = FakeSource(
        {contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(1))}
    )

    publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="run-rich")

    catalog = d1.catalog[contract.model_name]
    assert json.loads(catalog["public_gold"])["time"]["canonical_timezone"] == "Asia/Seoul"
    assert json.loads(catalog["mcp_projection"])["operation"]["id"] == "weather.get_current_outlook"
    assert catalog["publication_id"]


def test_snapshot_publish_writes_product_meta_rows():
    """핸드오프 메타(#638 §2.2) — 컬럼 설명·ext·질의 예시가 계약 선언에서 그대로 게시된다."""
    contract = _contract(
        grain="place_id마다 한 행.",
        column_descriptions={"product_row_id": "행 식별자", "place_id": ""},
        usage_patterns=(
            {
                "pattern_id": "hottest_places_now",
                "question_ko": "지금 가장 더운 장소는?",
                "axes": "장소 랭킹",
                "requires": ["select_columns", "sort"],
                "verified_rows": 10,
                "verified_at": "2026-07-30T09:00:00Z",
                "verified_publication_id": "prev-pub",
                "sql": "SELECT 1",
            },
            {"pattern_id": "sql_missing_dropped"},  # sql 없는 선언은 게시 제외
        ),
    )
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(2))})

    report = publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="run-meta")

    assert report.ok
    meta = d1.product_meta[contract.product_id]
    assert meta["publication_id"] == report.records[0].publication_id
    by_name = {row["column_name"]: row for row in meta["columns"]}
    assert by_name["product_row_id"]["description_ko"] == "행 식별자"
    assert by_name["place_id"]["description_ko"] is None  # 빈 설명은 NULL — 컬럼 행은 게시
    assert by_name["forecast_at"]["type"] == "TEXT"  # timestamp → D1 실물 타입
    assert meta["ext"][0]["grain"] == "place_id마다 한 행."
    assert meta["ext"][0]["primary_key"] == json.dumps(["product_row_id"], ensure_ascii=False)
    assert meta["ext"][0]["time_axis"] == "forecast_at"
    assert meta["ext"][0]["tier"] is None  # 물리 확장 미선언 도메인 — NULL
    patterns = meta["patterns"]
    assert [row["pattern_id"] for row in patterns] == ["hottest_places_now"]
    assert patterns[0]["requires"] == json.dumps(["select_columns", "sort"], ensure_ascii=False)
    assert patterns[0]["verified_publication_id"] == "prev-pub"
    assert patterns[0]["allow_empty"] == 0
    assert patterns[0]["publication_id"] == report.records[0].publication_id


def test_snapshot_publish_writes_source_and_quality_evidence():
    contract = _contract(
        freshness_slo_minutes=240,
        source_evidence=(
            {
                "source_id": "kma_vilage_fcst",
                "source_url": "https://example.test/kma",
                "license": "KOGL-1",
                "license_url": "https://example.test/kogl",
                "redistribution": "allowed_with_attribution",
                "attribution": "기상청",
                "rights_checked_at": "2026-08-04",
            },
        ),
    )
    d1 = FakeD1()

    report = publish(
        [contract],
        FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
        d1,
        FakeSmoke(status="passed"),
        source_run_id="run-evidence",
    )

    evidence = d1.product_evidence[contract.product_id]
    assert evidence["publication_id"] == report.records[0].publication_id
    assert evidence["sources"] == [dict(contract.source_evidence[0])]
    assert evidence["quality"] == {
        "source_row_count": 3,
        "d1_row_count": 3,
        "duplicate_primary_key_count": 0,
        "null_primary_key_count": 0,
        "freshness_as_of": "2026-07-22T00:00:00",
        "freshness_slo_minutes": 240,
        "serving_status": STATUS_PUBLISHED,
        "measured_at": report.records[0].published_at,
        "coverage": None,
        "projection_schema_version": None,
        "projection_schema_hash": None,
    }


def test_snapshot_publish_records_passing_distinct_coverage_gate():
    contract = _contract(
        quality_coverage={
            "field": "place_id",
            "expected_distinct_count": 1,
            "minimum_ratio": 1.0,
        }
    )
    d1 = FakeD1()

    publish(
        [contract],
        FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
        d1,
        FakeSmoke(status="passed"),
        source_run_id="coverage-passing",
    )

    assert d1.product_evidence[contract.product_id]["quality"]["coverage"] == {
        "field": "place_id",
        "expected_distinct_count": 1,
        "observed_distinct_count": 1,
        "minimum_ratio": 1.0,
        "ratio": 1.0,
        "status": "passed",
    }


def test_snapshot_publish_uses_source_relation_coverage_observation():
    contract = _contract(
        quality_coverage={
            "field": "dataset",
            "expected_distinct_count": 152,
            "minimum_ratio": 0.95,
            "measurement_scope": "source_relation",
        }
    )
    d1 = FakeD1()

    publish(
        [contract],
        FakeSource({
            contract.model_name: ReadPlan(
                columns=COLUMNS,
                rows=_rows(3),
                coverage_observed_distinct_count=147,
            )
        }),
        d1,
        FakeSmoke(status="passed"),
        source_run_id="coverage-source-relation",
    )

    assert d1.product_evidence[contract.product_id]["quality"]["coverage"] == {
        "field": "dataset",
        "expected_distinct_count": 152,
        "observed_distinct_count": 147,
        "minimum_ratio": 0.95,
        "ratio": 147 / 152,
        "status": "passed",
    }


def test_snapshot_publish_records_explicit_not_applicable_coverage():
    contract = _contract(
        quality_coverage={
            "not_applicable_reason": "eligible source population is dynamic",
        }
    )
    d1 = FakeD1()

    publish(
        [contract],
        FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
        d1,
        FakeSmoke(status="passed"),
        source_run_id="coverage-not-applicable",
    )

    assert d1.product_evidence[contract.product_id]["quality"]["coverage"] == {
        "status": "not_applicable",
        "reason": "eligible source population is dynamic",
    }


def test_snapshot_publish_rejects_coverage_below_contract_threshold_before_write():
    contract = _contract(
        quality_coverage={
            "field": "place_id",
            "expected_distinct_count": 2,
            "minimum_ratio": 1.0,
        }
    )
    d1 = FakeD1()

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
            d1,
            FakeSmoke(status="passed"),
            source_run_id="coverage-failing",
        )

    assert d1.replace_calls == 0
    record = excinfo.value.report.records[0]
    assert record.stage == "quality_coverage"
    assert "observed=1" in record.reason


def test_skip_retain_does_not_touch_product_meta():
    """스킵 제품은 upsert 자체를 건너뛴다 — 직전 메타·권리 증거가 자연 보존."""
    contract = _contract(zero_policy="retain_last_good")
    d1 = FakeD1()
    d1.tables[contract.model_name] = _rows(5)
    d1.catalog[contract.model_name] = {"name": contract.model_name, "row_count": 5}
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=[])})

    report = publish([contract], source, d1, FakeSmoke(), source_run_id="run-skip-meta")

    assert report.records[0].serving_status == STATUS_SKIPPED
    assert contract.product_id not in d1.product_meta
    assert contract.product_id not in d1.product_evidence


def test_product_meta_failure_restores_snapshot_and_catalog():
    """메타 게시 실패도 catalog 스테이지 롤백 경로를 탄다 — 스냅샷·_catalog 복원, 스테이지 기록."""
    contract = _contract()
    d1 = ExplodingMetaD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="meta-failure")

    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert d1.ledger[-1]["outcome"] == "failed"
    assert d1.ledger[-1]["stage"] == "product_meta"
    assert d1.ledger[-1]["rollback_status"] == "restored"
    assert any("product_meta 실패" in failure for failure in excinfo.value.report.failures)


def test_product_evidence_failure_restores_snapshot_and_catalog():
    """권리/품질 게시 실패는 새 snapshot을 live로 남기지 않는다."""
    contract = _contract()
    d1 = ExplodingEvidenceD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
            d1,
            FakeSmoke(status="passed"),
            source_run_id="evidence-failure",
        )

    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert d1.ledger[-1]["stage"] == "product_evidence"
    assert d1.ledger[-1]["rollback_status"] == "restored"
    assert any("product_evidence 실패" in failure for failure in excinfo.value.report.failures)


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


def test_snapshot_pk_readback_exception_after_activation_restores_last_good():
    contract = _contract()
    d1 = ExplodingPrimaryKeyStatsD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="pk-readback-failure")

    record = excinfo.value.report.records[0]
    assert record.stage == "read_back"
    assert record.rollback_status == "restored"
    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert any("primary key read-back" in failure for failure in excinfo.value.report.failures)


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
    # 핸드오프 메타(#638) — manifest 의 컬럼 설명·usage_patterns 가 계약까지 실려 온다
    assert contract.grain == "place_id마다 한 행."
    assert contract.column_descriptions == {
        "product_row_id": "행 식별자(장소).",
        "place_id": "서울 121 장소 코드.",
        "forecast_at": "",
    }
    assert [p["pattern_id"] for p in contract.usage_patterns] == [
        "hottest_places_now",
        "missing_sql_dropped",  # 계약은 선언 그대로 나른다 — sql 필터는 게시 시점(_product_meta_rows)
    ]
    assert contract.usage_patterns[0]["verified_at"] == "2026-07-30T09:00:00Z"
    assert contract.serving_tier is None and contract.rollup_rule is None


# ── incremental upsert (부분 쓰기 — D1 절약) ─────────────────────────────────────────

def _seed_full_table(d1: "FakeD1", model: str, rows: list[dict[str, Any]]) -> None:
    """직전-정상(전체) 테이블을 D1 에 미리 심는다 — incremental 은 이 위에 부분 upsert 한다."""
    d1.tables[model] = [dict(r) for r in rows]
    d1.primary_keys[model] = ("product_row_id",)
    d1.columns_by_table[model] = list(COLUMNS)
    d1.catalog[model] = {"name": model, "row_count": len(rows)}


def test_incremental_upsert_partial_write_passes_and_merges():
    """incremental upsert: 바뀐 그레인(2행)만 써도 전체-테이블 parity 로 실패하지 않고,
    나머지 D1 행은 보존한 채 해당 PK 만 덮인다(부분 INSERT OR REPLACE)."""
    contract = _contract(
        publication_mode="upsert",
        upsert_strategy="incremental",
        zero_policy="retain_last_good",
        event_time="forecast_at",
    )
    model = contract.model_name
    d1 = FakeD1()
    # 직전본 3행(오래된 forecast_at) — 워터마크 = max = 2026-07-22T00:00:00
    _seed_full_table(d1, model, [
        {"product_row_id": "r0", "place_id": "p", "forecast_at": "2026-07-20T00:00:00"},
        {"product_row_id": "r1", "place_id": "p", "forecast_at": "2026-07-21T00:00:00"},
        {"product_row_id": "r2", "place_id": "p", "forecast_at": "2026-07-22T00:00:00"},
    ])
    # 소스는 바뀐 것만: r1 갱신(place_id 변경) + r3 신규 — 둘 다 워터마크보다 최신
    changed = [
        {"product_row_id": "r1", "place_id": "UPDATED", "forecast_at": "2026-07-25T00:00:00"},
        {"product_row_id": "r3", "place_id": "p", "forecast_at": "2026-07-25T00:00:00"},
    ]
    source = FakeSource({model: ReadPlan(columns=COLUMNS, rows=changed)})

    report = publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="inc-1")

    assert report.ok
    rec = report.records[0]
    assert rec.serving_status == STATUS_PUBLISHED
    # 부분 소스(2) ≠ 전체 D1(4) 인데도 통과 — incremental 은 전체-parity 면제
    assert rec.source_row_count == 2
    assert rec.d1_row_count == 4
    assert rec.distinct_primary_key_count == 4 and rec.null_primary_key_count == 0
    # 워터마크(=직전 max event_time)가 reader 로 전달됐다
    assert source.seen_last_good_max[model] == "2026-07-22T00:00:00"
    # 부분 경로 — 전체 교체(replace_table) 는 호출되지 않았다
    assert d1.replace_calls == 0
    # 병합 결과: r0/r2 보존, r1 갱신, r3 추가
    by_id = {r["product_row_id"]: r for r in d1.tables[model]}
    assert set(by_id) == {"r0", "r1", "r2", "r3"}
    assert by_id["r1"]["place_id"] == "UPDATED"
    assert by_id["r0"]["place_id"] == "p"


def test_incremental_upsert_first_run_backfills_full_when_no_watermark():
    """최초 실행(D1 빈 테이블 → 워터마크 없음): reader 가 전량 읽어 백필하고 정상 게시."""
    contract = _contract(
        publication_mode="upsert",
        upsert_strategy="incremental",
        zero_policy="retain_last_good",
        event_time="forecast_at",
    )
    model = contract.model_name
    d1 = FakeD1()
    source = FakeSource({model: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    report = publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="inc-backfill")

    assert report.ok
    assert source.seen_last_good_max[model] is None  # 빈 테이블 → 워터마크 없음
    assert d1.table_row_count(model) == 3
    assert report.records[0].serving_status == STATUS_PUBLISHED


def test_incremental_upsert_rejects_content_parity():
    """incremental upsert 는 verify_content_parity 와 조합 불가(부분 소스 vs 전체 D1)."""
    contract = _contract(
        publication_mode="upsert",
        upsert_strategy="incremental",
        zero_policy="retain_last_good",
        event_time="forecast_at",
    )
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(2))})

    with pytest.raises(PublicationError) as exc:
        publish(
            [contract], source, d1, FakeSmoke(status="passed"),
            source_run_id="inc-parity", verify_content_parity=True,
        )
    assert "verify_content_parity" in str(exc.value)


def test_plain_upsert_still_enforces_full_parity():
    """비-incremental upsert(전량)는 종전대로 전체-테이블 parity 를 강제한다(회귀 방지)."""
    contract = _contract(
        publication_mode="upsert",
        zero_policy="retain_last_good",
        event_time="forecast_at",
    )
    model = contract.model_name
    d1 = FakeD1()
    _seed_full_table(d1, model, [
        {"product_row_id": "r0", "place_id": "p", "forecast_at": "2026-07-20T00:00:00"},
        {"product_row_id": "r1", "place_id": "p", "forecast_at": "2026-07-21T00:00:00"},
    ])
    # 부분 소스(1행)를 전량 upsert 로 쓰면 D1(2행) ≠ source(1) → parity 실패해야 정상
    source = FakeSource({model: ReadPlan(
        columns=COLUMNS, rows=[{"product_row_id": "r0", "place_id": "X", "forecast_at": "2026-07-25T00:00:00"}])})

    with pytest.raises(PublicationError):
        publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="plain-upsert")
