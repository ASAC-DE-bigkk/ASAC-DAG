from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import sys

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.errors import (
    TrafficBronzeConfigurationError,
    TrafficCompletenessError,
)
from traffic_ingest.link_reference_backfill import (
    build_backfill_link_sql,
    land_backfill_batch,
    materialize_backfill_batch,
    parse_backfill_conf,
    resolve_backfill_link_ids,
)


LINK_A = "1220003800"
LINK_B = "1220003900"
LINK_C = "1220004000"


class Cursor:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.statements: list[str] = []

    def execute(self, statement):
        self.statements.append(statement)

    def fetchall(self):
        return list(self.rows)


def test_parse_backfill_conf_has_bounded_defaults_and_normalizes_explicit_links():
    assert asdict(parse_backfill_conf({})) == {
        "start_after_link_id": None,
        "batch_size": 100,
        "force_refresh": False,
        "link_ids": None,
    }
    assert asdict(
        parse_backfill_conf(
            {
                "batch_size": 2,
                "force_refresh": True,
                "link_ids": f"{LINK_B},{LINK_A},{LINK_B}",
            }
        )
    ) == {
        "start_after_link_id": None,
        "batch_size": 2,
        "force_refresh": True,
        "link_ids": [LINK_B, LINK_A],
    }


@pytest.mark.parametrize("batch_size", [0, 501, True, "100", 3.5])
def test_backfill_rejects_invalid_batch_size(batch_size):
    with pytest.raises(TrafficBronzeConfigurationError, match="batch_size"):
        parse_backfill_conf({"batch_size": batch_size})


@pytest.mark.parametrize("force_refresh", [1, "true", None])
def test_backfill_rejects_non_boolean_force_refresh(force_refresh):
    with pytest.raises(TrafficBronzeConfigurationError, match="force_refresh"):
        parse_backfill_conf({"force_refresh": force_refresh})


def test_backfill_rejects_cursor_with_explicit_links_and_oversized_explicit_batch():
    with pytest.raises(TrafficBronzeConfigurationError, match="cannot be combined"):
        parse_backfill_conf(
            {"start_after_link_id": LINK_A, "link_ids": [LINK_B]}
        )
    with pytest.raises(TrafficBronzeConfigurationError, match="exceeds batch_size"):
        parse_backfill_conf(
            {"batch_size": 1, "link_ids": [LINK_A, LINK_B]}
        )


def test_backfill_resolves_distinct_incident_and_flow_links_after_cursor():
    cursor = Cursor(rows=[(LINK_B,), (LINK_C,)])

    result = resolve_backfill_link_ids(
        {"start_after_link_id": LINK_A, "batch_size": 2},
        cursor_factory=lambda: (cursor, "iceberg_dev", "ask_seoul_road_755"),
    )

    assert result == [LINK_B, LINK_C]
    sql = " ".join(cursor.statements[0].split())
    assert "bronze_seoul_traffic_incident" in sql
    assert "bronze_seoul_traffic_flow" in sql
    assert "UNION" in sql.upper()
    assert f"WHERE link_id > '{LINK_A}'" in sql
    assert "ORDER BY link_id" in sql
    assert "LIMIT 2" in sql


def test_backfill_link_sql_uses_only_validated_identifiers_and_exclusive_cursor():
    sql = build_backfill_link_sql(
        catalog="iceberg_dev",
        schema="ask_seoul_road_755",
        start_after_link_id=LINK_A,
        batch_size=500,
    )

    assert "iceberg_dev.ask_seoul_road_755.bronze_seoul_traffic_incident" in sql
    assert "iceberg_dev.ask_seoul_road_755.bronze_seoul_traffic_flow" in sql
    assert f"WHERE link_id > '{LINK_A}'" in sql
    assert "LIMIT 500" in sql


def test_explicit_link_ids_do_not_open_cursor():
    assert resolve_backfill_link_ids(
        {"link_ids": [LINK_B, LINK_A], "batch_size": 2},
        cursor_factory=lambda: pytest.fail("explicit links must not query Trino"),
    ) == [LINK_B, LINK_A]


class Landing:
    def __init__(self):
        self.calls = []

    def collect(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "raw_objects": ["descriptor"] if kwargs["link_ids"] else [],
            "manifest_key": "raw/manifest.json" if kwargs["link_ids"] else None,
        }


def test_land_backfill_fetches_only_cache_misses_and_returns_resume_cursor():
    landing = Landing()

    result = land_backfill_batch(
        conf={"link_ids": [LINK_A, LINK_B], "batch_size": 2},
        dag_run_id="manual__backfill-1",
        landing=landing,
        cursor_factory=lambda: pytest.fail("explicit links must not query universe"),
        unresolved_link_ids=lambda link_ids: [link_ids[-1]],
    )

    assert landing.calls == [
        {
            "link_ids": [LINK_B],
            "dag_run_id": "manual__backfill-1",
            "landing_load_date": None,
        }
    ]
    assert result["requested_link_ids"] == [LINK_A, LINK_B]
    assert result["unresolved_link_ids"] == [LINK_B]
    assert result["last_processed_link_id"] == LINK_B
    assert result["processed_link_count"] == 2
    assert result["remaining_unknown"] is True


def test_land_backfill_force_refresh_fetches_every_candidate():
    landing = Landing()

    land_backfill_batch(
        conf={
            "link_ids": [LINK_A, LINK_B],
            "batch_size": 3,
            "force_refresh": True,
        },
        dag_run_id="manual__backfill-force",
        landing=landing,
        cursor_factory=lambda: pytest.fail("explicit links must not query universe"),
        unresolved_link_ids=lambda _link_ids: pytest.fail(
            "force refresh must not query the cache before landing"
        ),
    )

    assert landing.calls[0]["link_ids"] == [LINK_A, LINK_B]


def test_materialize_backfill_loads_new_pairs_verifies_run_and_rechecks_all_links():
    calls = []
    raw_result = {
        "requested_link_ids": [LINK_A, LINK_B],
        "unresolved_link_ids": [LINK_B],
        "last_processed_link_id": LINK_B,
        "processed_link_count": 2,
        "remaining_unknown": True,
        "raw_objects": ["descriptor"],
    }

    result = materialize_backfill_batch(
        raw_result=raw_result,
        dag_run_id="manual__backfill-1",
        load_batch=lambda **kwargs: calls.append(("load", kwargs))
        or {
            "inserted_info": 1,
            "inserted_vertices": 3,
            "audit_rows": 2,
        },
        verify_runtime=lambda **kwargs: calls.append(("verify", kwargs))
        or {"info_rows": 1, "vertex_rows": 3, "raw_objects": 2},
        unresolved_link_ids=lambda link_ids: calls.append(("cache", link_ids))
        or [],
    )

    assert calls[0] == (
        "load",
        {"raw_result": raw_result, "dag_run_id": "manual__backfill-1"},
    )
    assert calls[1][0] == "verify"
    assert calls[2] == ("cache", [LINK_A, LINK_B])
    assert result["verification"]["vertex_rows"] == 3
    assert result["last_processed_link_id"] == LINK_B
    assert result["processed_link_count"] == 2
    assert result["remaining_unknown"] is True


def test_materialize_cache_only_batch_skips_loader_and_verifier():
    result = materialize_backfill_batch(
        raw_result={
            "requested_link_ids": [LINK_A],
            "unresolved_link_ids": [],
            "last_processed_link_id": LINK_A,
            "processed_link_count": 1,
            "remaining_unknown": False,
        },
        dag_run_id="manual__cache-only",
        load_batch=lambda **_kwargs: pytest.fail("cache hit must not load"),
        verify_runtime=lambda **_kwargs: pytest.fail("cache hit must not verify run"),
        unresolved_link_ids=lambda _link_ids: [],
    )

    assert result["inserted_info"] == 0
    assert result["inserted_vertices"] == 0
    assert result["verification"]["audit_rows"] == 0


def test_materialize_fails_closed_when_any_requested_link_remains_unresolved():
    with pytest.raises(TrafficCompletenessError, match=LINK_B):
        materialize_backfill_batch(
            raw_result={
                "requested_link_ids": [LINK_A, LINK_B],
                "unresolved_link_ids": [],
                "last_processed_link_id": LINK_B,
                "processed_link_count": 2,
                "remaining_unknown": False,
            },
            dag_run_id="manual__incomplete",
            load_batch=lambda **_kwargs: pytest.fail("cache-only batch must not load"),
            verify_runtime=lambda **_kwargs: pytest.fail(
                "cache-only batch must not verify run"
            ),
            unresolved_link_ids=lambda _link_ids: [LINK_B],
        )
