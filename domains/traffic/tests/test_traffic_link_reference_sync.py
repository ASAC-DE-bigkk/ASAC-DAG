from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.errors import TrafficBronzeConfigurationError  # noqa: E402


def _runtime():
    from traffic_ingest import link_reference_sync

    return link_reference_sync


def test_incremental_sql_prioritizes_missing_then_oldest_complete_pair():
    runtime = _runtime()

    sql = runtime.build_incremental_sync_link_sql(
        catalog="iceberg_dev",
        schema="traffic",
        stale_before=datetime(2026, 7, 11, 3, 37, tzinfo=timezone.utc),
        batch_size=100,
    )

    assert "iceberg_dev.traffic.bronze_seoul_traffic_incident" in sql
    assert "iceberg_dev.traffic.bronze_seoul_traffic_flow" in sql
    assert "iceberg_dev.traffic.bronze_seoul_traffic_link_request_audit" in sql
    assert "iceberg_dev.traffic.bronze_seoul_traffic_link_info" in sql
    assert "iceberg_dev.traffic.bronze_seoul_traffic_link_vertex" in sql
    assert "GROUP BY link_id, dag_run_id" in sql
    assert "info_success_count = 1" in sql
    assert "vertex_success_count = 1" in sql
    assert "vertex_sequence_distinct_count = vertex_actual_count" in sql
    assert "latest_complete_collected_at IS NULL" in sql
    assert "TIMESTAMP '2026-07-11 03:37:00.000000'" in sql
    assert "CASE WHEN latest_complete_collected_at IS NULL THEN 0 ELSE 1 END" in sql
    assert "latest_complete_collected_at ASC" in sql
    assert "link_universe.link_id ASC" in sql
    assert "LIMIT 100" in sql


@pytest.mark.parametrize("batch_size", [0, 501, True, "100"])
def test_incremental_sync_rejects_invalid_batch_size(batch_size):
    runtime = _runtime()

    with pytest.raises(TrafficBronzeConfigurationError, match="batch_size"):
        runtime.validate_incremental_sync_request(
            batch_size=batch_size,
            stale_after_days=30,
        )


@pytest.mark.parametrize("stale_after_days", [0, 366, True, "30"])
def test_incremental_sync_rejects_invalid_stale_days(stale_after_days):
    runtime = _runtime()

    with pytest.raises(TrafficBronzeConfigurationError, match="stale_after_days"):
        runtime.validate_incremental_sync_request(
            batch_size=100,
            stale_after_days=stale_after_days,
        )


def test_incremental_resolver_uses_exact_utc_cutoff_and_preserves_query_order():
    runtime = _runtime()

    class Cursor:
        def __init__(self):
            self.sql = None

        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [("1220003900",), ("1220004000",)]

    cursor = Cursor()
    result = runtime.resolve_incremental_sync_link_ids(
        batch_size=2,
        stale_after_days=30,
        now=datetime(2026, 8, 10, 3, 37, tzinfo=timezone.utc),
        cursor_factory=lambda: (cursor, "iceberg_dev", "traffic"),
    )

    assert result == ["1220003900", "1220004000"]
    assert "TIMESTAMP '2026-07-11 03:37:00.000000'" in cursor.sql
    assert "LIMIT 2" in cursor.sql


def test_incremental_landing_force_refreshes_only_selected_candidates():
    runtime = _runtime()
    calls = []
    expected = {
        "requested_link_ids": ["1220003900", "1220004000"],
        "unresolved_link_ids": ["1220003900", "1220004000"],
    }

    result = runtime.land_incremental_sync_batch(
        link_ids=["1220003900", "1220004000"],
        dag_run_id="scheduled__2026-08-10T03:37:00+09:00",
        land_batch=lambda **kwargs: calls.append(kwargs) or expected,
    )

    assert result is expected
    assert calls == [
        {
            "conf": {
                "link_ids": ["1220003900", "1220004000"],
                "batch_size": 2,
                "force_refresh": True,
            },
            "dag_run_id": "scheduled__2026-08-10T03:37:00+09:00",
        }
    ]


def test_incremental_materialization_reuses_complete_pair_verification():
    runtime = _runtime()
    calls = []
    raw_result = {
        "requested_link_ids": ["1220003900"],
        "unresolved_link_ids": ["1220003900"],
    }
    expected = {"inserted_info": 1, "inserted_vertices": 3}

    result = runtime.materialize_incremental_sync_batch(
        raw_result=raw_result,
        dag_run_id="scheduled__2026-08-10T03:37:00+09:00",
        materialize_batch=lambda **kwargs: calls.append(kwargs) or expected,
    )

    assert result is expected
    assert calls == [
        {
            "raw_result": raw_result,
            "dag_run_id": "scheduled__2026-08-10T03:37:00+09:00",
        }
    ]
