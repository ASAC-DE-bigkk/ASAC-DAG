import pytest
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.external_snapshot import (
    ExternalSnapshotUnavailableError,
    resolve_admin_dong_crosswalk_snapshot_id,
    resolve_citydata_crowding_snapshot_id,
)


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = None

    def execute(self, sql):
        self.sql = sql

    def fetchone(self):
        return self.rows[0] if self.rows else None


def test_resolve_citydata_crowding_snapshot_id_reads_latest_iceberg_snapshot():
    cursor = FakeCursor(rows=[(8738321387624398062,)])

    snapshot_id = resolve_citydata_crowding_snapshot_id(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze"),
        env={"SEOUL_CITYDATA_SCHEMA": "seoul_citydata"},
    )

    assert snapshot_id == 8738321387624398062
    assert (
        'iceberg_dev.seoul_citydata."gold_citydata_ppltn_by_time$snapshots"'
        in cursor.sql
    )
    assert "ORDER BY committed_at DESC, snapshot_id DESC" in cursor.sql
    assert cursor.sql.rstrip().endswith("LIMIT 1")


@pytest.mark.parametrize("row", [None, (None,), (0,), (-1,), ("not-a-number",)])
def test_resolve_citydata_crowding_snapshot_id_fails_closed(row):
    cursor = FakeCursor(rows=[] if row is None else [row])

    with pytest.raises(ExternalSnapshotUnavailableError):
        resolve_citydata_crowding_snapshot_id(
            cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze"),
            env={"SEOUL_CITYDATA_SCHEMA": "seoul_citydata"},
        )


def test_resolve_admin_dong_crosswalk_snapshot_id_reads_latest_iceberg_snapshot():
    cursor = FakeCursor(rows=[(8738321387624398062,)])

    snapshot_id = resolve_admin_dong_crosswalk_snapshot_id(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze"),
        env={"COMMON_SCHEMA": "common"},
    )

    assert snapshot_id == 8738321387624398062
    assert (
        'iceberg_dev.common."seoul_admin_dong_crosswalk$snapshots"' in cursor.sql
    )
    assert "ORDER BY committed_at DESC, snapshot_id DESC" in cursor.sql
    assert cursor.sql.rstrip().endswith("LIMIT 1")


def test_resolve_admin_dong_crosswalk_snapshot_id_fails_closed_when_no_snapshot_row():
    cursor = FakeCursor(rows=[])

    with pytest.raises(ExternalSnapshotUnavailableError, match="unavailable"):
        resolve_admin_dong_crosswalk_snapshot_id(
            cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze"),
            env={"COMMON_SCHEMA": "common"},
        )


@pytest.mark.parametrize("row", [(None,), (0,), (-1,), ("not-a-number",)])
def test_resolve_admin_dong_crosswalk_snapshot_id_fails_closed_on_invalid_id(row):
    cursor = FakeCursor(rows=[row])

    with pytest.raises(ExternalSnapshotUnavailableError, match="positive integer"):
        resolve_admin_dong_crosswalk_snapshot_id(
            cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze"),
            env={"COMMON_SCHEMA": "common"},
        )
