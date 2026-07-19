import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.silver_snapshot_fence import (
    ExternalCompactionRace,
    SilverSnapshotEvidence,
    SnapshotFenceTelemetryError,
    assert_safe_post_write,
    assert_snapshot_unchanged,
    collect_silver_snapshot_evidence,
)


def evidence(snapshot_id, operation, compacted_files):
    return SilverSnapshotEvidence(
        snapshot_id=snapshot_id,
        committed_at="2026-07-19T00:00:00Z",
        operation=operation,
        compacted_files=compacted_files,
    )


class FakeCursor:
    def __init__(self, snapshot_row, file_rows):
        self.snapshot_row = snapshot_row
        self.file_rows = file_rows
        self.statements = []
        self.closed = False

    def execute(self, statement):
        self.statements.append(statement)

    def fetchone(self):
        return self.snapshot_row

    def fetchall(self):
        return self.file_rows

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


def test_post_write_allows_existing_compacted_files_but_rejects_new_ones():
    baseline = evidence(10, "append", ("s3://dev/data/compacted-old.parquet",))
    safe = evidence(11, "overwrite", ("s3://dev/data/compacted-old.parquet",))
    assert_safe_post_write(baseline, safe)
    raced = evidence(
        12,
        "replace",
        (
            "s3://dev/data/compacted-old.parquet",
            "s3://dev/data/compacted-new.parquet",
        ),
    )
    with pytest.raises(ExternalCompactionRace, match="new managed-compaction"):
        assert_safe_post_write(baseline, raced)


def test_post_write_rejects_an_unexpected_replace_without_new_compacted_files():
    with pytest.raises(ExternalCompactionRace, match="unexpected replace"):
        assert_safe_post_write(evidence(10, "append", ()), evidence(11, "replace", ()))


def test_test_fence_requires_exact_post_write_snapshot():
    expected = evidence(11, "overwrite", ())
    assert_snapshot_unchanged(expected, expected)
    with pytest.raises(ExternalCompactionRace, match="snapshot changed"):
        assert_snapshot_unchanged(expected, evidence(12, "replace", ()))


def test_evidence_parser_rejects_malformed_telemetry_and_sorts_paths():
    parsed = SilverSnapshotEvidence.from_dict(
        {
            "snapshot_id": 11,
            "committed_at": "2026-07-19T00:00:00Z",
            "operation": "overwrite",
            "compacted_files": ["s3://dev/data/compacted-z.parquet", "s3://dev/data/compacted-a.parquet"],
        }
    )

    assert parsed.compacted_files == (
        "s3://dev/data/compacted-a.parquet",
        "s3://dev/data/compacted-z.parquet",
    )
    with pytest.raises(SnapshotFenceTelemetryError, match="fields are invalid"):
        SilverSnapshotEvidence.from_dict({"snapshot_id": 11})
    with pytest.raises(SnapshotFenceTelemetryError, match="snapshot id is invalid"):
        SilverSnapshotEvidence.from_dict(
            {
                "snapshot_id": True,
                "committed_at": "2026-07-19T00:00:00Z",
                "operation": "overwrite",
                "compacted_files": [],
            }
        )
    with pytest.raises(SnapshotFenceTelemetryError, match="compacted file list is invalid"):
        SilverSnapshotEvidence.from_dict(
            {
                "snapshot_id": 11,
                "committed_at": "2026-07-19T00:00:00Z",
                "operation": "overwrite",
                "compacted_files": [""],
            }
        )


def test_collect_evidence_queries_only_exact_dev_relation_and_closes_resources():
    cursor = FakeCursor(
        snapshot_row=(11, "2026-07-19T00:00:00Z", "overwrite"),
        file_rows=[
            ("s3://dev/data/compacted-z.parquet",),
            ("s3://dev/data/compacted-a.parquet",),
        ],
    )
    connection = FakeConnection(cursor)

    current = collect_silver_snapshot_evidence(connection_factory=lambda: connection)

    assert current == evidence(
        11,
        "overwrite",
        (
            "s3://dev/data/compacted-a.parquet",
            "s3://dev/data/compacted-z.parquet",
        ),
    )
    assert cursor.closed
    assert connection.closed
    assert len(cursor.statements) == 2
    assert all(
        'iceberg_dev.weather_traffic_bronze."silver_seoul_traffic_incident$'
        in statement
        for statement in cursor.statements
    )
    assert any("$snapshots\"" in statement for statement in cursor.statements)
    assert any("$files\"" in statement for statement in cursor.statements)
    assert all("SHOW TABLES" not in statement.upper() for statement in cursor.statements)


def test_collect_evidence_serializes_a_trino_snapshot_timestamp():
    cursor = FakeCursor(
        snapshot_row=(11, datetime(2026, 7, 19, tzinfo=timezone.utc), "overwrite"),
        file_rows=[],
    )

    current = collect_silver_snapshot_evidence(
        connection_factory=lambda: FakeConnection(cursor)
    )

    assert current.committed_at == "2026-07-19 00:00:00+00:00"


def test_collect_evidence_fails_closed_and_closes_resources_for_malformed_snapshot_row():
    cursor = FakeCursor(snapshot_row=(False, "2026-07-19T00:00:00Z", "overwrite"), file_rows=[])
    connection = FakeConnection(cursor)

    with pytest.raises(SnapshotFenceTelemetryError, match="snapshot id is invalid"):
        collect_silver_snapshot_evidence(connection_factory=lambda: connection)

    assert cursor.closed
    assert connection.closed
