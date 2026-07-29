import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_ingest.silver_snapshot_fence as snapshot_fence
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


def evidence_dict(**overrides):
    value = {
        "snapshot_id": 11,
        "committed_at": "2026-07-19T00:00:00Z",
        "operation": "overwrite",
        "compacted_files": [],
    }
    value.update(overrides)
    return value


class FakeCursor:
    def __init__(self, snapshot_row, file_rows, close_error=None):
        self.snapshot_row = snapshot_row
        self.file_rows = file_rows
        self.close_error = close_error
        self.statements = []
        self.closed = False
        self.close_calls = 0

    def execute(self, statement):
        self.statements.append(statement)

    def fetchone(self):
        return self.snapshot_row

    def fetchall(self):
        return self.file_rows

    def close(self):
        self.close_calls += 1
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class FakeConnection:
    def __init__(self, cursor, cursor_error=None, close_error=None):
        self._cursor = cursor
        self.cursor_error = cursor_error
        self.close_error = close_error
        self.closed = False
        self.close_calls = 0

    def cursor(self):
        if self.cursor_error is not None:
            raise self.cursor_error
        return self._cursor

    def close(self):
        self.close_calls += 1
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


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


@pytest.mark.parametrize(
    "changed_field",
    [
        {"committed_at": "2026-07-19T00:00:01Z"},
        {"operation": "append"},
        {"compacted_files": ("s3://dev/data/compacted-new.parquet",)},
    ],
)
def test_test_fence_rejects_same_id_with_different_immutable_evidence(changed_field):
    expected = evidence(11, "overwrite", ())

    with pytest.raises(ExternalCompactionRace, match="snapshot changed"):
        assert_snapshot_unchanged(expected, replace(expected, **changed_field))


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


@pytest.mark.parametrize("operation", ["append", "overwrite", "replace", "delete"])
def test_evidence_parser_accepts_only_needed_canonical_iceberg_operations(operation):
    assert SilverSnapshotEvidence.from_dict(
        evidence_dict(operation=operation)
    ).operation == operation


@pytest.mark.parametrize(
    "committed_at",
    [
        True,
        1721347200,
        datetime(2026, 7, 19, tzinfo=timezone.utc),
        None,
        "",
        "2026-07-19T00:00:00",
        "2026-07-19X00:00:00+00:00",
        "not-a-timestamp",
        " 2026-07-19T00:00:00Z",
    ],
)
def test_evidence_parser_rejects_noncanonical_commit_times(committed_at):
    with pytest.raises(SnapshotFenceTelemetryError, match="commit time is invalid"):
        SilverSnapshotEvidence.from_dict(evidence_dict(committed_at=committed_at))


@pytest.mark.parametrize("operation", [True, "", "replace ", " replace", "merge"])
def test_evidence_parser_rejects_whitespace_and_unknown_operations(operation):
    with pytest.raises(SnapshotFenceTelemetryError, match="operation is invalid"):
        SilverSnapshotEvidence.from_dict(evidence_dict(operation=operation))


def test_collect_evidence_queries_only_exact_dev_relation_and_closes_resources(
    monkeypatch,
):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
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
    snapshot_sql, files_sql = cursor.statements
    assert snapshot_sql == (
        "SELECT snapshot_id, committed_at, operation "
        "FROM iceberg_dev.traffic."
        '"silver_seoul_traffic_incident$snapshots" '
        "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
    )
    assert files_sql == (
        "SELECT file_path "
        "FROM iceberg_dev.traffic."
        '"silver_seoul_traffic_incident$files" '
        "WHERE content = 0 "
        "AND regexp_like(file_path, '(^|/)compacted-[^/]*$') "
        "ORDER BY file_path"
    )
    assert "LIKE" not in files_sql
    assert all("SHOW TABLES" not in statement.upper() for statement in cursor.statements)


def test_trino_connection_uses_the_exact_silver_relation_namespace(monkeypatch):
    captured = {}
    connection = object()
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")

    monkeypatch.setattr(
        "trino.dbapi.connect",
        lambda **kwargs: captured.update(kwargs) or connection,
    )

    assert snapshot_fence._trino_connection() is connection
    assert captured["catalog"] == "iceberg_dev"
    assert captured["schema"] == "traffic"


def test_prod_snapshot_fence_uses_only_the_prod_catalog(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "prod")
    monkeypatch.setenv("TRINO_ICEBERG_CATALOG", "iceberg")
    monkeypatch.setenv("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    cursor = FakeCursor(
        snapshot_row=(11, "2026-07-19T00:00:00Z", "overwrite"),
        file_rows=[],
    )
    connection = FakeConnection(cursor)

    collect_silver_snapshot_evidence(connection_factory=lambda: connection)

    assert all("iceberg.traffic." in statement for statement in cursor.statements)
    assert all("iceberg_dev" not in statement for statement in cursor.statements)


def test_prod_trino_connection_uses_the_prod_catalog(monkeypatch):
    captured = {}
    connection = object()
    monkeypatch.setenv("ASK_SEOUL_TARGET", "prod")
    monkeypatch.setenv("TRINO_ICEBERG_CATALOG", "iceberg")
    monkeypatch.setenv("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    monkeypatch.setattr(
        "trino.dbapi.connect",
        lambda **kwargs: captured.update(kwargs) or connection,
    )

    assert snapshot_fence._trino_connection() is connection
    assert captured["catalog"] == "iceberg"
    assert captured["schema"] == "traffic"


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


def test_collect_closes_connection_when_cursor_construction_fails():
    connection = FakeConnection(
        None,
        cursor_error=RuntimeError("cursor construction failed"),
    )

    with pytest.raises(RuntimeError, match="cursor construction failed"):
        collect_silver_snapshot_evidence(connection_factory=lambda: connection)

    assert connection.close_calls == 1


def test_collect_attempts_connection_close_when_cursor_close_fails():
    cursor = FakeCursor(
        snapshot_row=(11, "2026-07-19T00:00:00Z", "overwrite"),
        file_rows=[],
        close_error=RuntimeError("cursor close failed"),
    )
    connection = FakeConnection(cursor)

    with pytest.raises(RuntimeError, match="cursor close failed"):
        collect_silver_snapshot_evidence(connection_factory=lambda: connection)

    assert cursor.close_calls == 1
    assert connection.close_calls == 1


def test_collect_attempts_cursor_close_when_connection_close_fails():
    cursor = FakeCursor(
        snapshot_row=(11, "2026-07-19T00:00:00Z", "overwrite"),
        file_rows=[],
    )
    connection = FakeConnection(
        cursor,
        close_error=RuntimeError("connection close failed"),
    )

    with pytest.raises(RuntimeError, match="connection close failed"):
        collect_silver_snapshot_evidence(connection_factory=lambda: connection)

    assert cursor.close_calls == 1
    assert connection.close_calls == 1
