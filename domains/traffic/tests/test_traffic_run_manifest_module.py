from __future__ import annotations

import sys
import inspect
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.run_manifest import (  # noqa: E402
    STATUS_FAILED,
    STATUS_COALESCED,
    STATUS_STARTED,
    STATUS_SUCCESS,
    RunNotPublishableError,
    TrafficRun,
    TrafficRunManifest,
)


class RecordingCursor:
    def __init__(self, rows: list[tuple] | None = None) -> None:
        self.statements: list[str] = []
        self.rows = list(rows or [])

    def execute(self, statement: str) -> None:
        self.statements.append(" ".join(statement.split()))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


def test_manifest_status_constants_are_the_only_lifecycle_literals():
    assert (STATUS_STARTED, STATUS_SUCCESS, STATUS_FAILED) == (
        "STARTED",
        "SUCCESS",
        "FAILED",
    )
    assert "status=STATUS_STARTED" in inspect.getsource(TrafficRunManifest.start)
    assert "status=STATUS_SUCCESS" in inspect.getsource(TrafficRunManifest.publish)
    assert "status=STATUS_FAILED" in inspect.getsource(TrafficRunManifest.fail)


def test_publish_records_one_atomic_publishable_manifest_mutation():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.publish(
        TrafficRun(
            dag_id="traffic_incident_bronze",
            run_id="scheduled__2026-07-14T00:20:00Z",
        ),
        expected_rows=3,
        actual_rows=3,
        expected_raw_objects=2,
        actual_raw_objects=2,
    )

    mutations = [
        statement
        for statement in cursor.statements
        if statement.startswith(("DELETE ", "INSERT ", "MERGE "))
    ]
    assert len(mutations) == 1
    assert mutations[0].startswith("MERGE INTO ")
    assert "'seoul_traffic_incident'" in mutations[0]
    assert "'SUCCESS'" in mutations[0]
    assert "true" in mutations[0]
    assert "3, 3, 2, 2" in mutations[0]


def test_publish_can_record_a_verified_window_as_non_publishable():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.publish(
        TrafficRun(dag_id="traffic_incident_recollect", run_id="manual__window"),
        expected_rows=10,
        actual_rows=10,
        expected_raw_objects=1,
        actual_raw_objects=1,
        is_publishable=False,
    )

    mutation = next(
        statement for statement in cursor.statements if statement.startswith("MERGE ")
    )
    assert "'SUCCESS'" in mutation
    assert "false" in mutation
    assert "10, 10, 1, 1" in mutation


def test_fail_records_error_type_without_leaking_exception_message():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.fail(
        TrafficRun(dag_id="traffic_incident_bronze", run_id="manual__failed"),
        task_id="land_seoul_traffic_raw",
        error=RuntimeError("SEOUL_API_KEY_TRIC=do-not-store"),
        expected_raw_objects=2,
        actual_raw_objects=1,
    )

    mutation = next(
        statement for statement in cursor.statements if statement.startswith("MERGE ")
    )
    assert "'FAILED'" in mutation
    assert "false" in mutation
    assert "RuntimeError in land_seoul_traffic_raw" in mutation
    assert "do-not-store" not in mutation


def test_start_records_expected_raw_objects_without_marking_run_publishable():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.start(
        TrafficRun(dag_id="traffic_incident_bronze", run_id="manual__started"),
        expected_raw_objects=2,
    )

    mutation = next(
        statement for statement in cursor.statements if statement.startswith("MERGE ")
    )
    assert "'STARTED'" in mutation
    assert "false" in mutation
    assert "NULL, NULL, 2, NULL" in mutation


def test_require_publishable_returns_the_exact_verified_snapshot_id():
    cursor = RecordingCursor(rows=[("scheduled__verified",)])
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    result = manifest.require_publishable("scheduled__verified")

    assert result == "scheduled__verified"
    statement = cursor.statements[-1]
    assert "source_id = 'seoul_traffic_incident'" in statement
    assert "status = 'SUCCESS'" in statement
    assert "is_publishable" in statement
    assert "dag_run_id = 'scheduled__verified'" in statement


def test_coalesce_records_replacement_identity_without_marking_snapshot_publishable():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.coalesce("scheduled__old", replacement_run_id="scheduled__new")

    mutation = next(
        statement for statement in cursor.statements if statement.startswith("MERGE ")
    )
    assert f"'{STATUS_COALESCED}'" in mutation
    assert "false" in mutation
    assert "replaced_by=scheduled__new" in mutation


def test_coalesce_many_records_many_replacements_in_one_merge():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    assert manifest.coalesce_many(
        ["scheduled__old-a", "scheduled__old-b"],
        replacement_run_id="scheduled__new",
    ) == "iceberg_dev.weather_traffic_bronze.bronze_collection_run_manifest"

    schema_statements = [
        statement for statement in cursor.statements if statement.startswith("CREATE SCHEMA")
    ]
    table_statements = [
        statement for statement in cursor.statements if statement.startswith("CREATE TABLE")
    ]
    mutations = [statement for statement in cursor.statements if statement.startswith("MERGE ")]
    assert len(schema_statements) == 1
    assert len(table_statements) == 1
    assert len(mutations) == 1
    mutation = mutations[0]
    assert "'scheduled__old-a'" in mutation
    assert "'scheduled__old-b'" in mutation
    assert mutation.count("'COALESCED'") == 2
    assert mutation.count("'replaced_by=scheduled__new'") == 2
    assert "false" in mutation


def test_coalesce_many_normalizes_duplicate_blank_and_replacement_ids():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.coalesce_many(
        [" old-a ", "", "old-a", "new-run", "old-b", "   "],
        replacement_run_id="new-run",
    )

    mutation = next(statement for statement in cursor.statements if statement.startswith("MERGE "))
    assert mutation.count("'old-a'") == 1
    assert mutation.count("'old-b'") == 1
    assert ", 'new-run', 'COALESCED', false," not in mutation
    assert "replaced_by=new-run" in mutation


def test_coalesce_many_escapes_quoted_and_unicode_run_ids():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.coalesce_many(
        ["run'quote", "수동__교통"],
        replacement_run_id="new'run",
    )

    mutation = next(statement for statement in cursor.statements if statement.startswith("MERGE "))
    assert "'run''quote'" in mutation
    assert "'수동__교통'" in mutation
    assert "'replaced_by=new''run'" in mutation


def test_coalesce_many_empty_input_does_not_open_cursor():
    calls = []
    manifest = TrafficRunManifest(
        cursor_factory=lambda: calls.append("called")
        or (RecordingCursor(), "iceberg_dev", "weather_traffic_bronze")
    )

    assert manifest.coalesce_many([" ", "latest"], replacement_run_id="latest") is None
    assert calls == []


def test_single_coalesce_delegates_to_batch_interface(monkeypatch):
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (RecordingCursor(), "iceberg_dev", "weather_traffic_bronze")
    )
    calls = []

    def fake_coalesce_many(run_ids, *, replacement_run_id):
        calls.append((list(run_ids), replacement_run_id))
        return "qualified"

    monkeypatch.setattr(manifest, "coalesce_many", fake_coalesce_many)

    assert manifest.coalesce("old", replacement_run_id="new") == "qualified"
    assert calls == [(["old"], "new")]


def test_latest_publishable_run_id_uses_deterministic_latest_ordering():
    cursor = RecordingCursor(rows=[("scheduled__latest",)])
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    assert manifest.latest_publishable_run_id() == "scheduled__latest"

    statement = cursor.statements[-1]
    assert "source_id = 'seoul_traffic_incident'" in statement
    assert "status = 'SUCCESS'" in statement
    assert "is_publishable" in statement
    assert "ORDER BY successful.event_at DESC, successful.dag_run_id DESC" in statement
    assert statement.endswith("LIMIT 1")


def test_latest_publishable_run_id_fails_explicitly_when_manifest_is_empty():
    manifest = TrafficRunManifest(
        cursor_factory=lambda: (
            RecordingCursor(),
            "iceberg_dev",
            "weather_traffic_bronze",
        )
    )

    with pytest.raises(
        RunNotPublishableError,
        match="No publishable Traffic Bronze run is available",
    ):
        manifest.latest_publishable_run_id()


def test_manifest_tolerates_iceberg_namespace_creation_race():
    class NamespaceRaceCursor(RecordingCursor):
        def execute(self, statement: str) -> None:
            if statement.startswith("CREATE SCHEMA"):
                raise RuntimeError("Namespace already exists")
            super().execute(statement)

    cursor = NamespaceRaceCursor()

    TrafficRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    ).start(TrafficRun("traffic_incident_bronze", "manual__race"))

    assert any(statement.startswith("CREATE TABLE") for statement in cursor.statements)
    assert any(statement.startswith("MERGE ") for statement in cursor.statements)
