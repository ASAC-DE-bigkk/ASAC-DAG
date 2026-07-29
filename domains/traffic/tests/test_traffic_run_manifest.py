import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.run_manifest import TrafficRun, TrafficRunManifest  # noqa: E402


class RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(" ".join(statement.split()))


def test_failure_reason_does_not_store_exception_message():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.fail(
        TrafficRun("traffic_incident_bronze", "manual__failed"),
        task_id="verify_seoul_traffic_bronze_runtime",
        error=RuntimeError("api credential redacted"),
    )

    mutation = next(
        statement for statement in cursor.statements if statement.startswith("MERGE ")
    )
    assert "RuntimeError in verify_seoul_traffic_bronze_runtime" in mutation
    assert "api credential" not in mutation


def test_manifest_records_multiple_runs_with_one_merge_per_status():
    cursor = RecordingCursor()
    manifest = TrafficRunManifest(
        lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )
    runs = [
        TrafficRun("traffic_incident_bronze", "scheduled__first"),
        TrafficRun("traffic_incident_bronze", "scheduled__second"),
    ]

    manifest.start_many(
        [(runs[0], 1), (runs[1], 2)]
    )
    manifest.publish_many(
        [
            (
                runs[0],
                {
                    "expected_rows": 4,
                    "actual_rows": 4,
                    "expected_raw_objects": 1,
                    "actual_raw_objects": 1,
                    "is_publishable": True,
                },
            ),
            (
                runs[1],
                {
                    "expected_rows": 8,
                    "actual_rows": 8,
                    "expected_raw_objects": 2,
                    "actual_raw_objects": 2,
                    "is_publishable": True,
                },
            ),
        ]
    )

    merges = [
        statement
        for statement in cursor.statements
        if statement.startswith("MERGE ")
    ]
    assert len(merges) == 2
    assert all("scheduled__first" in statement for statement in merges)
    assert all("scheduled__second" in statement for statement in merges)
