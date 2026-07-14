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
