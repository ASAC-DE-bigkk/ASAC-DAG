import sys
from pathlib import Path
from types import SimpleNamespace


sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _shared.bronze_run_manifest import failure_reason_from_context  # noqa: E402


def test_failure_reason_does_not_store_exception_message():
    reason = failure_reason_from_context(
        {
            "task_instance": SimpleNamespace(task_id="verify_seoul_traffic_bronze_runtime"),
            "exception": RuntimeError("api credential redacted"),
        }
    )

    assert reason == "RuntimeError in verify_seoul_traffic_bronze_runtime"
    assert "api credential" not in reason
