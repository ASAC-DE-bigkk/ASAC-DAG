from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.run_ledger import (
    STATUS_FAILED,
    STATUS_STARTED,
    STATUS_SUCCESS,
    TrafficRunLedger,
)


class MemoryStorage:
    def __init__(self) -> None:
        self.objects: dict[str, object] = {}

    def write_json(self, key: str, value: object) -> None:
        self.objects[key] = value

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_json(self, key: str) -> object:
        return self.objects[key]


def _ledger(storage: MemoryStorage, now: datetime) -> TrafficRunLedger:
    return TrafficRunLedger(storage_factory=lambda: storage, clock=lambda: now)


def _record(
    ledger: TrafficRunLedger,
    *,
    run_id: str,
    status: str,
    logical_date: datetime,
    task_id: str | None = None,
    error: BaseException | None = None,
) -> None:
    ledger.record(
        dag_id="traffic_incident_bronze",
        run_id=run_id,
        status=status,
        logical_date=logical_date,
        task_id=task_id,
        error=error,
    )


def test_ledger_uses_deterministic_status_keys_and_never_persists_error_messages():
    storage = MemoryStorage()
    logical_date = datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc)
    ledger = _ledger(storage, logical_date)

    _record(
        ledger,
        run_id="scheduled__2026-07-15T00:00:00+00:00",
        status=STATUS_STARTED,
        logical_date=logical_date,
    )
    _record(
        ledger,
        run_id="scheduled__2026-07-15T00:00:00+00:00",
        status=STATUS_FAILED,
        logical_date=logical_date,
        task_id="load_seoul_traffic_bronze",
        error=RuntimeError("token=must-not-be-persisted"),
    )
    _record(
        ledger,
        run_id="scheduled__2026-07-15T00:00:00+00:00",
        status=STATUS_FAILED,
        logical_date=logical_date,
        task_id="load_seoul_traffic_bronze",
        error=RuntimeError("token=must-not-be-persisted"),
    )

    assert len(storage.objects) == 2
    failed_key = next(key for key in storage.objects if key.endswith("__FAILED.json"))
    failed = storage.objects[failed_key]
    assert "traffic-run-ledger/observed_date=2026-07-15/" in failed_key
    assert failed == {
        "dag_id": "traffic_incident_bronze",
        "run_id": "scheduled__2026-07-15T00:00:00+00:00",
        "logical_date": "2026-07-15T00:00:00+00:00",
        "status": "FAILED",
        "event_at": "2026-07-15T00:00:00+00:00",
        "task_id": "load_seoul_traffic_bronze",
        "failure_reason": "RuntimeError in load_seoul_traffic_bronze",
    }
    assert "must-not-be-persisted" not in repr(failed)


def test_scheduled_summary_counts_terminal_failure_and_nonstale_running_run():
    storage = MemoryStorage()
    start = datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc)
    ledger = _ledger(storage, datetime(2026, 7, 15, 0, 15, tzinfo=timezone.utc))
    _record(
        ledger,
        run_id="scheduled__2026-07-15T00:00:00+00:00",
        status=STATUS_SUCCESS,
        logical_date=start,
    )
    _record(
        ledger,
        run_id="scheduled__2026-07-15T00:05:00+00:00",
        status=STATUS_FAILED,
        logical_date=start.replace(minute=5),
        task_id="land_seoul_traffic_raw",
        error=ValueError("source rejected"),
    )
    _record(
        ledger,
        run_id="scheduled__2026-07-15T00:10:00+00:00",
        status=STATUS_STARTED,
        logical_date=start.replace(minute=10),
    )

    summary = ledger.collect_scheduled_run_summary(
        dag_id="traffic_incident_bronze",
        detected_at=datetime(2026, 7, 15, 0, 15, tzinfo=timezone.utc),
        lookback_hours=24,
        schedule_interval_minutes=5,
        stale_after_minutes=15,
    )

    assert summary == {
        "expected": 1,
        "success": 1,
        "failed": 1,
        "running": 1,
        "failures": [
            {
                "logical_date": "2026-07-15T00:05:00+00:00",
                "run_id": "scheduled__2026-07-15T00:05:00+00:00",
                "task_id": "land_seoul_traffic_raw",
                "reason": "ValueError in land_seoul_traffic_raw",
            }
        ],
    }


def test_scheduled_summary_reports_missing_and_stalled_slots_after_bootstrap():
    storage = MemoryStorage()
    start = datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc)
    ledger = _ledger(storage, datetime(2026, 7, 15, 0, 35, tzinfo=timezone.utc))
    _record(
        ledger,
        run_id="scheduled__2026-07-15T00:00:00+00:00",
        status=STATUS_SUCCESS,
        logical_date=start,
    )
    _record(
        ledger,
        run_id="scheduled__2026-07-15T00:10:00+00:00",
        status=STATUS_STARTED,
        logical_date=start.replace(minute=10),
    )

    summary = ledger.collect_scheduled_run_summary(
        dag_id="traffic_incident_bronze",
        detected_at=datetime(2026, 7, 15, 0, 35, tzinfo=timezone.utc),
        lookback_hours=24,
        schedule_interval_minutes=5,
        stale_after_minutes=15,
    )

    assert summary["expected"] == 5
    assert summary["success"] == 1
    assert summary["running"] == 0
    assert summary["failed"] == 4
    assert summary["failures"] == [
        {
            "logical_date": "2026-07-15T00:05:00+00:00",
            "run_id": "scheduled__2026-07-15T00:05:00+00:00",
            "task_id": "unknown",
            "reason": "missing_run_ledger_entry",
        },
        {
            "logical_date": "2026-07-15T00:10:00+00:00",
            "run_id": "scheduled__2026-07-15T00:10:00+00:00",
            "task_id": "unknown",
            "reason": "run_stalled",
        },
        {
            "logical_date": "2026-07-15T00:15:00+00:00",
            "run_id": "scheduled__2026-07-15T00:15:00+00:00",
            "task_id": "unknown",
            "reason": "missing_run_ledger_entry",
        },
        {
            "logical_date": "2026-07-15T00:20:00+00:00",
            "run_id": "scheduled__2026-07-15T00:20:00+00:00",
            "task_id": "unknown",
            "reason": "missing_run_ledger_entry",
        },
    ]


def test_scheduled_summary_is_explicitly_bootstrapping_until_first_scheduled_event():
    storage = MemoryStorage()
    ledger = _ledger(storage, datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc))

    summary = ledger.collect_scheduled_run_summary(
        dag_id="traffic_incident_bronze",
        detected_at=datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc),
        lookback_hours=24,
        schedule_interval_minutes=5,
        stale_after_minutes=15,
    )

    assert summary == {
        "expected": 0,
        "success": 0,
        "failed": 0,
        "running": 0,
        "failures": [],
        "reason": "run_ledger_bootstrapping",
    }
