"""R2-backed terminal lifecycle evidence for Traffic Bronze runs."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol
from urllib.parse import quote


# watchdog 이 "누락 run" 판정 근거로 읽으므로 TTL 로 지워지면 오탐이 난다 →
# ops/control 존이 목적지. 구 위치는 루트 `traffic-run-ledger`.
RUN_LEDGER_PREFIX = "ops/control/state/traffic/run_ledger"


def run_ledger_prefix() -> str:
    """ledger 루트 — 기본 ops 존, `TRAFFIC_RUN_LEDGER_PREFIX` 는 롤백용(#60).

    기본값이 곧 목적지이므로 배포만 하면 맞고, env 는 구 위치로 되돌릴 때만 쓴다.
    """
    configured = os.environ.get("TRAFFIC_RUN_LEDGER_PREFIX", "").strip()
    return configured.rstrip("/") if configured else RUN_LEDGER_PREFIX


STATUS_STARTED = "STARTED"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
TERMINAL_STATUSES = frozenset({STATUS_SUCCESS, STATUS_FAILED})


class JsonStorage(Protocol):
    def write_json(self, key: str, value: object) -> None: ...

    def list_keys(self, prefix: str) -> list[str]: ...

    def read_json(self, key: str) -> object: ...


def _as_utc_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_segment(value: object) -> str:
    return quote(str(value or "unknown"), safe="-._~")


def _build_r2_storage() -> JsonStorage:
    from common.storage import build_storage
    from traffic_ingest.common.runtime import r2_env

    return build_storage(
        "r2",
        bucket=r2_env("R2_BUCKET_NAME"),
        endpoint=r2_env("R2_ENDPOINT"),
        key=r2_env("R2_ACCESS_KEY_ID"),
        secret=r2_env("R2_SECRET_ACCESS_KEY"),
        region="auto",
    )


class TrafficRunLedger:
    """Persist and query the R2 lifecycle evidence owned by Traffic Bronze."""

    def __init__(
        self,
        *,
        storage_factory: Callable[[], JsonStorage] = _build_r2_storage,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._storage_factory = storage_factory
        self._clock = clock

    def record(
        self,
        *,
        dag_id: str,
        run_id: str,
        status: str,
        logical_date: datetime | str | None,
        task_id: str | None = None,
        error: BaseException | None = None,
    ) -> str:
        if status not in {STATUS_STARTED, STATUS_SUCCESS, STATUS_FAILED}:
            raise ValueError(f"Unsupported Traffic run ledger status: {status}")
        event_at = _as_utc_datetime(self._clock()) or datetime.now(timezone.utc)
        logical_at = _as_utc_datetime(logical_date) or event_at
        document = {
            "dag_id": str(dag_id),
            "run_id": str(run_id),
            "logical_date": logical_at.isoformat(),
            "status": status,
            "event_at": event_at.isoformat(),
            "task_id": str(task_id) if task_id else None,
            "failure_reason": (
                f"{type(error).__name__} in {task_id or 'unknown'}"
                if status == STATUS_FAILED
                else None
            ),
        }
        key = self._object_key(dag_id=dag_id, run_id=run_id, status=status, event_at=event_at)
        self._storage_factory().write_json(key, document)
        return key

    def collect_scheduled_run_summary(
        self,
        *,
        dag_id: str,
        detected_at: datetime,
        lookback_hours: int,
        schedule_interval_minutes: int,
        stale_after_minutes: int,
    ) -> dict[str, Any]:
        detected = _as_utc_datetime(detected_at) or datetime.now(timezone.utc)
        cutoff = detected - timedelta(hours=int(lookback_hours))
        storage = self._storage_factory()
        by_run: dict[str, list[dict[str, Any]]] = {}
        for key in self._keys_for_dates(storage, dag_id, cutoff, detected):
            document = storage.read_json(key)
            if not isinstance(document, Mapping):
                continue
            run_id = str(document.get("run_id") or "")
            logical_date = _as_utc_datetime(document.get("logical_date"))
            if not run_id.startswith("scheduled__") or logical_date is None:
                continue
            if not cutoff <= logical_date <= detected:
                continue
            by_run.setdefault(run_id, []).append(dict(document))

        if not by_run:
            return {
                "expected": 0,
                "success": 0,
                "failed": 0,
                "running": 0,
                "grace": 0,
                "failures": [],
                "reason": "run_ledger_bootstrapping",
            }

        records = {
            run_id: self._latest_run_record(events)
            for run_id, events in by_run.items()
        }
        schedule_interval = max(1, int(schedule_interval_minutes))
        stale_after = max(schedule_interval, int(stale_after_minutes))
        earliest = min(
            _as_utc_datetime(record.get("logical_date"))
            for record in records.values()
        )
        assert earliest is not None
        due_through = detected - timedelta(minutes=stale_after)
        expected_slots = self._expected_slots(earliest, detected, schedule_interval)
        records_by_slot: dict[datetime, tuple[str, dict[str, Any]]] = {}
        for run_id, record in records.items():
            logical_date = _as_utc_datetime(record.get("logical_date"))
            if logical_date is None:
                continue
            slot = self._slot_for(logical_date, schedule_interval)
            current = records_by_slot.get(slot)
            if current is None or (
                _as_utc_datetime(record.get("event_at"))
                or datetime.min.replace(tzinfo=timezone.utc)
            ) > (
                _as_utc_datetime(current[1].get("event_at"))
                or datetime.min.replace(tzinfo=timezone.utc)
            ):
                records_by_slot[slot] = (run_id, record)

        success = failed = running = grace = 0
        failures: list[dict[str, str]] = []
        for slot in sorted(expected_slots):
            observed = records_by_slot.get(slot)
            if observed is None:
                if slot <= due_through:
                    failed += 1
                    failures.append(
                        {
                            "logical_date": slot.isoformat(),
                            "run_id": f"scheduled__{slot.isoformat()}",
                            "task_id": "unknown",
                            "reason": "missing_run_ledger_entry",
                        }
                    )
                else:
                    grace += 1
                continue

            run_id, record = observed
            status = str(record.get("status") or "")
            logical_date = _as_utc_datetime(record.get("logical_date"))
            if logical_date is None:
                continue
            if status == STATUS_SUCCESS:
                success += 1
            elif status == STATUS_FAILED:
                failed += 1
                failures.append(self._failure_record(run_id, record, logical_date))
            elif status == STATUS_STARTED:
                if logical_date <= due_through:
                    failed += 1
                    failures.append(
                        self._failure_record(
                            run_id,
                            {"task_id": "unknown", "failure_reason": "run_stalled"},
                            logical_date,
                        )
                    )
                else:
                    running += 1
        failures.sort(key=lambda item: (item["logical_date"], item["run_id"]))
        return {
            "expected": len(expected_slots),
            "success": success,
            "failed": failed,
            "running": running,
            "grace": grace,
            "failures": failures,
        }

    def _object_key(
        self,
        *,
        dag_id: str,
        run_id: str,
        status: str,
        event_at: datetime,
    ) -> str:
        observed_date = event_at.date().isoformat()
        return (
            f"{run_ledger_prefix()}/observed_date={observed_date}/"
            f"dag_id={_safe_segment(dag_id)}/"
            f"{_safe_segment(run_id)}__{status}.json"
        )

    @staticmethod
    def _latest_run_record(events: list[dict[str, Any]]) -> dict[str, Any]:
        terminal = [event for event in events if event.get("status") in TERMINAL_STATUSES]
        candidates = terminal or events
        return max(
            candidates,
            key=lambda event: _as_utc_datetime(event.get("event_at"))
            or datetime.min.replace(tzinfo=timezone.utc),
        )

    @staticmethod
    def _failure_record(
        run_id: str,
        record: Mapping[str, Any],
        logical_date: datetime,
    ) -> dict[str, str]:
        return {
            "logical_date": logical_date.isoformat(),
            "run_id": run_id,
            "task_id": str(record.get("task_id") or "unknown"),
            "reason": str(record.get("failure_reason") or "unknown_failure"),
        }

    def _keys_for_dates(
        self,
        storage: JsonStorage,
        dag_id: str,
        cutoff: datetime,
        detected: datetime,
    ) -> list[str]:
        dates: set[str] = set()
        day = cutoff.date()
        while day <= detected.date():
            dates.add(day.isoformat())
            day += timedelta(days=1)
        keys: list[str] = []
        for observed_date in sorted(dates):
            prefix = (
                f"{run_ledger_prefix()}/observed_date={observed_date}/"
                f"dag_id={_safe_segment(dag_id)}/"
            )
            keys.extend(storage.list_keys(prefix))
        return keys

    @staticmethod
    def _expected_slots(
        first_observed: datetime,
        due_through: datetime,
        interval_minutes: int,
    ) -> set[datetime]:
        interval = interval_minutes * 60
        first_seconds = int(first_observed.timestamp())
        slot = datetime.fromtimestamp(
            first_seconds - (first_seconds % interval), tz=timezone.utc
        )
        slots: set[datetime] = set()
        while slot <= due_through:
            slots.add(slot)
            slot += timedelta(minutes=interval_minutes)
        return slots

    @staticmethod
    def _slot_for(value: datetime, interval_minutes: int) -> datetime:
        interval = interval_minutes * 60
        seconds = int(value.timestamp())
        return datetime.fromtimestamp(seconds - (seconds % interval), tz=timezone.utc)
