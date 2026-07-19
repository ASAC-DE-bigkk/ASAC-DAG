from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen


DEFAULT_MARQUEZ_BASE_URL = "http://marquez-api:5000/api/v1"
DEFAULT_MARQUEZ_NAMESPACE = "ask-seoul-dev-airflow"
MARQUEZ_RUN_LIMIT = 500
TERMINAL_FAILURE_STATES = frozenset({"ABORTED", "FAILED"})
ACTIVE_STATES = frozenset({"NEW", "RUNNING"})


@dataclass(frozen=True)
class StagePolicy:
    key: str
    label: str
    job_name: str
    stale_after_minutes: int
    required: bool = True


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _run_started_at(run: Mapping[str, Any]) -> datetime | None:
    for key in ("startedAt", "nominalStartTime", "createdAt", "updatedAt"):
        if parsed := _parse_timestamp(run.get(key)):
            return parsed
    return None


def _run_ended_at(run: Mapping[str, Any]) -> datetime | None:
    for key in ("endedAt", "nominalEndTime", "updatedAt", "startedAt"):
        if parsed := _parse_timestamp(run.get(key)):
            return parsed
    return None


def _nearest_rank(values: list[int], percentile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _age_minutes(detected_at: datetime, observed_at: datetime) -> int:
    seconds = max(0.0, (detected_at - observed_at).total_seconds())
    return int(seconds // 60)


def summarize_stage_runs(
    *,
    policy: StagePolicy,
    runs: list[dict[str, Any]],
    detected_at: datetime,
    lookback_hours: int,
) -> dict[str, Any]:
    detected_at_utc = detected_at.astimezone(timezone.utc)
    cutoff = detected_at_utc - timedelta(hours=lookback_hours)
    observed: list[tuple[datetime, Mapping[str, Any]]] = []
    for run in runs:
        if not isinstance(run, Mapping):
            continue
        if started_at := _run_started_at(run):
            if cutoff <= started_at <= detected_at_utc:
                observed.append((started_at, run))
    observed.sort(key=lambda item: item[0])

    base = {
        "key": policy.key,
        "label": policy.label,
        "job_name": policy.job_name,
        "required": policy.required,
        "stale_after_minutes": policy.stale_after_minutes,
        "observed": len(observed),
        "completed": 0,
        "failed": 0,
        "running": 0,
        "latest_state": None,
        "latest_terminal_state": None,
        "latest_started_at": None,
        "latest_success_at": None,
        "age_minutes": None,
        "duration_ms": {"p50": None, "p95": None},
    }
    if not observed:
        return {**base, "status": "UNKNOWN", "reason": "unobserved"}

    states = [str(run.get("state") or "UNKNOWN").upper() for _, run in observed]
    completed = [item for item in observed if str(item[1].get("state")).upper() == "COMPLETED"]
    failed = [
        item
        for item in observed
        if str(item[1].get("state")).upper() in TERMINAL_FAILURE_STATES
    ]
    active = [
        item
        for item in observed
        if str(item[1].get("state")).upper() in ACTIVE_STATES
    ]
    terminal = [
        item
        for item in observed
        if str(item[1].get("state")).upper()
        in TERMINAL_FAILURE_STATES | {"COMPLETED"}
    ]
    durations = [
        int(run["durationMs"])
        for _, run in completed
        if isinstance(run.get("durationMs"), (int, float))
        and int(run["durationMs"]) >= 0
    ]
    latest_started_at, _latest_run = observed[-1]
    latest_state = states[-1]
    latest_terminal = terminal[-1] if terminal else None
    latest_success = completed[-1] if completed else None
    latest_terminal_state = (
        str(latest_terminal[1].get("state")).upper() if latest_terminal else None
    )
    latest_success_at = (
        _run_ended_at(latest_success[1]) if latest_success is not None else None
    )

    summary = {
        **base,
        "completed": len(completed),
        "failed": len(failed),
        "running": len(active),
        "latest_state": latest_state,
        "latest_terminal_state": latest_terminal_state,
        "latest_started_at": latest_started_at.isoformat(),
        "latest_success_at": (
            latest_success_at.isoformat() if latest_success_at is not None else None
        ),
        "duration_ms": {
            "p50": _nearest_rank(durations, 0.50),
            "p95": _nearest_rank(durations, 0.95),
        },
    }

    if latest_state in ACTIVE_STATES:
        running_age = _age_minutes(detected_at_utc, latest_started_at)
        if running_age > policy.stale_after_minutes:
            return {
                **summary,
                "status": "FAIL",
                "reason": "stale_running",
                "age_minutes": running_age,
            }

    if latest_terminal_state in TERMINAL_FAILURE_STATES:
        terminal_at = _run_ended_at(latest_terminal[1]) or latest_terminal[0]
        return {
            **summary,
            "status": "FAIL",
            "reason": "latest_terminal_failed",
            "age_minutes": _age_minutes(detected_at_utc, terminal_at),
        }

    if latest_success is None or latest_success_at is None:
        return {**summary, "status": "UNKNOWN", "reason": "no_completed_run"}

    success_age = _age_minutes(detected_at_utc, latest_success_at)
    summary["age_minutes"] = success_age
    if success_age > policy.stale_after_minutes:
        return {**summary, "status": "FAIL", "reason": "stale_success"}
    if failed:
        return {**summary, "status": "WARN", "reason": "recovered_failure"}
    return {**summary, "status": "PASS", "reason": None}


def _fetch_json(url: str) -> dict[str, Any]:
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=5) as response:  # noqa: S310
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError("Marquez response must be an object")
    return payload


def _unknown_stage(policy: StagePolicy, exc: Exception) -> dict[str, Any]:
    return {
        "key": policy.key,
        "label": policy.label,
        "job_name": policy.job_name,
        "required": policy.required,
        "stale_after_minutes": policy.stale_after_minutes,
        "observed": 0,
        "completed": 0,
        "failed": 0,
        "running": 0,
        "latest_state": None,
        "latest_terminal_state": None,
        "latest_started_at": None,
        "latest_success_at": None,
        "age_minutes": None,
        "duration_ms": {"p50": None, "p95": None},
        "status": "UNKNOWN",
        "reason": "marquez_unavailable",
        "error_type": type(exc).__name__,
    }


def _overall_status(stages: list[dict[str, Any]]) -> str:
    if any(stage["status"] == "FAIL" for stage in stages):
        return "FAIL"
    required = [stage for stage in stages if stage["required"]]
    if required and all(stage["status"] == "UNKNOWN" for stage in required):
        return "UNKNOWN"
    if any(stage["status"] == "WARN" for stage in stages):
        return "WARN"
    if any(stage["status"] == "UNKNOWN" for stage in required):
        return "WARN"
    return "PASS" if stages else "UNKNOWN"


def collect_pipeline_stages(
    *,
    policies: tuple[StagePolicy, ...],
    detected_at: datetime,
    lookback_hours: int,
    namespace: str = DEFAULT_MARQUEZ_NAMESPACE,
    base_url: str = DEFAULT_MARQUEZ_BASE_URL,
    fetch_json: Callable[[str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    fetch = fetch_json or _fetch_json
    stages: list[dict[str, Any]] = []
    for policy in policies:
        url = (
            f"{base_url.rstrip('/')}/namespaces/{quote(namespace, safe='')}"
            f"/jobs/{quote(policy.job_name, safe='')}/runs?limit={MARQUEZ_RUN_LIMIT}"
        )
        try:
            payload = fetch(url)
            runs = payload.get("runs")
            if not isinstance(runs, list):
                raise ValueError("Marquez runs must be a list")
            stages.append(
                summarize_stage_runs(
                    policy=policy,
                    runs=runs,
                    detected_at=detected_at,
                    lookback_hours=lookback_hours,
                )
            )
        except Exception as exc:
            stages.append(_unknown_stage(policy, exc))
    return {
        "source": "marquez",
        "detected_at": detected_at.isoformat(),
        "lookback_hours": lookback_hours,
        "status": _overall_status(stages),
        "stages": stages,
    }
