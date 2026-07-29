from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import date, timedelta
from typing import Any, Protocol


DOMAIN = "weather"
HISTORY_VERSION = "pipeline-reliability-v2"
HISTORY_DAYS = 7
HISTORY_FIELDS = {
    "version",
    "domain",
    "report_date",
    "detected_at",
    "status",
    "stages",
    "source",
    "bottleneck",
}
VALID_STATUSES = frozenset({"PASS", "WARN", "FAIL", "UNKNOWN"})


class HistoryStorage(Protocol):
    def read_json(self, key: str) -> Any: ...

    def write_json(self, key: str, value: Any) -> None: ...


class HistoryWriteError(RuntimeError):
    def __init__(self, error_type: str):
        super().__init__(f"history_write_failed:{error_type}")


def history_prefix() -> str:
    """리포트 루트 — 기본 ops 존, `WEATHER_RELIABILITY_HISTORY_PREFIX` 는 롤백용(#60).

    지나간 실행의 기록이라 ops/reports 존(카테고리별 TTL 허용)이 목적지다.
    기본값이 곧 목적지이므로 배포만 하면 맞고, env 는 구 위치(루트 `reliability`)로
    되돌릴 때만 쓴다 — culture(#579) 의 `OPS_REPORTS_ROOT` 와 같은 목적지.
    """
    configured = os.environ.get(
        f"{DOMAIN.upper()}_RELIABILITY_HISTORY_PREFIX", ""
    ).strip()
    return configured.rstrip("/") if configured else f"ops/reports/{DOMAIN}/type=reliability"


def history_object_key(report_date: date) -> str:
    return (
        f"{history_prefix()}/date={report_date.isoformat()}/domain={DOMAIN}/"
        f"{HISTORY_VERSION}.json"
    )


def _compact_stage(stage: Mapping[str, Any]) -> dict[str, Any]:
    duration = stage.get("duration_ms")
    if not isinstance(duration, Mapping):
        duration = {}
    return {
        "key": stage.get("key"),
        "label": stage.get("label"),
        "status": stage.get("status"),
        "reason": stage.get("reason"),
        "age_minutes": stage.get("age_minutes"),
        "p50_ms": duration.get("p50"),
        "p95_ms": duration.get("p95"),
    }


def _compact_source(source: object) -> dict[str, Any]:
    if not isinstance(source, Mapping):
        return {}
    allowed = (
        "status",
        "freshness_minutes",
        "coverage_percent",
        "pending_count",
        "duplicate_keys",
        "publishability_ok",
    )
    return {key: source.get(key) for key in allowed if key in source}


def _compact_bottleneck(bottleneck: object) -> dict[str, Any] | None:
    if not isinstance(bottleneck, Mapping):
        return None
    allowed = ("key", "label", "status", "p95_ms")
    return {key: bottleneck.get(key) for key in allowed if key in bottleneck}


def compact_history_snapshot(report: Mapping[str, Any]) -> dict[str, Any]:
    report_date = str(report.get("report_date") or "")
    date.fromisoformat(report_date)
    stages = report.get("stages")
    if not isinstance(stages, list):
        stages = []
    status = str(report.get("status") or "UNKNOWN").upper()
    if status not in VALID_STATUSES:
        status = "UNKNOWN"
    return {
        "version": HISTORY_VERSION,
        "domain": DOMAIN,
        "report_date": report_date,
        "detected_at": report.get("detected_at"),
        "status": status,
        "stages": [
            _compact_stage(stage) for stage in stages if isinstance(stage, Mapping)
        ],
        "source": _compact_source(report.get("source")),
        "bottleneck": _compact_bottleneck(report.get("bottleneck")),
    }


def _build_history_storage() -> HistoryStorage:
    from common.storage import build_storage, r2_env

    region = os.environ.get("R2_DEV_REGION") or os.environ.get("R2_REGION", "auto")
    return build_storage(
        "r2",
        bucket=r2_env("R2_BUCKET_NAME"),
        endpoint=r2_env("R2_ENDPOINT"),
        key=r2_env("R2_ACCESS_KEY_ID"),
        secret=r2_env("R2_SECRET_ACCESS_KEY"),
        region=region,
    )


def _is_missing(exc: Exception) -> bool:
    if isinstance(exc, FileNotFoundError):
        return True
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return False
    error = response.get("Error")
    if not isinstance(error, Mapping):
        return False
    return str(error.get("Code")) in {"404", "NoSuchKey", "NotFound"}


def _unknown_snapshot(
    report_date: date, reason: str, error_type: str | None = None
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "version": HISTORY_VERSION,
        "domain": DOMAIN,
        "report_date": report_date.isoformat(),
        "detected_at": None,
        "status": "UNKNOWN",
        "stages": [],
        "source": {},
        "bottleneck": None,
        "reason": reason,
    }
    if error_type:
        result["error_type"] = error_type
    return result


def _validated_snapshot(payload: object, expected_date: date) -> dict[str, Any] | None:
    if not isinstance(payload, Mapping):
        return None
    if set(payload) != HISTORY_FIELDS:
        return None
    if payload.get("version") != HISTORY_VERSION or payload.get("domain") != DOMAIN:
        return None
    if payload.get("report_date") != expected_date.isoformat():
        return None
    if payload.get("status") not in VALID_STATUSES:
        return None
    return {field: payload.get(field) for field in HISTORY_FIELDS}


def load_recent_history(
    report_date: date,
    *,
    storage: HistoryStorage | None = None,
    days: int = HISTORY_DAYS,
) -> list[dict[str, Any]]:
    store = storage or _build_history_storage()
    history: list[dict[str, Any]] = []
    for offset in range(days, 0, -1):
        observed_date = report_date - timedelta(days=offset)
        try:
            payload = store.read_json(history_object_key(observed_date))
        except Exception as exc:
            if _is_missing(exc):
                history.append(_unknown_snapshot(observed_date, "unobserved"))
            else:
                history.append(
                    _unknown_snapshot(
                        observed_date, "history_read_failed", type(exc).__name__
                    )
                )
            continue
        validated = _validated_snapshot(payload, observed_date)
        history.append(
            validated or _unknown_snapshot(observed_date, "invalid_history")
        )
    return history


def write_history_snapshot(
    report: Mapping[str, Any], *, storage: HistoryStorage | None = None
) -> str:
    snapshot = compact_history_snapshot(report)
    report_date = date.fromisoformat(snapshot["report_date"])
    key = history_object_key(report_date)
    try:
        (storage or _build_history_storage()).write_json(key, snapshot)
    except Exception as exc:
        raise HistoryWriteError(type(exc).__name__) from None
    return key
