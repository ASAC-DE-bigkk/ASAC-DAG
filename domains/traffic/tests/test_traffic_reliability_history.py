from datetime import date
import json
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.reliability.history import (  # noqa: E402
    HistoryWriteError,
    compact_history_snapshot,
    history_object_key,
    load_recent_history,
    write_history_snapshot,
)


class FakeStorage:
    def __init__(self, objects=None, read_error=None, write_error=None):
        self.objects = dict(objects or {})
        self.read_error = read_error
        self.write_error = write_error
        self.read_keys: list[str] = []
        self.writes: list[tuple[str, object]] = []

    def read_json(self, key: str):
        self.read_keys.append(key)
        if self.read_error is not None:
            raise self.read_error
        if key not in self.objects:
            raise FileNotFoundError(key)
        value = self.objects[key]
        if isinstance(value, Exception):
            raise value
        return value

    def write_json(self, key: str, value: object):
        if self.write_error is not None:
            raise self.write_error
        self.writes.append((key, value))


def _report() -> dict[str, object]:
    return {
        "report_name": "traffic_pipeline_reliability_v2",
        "domain": "traffic",
        "report_date": "2026-07-20",
        "detected_at": "2026-07-20T09:00:00+09:00",
        "status": "WARN",
        "stages": [
            {
                "key": "gold",
                "label": "Traffic Gold",
                "status": "WARN",
                "reason": "recovered_failure",
                "age_minutes": 18,
                "duration_ms": {"p50": 10_000, "p95": 20_000},
                "error": "token=must-not-leak",
                "runs": [{"payload": "must-not-leak"}],
            }
        ],
        "source": {
            "status": "PASS",
            "freshness_minutes": 2,
            "coverage_percent": 100.0,
            "pending_count": 0,
            "error": "secret=must-not-leak",
        },
        "bottleneck": {
            "key": "gold",
            "label": "Traffic Gold",
            "status": "WARN",
            "p95_ms": 20_000,
            "query": "select secret",
        },
        "error": "webhook=must-not-leak",
        "run_payloads": [{"secret": "must-not-leak"}],
    }


def test_history_key_is_domain_and_kst_date_scoped():
    assert history_object_key(date(2026, 7, 20)) == (
        "ops/reports/traffic/type=reliability/date=2026-07-20/domain=traffic/"
        "pipeline-reliability-v2.json"
    )


def test_compact_snapshot_excludes_errors_and_run_payloads():
    snapshot = compact_history_snapshot(_report())

    assert set(snapshot) == {
        "version",
        "domain",
        "report_date",
        "detected_at",
        "status",
        "stages",
        "source",
        "bottleneck",
    }
    serialized = json.dumps(snapshot)
    assert "error" not in serialized
    assert "secret" not in serialized
    assert "payload" not in serialized


def test_load_recent_history_reads_exactly_seven_date_keys_and_marks_gaps_unknown():
    report_date = date(2026, 7, 20)
    prior_key = history_object_key(date(2026, 7, 19))
    storage = FakeStorage(
        {
            prior_key: {
                "version": "pipeline-reliability-v2",
                "domain": "traffic",
                "report_date": "2026-07-19",
                "detected_at": "2026-07-19T09:00:00+09:00",
                "status": "PASS",
                "stages": [],
                "source": {},
                "bottleneck": None,
            }
        }
    )

    history = load_recent_history(report_date, storage=storage)

    assert len(storage.read_keys) == 7
    assert len(set(storage.read_keys)) == 7
    assert all("date=" in key for key in storage.read_keys)
    assert [item["report_date"] for item in history] == [
        "2026-07-13",
        "2026-07-14",
        "2026-07-15",
        "2026-07-16",
        "2026-07-17",
        "2026-07-18",
        "2026-07-19",
    ]
    assert history[-1]["status"] == "PASS"
    assert history[0]["status"] == "UNKNOWN"
    assert history[0]["reason"] == "unobserved"


def test_malformed_or_cross_domain_history_is_unknown_without_payload_leak():
    secret = "must-not-leak"
    storage = FakeStorage(read_error=json.JSONDecodeError(secret, secret, 0))

    malformed = load_recent_history(date(2026, 7, 20), storage=storage)

    assert all(item["status"] == "UNKNOWN" for item in malformed)
    assert all(item["error_type"] == "JSONDecodeError" for item in malformed)
    assert secret not in str(malformed)

    wrong_key = history_object_key(date(2026, 7, 19))
    wrong_storage = FakeStorage(
        {
            wrong_key: {
                "version": "pipeline-reliability-v2",
                "domain": "weather",
                "report_date": "2026-07-19",
                "status": "PASS",
            }
        }
    )
    wrong = load_recent_history(date(2026, 7, 20), storage=wrong_storage)
    assert wrong[-1]["status"] == "UNKNOWN"
    assert wrong[-1]["reason"] == "invalid_history"


def test_write_history_snapshot_uses_compact_payload_and_sanitizes_failure():
    storage = FakeStorage()

    key = write_history_snapshot(_report(), storage=storage)

    assert key == history_object_key(date(2026, 7, 20))
    assert storage.writes[0][0] == key
    assert "must-not-leak" not in json.dumps(storage.writes[0][1])

    failing = FakeStorage(write_error=RuntimeError("token=must-not-leak"))
    with pytest.raises(HistoryWriteError, match="RuntimeError") as exc_info:
        write_history_snapshot(_report(), storage=failing)
    assert "must-not-leak" not in str(exc_info.value)
