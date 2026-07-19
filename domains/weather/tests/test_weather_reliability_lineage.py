from datetime import datetime, timezone
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.reliability.lineage import (  # noqa: E402
    StagePolicy,
    collect_pipeline_stages,
    summarize_stage_runs,
)


DETECTED_AT = datetime(2026, 7, 19, 2, 0, tzinfo=timezone.utc)


def _run(
    state: str,
    started_at: str,
    *,
    ended_at: str | None = None,
    duration_ms: int | None = None,
) -> dict[str, object]:
    return {
        "id": f"{state.lower()}-{started_at}",
        "createdAt": started_at,
        "updatedAt": ended_at or started_at,
        "nominalStartTime": started_at,
        "nominalEndTime": ended_at,
        "state": state,
        "startedAt": started_at,
        "endedAt": ended_at,
        "durationMs": duration_ms,
        "args": {},
        "jobVersion": None,
        "inputDatasetVersions": [],
        "outputDatasetVersions": [],
        "facets": {},
    }


def _policy() -> StagePolicy:
    return StagePolicy("transform", "Transform", "weather_vilage_fcst_transform", 360)


def test_latest_success_with_recovered_failure_is_warn():
    summary = summarize_stage_runs(
        policy=_policy(),
        runs=[
            _run(
                "FAILED",
                "2026-07-18T20:10:00Z",
                ended_at="2026-07-18T20:12:00Z",
                duration_ms=120_000,
            ),
            _run(
                "COMPLETED",
                "2026-07-19T01:00:00Z",
                ended_at="2026-07-19T01:02:00Z",
                duration_ms=120_000,
            ),
        ],
        detected_at=DETECTED_AT,
        lookback_hours=24,
    )

    assert summary["status"] == "WARN"
    assert summary["failed"] == 1
    assert summary["latest_state"] == "COMPLETED"


def test_stale_running_run_is_fail():
    summary = summarize_stage_runs(
        policy=_policy(),
        runs=[_run("RUNNING", "2026-07-18T18:00:00Z")],
        detected_at=DETECTED_AT,
        lookback_hours=24,
    )

    assert summary["status"] == "FAIL"
    assert summary["reason"] == "stale_running"


def test_latest_terminal_failure_is_fail():
    summary = summarize_stage_runs(
        policy=_policy(),
        runs=[
            _run(
                "COMPLETED",
                "2026-07-18T20:00:00Z",
                ended_at="2026-07-18T20:02:00Z",
                duration_ms=120_000,
            ),
            _run(
                "FAILED",
                "2026-07-19T01:00:00Z",
                ended_at="2026-07-19T01:01:00Z",
                duration_ms=60_000,
            ),
        ],
        detected_at=DETECTED_AT,
        lookback_hours=24,
    )

    assert summary["status"] == "FAIL"
    assert summary["latest_terminal_state"] == "FAILED"
    assert summary["reason"] == "latest_terminal_failed"


def test_fresh_running_with_recent_success_is_not_a_failure():
    summary = summarize_stage_runs(
        policy=_policy(),
        runs=[
            _run(
                "COMPLETED",
                "2026-07-18T23:00:00Z",
                ended_at="2026-07-18T23:02:00Z",
                duration_ms=120_000,
            ),
            _run("RUNNING", "2026-07-19T01:55:00Z"),
        ],
        detected_at=DETECTED_AT,
        lookback_hours=24,
    )

    assert summary["status"] == "PASS"
    assert summary["latest_state"] == "RUNNING"


def test_no_observed_runs_is_unknown():
    summary = summarize_stage_runs(
        policy=_policy(), runs=[], detected_at=DETECTED_AT, lookback_hours=24
    )

    assert summary["status"] == "UNKNOWN"
    assert summary["reason"] == "unobserved"


def test_completed_duration_percentiles_are_reported():
    summary = summarize_stage_runs(
        policy=_policy(),
        runs=[
            _run(
                "COMPLETED",
                "2026-07-18T20:00:00Z",
                ended_at="2026-07-18T20:00:01Z",
                duration_ms=1_000,
            ),
            _run(
                "COMPLETED",
                "2026-07-18T22:00:00Z",
                ended_at="2026-07-18T22:00:02Z",
                duration_ms=2_000,
            ),
            _run(
                "COMPLETED",
                "2026-07-19T00:00:00Z",
                ended_at="2026-07-19T00:00:10Z",
                duration_ms=10_000,
            ),
        ],
        detected_at=DETECTED_AT,
        lookback_hours=24,
    )

    assert summary["duration_ms"] == {"p50": 2_000, "p95": 10_000}


def test_collect_pipeline_stages_uses_exact_encoded_job_url():
    requested_urls: list[str] = []

    def fetch_json(url: str) -> dict[str, object]:
        requested_urls.append(url)
        return {
            "runs": [
                _run(
                    "COMPLETED",
                    "2026-07-18T23:00:00Z",
                    ended_at="2026-07-18T23:02:00Z",
                    duration_ms=120_000,
                )
            ]
        }

    result = collect_pipeline_stages(
        policies=(StagePolicy("source", "Source", "job/name", 360),),
        detected_at=DETECTED_AT,
        lookback_hours=24,
        namespace="ask-seoul-dev-airflow",
        base_url="http://marquez-api:5000/api/v1",
        fetch_json=fetch_json,
    )

    assert requested_urls == [
        "http://marquez-api:5000/api/v1/namespaces/"
        "ask-seoul-dev-airflow/jobs/job%2Fname/runs?limit=500"
    ]
    assert result["status"] == "PASS"


def test_malformed_marquez_payload_is_type_only_unknown():
    def fetch_json(_url: str) -> dict[str, object]:
        return {"runs": "not-a-list", "secret": "must-not-leak"}

    result = collect_pipeline_stages(
        policies=(_policy(),),
        detected_at=DETECTED_AT,
        lookback_hours=24,
        fetch_json=fetch_json,
    )

    stage = result["stages"][0]
    assert result["status"] == "UNKNOWN"
    assert stage["status"] == "UNKNOWN"
    assert stage["error_type"] == "ValueError"
    assert "secret" not in str(result)

