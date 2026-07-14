import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.reliability import airflow_evidence as evidence  # noqa: E402
from traffic_reliability_test_support import (  # noqa: E402
    _AirflowSession,
    _FailingAirflowSession,
    _ScheduledDagRun,
    _SessionContext,
)


ORIGINAL_COLLECT_AIRFLOW_SUMMARY = evidence.collect_airflow_scheduled_run_summary


def test_collect_airflow_scheduled_run_summary_includes_failed_task_reason(monkeypatch):
    logical_date = datetime(2026, 7, 12, 2, 30, tzinfo=timezone.utc)
    failed_ti = SimpleNamespace(
        state="failed",
        task_id="record_seoul_traffic_run_started",
        start_date=logical_date,
    )
    run = _ScheduledDagRun(
        state="failed",
        logical_date=logical_date,
        run_id="scheduled__2026-07-12T02:30:00+00:00",
        task_instances=[failed_ti],
    )
    session = _AirflowSession([run])

    import airflow.models.dagrun as airflow_dagrun
    import airflow.utils.session as airflow_session

    monkeypatch.setattr(airflow_dagrun, "DagRun", _ScheduledDagRun)
    monkeypatch.setattr(
        airflow_session,
        "create_session",
        lambda: _SessionContext(session),
    )
    monkeypatch.setattr(
        evidence,
        "_lookup_airflow_problem_reason",
        lambda **kwargs: "TrinoConnectionError: trino DNS 이름 해석 실패",
    )

    summary = ORIGINAL_COLLECT_AIRFLOW_SUMMARY(
        "traffic_incident_bronze",
        datetime(2026, 7, 13, 2, 30, tzinfo=timezone.utc),
        24,
    )

    assert summary["expected"] == 1
    assert summary["failed"] == 1
    assert summary["failures"][0]["task_id"] == "record_seoul_traffic_run_started"
    assert (
        summary["failures"][0]["reason"]
        == "TrinoConnectionError: trino DNS 이름 해석 실패"
    )


def test_collect_airflow_scheduled_run_summary_retries_once_after_transient_failure(
    monkeypatch, capfd
):
    secret = "traffic-metadata-secret"
    monkeypatch.setenv("ASK_SEOUL_METADATA_DIAGNOSTIC_TOKEN", secret)
    detected_at = datetime(2026, 7, 13, 2, 30, tzinfo=timezone.utc)
    successful_run = _ScheduledDagRun(
        state="success",
        logical_date=datetime(2026, 7, 12, 3, 0, tzinfo=timezone.utc),
        run_id="scheduled__recovered",
        task_instances=[],
    )
    sessions = [_FailingAirflowSession(secret), _AirflowSession([successful_run])]

    import airflow.models.dagrun as airflow_dagrun
    import airflow.utils.session as airflow_session

    monkeypatch.setattr(airflow_dagrun, "DagRun", _ScheduledDagRun)
    monkeypatch.setattr(
        airflow_session,
        "create_session",
        lambda: _SessionContext(sessions.pop(0)),
    )

    summary = ORIGINAL_COLLECT_AIRFLOW_SUMMARY(
        "traffic_incident_bronze", detected_at, 24
    )
    task_log_output = capfd.readouterr().out

    assert summary == {
        "expected": 1,
        "success": 1,
        "failed": 0,
        "running": 0,
        "failures": [],
    }
    assert sessions == []
    assert "attempt=1/2" in task_log_output
    assert "RuntimeError" in task_log_output
    assert secret not in task_log_output


def test_collect_airflow_scheduled_run_summary_logs_redacted_traceback_after_retry_exhaustion(
    monkeypatch, capfd
):
    secret = "traffic-metadata-secret"
    monkeypatch.setenv("ASK_SEOUL_METADATA_DIAGNOSTIC_TOKEN", secret)
    detected_at = datetime(2026, 7, 13, 2, 30, tzinfo=timezone.utc)
    sessions = [_FailingAirflowSession(secret), _FailingAirflowSession(secret)]

    import airflow.models.dagrun as airflow_dagrun
    import airflow.utils.session as airflow_session

    monkeypatch.setattr(airflow_dagrun, "DagRun", _ScheduledDagRun)
    monkeypatch.setattr(
        airflow_session,
        "create_session",
        lambda: _SessionContext(sessions.pop(0)),
    )

    with pytest.raises(RuntimeError) as exc_info:
        ORIGINAL_COLLECT_AIRFLOW_SUMMARY("traffic_incident_bronze", detected_at, 24)
    task_log_output = capfd.readouterr().out

    assert sessions == []
    assert secret not in str(exc_info.value)
    assert task_log_output.count("Traffic Airflow metadata query failed") == 2
    assert "Traceback" in task_log_output
    assert secret not in task_log_output


def test_collect_airflow_scheduled_run_summary_logs_safe_traceback_when_redaction_is_unavailable(
    monkeypatch,
    capfd,
):
    secret = "traffic-metadata-secret"
    detected_at = datetime(2026, 7, 13, 2, 30, tzinfo=timezone.utc)
    sessions = [_FailingAirflowSession(secret), _FailingAirflowSession(secret)]

    import airflow.models.dagrun as airflow_dagrun
    import airflow.utils.session as airflow_session
    import common.security as security

    monkeypatch.setattr(airflow_dagrun, "DagRun", _ScheduledDagRun)
    monkeypatch.setattr(
        airflow_session,
        "create_session",
        lambda: _SessionContext(sessions.pop(0)),
    )
    monkeypatch.setattr(
        security,
        "refresh_env_secrets",
        lambda: (_ for _ in ()).throw(RuntimeError("redaction unavailable")),
    )

    with pytest.raises(RuntimeError):
        ORIGINAL_COLLECT_AIRFLOW_SUMMARY("traffic_incident_bronze", detected_at, 24)
    task_log_output = capfd.readouterr().out

    assert sessions == []
    assert task_log_output.count("Traffic Airflow metadata query failed") == 2
    assert "diagnostic=redaction_unavailable" in task_log_output
    assert "Traceback" in task_log_output
    assert secret not in task_log_output


def test_collect_airflow_scheduled_run_summary_excludes_manual_and_outside_window(
    monkeypatch,
):
    detected_at = datetime(2026, 7, 13, 2, 30, tzinfo=timezone.utc)
    scheduled = _ScheduledDagRun(
        state="success",
        logical_date=datetime(2026, 7, 12, 3, 0, tzinfo=timezone.utc),
        run_id="scheduled__inside",
        task_instances=[],
    )
    manual = _ScheduledDagRun(
        state="failed",
        logical_date=datetime(2026, 7, 12, 4, 0, tzinfo=timezone.utc),
        run_id="manual__excluded",
        task_instances=[],
    )
    outside = _ScheduledDagRun(
        state="success",
        logical_date=datetime(2026, 7, 11, 2, 29, tzinfo=timezone.utc),
        run_id="scheduled__outside",
        task_instances=[],
    )
    session = _AirflowSession([scheduled, manual, outside])

    import airflow.models.dagrun as airflow_dagrun
    import airflow.utils.session as airflow_session

    monkeypatch.setattr(airflow_dagrun, "DagRun", _ScheduledDagRun)
    monkeypatch.setattr(
        airflow_session, "create_session", lambda: _SessionContext(session)
    )

    summary = ORIGINAL_COLLECT_AIRFLOW_SUMMARY(
        "traffic_incident_bronze", detected_at, 24
    )

    assert summary == {
        "expected": 1,
        "success": 1,
        "failed": 0,
        "running": 0,
        "failures": [],
    }


def test_lookup_airflow_problem_reason_normalizes_redacted_document(monkeypatch):
    class FakeStorage:
        def list_keys(self, prefix):
            assert prefix.startswith("errors/observed_date=2026-07-12/domain=traffic/")
            return [
                "errors/observed_date=2026-07-12/domain=traffic/dag_id=traffic_incident_bronze/"
                "scheduled__run__123_api-error.json"
            ]

        def read_json(self, key):
            return {
                "run_id": "scheduled__run",
                "task_id": "record_seoul_traffic_run_started",
                "title": "TrinoConnectionError",
                "detail": "trino DNS 이름 해석 실패\nsecret=do-not-show",
            }

    monkeypatch.setattr(
        evidence, "_build_airflow_problem_storage", lambda: FakeStorage()
    )

    reason = evidence._lookup_airflow_problem_reason(
        dag_id="traffic_incident_bronze",
        run_id="scheduled__run",
        task_id="record_seoul_traffic_run_started",
        logical_date=datetime(2026, 7, 12, 2, 30, tzinfo=timezone.utc),
    )

    assert reason == "TrinoConnectionError: trino DNS 이름 해석 실패"
    assert "secret=do-not-show" not in reason


def test_normalize_airflow_problem_reason_fails_closed_when_redaction_fails(
    monkeypatch,
):
    import common.security as security

    def fail_redaction(_value):
        raise RuntimeError("credential=redaction-secret")

    monkeypatch.setattr(security, "refresh_env_secrets", lambda: 0)
    monkeypatch.setattr(security, "redact", fail_redaction)

    reason = evidence._normalize_airflow_problem_reason(
        {
            "title": "TrinoConnectionError",
            "detail": "credential=should-not-appear",
        }
    )

    assert reason == evidence.AIRFLOW_FAILURE_REASON_FALLBACK
    assert "should-not-appear" not in reason
    assert len(reason) <= evidence.AIRFLOW_FAILURE_REASON_MAX_LENGTH


def test_lookup_airflow_problem_reason_falls_back_without_exposing_r2_error(
    monkeypatch, caplog
):
    monkeypatch.setattr(
        evidence,
        "_build_airflow_problem_storage",
        lambda: (_ for _ in ()).throw(RuntimeError("credential=do-not-show")),
    )

    reason = evidence._lookup_airflow_problem_reason(
        dag_id="traffic_incident_bronze",
        run_id="scheduled__run",
        task_id="record_seoul_traffic_run_started",
        logical_date=datetime(2026, 7, 12, 2, 30, tzinfo=timezone.utc),
    )

    assert reason == "원인 미확인"
    assert "do-not-show" not in caplog.text
