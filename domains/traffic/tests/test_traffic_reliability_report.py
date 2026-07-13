import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.error import URLError

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest import reliability_report as report  # noqa: E402


ORIGINAL_COLLECT_DAG_RUN_SUMMARY = report.collect_dag_run_summary
ORIGINAL_COLLECT_AIRFLOW_SUMMARY = report.collect_airflow_scheduled_run_summary


class RecordingCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.statements = []

    def execute(self, sql):
        self.statements.append(" ".join(sql.split()))

    def fetchone(self):
        return self.rows.pop(0)


@pytest.fixture(autouse=True)
def stub_dag_run_summary(monkeypatch):
    monkeypatch.setattr(
        report,
        "collect_dag_run_summary",
        lambda *args: {
            "dag_id": args[2],
            "success": 3,
            "failed": 0,
            "running": 1,
        },
    )
    monkeypatch.setattr(
        report,
        "collect_airflow_scheduled_run_summary",
        lambda *args: {
            "expected": 4,
            "success": 3,
            "failed": 0,
            "running": 1,
            "failures": [],
        },
    )


def test_build_traffic_report_passes_for_fresh_complete_data(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_SCHEMA", "weather_traffic_bronze")
    cursor = RecordingCursor(
        rows=[
            (1, 25, 25, 1000, 0, datetime(2026, 7, 2, 8, 55, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "PASS"
    assert result["dag_runs"] == {"dag_id": "traffic_incident_bronze", "success": 3, "failed": 0, "running": 1}
    assert result["traffic"]["parsed_row_count"] == 25
    assert "bronze_seoul_traffic_incident_request_audit" in result["blast_radius"][1]
    assert "current_timestamp - INTERVAL '24' HOUR" in cursor.statements[0]


def test_traffic_dag_run_summary_uses_manifest_table(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_SCHEMA", "weather_traffic_bronze")
    cursor = RecordingCursor(rows=[(3, 0, 1)])
    config = report.report_config()

    result = ORIGINAL_COLLECT_DAG_RUN_SUMMARY(
        cursor,
        config,
        "traffic_incident_bronze",
        datetime(2026, 7, 4, 9, 0, tzinfo=timezone.utc),
    )

    assert result == {"dag_id": "traffic_incident_bronze", "success": 3, "failed": 0, "running": 1}
    assert "bronze_collection_run_manifest" in cursor.statements[0]
    assert "dag_id = 'traffic_incident_bronze'" in cursor.statements[0]


def test_traffic_report_fails_when_total_exceeds_requested_range(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (1, 1000, 1500, 1000, 0, datetime(2026, 7, 2, 8, 55, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "FAIL"
    assert result["traffic"]["coverage_ok"] is False


def test_traffic_report_schedule_requires_dev_target_and_webhook(monkeypatch):
    monkeypatch.delenv("ASK_SEOUL_TRAFFIC_REPORT_DAG_SCHEDULE", raising=False)
    monkeypatch.delenv("ASK_SEOUL_REPORT_DAG_SCHEDULE", raising=False)
    monkeypatch.delenv("ASK_SEOUL_DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("TRAFFIC_DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    assert report.report_dag_schedule() is None

    monkeypatch.setenv("ASK_SEOUL_DISCORD_WEBHOOK_URL", "https://discord.example/webhook")
    assert report.report_dag_schedule() == "0 9 * * *"

    monkeypatch.setenv("ASK_SEOUL_TARGET", "prod")
    assert report.report_dag_schedule() is None


def test_traffic_message_does_not_include_webhook(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_DISCORD_WEBHOOK_URL", "https://discord.example/secret-token")
    cursor = RecordingCursor(
        rows=[
            (1, 25, 25, 1000, 0, datetime(2026, 7, 2, 8, 55, tzinfo=timezone.utc)),
        ]
    )
    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    message = report.format_traffic_discord_message(result)

    assert "secret-token" not in message
    assert message.splitlines()[0].startswith("서울시 돌발정보 Bronze 신뢰성 리포트 -")
    assert "✅ 리포트 상태: 성공" in message
    assert "success=3 failed=0 running=1" in message
    assert "Bronze" in message


def test_traffic_report_fails_and_describes_failed_scheduled_runs(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setattr(
        report,
        "collect_airflow_scheduled_run_summary",
        lambda *args: {
            "expected": 288,
            "success": 283,
            "failed": 5,
            "running": 0,
            "failures": [
                {
                    "logical_date": datetime(2026, 7, 12, 2, 30, tzinfo=timezone.utc),
                    "run_id": "scheduled__2026-07-12T02:30:00+00:00",
                    "task_id": "record_seoul_traffic_run_started",
                    "reason": "TrinoConnectionError: trino DNS 이름 해석 실패",
                },
                {
                    "logical_date": datetime(2026, 7, 12, 2, 50, tzinfo=timezone.utc),
                    "run_id": "scheduled__2026-07-12T02:50:00+00:00",
                    "task_id": "record_seoul_traffic_run_started",
                    "reason": "TrinoConnectionError: trino DNS 이름 해석 실패",
                },
            ],
        },
    )
    cursor = RecordingCursor(
        rows=[
            (1, 25, 25, 1000, 0, datetime(2026, 7, 2, 8, 55, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 13, 9, 0, tzinfo=report.KST),
    )
    message = report.format_traffic_discord_message(result)

    assert result["status"] == "FAIL"
    assert result["airflow_runs"]["failed"] == 5
    assert "스케줄 수집 상태: 283/288 성공, 5 실패" in message
    assert "실패 수집 공백: 2026-07-12 11:30~11:50 KST (25분)" in message
    assert "11:30 KST | task=record_seoul_traffic_run_started" in message
    assert "TrinoConnectionError" in message


def test_traffic_report_fails_safely_when_airflow_summary_query_fails(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")

    def fail_summary(*_args):
        raise RuntimeError("credential=must-not-be-in-report")

    monkeypatch.setattr(report, "collect_airflow_scheduled_run_summary", fail_summary)
    cursor = RecordingCursor(
        rows=[
            (1, 25, 25, 1000, 0, datetime(2026, 7, 2, 8, 55, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 13, 9, 0, tzinfo=report.KST),
    )

    assert result["status"] == "FAIL"
    assert result["airflow_runs"]["reason"] == "airflow_metadata_query_failed"
    assert result["airflow_runs"]["error_type"] == "RuntimeError"
    assert "must-not-be-in-report" not in json.dumps(result, ensure_ascii=False)


def test_traffic_send_discord_posts_payload(monkeypatch):
    calls = []

    class Response:
        status = 204

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout):
        calls.append((request, timeout))
        return Response()

    monkeypatch.setattr(report.urllib.request, "urlopen", fake_urlopen)

    assert report.send_discord_message("hello", webhook_url="https://discord.example/webhook") is True
    request, timeout = calls[0]
    payload = json.loads(request.data.decode("utf-8"))
    assert "content" not in payload
    assert payload["embeds"][0]["title"] == "hello"
    assert payload["embeds"][0]["color"] == report.DISCORD_GREEN
    failure_payload = json.loads(report._discord_payload("title\n❌ 리포트 상태: 실패").decode("utf-8"))
    assert failure_payload["embeds"][0]["color"] == report.DISCORD_RED
    assert request.get_method() == "POST"
    assert request.headers["User-agent"] == "ask-seoul-traffic-report/1.0"
    assert timeout == 10


def test_traffic_send_discord_swallows_failure_without_logging_webhook(monkeypatch, caplog):
    secret_url = "https://discord.com/api/webhooks/123/SECRET_TOKEN"

    def fake_urlopen(request, timeout):
        raise URLError("network blocked")

    monkeypatch.setattr(report.urllib.request, "urlopen", fake_urlopen)

    assert report.send_discord_message("hello", webhook_url=secret_url) is False
    assert "SECRET_TOKEN" not in caplog.text
    assert secret_url not in caplog.text


class _AirflowField:
    def __init__(self, name):
        self.name = name

    def __eq__(self, value):
        return (self.name, "eq", value)

    def __ge__(self, value):
        return (self.name, "ge", value)

    def __le__(self, value):
        return (self.name, "le", value)


class _ScheduledDagRun:
    dag_id = _AirflowField("dag_id")
    run_type = _AirflowField("run_type")
    logical_date = _AirflowField("logical_date")

    def __init__(self, *, state, logical_date, run_id, task_instances):
        self.state = state
        self.logical_date = logical_date
        self.run_id = run_id
        self._task_instances = task_instances

    def get_task_instances(self, session=None):
        return list(self._task_instances)


class _AirflowQuery:
    def __init__(self, rows):
        self.rows = rows
        self.filters = []

    def filter(self, *conditions):
        self.filters.extend(conditions)
        return self

    def order_by(self, *_columns):
        return self

    def all(self):
        return list(self.rows)


class _AirflowSession:
    def __init__(self, rows):
        self.query_calls = []
        self.query_result = _AirflowQuery(rows)

    def query(self, model):
        self.query_calls.append(model)
        return self.query_result


class _SessionContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, tb):
        return False


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
        report,
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
    assert summary["failures"][0]["reason"] == "TrinoConnectionError: trino DNS 이름 해석 실패"


def test_collect_airflow_scheduled_run_summary_excludes_manual_and_outside_window(monkeypatch):
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
    monkeypatch.setattr(airflow_session, "create_session", lambda: _SessionContext(session))

    summary = ORIGINAL_COLLECT_AIRFLOW_SUMMARY(
        "traffic_incident_bronze", detected_at, 24
    )

    assert summary == {"expected": 1, "success": 1, "failed": 0, "running": 0, "failures": []}


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

    monkeypatch.setattr(report, "_build_airflow_problem_storage", lambda: FakeStorage())

    reason = report._lookup_airflow_problem_reason(
        dag_id="traffic_incident_bronze",
        run_id="scheduled__run",
        task_id="record_seoul_traffic_run_started",
        logical_date=datetime(2026, 7, 12, 2, 30, tzinfo=timezone.utc),
    )

    assert reason == "TrinoConnectionError: trino DNS 이름 해석 실패"
    assert "secret=do-not-show" not in reason


def test_normalize_airflow_problem_reason_fails_closed_when_redaction_fails(monkeypatch):
    import common.security as security

    def fail_redaction(_value):
        raise RuntimeError("credential=redaction-secret")

    monkeypatch.setattr(security, "refresh_env_secrets", lambda: 0)
    monkeypatch.setattr(security, "redact", fail_redaction)

    reason = report._normalize_airflow_problem_reason(
        {
            "title": "TrinoConnectionError",
            "detail": "credential=should-not-appear",
        }
    )

    assert reason == report.AIRFLOW_FAILURE_REASON_FALLBACK
    assert "should-not-appear" not in reason
    assert len(reason) <= report.AIRFLOW_FAILURE_REASON_MAX_LENGTH


def test_lookup_airflow_problem_reason_falls_back_without_exposing_r2_error(monkeypatch, caplog):
    monkeypatch.setattr(
        report,
        "_build_airflow_problem_storage",
        lambda: (_ for _ in ()).throw(RuntimeError("credential=do-not-show")),
    )

    reason = report._lookup_airflow_problem_reason(
        dag_id="traffic_incident_bronze",
        run_id="scheduled__run",
        task_id="record_seoul_traffic_run_started",
        logical_date=datetime(2026, 7, 12, 2, 30, tzinfo=timezone.utc),
    )

    assert reason == "원인 미확인"
    assert "do-not-show" not in caplog.text
