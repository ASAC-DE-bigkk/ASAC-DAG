import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest import reliability_report as report  # noqa: E402


ORIGINAL_COLLECT_DAG_RUN_SUMMARY = report.collect_dag_run_summary


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


def test_build_traffic_report_passes_for_fresh_complete_data(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_SCHEMA", "dev_masondev1024")
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
    monkeypatch.setenv("ASK_SEOUL_SCHEMA", "dev_masondev1024")
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
