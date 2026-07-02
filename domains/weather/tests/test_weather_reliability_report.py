import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest import reliability_report as report  # noqa: E402


class RecordingCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.statements = []

    def execute(self, sql):
        self.statements.append(" ".join(sql.split()))

    def fetchone(self):
        return self.rows.pop(0)


def test_build_weather_report_passes_for_fresh_complete_data(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_SCHEMA", "dev_masondev1024")
    cursor = RecordingCursor(
        rows=[
            ("20260702", "0800", 80, 80, 6400, datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "PASS"
    assert result["weather"]["grid_count"] == 80
    assert result["blast_radius"] == ["iceberg_dev.dev_masondev1024.bronze_kma_vilage_fcst"]
    assert "current_timestamp - INTERVAL '24' HOUR" in cursor.statements[0]


def test_weather_report_fails_when_grid_coverage_is_incomplete(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            ("20260702", "0800", 79, 79, 6320, datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "FAIL"
    assert result["weather"]["coverage_ok"] is False


def test_weather_report_schedule_requires_dev_target_and_webhook(monkeypatch):
    monkeypatch.delenv("ASK_SEOUL_WEATHER_REPORT_DAG_SCHEDULE", raising=False)
    monkeypatch.delenv("ASK_SEOUL_REPORT_DAG_SCHEDULE", raising=False)
    monkeypatch.delenv("ASK_SEOUL_DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("WEATHER_DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    assert report.report_dag_schedule() is None

    monkeypatch.setenv("ASK_SEOUL_DISCORD_WEBHOOK_URL", "https://discord.example/webhook")
    assert report.report_dag_schedule() == "0 9 * * *"

    monkeypatch.setenv("ASK_SEOUL_TARGET", "prod")
    assert report.report_dag_schedule() is None


def test_weather_message_does_not_include_webhook(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_DISCORD_WEBHOOK_URL", "https://discord.example/secret-token")
    cursor = RecordingCursor(
        rows=[
            ("20260702", "0800", 80, 80, 6400, datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc)),
        ]
    )
    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    message = report.format_weather_discord_message(result)

    assert "secret-token" not in message
    assert "기상청 단기예보 Bronze 신뢰성 리포트" in message


def test_weather_send_discord_posts_payload(monkeypatch):
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
    assert request.data == b'{"content": "hello"}'
    assert request.get_method() == "POST"
    assert request.headers["User-agent"] == "ask-seoul-weather-report/1.0"
    assert timeout == 10


def test_weather_send_discord_swallows_failure_without_logging_webhook(monkeypatch, caplog):
    secret_url = "https://discord.com/api/webhooks/123/SECRET_TOKEN"

    def fake_urlopen(request, timeout):
        raise URLError("network blocked")

    monkeypatch.setattr(report.urllib.request, "urlopen", fake_urlopen)

    assert report.send_discord_message("hello", webhook_url=secret_url) is False
    assert "SECRET_TOKEN" not in caplog.text
    assert secret_url not in caplog.text
