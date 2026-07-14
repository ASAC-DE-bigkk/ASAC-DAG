import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest import reliability_report as report  # noqa: E402
from traffic_ingest.reliability import discord, report as composition  # noqa: E402
from traffic_reliability_test_support import (  # noqa: E402
    RecordingCursor,
    stub_report_dependencies,
)


@pytest.fixture(autouse=True)
def stub_reliability_dependencies(monkeypatch):
    stub_report_dependencies(monkeypatch)


def test_traffic_message_does_not_include_webhook(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv(
        "ASK_SEOUL_DISCORD_WEBHOOK_URL", "https://discord.example/secret-token"
    )
    cursor = RecordingCursor(
        rows=[
            (1, 25, 25, 1000, 0, datetime(2026, 7, 2, 8, 55, tzinfo=timezone.utc)),
        ]
    )
    result = report.build_traffic_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    message = discord.format_traffic_discord_message(result)

    assert "secret-token" not in message
    assert message.splitlines()[0].startswith("서울시 돌발정보 Bronze 신뢰성 리포트 -")
    assert "✅ 리포트 상태: 성공" in message
    assert "success=3 failed=0 running=1" in message
    assert "Bronze" in message


def test_traffic_report_fails_and_describes_failed_scheduled_runs(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setattr(
        composition,
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
    message = discord.format_traffic_discord_message(result)

    assert result["status"] == "FAIL"
    assert result["airflow_runs"]["failed"] == 5
    assert "스케줄 수집 상태: 283/288 성공, 5 실패" in message
    assert "실패 수집 공백: 2026-07-12 11:30~11:50 KST (25분)" in message
    assert "11:30 KST | task=record_seoul_traffic_run_started" in message
    assert "TrinoConnectionError" in message


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

    monkeypatch.setattr(discord.urllib.request, "urlopen", fake_urlopen)

    assert (
        discord.send_discord_message(
            "hello", webhook_url="https://discord.example/webhook"
        )
        is True
    )
    request, timeout = calls[0]
    payload = json.loads(request.data.decode("utf-8"))
    assert "content" not in payload
    assert payload["embeds"][0]["title"] == "hello"
    assert payload["embeds"][0]["color"] == discord.DISCORD_GREEN
    failure_payload = json.loads(
        discord._discord_payload("title\n❌ 리포트 상태: 실패").decode("utf-8")
    )
    assert failure_payload["embeds"][0]["color"] == discord.DISCORD_RED
    warning_payload = json.loads(
        discord._discord_payload("title\n⚠️ 리포트 상태: 경고").decode("utf-8")
    )
    assert warning_payload["embeds"][0]["color"] == discord.DISCORD_YELLOW
    assert request.get_method() == "POST"
    assert request.headers["User-agent"] == "ask-seoul-traffic-report/1.0"
    assert timeout == 10


def test_traffic_send_discord_swallows_failure_without_logging_webhook(
    monkeypatch, caplog
):
    secret_url = "https://discord.com/api/webhooks/123/SECRET_TOKEN"

    def fake_urlopen(request, timeout):
        raise URLError("network blocked")

    monkeypatch.setattr(discord.urllib.request, "urlopen", fake_urlopen)

    assert discord.send_discord_message("hello", webhook_url=secret_url) is False
    assert "SECRET_TOKEN" not in caplog.text
    assert secret_url not in caplog.text
