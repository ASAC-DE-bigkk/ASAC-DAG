import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest import reliability_report as report  # noqa: E402
from weather_ingest.reliability import discord  # noqa: E402
from weather_reliability_test_support import RecordingCursor, stub_report_dependencies  # noqa: E402


@pytest.fixture(autouse=True)
def stub_reliability_dependencies(monkeypatch):
    stub_report_dependencies(monkeypatch)


def test_weather_report_exposes_pagination_page_breakdown(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (
                8,
                640,
                800,
                640000,
                80,
                80,
                8,
                "20260702",
                "0800",
                datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc),
            ),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )
    message = discord.format_weather_discord_message(result)

    assert result["weather"]["additional_raw_page_count"] == 160
    assert "Bronze grid slots(커버리지): 640/640개" in message
    assert "Bronze raw API pages(실제 응답): 800개" in message
    assert "추가 pagination pages(page 2+): 160개" in message


def test_weather_message_does_not_include_webhook(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv(
        "ASK_SEOUL_DISCORD_WEBHOOK_URL", "https://discord.example/secret-token"
    )
    cursor = RecordingCursor(
        rows=[
            (
                8,
                640,
                640,
                512000,
                80,
                80,
                8,
                "20260702",
                "0800",
                datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc),
            ),
        ]
    )
    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    message = discord.format_weather_discord_message(result)

    assert "secret-token" not in message
    assert message.splitlines()[0].startswith("기상청 단기예보 Bronze 신뢰성 리포트 -")
    assert "✅ 리포트 상태: 성공" in message
    assert "success=2 failed=1 running=0" in message
    assert "API 호출건수" not in message
    assert "Bronze grid slots(커버리지): 640/640개" in message
    assert "Bronze raw API pages(실제 응답): 640개" in message
    assert "추가 pagination pages(page 2+): 0개" in message
    assert "Bronze" in message


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
    assert request.headers["User-agent"] == "ask-seoul-weather-report/1.0"
    assert timeout == 10


def test_weather_send_discord_swallows_failure_without_logging_webhook(
    monkeypatch, caplog
):
    secret_url = "https://discord.com/api/webhooks/123/SECRET_TOKEN"

    def fake_urlopen(request, timeout):
        raise URLError("network blocked")

    monkeypatch.setattr(discord.urllib.request, "urlopen", fake_urlopen)

    assert discord.send_discord_message("hello", webhook_url=secret_url) is False
    assert "SECRET_TOKEN" not in caplog.text
    assert secret_url not in caplog.text
