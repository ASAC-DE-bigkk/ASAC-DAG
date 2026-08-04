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
    # 제목 앞에 환경 표식이 붙는다(ASAC-DAG#692) — 여러 인스턴스가 같은 채널을 쓰므로
    # 어느 환경 결과인지 메시지만 보고 갈릴 수 있어야 한다. 표식 뒤 내용은 이전과 동일하다.
    assert payload["embeds"][0]["title"].endswith("hello")
    assert payload["embeds"][0]["title"].startswith("[")
    assert "env=" in payload["embeds"][0]["footer"]["text"]
    assert payload["embeds"][0]["color"] == discord.DISCORD_GREEN
    failure_payload = discord._discord_payload("title\n❌ 리포트 상태: 실패")
    assert failure_payload["embeds"][0]["color"] == discord.DISCORD_RED
    warning_payload = discord._discord_payload("title\n⚠️ 리포트 상태: 경고")
    assert warning_payload["embeds"][0]["color"] == discord.DISCORD_YELLOW
    assert request.get_method() == "POST"
    # 전송 계층은 공용이지만 UA 에는 도메인이 실린다 — 수신측에서 어느 파이프라인이
    # 보냈는지 갈려야 한다(#692). 제품 토큰은 하나로 고정.
    assert request.headers["User-agent"] == "asac-elt-notify/1.0 (weather)"
    assert timeout == 15.0


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


def _weather_pipeline_report(status="PASS"):
    return {
        "report_name": "weather_pipeline_reliability_v2",
        "domain": "weather",
        "report_date": "2026-07-19",
        "detected_at": "2026-07-19T09:00:00+09:00",
        "lookback_hours": 24,
        "status": status,
        "data_plane_status": "PASS",
        "control_plane_status": status,
        "source": {
            "status": "PASS",
            "freshness_minutes": 20,
            "coverage_percent": 100.0,
            "publishability_ok": True,
        },
        "weather": {
            "base_time_count": 8,
            "expected_base_time_count": 8,
            "grid_slot_count": 640,
            "expected_grid_slot_count": 640,
            "raw_object_count": 800,
            "additional_raw_page_count": 160,
        },
        "stages": [
            {
                "key": "bronze",
                "label": "Weather Bronze",
                "status": "PASS",
                "age_minutes": 20,
                "duration_ms": {"p50": 40_000, "p95": 60_000},
            },
            {
                "key": "transform",
                "label": "Weather Silver/Gold",
                "status": status,
                "age_minutes": 30,
                "duration_ms": {"p50": 100_000, "p95": 180_000},
            },
        ],
        "bottleneck": {
            "key": "transform",
            "label": "Weather Silver/Gold",
            "status": status,
            "p95_ms": 180_000,
        },
        "trend": [
            {"report_date": "2026-07-17", "status": "UNKNOWN"},
            {"report_date": "2026-07-18", "status": "PASS"},
            {"report_date": "2026-07-19", "status": status},
        ],
    }


@pytest.mark.parametrize(
    ("status", "color"),
    [
        ("PASS", discord.DISCORD_GREEN),
        ("WARN", discord.DISCORD_YELLOW),
        ("FAIL", discord.DISCORD_RED),
    ],
)
def test_weather_pipeline_card_uses_report_status_and_five_named_fields(
    status, color
):
    payload = discord.build_weather_discord_payload(
        _weather_pipeline_report(status)
    )
    embed = payload["embeds"][0]

    assert embed["color"] == color
    assert [field["name"] for field in embed["fields"]] == [
        "상태",
        "수집 품질",
        "파이프라인",
        "관측 병목",
        "7일 추세",
    ]
    assert "Weather Silver/Gold" in embed["fields"][3]["value"]
    assert "◻️ 07-17" in embed["fields"][4]["value"]
    assert "매일 09:00 KST" in embed["footer"]["text"]


def test_weather_pipeline_card_respects_discord_limits_and_utf8():
    report_value = _weather_pipeline_report("WARN")
    report_value["stages"] = [
        {
            "key": str(index),
            "label": "긴 단계 이름 " * 200,
            "status": "WARN",
            "age_minutes": index,
            "duration_ms": {"p50": 1, "p95": 2},
        }
        for index in range(40)
    ]

    payload = discord.build_weather_discord_payload(report_value)
    embed = payload["embeds"][0]
    serialized = json.dumps(payload, ensure_ascii=False)

    assert len(embed["title"]) <= 256
    assert len(embed.get("description", "")) <= 4096
    assert len(embed["fields"]) <= 25
    assert all(len(field["name"]) <= 256 for field in embed["fields"])
    assert all(len(field["value"]) <= 1024 for field in embed["fields"])
    assert "파이프라인" in serialized


def test_weather_send_discord_report_posts_structured_payload(monkeypatch):
    calls = []

    class Response:
        status = 204

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        discord.urllib.request,
        "urlopen",
        lambda request, timeout: calls.append((request, timeout)) or Response(),
    )

    assert discord.send_discord_report(
        _weather_pipeline_report(), webhook_url="https://discord.example/webhook"
    )
    request, timeout = calls[0]
    payload = json.loads(request.data.decode("utf-8"))
    assert payload["embeds"][0]["fields"][2]["name"] == "파이프라인"
    assert request.get_method() == "POST"
    assert timeout == 15.0   # 공용 전송 계층 기본값(#692) — DISCORD_TIMEOUT_SECONDS 로 조정
