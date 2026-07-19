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
        "collect_scheduled_run_summary",
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
                    "logical_date": datetime(2026, 7, 12, 2, 35, tzinfo=timezone.utc),
                    "run_id": "scheduled__2026-07-12T02:35:00+00:00",
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
    assert result["scheduled_runs"]["failed"] == 5
    assert "스케줄 수집 상태: 283/288 성공, 5 실패" in message
    assert "실패 수집 공백:" in message
    assert "2026-07-12 11:30~11:35 KST (10분)" in message
    assert "2026-07-12 11:50~11:50 KST (5분)" in message
    assert "11:30~11:50 KST (25분)" not in message
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


def _traffic_pipeline_report(status="PASS"):
    return {
        "report_name": "traffic_pipeline_reliability_v2",
        "domain": "traffic",
        "report_date": "2026-07-19",
        "detected_at": "2026-07-19T09:00:00+09:00",
        "lookback_hours": 24,
        "status": status,
        "data_plane_status": "PASS",
        "control_plane_status": status,
        "source": {
            "status": "PASS",
            "freshness_minutes": 3,
            "coverage_percent": 100.0,
            "pending_count": 0,
            "publishability_ok": True,
        },
        "scheduled_runs": {
            "expected": 288,
            "success": 288,
            "failed": 0,
            "running": 0,
        },
        "stages": [
            {
                "key": "flow_silver",
                "label": "Flow Silver",
                "status": "PASS",
                "age_minutes": 12,
                "duration_ms": {"p50": 20_000, "p95": 30_000},
            },
            {
                "key": "gold",
                "label": "Traffic Gold",
                "status": status,
                "age_minutes": 20,
                "duration_ms": {"p50": 80_000, "p95": 120_000},
            },
        ],
        "bottleneck": {
            "key": "gold",
            "label": "Traffic Gold",
            "status": status,
            "p95_ms": 120_000,
        },
        "trend": [
            {"report_date": "2026-07-17", "status": "UNKNOWN"},
            {"report_date": "2026-07-18", "status": "WARN"},
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
def test_traffic_pipeline_card_uses_report_status_and_five_named_fields(
    status, color
):
    payload = discord.build_traffic_discord_payload(
        _traffic_pipeline_report(status)
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
    assert "Traffic Gold" in embed["fields"][3]["value"]
    assert "◻️ 07-17" in embed["fields"][4]["value"]
    assert "매일 09:00 KST" in embed["footer"]["text"]


def test_traffic_pipeline_card_respects_discord_limits_and_utf8():
    report_value = _traffic_pipeline_report("WARN")
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

    payload = discord.build_traffic_discord_payload(report_value)
    embed = payload["embeds"][0]
    serialized = json.dumps(payload, ensure_ascii=False)

    assert len(embed["title"]) <= 256
    assert len(embed.get("description", "")) <= 4096
    assert len(embed["fields"]) <= 25
    assert all(len(field["name"]) <= 256 for field in embed["fields"])
    assert all(len(field["value"]) <= 1024 for field in embed["fields"])
    assert "파이프라인" in serialized


def test_traffic_send_discord_report_posts_structured_payload(monkeypatch):
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
        _traffic_pipeline_report(), webhook_url="https://discord.example/webhook"
    )
    request, timeout = calls[0]
    payload = json.loads(request.data.decode("utf-8"))
    assert payload["embeds"][0]["fields"][2]["name"] == "파이프라인"
    assert request.get_method() == "POST"
    assert timeout == 10
