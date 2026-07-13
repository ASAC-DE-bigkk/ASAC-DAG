import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest import reliability_report as report  # noqa: E402


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
            "success": 2,
            "failed": 1,
            "running": 0,
            "expected_raw_objects": 160,
            "actual_raw_objects": 160,
            "last_success_at": "2026-07-02 08:20:00+00:00",
            "last_publishable_at": "2026-07-02 08:20:00+00:00",
            "latest_dag_run_id": "scheduled__2026-07-02T08:00:00+00:00",
            "latest_status": "SUCCESS",
            "latest_is_publishable": True,
            "latest_event_at": "2026-07-02 08:20:00+00:00",
        },
    )


def test_build_weather_report_passes_for_fresh_complete_data(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_SCHEMA", "weather_traffic_bronze")
    cursor = RecordingCursor(
        rows=[
            (8, 640, 640, 512000, 80, 80, 8, "20260702", "0800", datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "PASS"
    assert result["dag_runs"] == {
        "dag_id": "weather_vilage_fcst_bronze",
        "success": 2,
        "failed": 1,
        "running": 0,
        "expected_raw_objects": 160,
        "actual_raw_objects": 160,
        "last_success_at": "2026-07-02 08:20:00+00:00",
        "last_publishable_at": "2026-07-02 08:20:00+00:00",
        "latest_dag_run_id": "scheduled__2026-07-02T08:00:00+00:00",
        "latest_status": "SUCCESS",
        "latest_is_publishable": True,
        "latest_event_at": "2026-07-02 08:20:00+00:00",
        "publishability_ok": True,
    }
    assert result["weather"]["base_time_count"] == 8
    assert result["weather"]["grid_slot_count"] == 640
    assert result["weather"]["raw_object_count"] == 640
    assert result["weather"]["latest_base_time"] == "0800"
    assert result["blast_radius"] == ["iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst"]
    assert "load_date >= '2026-06-30'" in cursor.statements[0]
    assert "collected_at >= TIMESTAMP '2026-07-01 09:00:00.000000'" in cursor.statements[0]
    assert "FROM by_base" in cursor.statements[0]
    assert result["weather"]["freshness_status"] == "PASS"
    assert result["publishability_ok"] is True
    assert result["late_publishability"] == {
        "status": "NOT_EVALUATED",
        "reason": "bounded late-repair contract is owned by ASAC-DBT #165",
    }


def test_weather_dag_run_summary_uses_manifest_table(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_SCHEMA", "weather_traffic_bronze")
    cursor = RecordingCursor(rows=[
        (
            2,
            1,
            0,
            160,
            120,
            datetime(2026, 7, 4, 8, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 4, 8, 0, tzinfo=timezone.utc),
            "scheduled__2026-07-04T08:00:00+00:00",
            "SUCCESS",
            True,
            datetime(2026, 7, 4, 8, 0, tzinfo=timezone.utc),
        )
    ])
    config = report.report_config()

    result = ORIGINAL_COLLECT_DAG_RUN_SUMMARY(
        cursor,
        config,
        "weather_vilage_fcst_bronze",
        datetime(2026, 7, 4, 9, 0, tzinfo=timezone.utc),
    )

    assert result == {
        "dag_id": "weather_vilage_fcst_bronze",
        "success": 2,
        "failed": 1,
        "running": 0,
        "expected_raw_objects": 160,
        "actual_raw_objects": 120,
        "last_success_at": "2026-07-04 08:00:00+00:00",
        "last_publishable_at": "2026-07-04 08:00:00+00:00",
        "latest_dag_run_id": "scheduled__2026-07-04T08:00:00+00:00",
        "latest_status": "SUCCESS",
        "latest_is_publishable": True,
        "latest_event_at": "2026-07-04 08:00:00+00:00",
    }
    statement = cursor.statements[0]
    assert "bronze_collection_run_manifest" in statement
    assert "dag_id = 'weather_vilage_fcst_bronze'" in statement
    assert "max_by(dag_run_id, ROW(latest_event_at, dag_run_id)) AS latest_dag_run_id" in statement
    assert "max_by(latest_status, ROW(latest_event_at, dag_run_id)) AS latest_status" in statement
    assert (
        "max_by(latest_is_publishable, ROW(latest_event_at, dag_run_id)) "
        "AS latest_is_publishable"
    ) in statement


@pytest.mark.parametrize(
    ("latest_status", "latest_is_publishable"),
    [("FAILED", False), ("SUCCESS", False)],
)
def test_weather_report_does_not_borrow_older_publishable_run(
    monkeypatch,
    latest_status,
    latest_is_publishable,
):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setattr(
        report,
        "collect_dag_run_summary",
        lambda *_args: {
            "dag_id": "weather_vilage_fcst_bronze",
            "success": 1,
            "failed": int(latest_status == "FAILED"),
            "running": 0,
            "expected_raw_objects": 160,
            "actual_raw_objects": 160,
            "last_success_at": "2026-07-04 07:20:00+00:00",
            "last_publishable_at": "2026-07-04 07:20:00+00:00",
            "latest_dag_run_id": "scheduled__2026-07-04T08:00:00+00:00",
            "latest_status": latest_status,
            "latest_is_publishable": latest_is_publishable,
            "latest_event_at": "2026-07-04 08:20:00+00:00",
        },
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
                "20260704",
                "0800",
                datetime(2026, 7, 4, 8, 20, tzinfo=timezone.utc),
            ),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 4, 9, 0, tzinfo=timezone.utc),
    )

    assert result["publishability_ok"] is False
    assert result["status"] == "FAIL"
    assert result["dag_runs"]["latest_dag_run_id"] == "scheduled__2026-07-04T08:00:00+00:00"
    assert result["dag_runs"]["latest_status"] == latest_status
    assert result["dag_runs"]["latest_is_publishable"] is latest_is_publishable
    assert result["dag_runs"]["latest_event_at"] == "2026-07-04 08:20:00+00:00"


def test_weather_report_fails_when_grid_coverage_is_incomplete(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (8, 639, 639, 511200, 79, 80, 7, "20260702", "0800", datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "FAIL"
    assert result["weather"]["coverage_ok"] is False


def test_weather_report_exposes_pagination_page_breakdown(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (8, 640, 800, 640000, 80, 80, 8, "20260702", "0800", datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )
    message = report.format_weather_discord_message(result)

    assert result["weather"]["additional_raw_page_count"] == 160
    assert "Bronze grid slots(커버리지): 640/640개" in message
    assert "Bronze raw API pages(실제 응답): 800개" in message
    assert "추가 pagination pages(page 2+): 160개" in message


def test_weather_report_fails_when_24h_base_time_coverage_is_incomplete(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(
        rows=[
            (6, 480, 480, 426800, 80, 80, 6, "20260706", "0800", datetime(2026, 7, 6, 8, 20, tzinfo=timezone.utc)),
        ]
    )

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 6, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "FAIL"
    assert result["weather"]["coverage_ok"] is False
    assert result["weather"]["base_time_count"] == 6
    assert result["weather"]["expected_base_time_count"] == 8


def test_weather_report_warns_after_four_hours_but_before_six(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(rows=[
        (8, 640, 640, 512000, 80, 80, 8, "20260702", "0800", datetime(2026, 7, 2, 4, 30, tzinfo=timezone.utc)),
    ])

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "WARN"
    assert result["weather"]["freshness_status"] == "WARN"


def test_weather_report_fails_after_six_hours(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(rows=[
        (8, 640, 640, 512000, 80, 80, 8, "20260702", "0800", datetime(2026, 7, 2, 2, 59, tzinfo=timezone.utc)),
    ])

    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "FAIL"
    assert result["weather"]["freshness_status"] == "FAIL"


def test_weather_summary_uses_load_date_and_collected_at_bounds(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    cursor = RecordingCursor(rows=[
        (8, 640, 640, 512000, 80, 80, 8, "20260702", "0800", datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc)),
    ])

    report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    assert "load_date >= '2026-06-30'" in cursor.statements[0]
    assert "collected_at >= TIMESTAMP '2026-07-01 09:00:00.000000'" in cursor.statements[0]


def test_weather_report_schedule_requires_dev_target_and_webhook(monkeypatch):
    monkeypatch.delenv("ASK_SEOUL_WEATHER_REPORT_DAG_SCHEDULE", raising=False)
    monkeypatch.delenv("ASK_SEOUL_REPORT_DAG_SCHEDULE", raising=False)
    monkeypatch.delenv("ASK_SEOUL_DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("WEATHER_DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    assert report.report_dag_schedule() is None

    monkeypatch.setenv("ASK_SEOUL_DISCORD_WEBHOOK_URL", "https://discord.example/webhook")
    assert report.report_dag_schedule() == "0 * * * *"

    monkeypatch.setenv("ASK_SEOUL_TARGET", "prod")
    monkeypatch.setenv("ASK_SEOUL_WEATHER_REPORT_DAG_SCHEDULE", "*/5 * * * *")
    assert report.report_dag_schedule() is None


def test_weather_message_does_not_include_webhook(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv("ASK_SEOUL_DISCORD_WEBHOOK_URL", "https://discord.example/secret-token")
    cursor = RecordingCursor(
        rows=[
            (8, 640, 640, 512000, 80, 80, 8, "20260702", "0800", datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc)),
        ]
    )
    result = report.build_weather_reliability_report(
        cursor=cursor,
        detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
    )

    message = report.format_weather_discord_message(result)

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

    monkeypatch.setattr(report.urllib.request, "urlopen", fake_urlopen)

    assert report.send_discord_message("hello", webhook_url="https://discord.example/webhook") is True
    request, timeout = calls[0]
    payload = json.loads(request.data.decode("utf-8"))
    assert "content" not in payload
    assert payload["embeds"][0]["title"] == "hello"
    assert payload["embeds"][0]["color"] == report.DISCORD_GREEN
    failure_payload = json.loads(report._discord_payload("title\n❌ 리포트 상태: 실패").decode("utf-8"))
    assert failure_payload["embeds"][0]["color"] == report.DISCORD_RED
    warning_payload = json.loads(report._discord_payload("title\n⚠️ 리포트 상태: 경고").decode("utf-8"))
    assert warning_payload["embeds"][0]["color"] == report.DISCORD_YELLOW
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
