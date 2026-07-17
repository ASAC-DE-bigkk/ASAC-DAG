import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest import reliability_report as report  # noqa: E402


def test_traffic_report_schedule_requires_dev_target_and_webhook(monkeypatch):
    monkeypatch.delenv("ASK_SEOUL_TRAFFIC_REPORT_DAG_SCHEDULE", raising=False)
    monkeypatch.delenv("ASK_SEOUL_REPORT_DAG_SCHEDULE", raising=False)
    monkeypatch.delenv("ASK_SEOUL_DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("TRAFFIC_DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    assert report.report_dag_schedule() is None

    monkeypatch.setenv(
        "ASK_SEOUL_DISCORD_WEBHOOK_URL", "https://discord.example/webhook"
    )
    assert report.report_dag_schedule() == "0 9 * * *"

    monkeypatch.setenv("ASK_SEOUL_TARGET", "prod")
    monkeypatch.setenv("ASK_SEOUL_TRAFFIC_REPORT_DAG_SCHEDULE", "*/5 * * * *")
    assert report.report_dag_schedule() is None
