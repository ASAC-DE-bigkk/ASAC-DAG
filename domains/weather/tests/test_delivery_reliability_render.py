import json
import subprocess
import sys
from pathlib import Path

import pytest

WEATHER_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEATHER_ROOT))

from weather_ingest.delivery_reliability.render import (  # noqa: E402
    report_from_document,
    render_csv,
    render_json,
    render_markdown,
)


ROOT = Path(__file__).resolve().parents[3]
CLI = WEATHER_ROOT / "weather_ingest" / "delivery_reliability_pilot.py"


def run_mapping(**overrides):
    value = {
        "domain": "traffic",
        "scheduled_run_id": "scheduled__2026-07-19T00:00:00Z",
        "scheduled_at": "2026-07-19T00:00:00Z",
        "bronze_status": "SUCCESS",
        "is_publishable": True,
        "completeness_status": "COMPLETE",
        "source_status": "ZERO_ROW",
        "transform_status": "SUCCESS",
        "gold_query_status": "SUCCESS",
        "gold_available_at": "2026-07-19T00:05:00Z",
        "gold_row_count": 0,
        "sla_minutes": 30,
    }
    value.update(overrides)
    return value


def document(*runs):
    return {
        "observed_at": "2026-07-19T01:00:00Z",
        "lookback_days": 7,
        "runs": list(runs),
    }


def test_renderers_preserve_run_evidence_and_not_available_values():
    report = report_from_document(
        document(
            run_mapping(
                transform_status=None,
                gold_query_status=None,
                gold_available_at=None,
                gold_row_count=None,
            )
        )
    )

    json_output = render_json(report)
    csv_output = render_csv(report)
    markdown_output = render_markdown(report)

    assert json.loads(json_output)["runs"][0]["gold_row_count"] is None
    assert "scheduled_run_id" in csv_output
    assert "NOT_AVAILABLE" in csv_output
    assert "# Delivery Reliability Pilot" in markdown_output
    assert "Gold delivery" in markdown_output
    assert "scheduled__2026-07-19T00:00:00Z" in markdown_output


def test_report_document_rejects_duplicate_run_grain():
    duplicate = run_mapping()

    with pytest.raises(ValueError, match="duplicate delivery evidence grain"):
        report_from_document(document(duplicate, duplicate))


def test_cli_reads_normalized_json_and_writes_only_stdout(tmp_path):
    input_path = tmp_path / "evidence.json"
    input_path.write_text(
        json.dumps(document(run_mapping()), ensure_ascii=False), encoding="utf-8"
    )

    completed = subprocess.run(
        [sys.executable, str(CLI), str(input_path), "--format", "json"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["contract_version"] == "delivery-reliability-pilot-v1"
    assert payload["runs"][0]["state"] == "DELIVERED_ZERO_ROW"
    assert list(tmp_path.iterdir()) == [input_path]


def test_cli_returns_nonzero_and_no_report_for_invalid_evidence(tmp_path):
    input_path = tmp_path / "duplicate.json"
    duplicate = run_mapping()
    input_path.write_text(
        json.dumps(document(duplicate, duplicate), ensure_ascii=False), encoding="utf-8"
    )

    completed = subprocess.run(
        [sys.executable, str(CLI), str(input_path), "--format", "markdown"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2
    assert completed.stdout == ""
    assert "duplicate delivery evidence grain" in completed.stderr
