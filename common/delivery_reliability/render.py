"""Stable JSON, CSV, and Markdown output for pilot evidence review."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import Any, Mapping

from .aggregate import build_pilot_report
from .contract import DeliveryEvidence


RUN_FIELDS = (
    "domain",
    "scheduled_run_id",
    "scheduled_at",
    "detected_at",
    "state",
    "bronze_supplied",
    "gold_delivered",
    "sla_met",
    "delivery_latency_minutes",
    "completeness_status",
    "source_status",
    "transform_status",
    "gold_query_status",
    "gold_available_at",
    "gold_row_count",
    "api_failure_type",
    "retry_count",
)


def _observed_at(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("observed_at must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("observed_at must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("observed_at must include a timezone")
    return parsed


def report_from_document(document: Mapping[str, Any]) -> dict[str, Any]:
    raw_runs = document.get("runs")
    if not isinstance(raw_runs, list):
        raise ValueError("runs must be a JSON array")
    lookback_days = document.get("lookback_days", 7)
    if isinstance(lookback_days, bool) or not isinstance(lookback_days, int):
        raise ValueError("lookback_days must be an integer")
    rows = []
    for index, value in enumerate(raw_runs):
        if not isinstance(value, Mapping):
            raise ValueError(f"runs[{index}] must be a JSON object")
        rows.append(DeliveryEvidence.from_mapping(value))
    return build_pilot_report(
        rows,
        observed_at=_observed_at(document.get("observed_at")),
        lookback_days=lookback_days,
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _display(value: Any) -> Any:
    if value is None:
        return "NOT_AVAILABLE"
    if isinstance(value, bool):
        return str(value).lower()
    return value


def render_csv(report: Mapping[str, Any]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=RUN_FIELDS, lineterminator="\n")
    writer.writeheader()
    for run in report.get("runs", []):
        writer.writerow({field: _display(run.get(field)) for field in RUN_FIELDS})
    return output.getvalue()


def _rate(value: Mapping[str, Any], numerator: str) -> str:
    rate = value.get("rate")
    rendered_rate = "NOT_AVAILABLE" if rate is None else f"{float(rate):.2%}"
    return (
        f"{rendered_rate} "
        f"({value.get(numerator, 0)}/{value.get('evaluable_runs', 0)})"
    )


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Delivery Reliability Pilot",
        "",
        f"- Contract: `{report.get('contract_version')}`",
        f"- Observed at: `{report.get('observed_at')}`",
        f"- Window start: `{report.get('window_start')}`",
        f"- Excluded outside window: {report.get('excluded_outside_window', 0)}",
        "",
        "## Domain summary",
        "",
        "| Domain | Scheduled | Supply | Gold delivery | SLA delivery | Completeness | MTTR (minutes) |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for domain, summary in report.get("domains", {}).items():
        mttr = _display(summary["recovery"].get("mttr_minutes"))
        lines.append(
            "| "
            + " | ".join(
                [
                    domain,
                    str(summary["scheduled_runs"]),
                    _rate(summary["supply"], "successful_runs"),
                    _rate(summary["gold_delivery"], "delivered_runs"),
                    _rate(summary["sla_delivery"], "within_sla_runs"),
                    _rate(summary["completeness"], "complete_runs"),
                    str(mttr),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Run evidence",
            "",
            "| Domain | Scheduled run ID | Scheduled at | State | Gold available at | Final row count |",
            "| --- | --- | --- | --- | --- | ---: |",
        ]
    )
    for run in report.get("runs", []):
        lines.append(
            "| "
            + " | ".join(
                str(_display(run.get(field)))
                for field in (
                    "domain",
                    "scheduled_run_id",
                    "scheduled_at",
                    "state",
                    "gold_available_at",
                    "gold_row_count",
                )
            )
            + " |"
        )
    return "\n".join(lines) + "\n"
