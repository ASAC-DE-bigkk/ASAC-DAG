"""Weather-owned dbt failure classification for Airflow retry semantics."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


_ADAPTER_MARKERS = ("trino", "adapter")
_TRANSIENT_MARKERS = (
    "temporary failure in name resolution",
    "name or service not known",
    "getaddrinfo",
    "timed out",
    "timeout",
    "connection refused",
    "connection reset",
    "connection aborted",
)


@dataclass(frozen=True)
class WeatherDbtFailure:
    classification: str
    retryable: bool


def _load_results(artifact_path: str | None) -> tuple[dict, ...]:
    if artifact_path is None:
        return ()
    try:
        document = json.loads(Path(artifact_path).read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return ()
    results = document.get("results") if isinstance(document, dict) else None
    if not isinstance(results, list):
        return ()
    return tuple(result for result in results if isinstance(result, dict))


def classify_weather_dbt_failure(
    *,
    dbt_command: str,
    returncode: int,
    artifact_path: str | None,
    missing_expected_artifacts: tuple[str, ...],
    command_output: str,
) -> WeatherDbtFailure:
    """Map a failed invocation to deterministic or retryable Airflow behavior."""
    results = _load_results(artifact_path)
    result_details = "\n".join(
        str(value)
        for result in results
        for value in (result.get("message"), result.get("adapter_response"))
        if value
    )
    detail = f"{command_output}\n{result_details}".lower()

    if "dbt selection resolved to no" in detail:
        return WeatherDbtFailure("empty-selection", retryable=False)
    if any(marker in detail for marker in _ADAPTER_MARKERS) and any(
        marker in detail for marker in _TRANSIENT_MARKERS
    ):
        return WeatherDbtFailure(
            "retryable-infrastructure-error",
            retryable=True,
        )
    if missing_expected_artifacts:
        return WeatherDbtFailure("artifact-contract-violation", retryable=False)

    failed_tests = any(
        str(result.get("unique_id") or "").startswith("test.")
        and str(result.get("status") or "").lower() in {"error", "fail"}
        for result in results
    )
    if failed_tests or (dbt_command == "test" and returncode != 0):
        return WeatherDbtFailure("data-contract-violation", retryable=False)
    return WeatherDbtFailure("dbt-execution-failed", retryable=False)
