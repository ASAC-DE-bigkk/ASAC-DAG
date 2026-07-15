"""Read-only benchmark collection orchestration."""

from __future__ import annotations

import contextlib
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from weather_ingest.cost_proxy.compile import (
    _aggregate_query_metrics,
    _compile_model,
    _dbt_vars_for_case,
    _execution_fingerprint,
    _fingerprint_sources,
    _source_tables,
)
from weather_ingest.cost_proxy.config import (
    CASES,
    EXECUTION_FINGERPRINT_KEY,
    MIN_REPEAT,
    _catalog,
    _ensure_dev_target,
    _schema,
)
from weather_ingest.trino_query_metrics import TelemetryCursor


def _execute_dbt_case(
    telemetry: TelemetryCursor, compiled_code: str
) -> list[dict[str, Any]]:
    start = len(telemetry.records)
    telemetry.execute("EXPLAIN ANALYZE " + compiled_code)
    telemetry.fetchall()
    return telemetry.records[start:]


def _collect_suite(
    name: str,
    case: Mapping[str, Any],
    telemetry: TelemetryCursor,
    metadata_cursor: Any,
    *,
    catalog: str,
    schema: str,
    repeat: int,
) -> dict[str, Any]:
    tables = _source_tables(case)
    before_fingerprint = _fingerprint_sources(
        metadata_cursor, catalog=catalog, schema=schema, tables=tables
    )
    dbt_vars = _dbt_vars_for_case(
        case,
        metadata_cursor,
        catalog=catalog,
        schema=schema,
    )
    execution_fingerprint = _execution_fingerprint(
        name,
        case,
        dbt_vars,
        catalog=catalog,
        schema=schema,
    )
    compiled_code = _compile_model(case, dbt_vars=dbt_vars, suite_name=name)
    runs: list[dict[str, Any]] = []
    for iteration in range(1, repeat + 1):
        records = _execute_dbt_case(telemetry, compiled_code)
        runs.append(
            {
                "iteration": iteration,
                "metrics": _aggregate_query_metrics(records),
                "queries": records,
            }
        )
    after_fingerprint = _fingerprint_sources(
        metadata_cursor, catalog=catalog, schema=schema, tables=tables
    )
    return {
        "name": name,
        "domain": case["domain"],
        "source_tables": tables,
        "dbt_vars": dbt_vars,
        EXECUTION_FINGERPRINT_KEY: execution_fingerprint,
        "fingerprints": {"before": before_fingerprint, "after": after_fingerprint},
        "comparable_within_run": before_fingerprint == after_fingerprint,
        "runs": runs,
    }


def _connect_cursors(*, catalog: str, schema: str) -> tuple[Any, Any, Any]:
    import trino.dbapi

    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog,
        schema=schema,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return connection, connection.cursor(), connection.cursor()


def collect_bundle(label: str, repeat: int) -> dict[str, Any]:
    """Collect one dev-only benchmark bundle.  This never runs dbt models."""
    _ensure_dev_target()
    if repeat < MIN_REPEAT:
        raise ValueError(f"repeat must be at least {MIN_REPEAT}")
    catalog = _catalog()
    schema = _schema()
    connection, workload_cursor, metadata_cursor = _connect_cursors(
        catalog=catalog, schema=schema
    )
    try:
        telemetry = TelemetryCursor(workload_cursor, metadata_cursor)
        suites = [
            _collect_suite(
                name,
                case,
                telemetry,
                metadata_cursor,
                catalog=catalog,
                schema=schema,
                repeat=repeat,
            )
            for name, case in CASES.items()
        ]
        return {
            "label": label,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "target": "dev",
            "catalog": catalog,
            "schema": schema,
            "repeat": repeat,
            "fingerprint": {
                suite["name"]: suite["fingerprints"]["after"] for suite in suites
            },
            EXECUTION_FINGERPRINT_KEY: {
                suite["name"]: suite[EXECUTION_FINGERPRINT_KEY] for suite in suites
            },
            "suites": suites,
            "proxy_notice": "실제 Cloudflare 청구액이 아닌 Trino·Iceberg 비용 대리 지표입니다.",
        }
    finally:
        for resource in (metadata_cursor, workload_cursor, connection):
            with contextlib.suppress(Exception):
                resource.close()
