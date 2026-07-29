"""Fail-visible daily Traffic dbt assurance outside the transform hot path."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from typing import Any

from traffic_ingest.transform_admission import (
    TransformAdmissionError,
    TransformSuccessMarker,
)


DAILY_ASSURANCE_SELECTOR = "ask_seoul_traffic_daily_assurance"
DAILY_ASSURANCE_TASK_ID = "audit_traffic_dbt_contracts"


def build_daily_contract_variables(
    gold_marker_json: object,
    *,
    admin_dong_crosswalk_snapshot_id: object,
) -> dict[str, object]:
    """Recover the exact last-known-good Traffic identity for the daily audit."""
    try:
        marker = TransformSuccessMarker.from_json(gold_marker_json)
    except TransformAdmissionError as exc:
        raise ValueError("Traffic Gold success marker is unavailable") from exc
    if marker.pipeline != "gold":
        raise ValueError("Traffic Gold success marker is unavailable")
    if (
        not isinstance(admin_dong_crosswalk_snapshot_id, int)
        or isinstance(admin_dong_crosswalk_snapshot_id, bool)
        or admin_dong_crosswalk_snapshot_id <= 0
    ):
        raise ValueError("admin_dong crosswalk snapshot id must be positive")

    identity = marker.identity
    variables: dict[str, object] = {
        "traffic_snapshot_dag_run_id": identity.incident_run_id,
        "traffic_citydata_crowding_snapshot_id": identity.citydata_snapshot_id,
        "admin_dong_crosswalk_pin_snapshot_id": (
            admin_dong_crosswalk_snapshot_id
        ),
    }
    if identity.flow_run_id:
        variables["traffic_flow_snapshot_dag_run_id"] = identity.flow_run_id
    return variables


def _result(
    *,
    status: str,
    elapsed_seconds: float,
    selected_count: int,
    failure: str | None,
) -> dict[str, object]:
    return {
        "status": status,
        "selector": DAILY_ASSURANCE_SELECTOR,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "selected_count": selected_count,
        "failure": failure,
    }


def run_daily_contract_audit(
    *,
    variables: Mapping[str, object],
    target: str,
    run_id: str,
    execute: Callable[..., Any] | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    """Run all Traffic contracts and return RED evidence instead of hiding failure."""
    if execute is None:
        from traffic_dbt_execution import execute_dbt_phase

        execute = execute_dbt_phase

    started = clock()
    try:
        execution = execute(
            dbt_command="test",
            selector=DAILY_ASSURANCE_SELECTOR,
            invocation_id=run_id,
            pipeline="traffic_daily_assurance",
            run_id=run_id,
            task_id=DAILY_ASSURANCE_TASK_ID,
            try_number=1,
            target=target,
            variables=json.dumps(
                dict(variables),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            threads=2,
            fresh_parse=True,
        )
        selected_count = len(tuple(execution.selected_unique_ids))
        if execution.completed.returncode != 0:
            return _result(
                status="FAIL",
                elapsed_seconds=clock() - started,
                selected_count=selected_count,
                failure=f"dbt_exit_{execution.completed.returncode}",
            )
        if execution.missing_expected_artifacts:
            return _result(
                status="FAIL",
                elapsed_seconds=clock() - started,
                selected_count=selected_count,
                failure="missing_dbt_artifact",
            )
        return _result(
            status="PASS",
            elapsed_seconds=clock() - started,
            selected_count=selected_count,
            failure=None,
        )
    except Exception as exc:
        return _result(
            status="FAIL",
            elapsed_seconds=clock() - started,
            selected_count=0,
            failure=f"exception_{type(exc).__name__}",
        )


__all__ = [
    "DAILY_ASSURANCE_SELECTOR",
    "DAILY_ASSURANCE_TASK_ID",
    "build_daily_contract_variables",
    "run_daily_contract_audit",
]
