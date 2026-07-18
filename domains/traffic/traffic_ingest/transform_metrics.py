"""Traffic transform current-run artifact lookup and metrics publishing."""

from __future__ import annotations

import logging
import os

from common.runmetrics import dump_dbt_run_results
from traffic_ingest.transform_specs import DBT_PHASE_TASK_IDS


DOMAIN = "traffic"
DBT_FAILURE_XCOM_KEY = "traffic_dbt_failure"
DBT_RUN_RESULTS_RECORD_KEY = "dbt_run_results_path"
LOGGER = logging.getLogger(__name__)


def _current_run_results_path(**context) -> str | None:
    """Return the latest isolated dbt artifact recorded by this DAG run."""
    ti = context.get("ti") or context.get("task_instance")
    if ti is None:
        return None
    for task_id in reversed(DBT_PHASE_TASK_IDS):
        try:
            result = ti.xcom_pull(task_ids=task_id)
        except Exception as exc:  # noqa: BLE001 - inspect earlier current-run phases
            LOGGER.debug(
                "traffic dbt result XCom lookup failed for %s: %s",
                task_id,
                type(exc).__name__,
            )
            result = None
        if isinstance(result, dict) and result.get("run_results_path"):
            return str(result["run_results_path"])
        try:
            failure = ti.xcom_pull(task_ids=task_id, key=DBT_FAILURE_XCOM_KEY)
        except Exception as exc:  # noqa: BLE001 - inspect earlier current-run phases
            LOGGER.debug(
                "traffic dbt failure XCom lookup failed for %s: %s",
                task_id,
                type(exc).__name__,
            )
            failure = None
        if isinstance(failure, dict) and failure.get(DBT_RUN_RESULTS_RECORD_KEY):
            return str(failure[DBT_RUN_RESULTS_RECORD_KEY])
    return None


def publish_dbt_run_metrics(
    run_results_path: str | None = None,
    *,
    dump_results=dump_dbt_run_results,
    **context,
) -> dict:
    """Persist Traffic dbt metrics while preserving terminal dbt failure semantics."""
    resolved_path = (
        run_results_path
        if run_results_path is not None
        else _current_run_results_path(**context)
    )
    if not resolved_path or not os.path.exists(resolved_path):
        print(f"run_results.json 없음 — 메트릭 적재 skip: {resolved_path}")
        return {"rows": 0, "skipped": True}
    target = (context.get("params") or {}).get("target")
    records = dump_results(resolved_path, domain=DOMAIN, target=target)
    print(
        f"dbt 실행 메트릭 적재: {len(records)} records (domain={DOMAIN}, target={target})"
    )
    return {"rows": len(records), "skipped": False}


__all__ = [
    "DBT_FAILURE_XCOM_KEY",
    "DBT_RUN_RESULTS_RECORD_KEY",
    "DOMAIN",
    "_current_run_results_path",
    "publish_dbt_run_metrics",
]
