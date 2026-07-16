"""Airflow 콜백 어댑터 — 태스크 성공/실패 시 ``ops.run_metadata`` 에 1행 append.

``record_run_metadata(domain, layer, status=...)`` 가 ``on_success_callback`` /
``on_failure_callback`` 에 넣을 콜러블을 만든다. Airflow context 에서 신원·타이밍·상태를
뽑아 :func:`common.ops.run_metadata.append_run_row` 로 보낸다.

- 기존 도메인 실패 콜백(problem_failure_callback)과 **병행** 가능(둘 다 리스트로 배선).
- best-effort: 콜백 내부 예외는 삼킨다 — run-metadata 기록이 태스크 판정을 가리지 않게.
- 완전성(expected/actual)은 태스크가 XCom key ``ops_run_completeness`` (dict)로 밀어두면
  그 태스크 행에 채워진다(예: bronze load 가 coverage 를 push). 없으면 null.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from .run_metadata import RunRow, append_run_row

LOGGER = logging.getLogger(__name__)


def _dt(value) -> datetime | None:
    """pendulum/naive/aware → aware datetime(UTC 보정은 append 단에서). None 통과."""
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(value.timestamp(), tz=timezone.utc)
    except Exception:  # noqa: BLE001
        return value if isinstance(value, datetime) else None


def record_run_metadata(domain: str, layer: str, *, status: str):
    """``on_success_callback`` / ``on_failure_callback`` 용 콜러블 생성.

    status: 이 콜백이 붙는 자리에 맞춰 'success' | 'failed' | 'skipped' 를 명시한다.
    """

    def _callback(context) -> None:
        try:
            ti = context.get("task_instance") or context.get("ti")
            dag = context.get("dag")
            dag_run = context.get("dag_run")
            scheduled = _dt(context.get("data_interval_end") or context.get("logical_date"))
            started = _dt(getattr(ti, "start_date", None))
            ended = _dt(getattr(ti, "end_date", None)) or datetime.now(timezone.utc)

            reason = None
            exc = context.get("exception")
            if exc is not None:
                reason = str(exc)[:500]

            comp = {}
            try:
                pulled = ti.xcom_pull(task_ids=ti.task_id, key="ops_run_completeness")
                if isinstance(pulled, dict):
                    comp = pulled
            except Exception:  # noqa: BLE001
                comp = {}

            target = os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "dev"))

            row = RunRow(
                domain=domain,
                layer=layer,
                dag_id=getattr(dag, "dag_id", None) or getattr(ti, "dag_id", None) or "unknown",
                task_id=getattr(ti, "task_id", None) or "unknown",
                run_id=getattr(dag_run, "run_id", None) or getattr(ti, "run_id", None) or "unknown",
                try_number=getattr(ti, "try_number", None),
                target=target,
                scheduled_at=scheduled,
                started_at=started,
                ended_at=ended,
                status=status,
                failure_reason=reason,
                expected_rows=comp.get("expected_rows"),
                actual_rows=comp.get("actual_rows"),
                expected_raw_objects=comp.get("expected_raw_objects"),
                actual_raw_objects=comp.get("actual_raw_objects"),
            )
            append_run_row(row, target=target)
        except Exception as exc:  # noqa: BLE001 -- 콜백 실패가 태스크 판정을 가리지 않게
            LOGGER.warning("[ops.run_metadata] 콜백 실패(무시): %s", exc)

    return _callback
