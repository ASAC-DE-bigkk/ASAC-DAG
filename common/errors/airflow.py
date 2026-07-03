"""Airflow on_failure_callback 팩토리 — DAG 에 한 줄로 연결 (#77 수집 경로 1).

사용 (기존 콜백이 있으면 리스트로 나란히 건다 — 기존 동작 불변):

    from common.errors.airflow import problem_failure_callback
    record_problem = problem_failure_callback(domain="traffic", source_system="seoul_topis")

    PythonOperator(..., on_failure_callback=[existing_callback, record_problem])

콜백은 어떤 경우에도 예외를 밖으로 던지지 않는다 — 에러 기록 실패가
다른 콜백(manifest 기록·알림)이나 태스크 상태를 오염시키지 않게 한다.
"""
from __future__ import annotations

import logging
from typing import Any, Callable

from common.errors.problem import Problem
from common.errors.sink import R2ErrorSink

LOGGER = logging.getLogger(__name__)


def problem_from_airflow_context(context: dict[str, Any], *, domain: str,
                                 source_system: str | None = None) -> Problem:
    ti = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    dag_id = (getattr(dag, "dag_id", None) or getattr(ti, "dag_id", None)
              or getattr(dag_run, "dag_id", None))
    run_id = context.get("run_id") or getattr(dag_run, "run_id", None)
    try_number = getattr(ti, "try_number", None)
    return Problem.from_exception(
        context.get("exception"),
        domain=domain,
        dag_id=dag_id,
        task_id=getattr(ti, "task_id", None),
        run_id=run_id,
        try_number=try_number if isinstance(try_number, int) else None,
        source_system=source_system,
    )


def problem_failure_callback(domain: str, *, source_system: str | None = None,
                             sink: R2ErrorSink | None = None) -> Callable[[dict[str, Any]], None]:
    error_sink = sink or R2ErrorSink()

    def record_problem(context: dict[str, Any]) -> None:
        try:
            problem = problem_from_airflow_context(
                context, domain=domain, source_system=source_system)
            error_sink.write(problem)
        except Exception as exc:
            # 예외 메시지는 찍지 않는다(시크릿 포함 가능) — 타입명만 남긴다.
            LOGGER.warning("Failed to store problem document: %s", type(exc).__name__)

    return record_problem
