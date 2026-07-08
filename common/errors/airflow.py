"""Airflow on_failure_callback 팩토리 — DAG 에 한 줄로 연결 (#77 수집 경로 1).

사용 (기존 콜백이 있으면 리스트로 나란히 건다 — 기존 동작 불변):

    from common.errors.airflow import problem_failure_callback
    record_problem = problem_failure_callback(domain="traffic", source_system="seoul_topis")

    PythonOperator(..., on_failure_callback=[existing_callback, record_problem])

콜백은 어떤 경우에도 예외를 밖으로 던지지 않는다 — 에러 기록 실패가
다른 콜백(manifest 기록·알림)이나 태스크 상태를 오염시키지 않게 한다.

#161: R2 Problem 기록에 더해 **Discord 에러 embed 도 전송**한다(전 도메인 합의).
- webhook 은 `<DOMAIN>_DISCORD_WEBHOOK_URL` → `DISCORD_WEBHOOK_URL` 폴백 체인.
- 같은 dag_run 의 반복 실패는 첫 1건만 전송(알림 폭풍 가드 — culture 요구).
- 자체 실패 알림이 이미 있는 도메인(traffic/weather)은 중복 제거 전까지
  `DISCORD_ERROR_NOTIFY_OPTOUT=traffic,weather` 로 임시 제외 가능.
- R2 기록과 Discord 전송은 서로 독립(한쪽 실패가 다른 쪽을 막지 않음).
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta, timezone
from typing import Any, Callable

from common.discord import COLOR_FAIL, first_notice_for_run, send_embed
from common.errors.problem import Problem
from common.errors.sink import R2ErrorSink

LOGGER = logging.getLogger(__name__)

OPTOUT_ENV = "DISCORD_ERROR_NOTIFY_OPTOUT"
_KST = timezone(timedelta(hours=9))


def _notify_optout(domain: str) -> bool:
    raw = os.environ.get(OPTOUT_ENV, "")
    return domain in {d.strip() for d in raw.split(",") if d.strip()}


def _problem_embed(problem: Problem) -> tuple[str, str, str]:
    """Problem → (title, description, footer). 내용은 요약만 — 상세는 R2 문서가 원본."""
    title = f"❌ {problem.domain or '?'} · {problem.dag_id or '?'} 실패"
    lines = [
        f"**task**: `{problem.task_id or '?'}` (try {problem.try_number if problem.try_number is not None else '?'})",
        f"**유형**: {problem.title}",
    ]
    if problem.detail:
        lines.append(f"**상세**: {problem.detail[:500]}")
    lines.append("같은 run 의 추가 실패는 반복 전송하지 않습니다 — 상세는 R2 errors/ 참고.")
    occurred = problem.occurred_at.astimezone(_KST).strftime("%Y-%m-%d %H:%M:%S KST")
    footer = f"run_id={problem.run_id or '?'} · {occurred}"
    return title, "\n".join(lines), footer


def problem_from_airflow_context(context: dict[str, Any], *, domain: str,
                                 source_system: str | None = None) -> Problem:
    # dag_id/task_id 를 여러 컨텍스트 객체에서 폴백 탐색한다. task-level 콜백 외에
    # 수동 트리거·DAG-level 콜백 등 일부 객체(ti/dag/dag_run)가 빠지는 경로가 있어,
    # 그중 하나만 없어도 dag_id/task_id 가 None 이 되면 알림 카드가 '?'·R2 키가
    # dag_id=unknown/ 으로 degrade 된다(#194 리뷰). `task` 를 포함해 넓게 훑는다.
    ti = context.get("task_instance") or context.get("ti")
    task = context.get("task")
    dag = context.get("dag")
    dag_run = context.get("dag_run")

    def _pick(*values: Any) -> Any:
        for value in values:
            if value:
                return value
        return None

    dag_id = _pick(
        getattr(ti, "dag_id", None),
        getattr(task, "dag_id", None),
        getattr(dag, "dag_id", None),
        getattr(dag_run, "dag_id", None),
    )
    task_id = _pick(
        getattr(ti, "task_id", None),
        getattr(task, "task_id", None),
    )
    run_id = _pick(
        context.get("run_id"),
        getattr(dag_run, "run_id", None),
        getattr(ti, "run_id", None),
    )
    try_number = getattr(ti, "try_number", None)
    return Problem.from_exception(
        context.get("exception"),
        domain=domain,
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        try_number=try_number if isinstance(try_number, int) else None,
        source_system=source_system,
    )


def problem_failure_callback(domain: str, *, source_system: str | None = None,
                             sink: R2ErrorSink | None = None) -> Callable[[dict[str, Any]], None]:
    error_sink = sink or R2ErrorSink()

    def record_problem(context: dict[str, Any]) -> None:
        problem: Problem | None = None
        try:
            problem = problem_from_airflow_context(
                context, domain=domain, source_system=source_system)
            error_sink.write(problem)
        except Exception as exc:
            # 예외 메시지는 찍지 않는다(시크릿 포함 가능) — 타입명만 남긴다.
            LOGGER.warning("Failed to store problem document: %s", type(exc).__name__)

        # Discord 에러 알림 (#161) — R2 기록과 독립. 어떤 실패도 밖으로 안 던진다.
        try:
            if problem is None or _notify_optout(domain):
                return
            if not first_notice_for_run(problem.dag_id, problem.run_id):
                LOGGER.info("[discord] 같은 run 의 실패 알림 이미 전송 — 스킵 (%s)",
                            problem.dag_id)
                return
            title, description, footer = _problem_embed(problem)
            send_embed(title, description, color=COLOR_FAIL, footer=footer, domain=domain)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to send discord error notice: %s", type(exc).__name__)

    return record_problem
