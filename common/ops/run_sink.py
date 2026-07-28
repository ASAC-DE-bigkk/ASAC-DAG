"""파일 기반 run 기록 — 모든 태스크 실행을 R2 ``runs/`` 에 JSON 1개 (Trino 무관).

관측을 관측대상(Trino)과 분리한다: Trino 가 OOM 으로 죽어도 실행 기록이 남는다(맹점 해소).
데이터 품질 대시보드가 ``runs/`` 를 집계해 도메인별 잔디를 그린다.

- ``record_run_metadata`` (Iceberg via Trino) 를 대체 — 콜백 자리는 그대로, 타겟만 R2 파일.
- ``errors/`` (실패 상세·Discord 알림) 와 ``_reports`` (bronze 수집 감사) 는 **별개로 유지**.

경로: ``runs/observed_date=YYYY-MM-DD(KST)/domain=<d>/dag_id=<dag>/<run>__<task>__try<N>.json``
R2 자격증명·put 은 common.errors.sink 의 것을 재활용(같은 R2 버킷).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

LOGGER = logging.getLogger(__name__)

RUNS_PREFIX = "runs"
_KST = timezone(timedelta(hours=9))


def _safe(value: object) -> str:
    """경로 세그먼트 안전화 — 영숫자/-_. 만 허용."""
    return "".join(c if (c.isalnum() or c in "-_.") else "_" for c in str(value))[:120]


def _iso(value: object) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _put_r2(object_key: str, payload: bytes) -> None:
    # boto3 R2 put 재활용(errors sink 와 동일 자격증명/버킷) — 중복 구현 회피.
    from common.errors.sink import R2ErrorSink

    R2ErrorSink._put_r2_object(object_key, payload)


def build_run_record(context: dict, *, domain: str, layer: str, status: str) -> tuple[str, dict]:
    """Airflow context → (R2 object key, run record dict). 테스트·백필에서 재사용 가능."""
    ti = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    dag_run = context.get("dag_run")

    started = getattr(ti, "start_date", None)
    ended = getattr(ti, "end_date", None) or datetime.now(timezone.utc)
    scheduled = context.get("data_interval_end") or context.get("logical_date")
    duration_s = (
        (ended - started).total_seconds()
        if isinstance(started, datetime) and isinstance(ended, datetime)
        else None
    )
    dag_id = getattr(dag, "dag_id", None) or getattr(ti, "dag_id", None) or "unknown"
    task_id = getattr(ti, "task_id", None) or "unknown"
    run_id = getattr(ti, "run_id", None) or getattr(dag_run, "run_id", None) or "unknown"
    try_number = getattr(ti, "try_number", None)
    exc = context.get("exception")

    anchor = scheduled or started or ended
    obs_date = anchor.astimezone(_KST).strftime("%Y-%m-%d") if isinstance(anchor, datetime) else "unknown"

    record = {
        "domain": domain,
        "layer": layer,
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "try_number": try_number,
        "status": status,
        "scheduled_at": _iso(scheduled if isinstance(scheduled, datetime) else None),
        "started_at": _iso(started if isinstance(started, datetime) else None),
        "ended_at": _iso(ended if isinstance(ended, datetime) else None),
        "duration_s": round(duration_s, 3) if duration_s is not None else None,
        "error": (str(exc)[:500] if exc else None),  # 짧은 요약 — 전체 상세는 errors/
    }
    object_key = (
        f"{RUNS_PREFIX}/observed_date={obs_date}"
        f"/domain={_safe(domain)}/dag_id={_safe(dag_id)}"
        f"/{_safe(run_id)}__{_safe(task_id)}__try{try_number}__{_safe(status)}.json"
    )
    return object_key, record


def record_run(domain: str, layer: str, *, status: str):
    """Airflow on_success/on_failure 콜백 — 태스크 1건 = R2 ``runs/`` 파일 1개.

    status: 'success' | 'failed' (콜백 붙는 자리에 맞춰 명시).
    관측 실패가 태스크를 죽이면 안 되므로 fail-open(경고만).
    """

    def _callback(context) -> None:
        try:
            object_key, record = build_run_record(context, domain=domain, layer=layer, status=status)
            _put_r2(object_key, json.dumps(record, ensure_ascii=False, sort_keys=True).encode("utf-8"))
            LOGGER.info("[ops.runs] %s", object_key)
        except Exception as exc:  # noqa: BLE001 — 관측 실패로 태스크 죽이지 않음
            LOGGER.warning("[ops.runs] 기록 실패(무시): %s", type(exc).__name__)

    return _callback
