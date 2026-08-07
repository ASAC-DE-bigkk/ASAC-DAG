"""Transit bronze loader 장기 실행 판정 — Airflow/R2 없이 단위 테스트한다.

``transit_bronze_loader``는 pending 마커를 멱등적으로 소비하므로 한 run이 길어도
즉시 중단하면 backlog를 더 악화시킬 수 있다. 이 모듈은 실행을 중단하지 않고,
수집과 변환 사이의 **적재 지연(loader_delay)** 을 관측할 대상 run만 고른다.
실제 메타DB 조회와 알림은 ``transit_bronze_loader_watchdog.py``가 담당한다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, Protocol


class DagRunLike(Protocol):
    """Airflow ``DagRun``에서 감시에 필요한 최소 필드."""

    run_id: str
    start_date: datetime
    state: object


@dataclass(frozen=True)
class OverdueLoaderRun:
    """허용 실행 시간을 초과한 loader run의 안전한 관측값."""

    run_id: str
    started_at: datetime
    elapsed: timedelta


def _state_name(state: object) -> str:
    """Airflow enum과 문자열 상태를 모두 소문자 값으로 정규화한다."""
    return str(getattr(state, "value", state) or "").lower()


def _utc(value: object, *, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def overdue_running_runs(
    runs: Iterable[DagRunLike], *, now: datetime, max_runtime: timedelta
) -> tuple[OverdueLoaderRun, ...]:
    """``running`` 상태가 ``max_runtime``을 **초과**한 run만 반환한다.

    Airflow 메타 시간은 UTC/offset-aware여야 한다. 이를 억지로 KST 또는 UTC로
    추정하면 timestamp 오류를 loader 지연으로 오분류할 수 있으므로, 불완전한 시간은
    감시 DAG 자체의 실패로 드러낸다.
    """
    if max_runtime <= timedelta(0):
        raise ValueError("max_runtime must be positive")

    now_utc = _utc(now, field="now")
    overdue: list[OverdueLoaderRun] = []
    for run in runs:
        if _state_name(getattr(run, "state", None)) != "running":
            continue

        run_id = getattr(run, "run_id", None)
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("running DagRun must have a run_id")
        started_at = _utc(getattr(run, "start_date", None), field="start_date")
        elapsed = now_utc - started_at
        if elapsed > max_runtime:
            overdue.append(OverdueLoaderRun(
                run_id=run_id,
                started_at=started_at,
                elapsed=elapsed,
            ))
    return tuple(overdue)


def claim_first_unalerted_run(
    runs: Iterable[OverdueLoaderRun], *, claim: Callable[[str], bool]
) -> OverdueLoaderRun | None:
    """아직 경보하지 않은 가장 오래된 초과 run 하나를 원자적으로 claim한다.

    watcher task 한 번은 Airflow failure 하나만 만들 수 있으므로, 복수 run이 비정상적으로
    겹쳐도 매 tick마다 다음 unalerted run을 하나씩 승격한다. ``claim``은 공유 알림 guard의
    create-if-absent 연산이며 True인 경우만 이 watchdog이 알림 소유자가 된다.
    """
    for run in runs:
        if claim(run.run_id):
            return run
    return None
