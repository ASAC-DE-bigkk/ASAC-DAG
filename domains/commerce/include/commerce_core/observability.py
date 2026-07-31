"""commerce 실행 기록 배선 — 각 태스크가 규약대로 기록을 **파일로** 남기게 한다.

commerce 는 지금까지 ops 존에 **상태값**(마커·diff 기준·watchdog)과 **로그 압축본**만 남기고
실행 기록은 남기지 않았다. 그래서 조회 DB 에서 commerce 는 통째로 관측 공백이었다.

여기서 하는 일은 한 가지다: Airflow 콜백 자리에서 **공용 관문**(`common.ops.contract`)을
호출한다. 경로·값 집합·필수 항목은 관문이 정하므로 이 모듈은 컨텍스트에서 값을 꺼내는 일만
한다. 필수 항목이 빠지면 관문이 그 자리에서 거부하고, 그 거부는 로그에만 남긴다 —
관측이 본 파이프라인을 죽이면 관측이 장애의 원인이 된다(C-2).

쓰는 법 (DAG 파일에서 한 줄):

    from commerce_core.observability import ops_default_args
    _DEFAULT_ARGS = {"owner": "data-eng", ..., **ops_default_args(Layer.RAW)}

DAG 마다 `layer` 를 명시하는 이유는 그 값이 그 DAG 의 정체(수집/정제/변환/집계/서빙)이고,
추측으로 채울 수 있는 값이 아니기 때문이다(V-4).
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Mapping

from common.ops import Grain, Layer, OpsCategory, OpsContractError, RowsSource, RunStatus, emit_ops_event

log = logging.getLogger(__name__)

DOMAIN = "commerce"

#: 태스크 반환 dict 에서 행 수로 인정하는 키. 이름이 다르면 **못 잰 것**으로 둔다 —
#: 아무 숫자나 행 수로 승격하면 그 순간부터 지표가 조용히 거짓말을 한다(F-3·N-5).
_ROW_KEYS = ("row_count", "rows", "inserted", "loaded")


def _identity(context: Mapping[str, Any]) -> dict[str, Any]:
    ti = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    return {
        "dag_id": (getattr(dag, "dag_id", None) or getattr(ti, "dag_id", None)
                   or getattr(dag_run, "dag_id", None)),
        "task_id": getattr(ti, "task_id", None),
        "run_id": (getattr(ti, "run_id", None) or getattr(dag_run, "run_id", None)
                   or context.get("run_id")),
        "try_number": getattr(ti, "try_number", None),
        "started_at": getattr(ti, "start_date", None),
        "ended_at": getattr(ti, "end_date", None),
    }


def _is_final_try(context: Mapping[str, Any], *, status: RunStatus) -> bool | None:
    """이번이 마지막 시도인가(C-7). 성공은 정의상 마지막이고, 실패는 재시도가 남았는지로 갈린다.

    판단 근거가 없으면 ``None`` 을 돌려준다 — 관측 공백은 ``NULL`` 이지 ``False`` 가 아니다.
    """
    if status is RunStatus.SUCCESS:
        return True
    ti = context.get("task_instance") or context.get("ti")
    try_number = getattr(ti, "try_number", None)
    max_tries = getattr(ti, "max_tries", None)
    if not isinstance(try_number, int) or not isinstance(max_tries, int):
        return None
    return try_number > max_tries


def _rows_from_xcom(context: Mapping[str, Any]) -> tuple[int | None, RowsSource]:
    """태스크가 돌려준 dict 에서 행 수를 읽는다. 못 읽으면 ``not_observed`` (지어내지 않는다)."""
    ti = context.get("task_instance") or context.get("ti")
    if ti is None or not hasattr(ti, "xcom_pull"):
        return None, RowsSource.NOT_OBSERVED
    try:
        result = ti.xcom_pull(task_ids=getattr(ti, "task_id", None))
    except Exception:  # noqa: BLE001 - XCom 조회 실패는 관측 공백일 뿐이다
        return None, RowsSource.NOT_OBSERVED
    if not isinstance(result, Mapping):
        return None, RowsSource.NOT_OBSERVED
    for key in _ROW_KEYS:
        value = result.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value, RowsSource.BRONZE_RUN_MANIFEST
    return None, RowsSource.NOT_OBSERVED


def _schedule_delay_s(context: Mapping[str, Any], started: Any) -> float | None:
    scheduled = context.get("data_interval_end") or context.get("logical_date")
    if scheduled is None or started is None:
        return None
    try:
        delay = (started - scheduled).total_seconds()
    except (AttributeError, TypeError):
        return None
    return delay if delay >= 0 else None


def record_task_event(layer: Layer | str, status: RunStatus | str) -> Callable[[Any], None]:
    """Airflow ``on_success_callback`` / ``on_failure_callback`` — 태스크 1건 = 기록 1건.

    grain 은 ``airflow_task`` 다(V-5·V-6: runs 는 Airflow 작업 1건, metrics 는 dbt 노드 1건).
    """
    resolved_status = RunStatus(str(status))

    def _callback(context: Mapping[str, Any]) -> None:
        try:
            identity = _identity(context)
            row_count, rows_source = (
                _rows_from_xcom(context) if resolved_status is RunStatus.SUCCESS
                else (None, RowsSource.NOT_OBSERVED))
            exception = context.get("exception")
            emit_ops_event(
                OpsCategory.RUNS,
                domain=DOMAIN,
                layer=layer,
                grain=Grain.AIRFLOW_TASK,
                status=resolved_status,
                dag_id=identity["dag_id"],
                task_id=identity["task_id"],
                run_id=identity["run_id"],
                try_number=identity["try_number"],
                is_final_try=_is_final_try(context, status=resolved_status),
                started_at=identity["started_at"],
                ended_at=identity["ended_at"],
                schedule_delay_s=_schedule_delay_s(context, identity["started_at"]),
                row_count=row_count,
                rows_source=rows_source,
                # 실패 상세 본문은 ops/errors/ 문서의 몫이다. 여기엔 타입만 남긴다 —
                # 예외 메시지에는 URL·자격증명이 섞여 들어올 수 있다(X-1·X-2).
                error_ref=type(exception).__name__ if exception is not None else None,
                failure_count=1 if resolved_status is RunStatus.FAILED else 0,
            )
        except OpsContractError as exc:
            # 관문이 거부했다 = 배선이 규약을 못 지키고 있다. 즉시 눈에 띄게 남기되,
            # 본 태스크의 성패는 바꾸지 않는다.
            log.error("[ops] 실행 기록이 규약을 위반해 거부됐습니다 — 배선을 고치세요: %s", exc)
        except Exception as exc:  # noqa: BLE001 - 관측 실패가 본 작업을 죽이지 않는다(C-2)
            log.warning("[ops] 실행 기록 실패(무시): %s", type(exc).__name__)

    return _callback


def _compose(existing: Any, added: Callable[[Any], None]) -> Any:
    """이미 걸린 콜백을 밀어내지 않고 뒤에 붙인다 — 실패 상세(ops/errors)는 계속 남아야 한다."""
    if existing is None:
        return added
    if isinstance(existing, (list, tuple)):
        return [*existing, added]
    return [existing, added]


def ops_default_args(layer: Layer | str, *, on_success: Any = None,
                     on_failure: Any = None) -> dict[str, Any]:
    """DAG ``default_args`` 에 얹는 콜백 한 쌍 — 그 DAG 의 모든 태스크가 기록을 남긴다.

    이미 쓰던 콜백이 있으면 인자로 넘겨 함께 돌린다(교체가 아니라 추가).
    """
    resolved = Layer(str(layer))
    return {
        "on_success_callback": _compose(on_success, record_task_event(resolved, RunStatus.SUCCESS)),
        "on_failure_callback": _compose(on_failure, record_task_event(resolved, RunStatus.FAILED)),
    }
