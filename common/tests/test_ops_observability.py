"""실행 기록 배선(공용) — 어느 도메인이든 태스크가 관문을 통해 기록을 남기는지.

도메인을 인자로 받는다(ASAC-DAG#659) — 도메인은 저장 경로를 가르는 값이라 추측할 수 없다.

배선의 계약은 셋이다: 기록을 남긴다 / 관측 실패가 본 태스크를 죽이지 않는다 / 규약 위반은
조용히 넘어가지 않는다.
"""
from __future__ import annotations

import logging

import pytest

from common.ops import observability
from common.ops import Layer, RunStatus


class _TaskInstance:
    dag_id = "commerce_collect_raw"
    task_id = "ingest_one"
    run_id = "scheduled__2026-08-01T00:00:00+00:00"
    try_number = 1
    max_tries = 2
    start_date = None
    end_date = None

    def __init__(self, xcom=None) -> None:
        self._xcom = xcom

    def xcom_pull(self, task_ids=None):
        return self._xcom


def _context(**overrides):
    base = {"task_instance": _TaskInstance(), "dag": None, "dag_run": None}
    base.update(overrides)
    return base


@pytest.fixture
def captured(monkeypatch):
    """관문 호출을 가로채 인자를 그대로 본다 — R2·D1 은 건드리지 않는다."""
    calls: list[dict] = []

    def _emit(category, **fields):
        calls.append({"category": category, **fields})
        return dict(fields)

    monkeypatch.setattr(observability, "emit_ops_event", _emit)
    return calls


def test_success_callback_records_one_run_event(captured):
    observability.record_task_event("commerce", Layer.RAW, RunStatus.SUCCESS)(_context())
    assert len(captured) == 1
    call = captured[0]
    assert call["domain"] == "commerce"
    assert call["layer"] is Layer.RAW
    assert call["status"] is RunStatus.SUCCESS
    assert call["dag_id"] == "commerce_collect_raw" and call["task_id"] == "ingest_one"
    assert call["is_final_try"] is True          # 성공은 정의상 마지막 시도다


def test_failure_before_the_last_retry_is_not_final(captured):
    """재시도가 남아 있으면 최종 시도가 아니다 — 재시도로 살아난 실행을 놓치지 않는다(C-7)."""
    ti = _TaskInstance()
    ti.try_number, ti.max_tries = 1, 2
    observability.record_task_event("commerce", Layer.RAW, RunStatus.FAILED)(
        _context(task_instance=ti, exception=RuntimeError("boom")))
    assert captured[0]["is_final_try"] is False
    assert captured[0]["failure_count"] == 1
    # 예외 메시지에는 URL·자격증명이 섞일 수 있어 타입만 남긴다(X-1·X-2).
    assert captured[0]["error_ref"] == "RuntimeError"


def test_final_failure_is_marked(captured):
    ti = _TaskInstance()
    ti.try_number, ti.max_tries = 3, 2
    observability.record_task_event("commerce", Layer.RAW, RunStatus.FAILED)(_context(task_instance=ti))
    assert captured[0]["is_final_try"] is True


def test_unknowable_final_try_stays_null(captured):
    """판단 근거가 없으면 ``None`` — 관측 공백은 ``False`` 가 아니다(F-3)."""
    ti = _TaskInstance()
    ti.try_number, ti.max_tries = None, None
    observability.record_task_event("commerce", Layer.RAW, RunStatus.FAILED)(_context(task_instance=ti))
    assert captured[0]["is_final_try"] is None


def test_row_count_is_read_from_the_task_return_value(captured):
    observability.record_task_event("commerce", Layer.BRONZE, RunStatus.SUCCESS)(
        _context(task_instance=_TaskInstance(xcom={"rows": 19377})))
    assert captured[0]["row_count"] == 19377
    assert captured[0]["rows_source"].value == "bronze_run_manifest"


def test_unmeasured_row_count_is_not_zero(captured):
    """행 수를 못 읽었을 때 0 을 쓰지 않는다 — 그러면 빈 실행과 구분이 사라진다(F-3·N-5)."""
    observability.record_task_event("commerce", Layer.BRONZE, RunStatus.SUCCESS)(
        _context(task_instance=_TaskInstance(xcom={"note": "no rows key"})))
    assert captured[0]["row_count"] is None
    assert captured[0]["rows_source"].value == "not_observed"


def test_failure_does_not_claim_a_row_count(captured):
    observability.record_task_event("commerce", Layer.BRONZE, RunStatus.FAILED)(
        _context(task_instance=_TaskInstance(xcom={"rows": 5})))
    assert captured[0]["row_count"] is None


def test_observability_failure_never_fails_the_task(monkeypatch, caplog):
    def _boom(*_a, **_k):
        raise RuntimeError("R2 down")

    monkeypatch.setattr(observability, "emit_ops_event", _boom)
    with caplog.at_level(logging.WARNING):
        observability.record_task_event("commerce", Layer.RAW, RunStatus.SUCCESS)(_context())
    assert "실행 기록 실패" in caplog.text


def test_contract_violation_is_logged_as_an_error_not_swallowed(monkeypatch, caplog):
    """관문이 거부하면 배선이 규약을 못 지키고 있다는 뜻이다 — 눈에 띄게 남긴다."""
    from common.ops import OpsContractError

    def _reject(*_a, **_k):
        raise OpsContractError("layer 가 닫힌 집합 밖입니다")

    monkeypatch.setattr(observability, "emit_ops_event", _reject)
    with caplog.at_level(logging.ERROR):
        observability.record_task_event("commerce", Layer.RAW, RunStatus.SUCCESS)(_context())
    assert "규약을 위반" in caplog.text


def test_ops_default_args_appends_to_existing_callbacks():
    """이미 걸린 실패 상세 콜백(ops/errors)을 밀어내지 않는다 — 담는 단위가 다르다(V-6)."""
    def _existing(_context):
        return None

    args = observability.ops_default_args("commerce", Layer.RAW, on_failure=_existing)
    assert args["on_failure_callback"][0] is _existing
    assert len(args["on_failure_callback"]) == 2
    assert callable(args["on_success_callback"])


def test_layer_must_be_a_declared_stage():
    with pytest.raises(ValueError):
        observability.ops_default_args("commerce", "warehouse")


def test_domain_is_carried_into_the_record(captured):
    """도메인이 인자로 전달돼야 한다 — 저장 경로를 가르는 값이라 고정하면 안 된다."""
    observability.record_task_event("weather", Layer.BRONZE, RunStatus.SUCCESS)(_context())
    assert captured[0]["domain"] == "weather"
    assert captured[0]["layer"] is Layer.BRONZE
