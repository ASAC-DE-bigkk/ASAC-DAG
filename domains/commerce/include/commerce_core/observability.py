"""commerce 실행 기록 배선 — 공용 모듈에 위임한다.

구현은 :mod:`common.ops.observability` 에 있다(전 도메인 공용, ASAC-DAG#659). 이 모듈은
commerce 도메인 이름을 미리 채워 주는 얇은 래퍼일 뿐이라, 기존 호출부가 그대로 동작한다.

    _DEFAULT_ARGS = {..., **ops_default_args(Layer.RAW)}
"""
from __future__ import annotations

from typing import Any

from common.ops import Layer
from common.ops.observability import ops_default_args as _shared
from common.ops.observability import record_task_event as _record

DOMAIN = "commerce"


def record_task_event(layer: Layer | str, status) -> Any:
    """commerce 도메인이 채워진 콜백. 인자 순서는 기존 호출부 호환."""
    return _record(DOMAIN, layer, status)


def ops_default_args(layer: Layer | str, *, on_success: Any = None,
                     on_failure: Any = None) -> dict[str, Any]:
    """commerce DAG 용 — 도메인은 고정, 나머지는 공용 구현 그대로."""
    return _shared(DOMAIN, layer, on_success=on_success, on_failure=on_failure)
