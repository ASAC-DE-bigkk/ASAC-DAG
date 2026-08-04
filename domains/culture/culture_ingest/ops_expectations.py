"""culture 기대 주기 — 공용 레지스트리 등록 (ASK-Seoul#78 §9 `S-1`~`S-5`).

**여기에 값을 새로 적지 않는다.** culture 는 #619 때 이미 등록값을 코드로 갖고 있고
(:mod:`culture_ingest.ops.schedule_registry`), 그 표는 테스트가 실제 DAG 의 ``schedule=``
과 대조한다. 공용 스키마가 생겼다고 값을 여기 다시 쓰면 **정본이 셋(DAG·도메인 표·공용
표)** 이 되고, `S-1` 이 막으려는 게 정확히 그 상태다 — 스케줄을 바꿀 때 하나만 고치면
어긋난 채로 남는다. 그래서 이 파일은 **어댑터**이고, 값은 전부 도메인 표에서 온다.

오너 확인: @yooseongjin527 · 2026-08-04 (#78 §9 culture 행과 같은 값 — `culture_bronze`
일 03:00 외 5종, ``culture_transform`` 은 상류 이벤트형).

**수동 전용 DAG 는 없다**(`S-4` 해당 없음). 주간 DAG 둘(``culture_maintenance``·
``culture_facility_refresh``)은 정비·갱신 주기일 뿐 감시 대상이며, 이 둘만 보고 도메인
전체를 "주 1회"로 등록할 뻔한 것이 `S-5` 가 붙은 계기다.
"""
from __future__ import annotations

from common.ops.expectations import Expectation, register

from culture_ingest.ops.schedule_registry import (
    REGISTRY,
    TIMEZONE,
    TRIGGER_ASSET,
)

DOMAIN = "culture"
OWNER = "yooseongjin527"
OWNER_CONFIRMED_ON = "2026-08-04"


def _as_expectation(cadence) -> Expectation:
    """도메인 등록 1건 → 공용 스키마 1건.

    상류 이벤트형은 ``expected_interval`` 을 비운다 — 고정 주기를 적으면 상류가 늦을
    때마다 오탐이 난다(`S-3`). 화면에 뜨는 사람 말은 ``interval_ko`` 를 그대로 쓴다.
    """
    is_asset = cadence.trigger_type == TRIGGER_ASSET
    return Expectation(
        cadence.dag_id,
        cadence.trigger_type,
        expected_interval=None if is_asset else cadence.interval_ko,
        upstream=cadence.upstream,
        max_delay_minutes=cadence.max_delay_min,
        schedule_timezone=TIMEZONE,
    )


EXPECTATIONS = tuple(_as_expectation(c) for c in REGISTRY)

register(DOMAIN, owner=OWNER, confirmed_on=OWNER_CONFIRMED_ON, items=EXPECTATIONS)
