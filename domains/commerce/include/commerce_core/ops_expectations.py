"""commerce 파이프라인 기대 주기 — 공용 레지스트리에 등록한다.

**값의 정본은 각 DAG 파일의 ``schedule=`` 이고 이 표는 사본이다**(ASK-Seoul#78 `S-1`).
스케줄을 바꾸면 같은 커밋에서 여기도 고친다 — 테스트가 양방향으로 대조한다.

스키마·등록 규칙: :mod:`common.ops.expectations`.
오너 확인: @Exisign · 2026-08-01 (#78 §9 commerce 표와 같은 값).
"""
from __future__ import annotations

from typing import Any

from common.ops.expectations import Expectation, register, rows as _rows

DOMAIN = "commerce"
OWNER = "Exisign"
OWNER_CONFIRMED_ON = "2026-08-01"

EXPECTATIONS = (
    Expectation("commerce_collect_raw", "schedule", "일 1회 00:00", max_delay_minutes=24 * 60),
    Expectation("commerce_recollect_raw", "schedule", "6시간", max_delay_minutes=6 * 60),
    Expectation("commerce_load_bronze", "schedule", "일 1회 04:00", max_delay_minutes=24 * 60),
    Expectation("commerce_load_silver", "schedule", "일 1회 05:00", max_delay_minutes=24 * 60),
    Expectation("commerce_load_gold", "schedule", "일 1회 06:00", max_delay_minutes=24 * 60),
    # 간격이 고르지 않다(08·12·16·20시) — 최장 공백이 20시→08시의 12시간이라 그 값을 쓴다.
    Expectation("commerce_collect_watchdog", "schedule", "하루 4회(08·12·16·20시)",
                max_delay_minutes=12 * 60),
    # 상류 이벤트형은 고정 주기 대신 트리거·상류·최대 허용 지연으로 등록한다(S-3).
    Expectation("commerce_serving_export", "asset",
                upstream="commerce_load_gold (gold 완료 Asset)", max_delay_minutes=3 * 60),
)

register(DOMAIN, owner=OWNER, confirmed_on=OWNER_CONFIRMED_ON, items=EXPECTATIONS)


def expectation_rows(*, updated_at: str) -> list[dict[str, Any]]:
    """(하위호환) commerce 행만. 신규 호출부는 `common.ops.expectations.rows` 를 쓴다."""
    return _rows(updated_at=updated_at, domains=[DOMAIN])
