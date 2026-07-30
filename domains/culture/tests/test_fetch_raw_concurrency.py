"""#201 — fetch_raw 동적 매핑의 동시성 상한 회귀 검증.

KOPIS 400 은 벽시계가 아니라 **런 시작 burst**(15개 매핑 태스크가 같은 키로 동시에
첫 요청을 터뜨리는 순간)를 따라온다 — 03:00 이동 후에도 400 이 03:00 직후로 따라온
것으로 확정(#201 재오픈, 7/15·18·21·22 4회). 완화 = ``max_active_tis_per_dagrun``
으로 동시 첫 요청 수를 깎는다(#201).

**상한 4(완화 ①)는 듣지 않았다.** 상한이 원천별이 아니라 런 전체라, 15종 중 KOPIS 가
6종이면 4슬롯이 통째로 KOPIS 로 채워진다 — 7/25 는 정확히 4개가 같은 초에 400 을
받았다(슬롯 수와 같다). 그래서 완화 ② 는 두 갈래다: 상한을 **2** 로 내려 동시 KOPIS
호출의 천장을 낮추고, 데이터셋 순서를 원천 라운드로빈으로 섞어 그 2 칸마저 한 원천에
몰리지 않게 한다(``datasets._interleave_by_source``, test_plan_selection 이 지킨다).
직렬화 비용 실측 = 태스크 합 550s ÷ 2 ≈ 275s(상한 4 의 138s 대비 +2.3분), 신선도 SLA 30h.

이 테스트는 누군가 상한을 지우거나 값을 되돌려 burst 노출을 재도입하는 것을 막는
그물이다. 소스 텍스트만 보므로 airflow 가 없는 호스트에서도 돈다.
"""
from __future__ import annotations

import os
import re

_CULTURE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_BRONZE = os.path.join(_CULTURE, "culture_bronze.py")


def _fetch_raw_partial_block() -> str:
    """``PythonOperator.partial(...)`` 중 task_id="fetch_raw" 인 호출 블록을 반환."""
    with open(_BRONZE, encoding="utf-8") as f:
        src = f.read()
    m = re.search(
        r"PythonOperator\.partial\((?P<body>[^)]*task_id=[\"']fetch_raw[\"'][^)]*)\)",
        src,
        re.DOTALL,
    )
    assert m, "culture_bronze.py 에서 fetch_raw 의 .partial( 블록을 찾지 못했습니다"
    return m.group("body")


def test_fetch_raw_has_concurrency_cap():
    """상한 자체가 없으면 15개 동시 burst → #201 400 창 재노출."""
    body = _fetch_raw_partial_block()
    assert "max_active_tis_per_dagrun" in body, (
        "fetch_raw .partial() 에 max_active_tis_per_dagrun 이 없습니다 — "
        "런 시작 burst(#201 cause B)에 그대로 노출됩니다"
    )


def test_fetch_raw_concurrency_cap_is_2():
    """현행 완화값 고정 — 2 (동시 KOPIS 호출 천장, 런 증가 +2.3분 실측)."""
    body = _fetch_raw_partial_block()
    m = re.search(r"max_active_tis_per_dagrun\s*=\s*(\d+)", body)
    assert m, "max_active_tis_per_dagrun 값이 정수 리터럴이 아닙니다"
    assert int(m.group(1)) == 2, (
        f"상한이 2 가 아닙니다: {m.group(1)} — 값을 바꾸려면 #201 의 "
        "burst 실측(재발 시 400 건수·창 길이)과 직렬화 비용을 함께 갱신하세요. "
        "되돌려 4 로 올리면 4슬롯이 KOPIS 로만 채워지던 7/25 상태로 돌아갑니다"
    )
