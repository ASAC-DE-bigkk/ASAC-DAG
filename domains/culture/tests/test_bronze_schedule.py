"""#201 — culture_bronze 스케줄이 자정 혼잡창을 벗어나 있는지 회귀 검증.

KOPIS 는 자정 직후 짧은 창(00:00~00:02 KST)에서 간헐 400 을 뱉는다(cause B, #201).
동일 요청이 낮/새벽엔 정상이므로, 자정 정각 스케줄을 벗어나는 것이 가장 값싼 완화책이다
(#201 후보 ③ — 자정 창 이탈). 이 테스트는 누군가 무심코 ``@daily``(=자정)로 되돌려
노출을 재도입하는 것을 막는 그물이다. 소스 텍스트만 보므로 airflow 가 없는 호스트에서도 돈다.
"""
from __future__ import annotations

import os
import re

_CULTURE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_BRONZE = os.path.join(_CULTURE, "culture_bronze.py")


def _schedule_literal() -> str:
    with open(_BRONZE, encoding="utf-8") as f:
        src = f.read()
    m = re.search(r"schedule\s*=\s*([\"'])(?P<val>.+?)\1", src)
    assert m, "culture_bronze.py 에서 schedule= 리터럴을 찾지 못했습니다"
    return m.group("val")


def test_bronze_not_scheduled_at_midnight():
    """자정 정각(@daily 또는 0 0)이면 #201 자정 400 창에 그대로 노출된다."""
    sched = _schedule_literal()
    assert sched != "@daily", "@daily = 자정 정각 → #201 자정 400 창 노출 (03시 등으로 이동 필요)"
    assert not re.match(r"^\s*\S+\s+0\s", sched), f"분=0시=0(자정) 스케줄 금지: {sched!r}"


def test_bronze_schedule_is_0300_kst():
    """현행 완화값 고정 — 03:00 KST(DAG 타임존이 KST 라 cron 도 KST 해석)."""
    assert _schedule_literal() == "0 3 * * *"
