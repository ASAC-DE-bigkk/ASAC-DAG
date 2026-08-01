"""commerce 파이프라인 기대 주기 — 조회 DB ``_ops_pipeline_expectation`` 의 commerce 몫.

**값의 정본은 DAG 선언(코드)이고 조회 DB 는 사본이다**(ASK-Seoul#78 S-1). 두 곳에 두면 스케줄을
바꿀 때 어긋나므로, 여기 표는 각 DAG 파일의 ``schedule=`` 를 사람이 읽는 형태로 옮긴 것이고
바뀌면 같은 PR 에서 함께 고친다.

이 표가 필요한 이유는 알림 때문이다: "기록이 없다"를 곧바로 장애로 부르지 않고 **기대 주기
초과와 함께** 판정해야 한다(C-9). 기대치가 없으면 매일 도는 파이프라인이 죽어도 "원래 안 도는
시간"으로 처리된다 — 초안에서 실제로 한 번 났던 사고다(S-5).

오너 확인: @Exisign · 2026-08-01 (#78 §9 commerce 표와 같은 값).
"""
from __future__ import annotations

from typing import Any

DOMAIN = "commerce"
OWNER = "Exisign"
OWNER_CONFIRMED_ON = "2026-08-01"
SCHEDULE_TIMEZONE = "Asia/Seoul"     # 전 DAG 가 이 tz 로 선언돼 있다

#: (dag_id, 트리거 방식, 기대 주기, 상류, 최대 허용 지연(분), 감시 여부)
#: 상류 이벤트형은 고정 주기 대신 **트리거 방식 + 상류 + 최대 허용 지연**으로 등록한다(S-3).
#: 수동 실행 전용 DAG 는 감시 대상에서 제외한다(S-4) — 안 도는 것이 정상이라 알림이 무의미하다.
EXPECTATIONS: tuple[tuple[str, str, str | None, str | None, int | None, bool], ...] = (
    ("commerce_collect_raw",       "schedule", "일 1회 00:00",  None, 24 * 60, True),
    ("commerce_recollect_raw",     "schedule", "6시간",         None, 6 * 60,  True),
    ("commerce_ops_logship",       "schedule", "일 1회 01:30",  None, 24 * 60, True),
    ("commerce_load_bronze",       "schedule", "일 1회 04:00",  None, 24 * 60, True),
    ("commerce_load_silver",       "schedule", "일 1회 05:00",  None, 24 * 60, True),
    ("commerce_load_gold",         "schedule", "일 1회 06:00",  None, 24 * 60, True),
    # 간격이 고르지 않다(08·12·16·20시) — 최장 공백이 20시→08시의 12시간이므로 그 값을 쓴다.
    ("commerce_collect_watchdog",  "schedule", "하루 4회(08·12·16·20시)", None, 12 * 60, True),
    ("commerce_serving_export",    "asset",    None, "commerce_load_gold (gold 완료 Asset)", 3 * 60, True),
    ("commerce_load_gold_refresh", "manual",   None, None, None, False),   # S-4 감시 제외
)


def expectation_rows(*, updated_at: str) -> list[dict[str, Any]]:
    """``_ops_pipeline_expectation`` 행. 자연키는 ``dag_id`` 라 commerce 행만 갱신된다(D-4)."""
    return [
        {
            "dag_id": dag_id,
            "domain": DOMAIN,
            "trigger_type": trigger_type,
            "expected_interval": expected_interval,
            "upstream": upstream,
            "max_delay_minutes": max_delay_minutes,
            "schedule_timezone": SCHEDULE_TIMEZONE,
            "monitored": 1 if monitored else 0,
            "owner": OWNER,
            "owner_confirmed_on": OWNER_CONFIRMED_ON,
            "updated_at": updated_at,
        }
        for (dag_id, trigger_type, expected_interval, upstream,
             max_delay_minutes, monitored) in EXPECTATIONS
    ]
