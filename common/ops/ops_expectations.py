"""common(공용 축) 파이프라인 기대 주기 — 공용 레지스트리에 등록한다 (ASK-Seoul#78 §9, ASAC-DAG#733).

**값의 정본은 dags 루트 DAG 파일의 ``schedule=`` 선언이고 이 표는 사본이다**(`S-1`).
스케줄을 바꾸면 같은 커밋에서 여기도 고친다 — ``common/tests`` 의 통합 census 가 실제
선언과 이 표를 양방향 대조한다.

#733 이 "등록 주체 확인 필요"로 남겨 뒀던 common 2종을 파이프라인 공용 축 소유로 등록한다.
로더(``common_ops_d1_load``)가 자기 자신을 등록하는 구조라 **로더가 완전히 죽으면 이 표의
사본 갱신도 함께 멈춘다** — 그 공백은 이 표가 아니라 콘솔의 "마지막 적재 시각" 카드가 잡는다
(`C-9`: 기록 없음은 기대 주기 초과와 함께 판정).

등록 대상 = ops runs 관측을 내는 루트 DAG. 제외(관측 미배선 — ops_default_args/emit 0건 실측):
  - ``collection_slot_materializer`` · ``common_admin_dong_bronze`` · ``common_dbt_smoke``
    — 상태표(`_ops_pipeline_state`)에 행이 생기지 않아 '미등록' 판정 자체가 없다. 관측을
    배선하는 날 여기에도 같이 등록한다(통합 census 가 그때 강제한다).
"""
from __future__ import annotations

from common.ops.expectations import Expectation, register

DOMAIN = "common"
OWNER = "Exisign"
OWNER_CONFIRMED_ON = "2026-08-12"

EXPECTATIONS = (
    # 저장소 → 조회 DB 적재 + 기대주기 사본 upsert. 3시간마다(매 3시간의 15분, KST).
    # 한 사이클+여유(weather 3시간 관례 240분) 공백이면 지연/장애로 본다.
    Expectation("common_ops_d1_load", "schedule",
                "3시간마다 (매 3시간의 15분, KST)", max_delay_minutes=240),
    # 전 도메인 로컬 태스크 로그 tar.gz 이관 — 일 1회 02:30 KST.
    Expectation("common_ops_logship", "schedule",
                "일 1회 02:30 (KST)", max_delay_minutes=1440),
)

register(DOMAIN, owner=OWNER, confirmed_on=OWNER_CONFIRMED_ON, items=EXPECTATIONS)
