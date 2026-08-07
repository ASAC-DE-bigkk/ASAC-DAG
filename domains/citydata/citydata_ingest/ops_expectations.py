"""citydata 파이프라인 기대 주기 — 공용 레지스트리에 등록한다 (ASK-Seoul#78 §9, ASAC-DAG#733).

**값의 정본은 각 DAG 파일의 ``schedule=`` 이고 이 표는 사본이다**(`S-1`).
스케줄을 바꾸면 같은 커밋에서 여기도 고친다 — 테스트가 양방향으로 대조한다.

스키마·등록 규칙: :mod:`common.ops.expectations`.
오너 확인: @kang-gyeongmin (#78 §9 citydata 표와 같은 값). ``max_delay_minutes`` 는
"이 시간 넘게 소식 없으면 지연/장애로 판정"이라 각 주기의 여유분을 얹었다.

등록 대상 = 운영 콘솔이 추적하는 현행 DAG. 제외:
  - ``citydata_serving_export_critical`` — 실시간 스냅샷 서빙 폐지(ASAC-DBT#404)로 제거된 DAG.
    운영 D1 ``_ops_pipeline_state`` 에 낡은 행만 남아 있어 상태표에 뜨지만, 현행 코드에 없어
    기대치 등록 대상이 아니다(낡은 state 는 별도로 정리 대상 — #733 범위 밖).
  - ``citydata_maintenance`` · ``citydata_ops_digest`` — 현재 ops runs 관측을 내지 않아
    상태표에 없다(등록해도 state 가 없어 판정 근거가 안 생김). 관측 emit 은 별건.
"""
from __future__ import annotations

from common.ops.expectations import Expectation, register

DOMAIN = "citydata"
OWNER = "kang-gyeongmin"
OWNER_CONFIRMED_ON = "2026-08-08"

EXPECTATIONS = (
    # 원천 수집 — 5분마다. 6회(30분) 이상 공백이면 지연/장애로 본다.
    Expectation("citydata_bronze", "schedule", "5분", max_delay_minutes=30),
    # 변환 — bronze 완료 Asset 으로 도는 이벤트형. 고정 주기 대신 트리거+상류+최대 지연으로(S-3).
    Expectation("citydata_transform_cosmos", "asset",
                upstream="citydata_bronze (bronze 완료 Asset)", max_delay_minutes=30),
    # 시간 서빙 — 매시 :15. 2시간 넘게 공백이면 지연.
    Expectation("citydata_serving_export_fast", "schedule", "1시간 (매시 :15)",
                max_delay_minutes=120),
    # 일 서빙 — 매일 00:30. 하루 넘게 공백이면 지연.
    Expectation("citydata_serving_export_daily", "schedule", "일 1회 00:30",
                max_delay_minutes=24 * 60),
    # 신선도/정체 워치독 — 15분마다. 4회(60분) 이상 공백이면 감시 자체가 죽은 것.
    Expectation("citydata_serving_monitor", "schedule", "15분", max_delay_minutes=60),
    # 품질 지표 게시 — 매시 :45. 2시간 넘게 공백이면 지연.
    Expectation("citydata_quality_export", "schedule", "1시간 (매시 :45)",
                max_delay_minutes=120),
)

register(DOMAIN, owner=OWNER, confirmed_on=OWNER_CONFIRMED_ON, items=EXPECTATIONS)
