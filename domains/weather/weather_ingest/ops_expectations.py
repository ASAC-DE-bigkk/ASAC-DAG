"""weather 파이프라인 기대 주기 — 공용 레지스트리에 등록한다 (ASK-Seoul#78 §9, ASAC-DAG#733).

**값의 정본은 각 DAG 파일의 ``schedule=``/Asset 선언이고 이 표는 사본이다**(`S-1`).
스케줄을 바꾸면 같은 커밋에서 여기도 고친다 — 테스트가 실제 DAG 파일의 ``dag_id`` 집합과
이 표를 양방향 대조한다.

스키마·등록 규칙: :mod:`common.ops.expectations`.
오너 확인: @masondev1024. ``confirmed_on`` 은 ASK-Seoul#78 §9 "기대 주기 등록" 절에서
weather·traffic 담당(masondev1024) 예시로 제시된 등록일(2026-08-03)을 쓴다 — #78 §9 표
자체에는 commerce 처럼 도메인별 개별 확인일이 박혀 있지 않고, 이 날짜가 이슈에 남은 유일한
weather 소유 확인 날짜다.

등록 대상 = 2026-08-08 기준 ``domains/weather/`` 실제 DAG 파일 전수(11개). ``ask_seoul_iceberg_maintenance``
는 ``weather_`` 접두를 따르지 않는 유일한 예외지만(#78 코멘트), weather 번들 안에 물리적으로
있고 weather·traffic Iceberg 테이블을 함께 정리하므로 여기서 등록한다.

``max_delay_minutes`` 는 "이 시간 넘게 소식 없으면 지연/장애로 판정"이라 각 주기의 여유분을
얹었다 — 정확한 값을 확정하는 실측이 아니라 첫 등록 기준값이며, 오탐이 나면 조정 대상이다.
"""
from __future__ import annotations

from common.ops.expectations import Expectation, register

DOMAIN = "weather"
OWNER = "masondev1024"
OWNER_CONFIRMED_ON = "2026-08-03"

EXPECTATIONS = (
    # KMA getVilageFcst raw 수집 + bronze 발행 — 3시간마다(02·05·08·11·14·17·20·23시 20분).
    # 한 주기(180분) 넘게 공백이면 지연/장애로 본다.
    Expectation("weather_vilage_fcst_bronze", "schedule",
                "3시간마다 (02·05·08·11·14·17·20·23시 20분, KST)", max_delay_minutes=240),
    # 특정 base_date/base_time TOPIS 재수집 — 수동 전용, 감시 제외(S-4).
    Expectation("weather_vilage_fcst_recollect", "manual", monitored=False),
    # 기존 raw object_keys로 bronze만 재적재(API 호출 없음) — 수동 전용, 감시 제외(S-4).
    Expectation("weather_vilage_fcst_bronze_backfill", "manual", monitored=False),
    # bronze 완료 Asset 트리거 — 상류 이벤트형은 고정 주기 대신 트리거+상류+최대 지연으로(S-3).
    Expectation("weather_vilage_fcst_transform", "asset",
                upstream="weather_vilage_fcst_bronze (bronze 완료 Asset)", max_delay_minutes=90),
    Expectation("weather_w2_canonical_transform", "asset",
                upstream="weather_vilage_fcst_bronze (bronze 완료 Asset)", max_delay_minutes=90),
    # W2 canonical 계약 감사(데이터 쓰기 없음) — 매일 09:15.
    Expectation("weather_w2_canonical_contract_audit", "schedule",
                "일 1회 09:15", max_delay_minutes=24 * 60),
    # 신뢰도 리포트 — 매일 09:00(Discord webhook 설정 시).
    Expectation("weather_bronze_reliability_report", "schedule",
                "일 1회 09:00", max_delay_minutes=24 * 60),
    # gold 발행 완료 Asset 트리거(weather_vilage_fcst_transform 이 발행).
    Expectation("weather_serving_export", "asset",
                upstream="weather_vilage_fcst_transform (gold 발행 완료 Asset)", max_delay_minutes=120),
    # W1 bridge 계약 스모크 — 격리 스키마에서 수동 실행, 감시 제외(S-4).
    Expectation("weather_w1_contract_smoke", "manual", monitored=False),
    # W2 과거 관측 복구(<=6h 창 단위) — 수동 전용, 감시 제외(S-4).
    Expectation("weather_w2_observation_recovery", "manual", monitored=False),
    # weather/traffic Iceberg 메타데이터 정리 — 주 1회(일요일 04:00). weather 번들 소속이라 여기서 등록.
    Expectation("ask_seoul_iceberg_maintenance", "schedule",
                "주 1회 (일요일 04:00)", max_delay_minutes=2 * 24 * 60),
)

register(DOMAIN, owner=OWNER, confirmed_on=OWNER_CONFIRMED_ON, items=EXPECTATIONS)
