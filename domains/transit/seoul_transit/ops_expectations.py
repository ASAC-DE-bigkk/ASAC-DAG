"""transit 파이프라인 기대 주기 — 공용 레지스트리에 등록한다 (ASK-Seoul#78 §9, ASAC-DAG#733).

**값의 정본은 각 DAG 파일의 ``schedule=`` 선언이고 이 표는 사본이다**(`S-1`).
스케줄을 바꾸면 같은 커밋에서 여기도 고친다 — ``common/tests`` 통합 census 와
``domains/transit/tests/test_ops_expectations.py`` 가 실제 선언과 양방향 대조한다.

#733 경위: ``load_all()`` 후보명이 ``transit_ingest.ops_expectations`` 였는데 transit 번들의
실제 패키지는 ``seoul_transit`` 라 영원히 임포트되지 않았고, 침묵 스킵에 가려 transit 전
DAG 가 기대치 없이(죽어도 "원래 안 도는 시간") 돌았다. 이 모듈이 그 구멍을 메운다 —
후보명 정정과 재발 방지 게이트는 ``common/ops/expectations.py``·``common/tests`` 참조.

⏰ **transit 은 시간대가 혼재한다** (2026-08-12 origin/dev 실측):
  - 수집계(bronze/master/maintenance/timetable)는 naive ``start_date`` → 크론이 **UTC** 해석
    (배포에 ``AIRFLOW__CORE__DEFAULT_TIMEZONE`` 오버라이드 없음 실측 · subway_timetable 의
    ``20 1 * * *  # 10:20 KST`` 주석이 그 전제).
  - 서빙 export·transform·워치독 계열은 tz-aware(``Asia/Seoul``) → 크론이 **KST** 해석.
  그래서 ``schedule_timezone`` 을 DAG 별로 명시하고, 사람 읽는 주기엔 KST 를 병기한다.

오너: @codingpoppy94 (#78 §9 오너 확인 2026-07-31). 값은 origin/dev DAG 선언 실측이며,
``config.schedule_for(key, default)`` 류는 **기본값**을 적었다(compose 에 ``*_SCHEDULE``
env 없음 실측 — 기본값이 실효값). ``max_delay_minutes`` 는 기존 등록부(weather·commerce)
관례를 따른 첫 등록 기준값 — 오탐이 나면 조정 대상이다.
"""
from __future__ import annotations

from common.ops.expectations import Expectation, register

DOMAIN = "transit"
OWNER = "codingpoppy94"
OWNER_CONFIRMED_ON = "2026-07-31"

EXPECTATIONS = (
    # ── raw→bronze 적재 루프 (UTC 크론) ──────────────────────────────────────
    Expectation("transit_bronze_loader", "schedule",
                "10분마다", max_delay_minutes=30, schedule_timezone="UTC"),
    # ── 실시간 수집 (UTC 크론) ────────────────────────────────────────────────
    # 크론은 "깨어나는 주기"이고 실제 수집은 요일별 시간창 판정(창 밖 런은 호출 0건 종료).
    # 런 자체는 매 10분 발생하므로 런 기준 감시는 유효하다.
    Expectation("transit_bus_bronze", "schedule",
                "10분마다 (수집은 시간창 내에서만)", max_delay_minutes=30,
                schedule_timezone="UTC"),
    Expectation("transit_subway_bronze", "schedule",
                "3분마다", max_delay_minutes=10, schedule_timezone="UTC"),
    Expectation("transit_parking_bronze", "schedule",
                "5분마다", max_delay_minutes=15, schedule_timezone="UTC"),
    # ── 저빈도 수집·마스터 (UTC 크론) ────────────────────────────────────────
    Expectation("transit_bus_route_master", "schedule",
                "주 1회 (일 09:00 KST)", max_delay_minutes=2880, schedule_timezone="UTC"),
    # 특정 load_date 재적재 — 수동 전용(schedule=None), 감시 제외(S-4).
    Expectation("transit_bus_route_master_backfill", "manual", monitored=False),
    Expectation("transit_master_bronze", "schedule",
                "주 1회 (일 09:00 KST)", max_delay_minutes=2880, schedule_timezone="UTC"),
    Expectation("transit_subway_timetable_bronze", "schedule",
                "일 1회 10:20 KST (01:20 UTC)", max_delay_minutes=1440,
                schedule_timezone="UTC"),
    Expectation("transit_maintenance", "schedule",
                "일 1회 09:00 KST (@daily UTC)", max_delay_minutes=1440,
                schedule_timezone="UTC"),
    # ── 워치독 (KST) ─────────────────────────────────────────────────────────
    # 로더 15분 SLO 감시(#719) — 자기도 죽을 수 있어 감시 대상.
    Expectation("transit_bronze_loader_watchdog", "schedule",
                "5분마다", max_delay_minutes=15),
    Expectation("transit_serving_freshness_watchdog", "schedule",
                "15분마다 (매시 7·22·37·52분, KST)", max_delay_minutes=45),
    # ── 변환(dbt, KST) ───────────────────────────────────────────────────────
    # fresh 모델(*/15, tag:heavy 제외) — '지금' 카드 신선도 축(#443).
    Expectation("transit_transform", "schedule",
                "15분마다", max_delay_minutes=45),
    # heavy 프로파일 4종 — 30분 주기(5,35 오프셋: fresh 와 동시 트리거 회피).
    Expectation("transit_transform_heavy", "schedule",
                "30분마다 (매시 5·35분)", max_delay_minutes=90),
    # ── 서빙 export (KST) ────────────────────────────────────────────────────
    Expectation("transit_serving_export_fast", "schedule",
                "15분마다 (매시 10·25·40·55분, KST)", max_delay_minutes=45),
    Expectation("transit_serving_export_hourly", "schedule",
                "매시 40분 (KST)", max_delay_minutes=90),
    Expectation("transit_serving_export_daily", "schedule",
                "일 1회 08:40 KST", max_delay_minutes=1440),
)

register(DOMAIN, owner=OWNER, confirmed_on=OWNER_CONFIRMED_ON, items=EXPECTATIONS)
