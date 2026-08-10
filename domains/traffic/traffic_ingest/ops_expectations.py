"""traffic 파이프라인 기대 주기 — 공용 레지스트리에 등록한다 (ASK-Seoul#78 §9, ASAC-DAG#733).

**값의 정본은 각 DAG 파일의 ``schedule=``/Asset 선언이고 이 표는 사본이다**(`S-1`).
스케줄을 바꾸면 같은 커밋에서 여기도 고친다 — 테스트가 실제 DAG 파일의 ``dag_id`` 집합과
이 표를 양방향 대조한다.

스키마·등록 규칙: :mod:`common.ops.expectations`.
오너 확인: @masondev1024. ``confirmed_on`` 은 [[weather_ingest.ops_expectations]] 와 같은
근거(ASK-Seoul#78 §9 "기대 주기 등록" 절의 weather·traffic 담당 예시, 2026-08-03)를 쓴다.

등록 대상 = 2026-08-10 기준 ``domains/traffic/`` 실제 DAG 파일 전수(15개). 이 시점 운영 D1
상태표(2026-08-07 실측, #733)는 8개로 더 적은데, 그중 3개는 수동 전용(`S-4` 로 여기 등록은
하되 감시 제외)이고 ``traffic_cross_domain_gold_transform``·``traffic_cross_domain_serving_export``
는 이후 머지된 Traffic×Weather Gold 신규 DAG라 그 실측 스냅샷에는 없었다.

``max_delay_minutes`` 는 "이 시간 넘게 소식 없으면 지연/장애로 판정"이라 각 주기의 여유분을
얹었다 — 정확한 값을 확정하는 실측이 아니라 첫 등록 기준값이며, 오탐이 나면 조정 대상이다.
"""
from __future__ import annotations

from common.ops.expectations import Expectation, register

DOMAIN = "traffic"
OWNER = "masondev1024"
OWNER_CONFIRMED_ON = "2026-08-03"

EXPECTATIONS = (
    # TOPIS incident raw 랜딩 — 5분마다. 6회(30분) 이상 공백이면 지연/장애로 본다.
    Expectation("traffic_incident_landing", "schedule", "5분마다", max_delay_minutes=30),
    # 5분 랜딩을 15분 단위로 드레인하는 materializer — 고정 주기(자산 이벤트를 두 번째
    # 스케줄 경로로 쓰지 않는다, traffic_ingest.assets.materializer_schedule 주석 참조).
    Expectation("traffic_incident_bronze", "schedule", "15분마다", max_delay_minutes=60),
    # 특정 TOPIS page window 재수집 — 수동 전용, 감시 제외(S-4).
    Expectation("traffic_incident_recollect", "manual", monitored=False),
    # 기존 raw object로 bronze만 재적재(API 호출 없음) — 수동 전용, 감시 제외(S-4).
    Expectation("traffic_incident_bronze_backfill", "manual", monitored=False),
    Expectation("traffic_link_reference_backfill", "manual", monitored=False),
    # 도로명·좌표 기준정보 — 매일 03:37, 신규/30일 stale link만 최대 100건 처리.
    Expectation("traffic_link_reference_sync", "schedule", "일 1회 03:37",
                max_delay_minutes=26 * 60),
    # incident bronze 완료 Asset 트리거.
    Expectation("traffic_incident_transform", "asset",
                upstream="traffic_incident_bronze (bronze 완료 Asset)", max_delay_minutes=60),
    # flow bronze 도 incident bronze 완료 Asset 을 트리거로 쓴다(공유 수집 순서 제약).
    Expectation("traffic_flow_bronze", "asset",
                upstream="traffic_incident_bronze (bronze 완료 Asset)", max_delay_minutes=60),
    Expectation("traffic_flow_transform", "asset",
                upstream="traffic_flow_bronze (flow bronze 완료 Asset)", max_delay_minutes=60),
    # incident silver 완료 Asset 트리거(gold).
    Expectation("traffic_gold_transform", "asset",
                upstream="traffic_incident_transform (silver 완료 Asset)", max_delay_minutes=90),
    Expectation("traffic_serving_export", "asset",
                upstream="traffic_gold_transform (core gold 발행 완료 Asset)", max_delay_minutes=120),
    # Cross-domain gold — 세 상류 Asset 중 먼저 도착하는 쪽으로 트리거된다.
    Expectation("traffic_cross_domain_gold_transform", "asset",
                upstream=("traffic_incident_transform (silver 완료 Asset) 또는 "
                          "traffic_gold_transform (core gold 발행 완료 Asset) 또는 "
                          "weather_vilage_fcst_transform (weather gold 발행 완료 Asset)"),
                max_delay_minutes=180),
    Expectation("traffic_cross_domain_serving_export", "asset",
                upstream="traffic_cross_domain_gold_transform (cross-domain gold 발행 완료 Asset)",
                max_delay_minutes=120),
    # 신뢰도 리포트 — 매일 09:00(Discord webhook 설정 시).
    Expectation("traffic_bronze_reliability_report", "schedule",
                "일 1회 09:00", max_delay_minutes=24 * 60),
    # 스냅샷 결손 구간 복구 — 수동 전용, 감시 제외(S-4).
    Expectation("traffic_snapshot_recovery", "manual", monitored=False),
)

register(DOMAIN, owner=OWNER, confirmed_on=OWNER_CONFIRMED_ON, items=EXPECTATIONS)
