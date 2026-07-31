"""Airflow DAGs: transit 골드 → Cloudflare D1 서빙 (공통 Serving Contract v1 Publisher, #478).

프로토타입 수동 export(ASK-Seoul serving/export_transit_to_d1.py, #475)를 공통
``build_serving_export_dag`` 로 대체한다. 발행 규칙의 정본은 dbt schema.yml 의
``meta.serving`` 계약 — 0행 보호(retain_last_good)·append 윈도우·행수 검증·
``_catalog`` upsert(공유 D1, DROP 금지)·API smoke 를 공통 Publisher 가 처리한다.

3-tier 주기는 프로토타입 --mode(fast/hourly/full)를 스케줄 분리로 승계(#475):
  fast(:10/:25/:40/:55) : 실시간 스냅샷 2종 — dong_now·parking_full_risk (15분 전량 교체)
  hourly(:40)           : dong_hourly — append(hour_at, lookback 2h), D1 비면 전체 백필
  daily(08:40 KST)      : forecast_card·event_access·parking_profile — 일 1회 전량 교체

신선도 임계(75분 등)는 각 모델 계약의 ``freshness_slo_minutes`` 로 이식 — 감시는
watchdog 몫(계약 §7.4), Publisher 는 freshness 실측값만 ``_catalog`` 에 기록한다.

계약 로드는 manifest(meta.serving) 기반 — 모델/계약 변경 시 manifest 재생성 필요.
"""

from __future__ import annotations

import os

from common.serving.dag_factory import build_serving_export_dag

# 프로젝트 target 관례(DBT_TARGET, 기본 prod) — 컷오버(#556), citydata_serving_export 와 동일.
# 스키마는 prod/dev 동일 transit(#204 — 환경 분리는 카탈로그 몫)이라 노브 불필요.
_TARGET = os.environ.get("DBT_TARGET", "prod")

# 티어 = product_id 묶음. 각 골드의 publication_trigger.schedule_cron(계약)이 아래 스케줄과 정렬돼 있다.
FAST = [
    "transit_dong_now",
    "transit_parking_full_risk",
]
HOURLY = [
    "transit_dong_hourly",            # append(hour_at)
]
DAILY = [
    "transit_forecast_card",
    "transit_event_access",
    "transit_parking_profile",
]

transit_serving_export_fast = build_serving_export_dag(
    domain="transit", product_ids=FAST, schedule="10,25,40,55 * * * *",
    dag_id="transit_serving_export_fast", target=_TARGET)

transit_serving_export_hourly = build_serving_export_dag(
    domain="transit", product_ids=HOURLY, schedule="40 * * * *",
    dag_id="transit_serving_export_hourly", target=_TARGET)

transit_serving_export_daily = build_serving_export_dag(
    domain="transit", product_ids=DAILY, schedule="40 8 * * *",
    dag_id="transit_serving_export_daily", target=_TARGET)
