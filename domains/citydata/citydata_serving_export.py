"""Airflow DAGs: citydata 골드 → Cloudflare D1 서빙 (공통 Serving Contract v1 Publisher, #478).

기존 손수 export(_export)를 공통 ``build_serving_export_dag`` 로 대체한다. 발행 규칙의 정본은
dbt yml 의 ``meta.serving`` 계약 — 0행 보호(zero_policy=retain_last_good)·볼륨 절단(partial_policy)·
append 윈도우(누적 이력)·행수 검증·``_catalog`` 등록·API smoke 를 공통 Publisher 가 처리한다.
과거 여기 있던 손 관리 리스트(FAST/CRITICAL/APPEND)·PK·append 로직은 전부 계약으로 흡수됐다.

신선도/정체 경보(발표지연·정체 두 지표)는 발행과 책임 분리 — ``citydata_serving_monitor`` DAG 로 이관.

D1 쓰기 한도 관리를 위해 3 티어로 스케줄만 분리(현행 정책 유지):
  critical(*/5)  : "지금 어때" 핵심 실시간 스냅샷 4종 (골드가 5분마다 재빌드 → D1 도 5분 동기)
  fast(:15 매시) : 시간단위면 충분한 실시간/시간 골드 7종 (hourly transform 직후)
  daily(00:30)   : 일 집계·패턴 6종 (대부분 append — 최근 구간만 재적재)

계약 로드는 manifest(meta.serving) 기반 — 모델/계약 변경 시 manifest 재생성 필요.
"""

from __future__ import annotations

import os

from common.serving.dag_factory import build_serving_export_dag

# 프로젝트 target 관례(DBT_TARGET, 기본 prod) — 컷오버(#556). runmetrics._resolve_target 와 동일.
_TARGET = os.environ.get("DBT_TARGET", "prod")
# 스키마: prod=citydata, dev=seoul_citydata (transform·dbt profiles 와 정렬).
_SCHEMA = "citydata" if _TARGET == "prod" else "seoul_citydata"

# 티어 = product_id 묶음. 각 골드의 publication_trigger.schedule_cron(계약)이 아래 스케줄과 정렬돼 있다.
# 실시간 스냅샷 제거 + 시계열 집중(ASAC-DBT#404) — CRITICAL(5분마다 전량) tier 폐지.
# 실시간은 PlayMCP 실시간 MCP 가 커버 → citydata 는 시계열·패턴만 서빙.
FAST = [
    "citydata_ppltn_hourly",               # append(시간축) — 새 hour 만 게시(저비용)
]
DAILY = [
    "citydata_ppltn_dow_hour",
    "citydata_ppltn_demographics",         # 신설: 성별·나이대 패턴 — incremental upsert(바뀐 버킷만 D1 갱신)
    "citydata_ppltn_daily",                # append(일축)
    "citydata_cmrcl_daily",                # append
    "citydata_purchasing_power_daily",     # append
    "citydata_ppltn_x_culture_daily",      # append
    "citydata_transit_x_incident_hourly",  # 시계열 크로스 — append(새 time_bucket만) daily 게시
    "citydata_ppltn_x_weather_hourly",     # 시계열 크로스 — 동상(snapshot→append 전환 후 편입)
    "citydata_sbike_dow_hour",             # 신설: 영역 요일×시간 따릉이 가용
    "citydata_air_daily",                  # 신설: 일별 대기질 추이 (append)
    "citydata_charger_dow_hour",           # 신설: 영역 요일×시간 충전소 가용 (as-of 리샘플링)
    "citydata_dst_daily",                  # 신설: 일별 재난 경보 현황 (append)
    "citydata_dst_dow_hour",               # 신설: 재난 경보 요일×시간 패턴 (snapshot)
]

citydata_serving_export_fast = build_serving_export_dag(
    domain="citydata", product_ids=FAST, schedule="15 * * * *",
    dag_id="citydata_serving_export_fast", schema=_SCHEMA, target=_TARGET)

citydata_serving_export_daily = build_serving_export_dag(
    domain="citydata", product_ids=DAILY, schedule="30 0 * * *",
    dag_id="citydata_serving_export_daily", schema=_SCHEMA, target=_TARGET)
