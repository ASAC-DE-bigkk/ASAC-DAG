"""culture 외부 gold 7종 → Cloudflare D1 서빙 export (#520).

공통 D1 Publisher(#505) `build_serving_export_dag` factory 의 도메인 소비자 1호.
계약은 ASAC-DBT culture manifest 의 `meta.serving`(v1.1, ASAC-DBT#346)이 정본이고,
여기는 product_ids 와 스케줄 선언만 한다.

- 스케줄 30 4 * * * (KST): culture_transform 완료(~03:22) 후 · culture_slo(05:01) 전.
  계약의 publication_trigger.schedule_cron 과 일치 의무(계약 §6).
- 7종 전량 스냅샷(A안) — 설계: docs/design/2026-07-27-culture-serving-export-d1.md
"""
from common.serving.dag_factory import build_serving_export_dag

dag = build_serving_export_dag(
    domain="culture",
    product_ids=[
        "culture_activity_by_dong",
        "culture_calendar_density",
        "culture_event_schedule",
        "culture_event_crowd",
        "culture_boxoffice_daily",
        "culture_dine_around",
        "culture_booking_curve",
    ],
    schedule="30 4 * * *",
)
