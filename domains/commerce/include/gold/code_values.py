"""commerce_code_value 채움 — 정규화 Option 1(공유 코드 테이블).

dbt/domains/commerce/docs/DB/gold/normalization-plan.md 근거로 detail payload 저카디널리티 컬럼을
검증했다: pg_stats 카디널리티 실측(85테이블/1031컬럼) → 접미사 휴리스틱(날짜/플래그/수량 제외) →
**실제 값 표본 조회로 최종 검증**(상수·결측만·0/1 플래그는 제외 — 예: mail_order_sale.uptaenm 은
동일 필드명이라도 977종 준자유텍스트라 제외, food_sanitation_business.uptaenm 은 75종 진짜
통제어휘라 채택). 아래 CANDIDATES 는 그 검증을 통과한 72쌍(2026-07-10 실측)만 담는다.

detail 테이블 스키마는 바꾸지 않는다(테이블 폭증 방지) — commerce_code_value 1개에 domain(=
"<detail 테이블>.<컬럼>") 으로 구분해 전부 수용한다. 값 자체가 자연키(서러게이트 코드 미발급).
"""
from __future__ import annotations

import logging

from gold import pg

log = logging.getLogger(__name__)

CANDIDATES: list[tuple[str, str]] = [
    ("commerce_air_pollution_facility_detail", "ctgry_nm"),
    ("commerce_air_pollution_facility_detail", "main_prdt_nm"),
    ("commerce_amusement_park_detail", "bdngsrvnm"),
    ("commerce_amusement_park_detail", "culphyedcobnm"),
    ("commerce_amusement_park_detail", "nearenvnm"),
    ("commerce_amusement_park_detail", "regnsenm"),
    ("commerce_building_sanitation_detail", "bldg_psn_se_nm"),
    ("commerce_building_sanitation_detail", "bzstat_se_nm"),
    ("commerce_building_sanitation_detail", "hygn_bzstat_nm"),
    ("commerce_construction_waste_detail", "dscd_prcs_se_nm"),
    ("commerce_env_construction_detail", "bzstat_se_nm"),
    ("commerce_feed_manufacturing_detail", "lindprcbgbnnm"),
    ("commerce_feed_manufacturing_detail", "uptaenm"),
    ("commerce_food_sanitation_business_detail", "bdngownsenm"),
    ("commerce_food_sanitation_business_detail", "lvsenm"),
    ("commerce_food_sanitation_business_detail", "sntuptaenm"),
    ("commerce_food_sanitation_business_detail", "trdpjubnsenm"),
    ("commerce_food_sanitation_business_detail", "uptaenm"),
    ("commerce_food_sanitation_business_detail", "wtrsplyfacilsenm"),
    ("commerce_free_job_agency_detail", "bupgbnnm"),
    ("commerce_game_entertainment_venue_detail", "bdngsrvnm"),
    ("commerce_game_entertainment_venue_detail", "bfgameocptectcobnm"),
    ("commerce_game_entertainment_venue_detail", "culphyedcobnm"),
    ("commerce_game_entertainment_venue_detail", "nearenvnm"),
    ("commerce_game_entertainment_venue_detail", "prvdgathinnm"),
    ("commerce_game_entertainment_venue_detail", "regnsenm"),
    ("commerce_game_entertainment_venue_detail", "vdoretornm"),
    ("commerce_garbage_bag_sale_detail", "comm_se_nm"),
    ("commerce_high_pressure_gas_detail", "prdsenm"),
    ("commerce_high_pressure_gas_detail", "uptaenm"),
    ("commerce_high_pressure_gas_detail", "wrkpgrdsrvsenm"),
    ("commerce_hospital_detail", "uptaenm"),
    ("commerce_large_store_detail", "jpsenm"),
    ("commerce_large_store_detail", "uptaenm"),
    ("commerce_livestock_processing_detail", "lindprcbgbnnm"),
    ("commerce_livestock_processing_detail", "uptaenm"),
    ("commerce_livestock_sale_detail", "lindprcbgbnnm"),
    ("commerce_livestock_sale_detail", "uptaenm"),
    ("commerce_lodging_detail", "bdngownsenm"),
    ("commerce_lodging_detail", "snttn_bzstat_nm"),
    ("commerce_lodging_detail", "sntuptaenm"),
    ("commerce_mail_order_sale_detail", "silmetnm"),
    ("commerce_media_content_business_detail", "bdngsrvnm"),
    ("commerce_media_content_business_detail", "culphyedcobnm"),
    ("commerce_media_content_business_detail", "culwrkrsenm"),
    ("commerce_media_content_business_detail", "nearenvnm"),
    ("commerce_media_content_business_detail", "perplaformsenm"),
    ("commerce_media_content_business_detail", "regnsenm"),
    ("commerce_medical_institution_detail", "metrorgassrnm"),
    ("commerce_medical_institution_detail", "uptaenm"),
    ("commerce_medical_similar_detail", "metrbosassrnm"),
    ("commerce_medical_similar_detail", "uptaenm"),
    ("commerce_paid_job_agency_detail", "bupgbnnm"),
    ("commerce_petroleum_sale_detail", "uptaenm"),
    ("commerce_public_sanitation_service_detail", "bdngownsenm"),
    ("commerce_public_sanitation_service_detail", "sntuptaenm"),
    ("commerce_public_sanitation_service_detail", "uptaenm"),
    ("commerce_septic_tank_construction_detail", "bzstat_se_nm"),
    ("commerce_septic_tank_construction_detail", "envm_task_se_nm"),
    ("commerce_septic_tank_construction_detail", "tpbiz_se_nm"),
    ("commerce_sports_facility_detail", "culphyedcobnm"),
    ("commerce_sports_facility_detail", "puprsenm"),
    ("commerce_sports_facility_detail", "uptaenm"),
    ("commerce_timber_import_dist_detail", "statesenm"),
    ("commerce_tobacco_retail_detail", "mwsrnm"),
    ("commerce_tourism_business_detail", "bdngsrvnm"),
    ("commerce_tourism_business_detail", "culphyedcobnm"),
    ("commerce_tourism_business_detail", "nearenvnm"),
    ("commerce_tourism_business_detail", "regnsenm"),
    ("commerce_water_pollution_facility_detail", "bzstat_se_nm"),
    ("commerce_water_pollution_facility_detail", "main_prdt_nm"),
    ("commerce_water_pollution_facility_detail", "tpbiz_se_nm"),
]

_SAFE = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


def _safe_ident(name: str) -> str:
    if not name or any(c not in _SAFE for c in name):
        raise ValueError(f"안전하지 않은 식별자: {name!r}")
    return name


def build_code_values(pgconn) -> dict:
    """CANDIDATES 의 실제 값을 detail 테이블에서 집계해 commerce_code_value 에 upsert.

    존재하지 않는 테이블/컬럼(카탈로그 드리프트로 대상이 빠졌을 수 있음)은 사전에 information_schema
    로 걸러 skip(쿼리 실패로 인한 트랜잭션 중단 방지). domain 단위 delete+insert(멱등 — 그 domain 의
    최신 값 집합으로 항상 교체, 사라진 옛 값은 자연 제거).
    """
    populated, skipped = 0, 0
    with pgconn.cursor() as cur:
        cur.execute("select table_name, column_name from information_schema.columns "
                     "where table_schema = 'public'")
        existing = {(r[0], r[1]) for r in cur.fetchall()}

        for table, col in CANDIDATES:
            t, c = _safe_ident(table), _safe_ident(col)
            if (t, c) not in existing:
                skipped += 1
                continue
            cur.execute(  # security: allow-sql - 식별자는 코드 상수(CANDIDATES, 존재 확인 완료)
                f"select {c}, count(*) from {t} where {c} is not null group by {c}")
            rows = cur.fetchall()
            domain = f"{t.removeprefix('commerce_')}.{c}"
            values = [(domain, v, n) for v, n in rows if v not in ("", "0", "1")]
            cur.execute("delete from commerce_code_value where domain = %s", (domain,))
            if len(values) < 2:   # 상수/무의미(distinct<=1) — normalization-plan Tier B, 정규화 무의미
                skipped += 1
                continue
            pg.execute_values(cur, "insert into commerce_code_value "
                                    "(domain, value, n_occurrences) values %s", values)
            populated += 1
    pgconn.commit()
    log.info("code_value: domain %d개 채움(%d개 skip — 대상 테이블/컬럼 없음/값 없음)", populated, skipped)
    return {"domains": populated, "skipped": skipped}
