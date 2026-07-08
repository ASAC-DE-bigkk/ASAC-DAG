"""Silver data-quality summaries and warning notifications."""
from __future__ import annotations

import logging
from decimal import Decimal

from bronze.warehouse import _connect, _qualified
from commerce_core.notify import notify_quality_event
from security import log_event

log = logging.getLogger(__name__)

CURRENT_TABLE = "silver_license_current"
MASKED_ADDRESS_TASK = "commerce_load_silver.masked_address_dong_mapping_skip"
MASK_EXPR = "(coalesce(road_address, '') like '%*%' or coalesce(jibun_address, '') like '%*%')"


def _num(value) -> int | float:
    if value is None:
        return 0
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, float) and not value.is_integer():
        return round(value, 4)
    return int(value)


def notify_masked_address_dong_skip_summary() -> dict:
    """Warn when masked addresses were skipped for dong-level mapping.

    The query returns one aggregate row only; no silver records are loaded into
    Airflow memory.
    """
    catalog, schema, qschema = _qualified()
    qcurrent = f"{qschema}.{CURRENT_TABLE}"
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(  # security: allow-sql - qcurrent is built from _qualified() identifiers.
            f"""
            select
                count(*) as settled_rows,
                sum(case when {MASK_EXPR} then 1 else 0 end) as affected_rows,
                round(100.0 * sum(case when {MASK_EXPR} then 1 else 0 end) / nullif(count(*), 0), 4)
                    as affected_ratio_pct,
                sum(case when {MASK_EXPR} and gu is not null then 1 else 0 end) as affected_rows_with_gu,
                sum(
                    case
                        when {MASK_EXPR}
                         and (
                            legal_dong is not null or legal_code is not null
                            or admin_dong is not null or admin_dong_code is not null
                         )
                        then 1 else 0
                    end
                ) as affected_rows_with_dong_mapping
            from {qcurrent}
            """)
        row = cur.fetchall()[0]
    finally:
        conn.close()

    settled_rows = _num(row[0])
    affected_rows = _num(row[1])
    affected_ratio_pct = _num(row[2])
    affected_rows_with_gu = _num(row[3])
    affected_rows_with_dong_mapping = _num(row[4])
    unaffected_rows = max(settled_rows - affected_rows, 0)
    metrics = {
        "affected_rows": affected_rows,
        "settled_rows": settled_rows,
        "unaffected_rows": unaffected_rows,
        "affected_ratio_pct": affected_ratio_pct,
        "dong_mapping_skipped_rows": affected_rows,
        "affected_rows_with_gu": affected_rows_with_gu,
        "affected_rows_with_dong_mapping": affected_rows_with_dong_mapping,
    }
    level = "warning" if affected_rows else "info"
    summary = log_event(
        "masked_address_dong_mapping_skipped",
        where=MASKED_ADDRESS_TASK,
        level=level,
        table=qcurrent,
        **metrics,
    )
    if affected_rows:
        notify_quality_event(
            task=MASKED_ADDRESS_TASK,
            level="warning",
            title="마스킹 주소 동단위 매핑 스킵",
            description=(
                "주소에 '*'가 포함된 행은 원천 주소가 마스킹된 것으로 보고, silver에서 "
                "법정동/행정동 명칭과 코드를 산출하지 않습니다. 구/시군구 수준 파싱은 "
                "유지하며, 이 알림은 해당 품질 이슈가 전체 silver current 중 어느 정도인지 "
                "운영자가 바로 확인할 수 있게 집계합니다."
            ),
            metrics=metrics,
            context={"table": qcurrent},
        )
    log.info("masked address dong-skip summary: %s", metrics)
    return summary
