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


def report_silver_run() -> dict:
    """silver current 를 데이터셋(API)별로 집계해 DAG 완료 리포트 전송(#218, stage=silver).

    silver 는 dbt 로 전 데이터셋을 한 번에 변환하므로(태스크 단위 API 구분 없음) 변환 결과인
    `silver_license_current` 를 **데이터셋(=short=API)별 현재 행수**로 집계해 리포트 내부를 API
    단위로 채운다. dbt run/test 실패 등으로 조회가 불가하면 DAG 단위 실패로 리포트한다(best-effort).
    """
    from datetime import datetime, timedelta, timezone

    from commerce_core import run_report

    observed = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d")
    try:
        catalog, schema, qschema = _qualified()
        qcurrent = f"{qschema}.{CURRENT_TABLE}"
        conn = _connect(catalog, schema)
        try:
            cur = conn.cursor()
            cur.execute(  # security: allow-sql - qcurrent is built from _qualified() identifiers.
                f"select cast(dataset as varchar) as dataset, count(*) as n "
                f"from {qcurrent} group by 1")
            rows = cur.fetchall()
        finally:
            conn.close()
        # silver 는 SCD 누적 → API별 '현재' 행수(신규가 아니라 현재 상태 지표).
        results = [{"short": r[0], "status": "ok", "new": _num(r[1])} for r in rows]
    except Exception as exc:  # noqa: BLE001 — dbt 실패 등 조회 불가: DAG 단위 실패로 리포트
        log.warning("silver 리포트 집계 실패(%s) — 실패 리포트로 대체", type(exc).__name__)
        results = [{"short": "silver", "status": "failed",
                    "error": "silver current 집계 실패(dbt run/test 결과 확인)"}]
    counts = run_report.send_run_report(
        dag_id="commerce_load_silver", run_id=observed, observed_date=observed,
        stage="silver", results=results, count_label="현재", show_total=False)
    log.info("silver run report: %s", counts)
    return counts
