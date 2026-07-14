"""Silver data-quality summaries and warning notifications."""
from __future__ import annotations

import logging
from datetime import datetime
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


def _prev_silver_watermark() -> datetime | None:
    """직전 silver run 의 history max(collected_at) — gold 핸드셰이크 파일(silver_state).

    이 task 는 배선상 `mark_silver_done`(워터마크 전진) **이전** 단계라, 여기서 읽는 값은 아직
    '직전 run' 워터마크다 → `collected_at > 이 값` = **이번 run 에 신규 유입된 current 행**.
    (평상시 증분 = silver_license_current 의 affected 판정 `collected_at > max` 와 동일 계약.)
    파일 부재/파싱 실패 → None(그 경우 전량 집계로 폴백 — 신규 판정 불가 시 전체를 보고).
    """
    try:
        from commerce_core import silver_state
        from commerce_core.settings import get_settings
        from commerce_core.storage import get_storage

        doc = silver_state.read_watermark(get_storage(), get_settings().storage_prefix)
        raw = (doc or {}).get("max_collected_at")
        return datetime.fromisoformat(raw) if raw else None
    except Exception as exc:  # noqa: BLE001 — 워터마크 문제로 품질 집계를 막지 않는다
        log.warning("silver 워터마크 읽기 실패(전량 집계로 폴백): %s", type(exc).__name__)
        return None


def notify_masked_address_dong_skip_summary() -> dict:
    """Warn when masked addresses were skipped for dong-level mapping — **이번 run 신규분만**.

    기존엔 `silver_license_current` **전량**(기적재 포함)을 매일 집계해, 신규 적재가 없어도 현재
    존재하는 마스킹 주소 전부를 반복 경고했다. 이제 직전 워터마크 이후(`collected_at >`) 신규 유입
    행으로 **스코프**해, "현재 존재하는 데이터"가 아니라 "이번에 새로 들어온 데이터"의 품질만 알린다.
    신규 유입이 없으면 settled=0 → level=info(외부 알림 없음). 워터마크 미상(첫 배포 등)이면 전량 집계.

    쿼리는 집계 1행만 반환 — silver 레코드를 Airflow 메모리에 적재하지 않는다.
    """
    prev_hi = _prev_silver_watermark()
    catalog, schema, qschema = _qualified()
    qcurrent = f"{qschema}.{CURRENT_TABLE}"
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(  # security: allow-sql - qcurrent is built from _qualified() identifiers; value bound.
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
            where collected_at > coalesce(cast(? as timestamp(6)), timestamp '1970-01-01 00:00:00')
            """, (prev_hi,))
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
        "scope": "new_since_last_run",
        "since_collected_at": prev_hi.isoformat() if prev_hi else None,
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
            title="마스킹 주소 동단위 매핑 스킵(신규 적재분)",
            description=(
                "주소에 '*'가 포함된 행은 원천 주소가 마스킹된 것으로 보고, silver에서 "
                "법정동/행정동 명칭과 코드를 산출하지 않습니다. 구/시군구 수준 파싱은 "
                "유지하며, 이 알림은 **이번 실행에 신규 유입된 current 행**(직전 처리 이후 "
                "collected_at) 중 해당 품질 이슈가 어느 정도인지 집계합니다(기적재분 제외)."
            ),
            metrics=metrics,
            context={"table": qcurrent, "since_collected_at": metrics["since_collected_at"]},
        )
    log.info("masked address dong-skip summary (new since %s): %s", prev_hi, metrics)
    return summary


def report_silver_run(elapsed_seconds: float | None = None,
                      marked: dict | None = None) -> dict:
    """silver current 를 데이터셋(API)별로 집계해 DAG 완료 리포트 전송(#218, stage=silver).

    silver 는 dbt 로 전 데이터셋을 한 번에 변환하므로(태스크 단위 API 구분 없음) 변환 결과인
    `silver_license_current` 를 **데이터셋(=short=API)별 현재 행수**로 집계해 리포트 내부를 API
    단위로 채운다. dbt run/test 실패 등으로 조회가 불가하면 DAG 단위 실패로 리포트한다(best-effort).

    marked: mark_silver_done XCom(inserted/inserted_no_rows) — 이번 실행이 **새로 처리한 run**
    규모를 리포트에 표기한다. 0이면 "신규 없음(기적재만·변경 없음)" — '현재 행수'만으로는
    신규 0건 여부가 드러나지 않는 문제의 보완(0건 가시성).
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
        results = [{"short": "silver", "status": "failed", "task": "dbt_run_silver·dbt_test_silver",
                    "error": "silver current 집계 실패(dbt run/test 결과 확인)"}]
    extra = None
    if marked is not None:
        n_new = int(marked.get("inserted") or 0)
        n_zero = int(marked.get("inserted_no_rows") or 0)
        if n_new <= 0 and n_zero <= 0:
            extra = ["**이번 실행 신규 처리 run 0건** — 기적재만(변경 없음), 재적재 없음"]
        else:
            extra = [f"**이번 실행 신규 처리 run 마킹** {max(n_new, 0)}건"
                     + (f" · 0행(dedup) run {n_zero}건" if n_zero > 0 else "")]
    counts = run_report.send_run_report(
        dag_id="commerce_load_silver", run_id=observed, observed_date=observed,
        stage="silver", results=results, count_label="현재", show_total=False,
        elapsed_seconds=elapsed_seconds, extra_sections=extra)
    log.info("silver run report: %s", counts)
    return counts
