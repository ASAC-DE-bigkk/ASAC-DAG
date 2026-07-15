"""gold(집계 전용) 리포트 — 집계 테이블 현황(행수) Discord 전송(#70 레이어 재분류).

원형(entity/detail) 적재 리포트는 silver DAG(quality_tasks.report_silver_run — detail 섹션)로
이동. 이 모듈은 gold = 집계·인사이트 테이블의 빌드 결과만 보고한다.
"""
from __future__ import annotations

import logging

from commerce_core.run_report import _fmt_elapsed, _num

log = logging.getLogger(__name__)
_DOMAIN = "commerce"

# gold 집계 명단 **정본** — DAG select·유지보수·메타상한이 전부 이 목록을 소비한다(1곳 관리).
AGG_TABLES = (
    "gold_license_dong_summary",
    "gold_license_flow_daily",
    "gold_license_flow_monthly",
    "gold_license_flow_yearly",
    "gold_license_status_duration",
    "gold_env_facility_operation",
    "gold_license_lifespan",
    "gold_license_cohort_survival",
    "gold_license_seasonality",
    "gold_license_stock_age_band",
    "gold_license_gu_specialization",
    "gold_license_churn_yearly",
    "gold_license_data_quality",
    "gold_license_status_transition",
    "gold_license_dong_category_matrix",
    "gold_license_change_activity",
    "gold_detail_area_profile",
    "gold_license_multi_site",
)


def agg_counts() -> dict[str, int]:
    """집계 테이블 행수(Trino). 부재/실패 -1(미빌드)."""
    from bronze.warehouse import _connect, _qualified

    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    out: dict[str, int] = {}
    try:
        cur = conn.cursor()
        for t in AGG_TABLES:
            try:
                cur.execute(f"SELECT count(*) FROM {qschema}.{t}")  # security: allow-sql - 상수 식별자
                out[t] = int(cur.fetchone()[0])
            except Exception:  # noqa: BLE001 — 미빌드/실패면 -1 로 표기
                out[t] = -1
    finally:
        conn.close()
    return out


def send_gold_report(*, elapsed_seconds: float | None = None) -> dict:
    """집계 테이블 현황 → Discord. 반환: 요약 counts (행수 -1 = 미빌드/실패 → 실패색)."""
    from common.discord import COLOR_FAIL, COLOR_OK, send_embed

    counts = agg_counts()
    failed = [t for t, v in counts.items() if v < 0]
    body = "\n".join(
        f"　◦ `{t}` · " + (_num(v) + "행" if v >= 0 else "**미빌드/실패**")
        for t, v in counts.items())
    head = f"**집계(gold) 현황** — {len(counts)}객체"
    if elapsed_seconds is not None:
        head += f" · ⏱ {_fmt_elapsed(elapsed_seconds)}"
    icon, color = ("❌", COLOR_FAIL) if failed else ("✅", COLOR_OK)
    title = f"{icon} [commerce] commerce_load_gold (gold 집계) — {len(counts) - len(failed)}/{len(counts)} OK"
    try:
        send_embed(title, head + "\n" + body, color=color, domain=_DOMAIN)
    except Exception as exc:  # noqa: BLE001
        log.warning("[commerce] gold report 전송 실패(무시): %s", type(exc).__name__)
    out = {"status": "failed" if failed else "ok", "tables": counts}
    log.info("[commerce] gold report: %s", out)
    return out
