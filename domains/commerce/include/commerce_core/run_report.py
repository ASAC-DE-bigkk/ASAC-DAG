"""DAG 단위 완료 리포트 → 공통 Discord(common.discord, #161) 전송 (#218).

각 commerce DAG(collect/recollect · bronze 적재 · silver 변환)의 finalize 에서 **API(short) 단위**
결과를 모아 한 임베드로 보낸다: 전체 몇 종 중 성공/경고/실패 · 알림 종류(에러/경고) 세분화 ·
**API category(한글) 성공/실패**(0건 수집도 표기). 메시지 내용만 도메인 소유이고, 전송·webhook·
redaction 은 common.discord 담당.

webhook 폴백: `COMMERCE_DISCORD_WEBHOOK_URL` → `DISCORD_WEBHOOK_URL`(common.discord.resolve_webhook).
미설정이면 조용히 스킵(best-effort). 인증키/URL 은 로그·메시지에 남기지 않는다(§2.5).

results 각 원소(스테이지별 키가 달라도 흡수): short(=API), status(ok|partial|failed),
rows(rows_total|rows|rows_loaded), total(list_total_count|total), error.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from common.discord import COLOR_FAIL, COLOR_OK, COLOR_WARN, send_embed
from commerce_core import registry
from security import redact

log = logging.getLogger(__name__)
_KST = timezone(timedelta(hours=9))
_DOMAIN = "commerce"

# 대분류(category) → 한글. 미등록 category 는 원문 그대로.
CATEGORY_KO: dict[str, str] = {
    "food": "식품", "livestock": "축산", "health_medical": "의료", "pharmacy": "약국",
    "animal": "동물", "hygiene_beauty": "위생·미용", "optical_dental": "안경·치과",
    "lodging": "숙박", "culture": "문화", "industry": "산업", "environment": "환경",
}
# 스테이지(DAG 동작) → 한글 라벨
STAGE_KO: dict[str, str] = {"collect": "수집", "recollect": "재수집",
                            "bronze_load": "bronze 적재", "silver": "silver 변환"}


def _cat_ko(category: str | None) -> str:
    return CATEGORY_KO.get(category or "", category or "기타")


def _level(status: str | None) -> str:
    """상태 → 알림 종류. ok=성공 · partial=경고 · 그 외(failed 등)=에러."""
    return "ok" if status == "ok" else ("warn" if status == "partial" else "fail")


def _rows(s: dict) -> int:
    return int(s.get("rows_total") or s.get("rows") or s.get("rows_loaded") or 0)


def _total(s: dict) -> int:
    return int(s.get("list_total_count") or s.get("total") or 0)


def build_run_report(*, dag_id: str, run_id: str, observed_date: str,
                     stage: str, results: list[dict]) -> dict:
    """results(API별 결과) → Discord 임베드 필드(title/description/footer/color) + counts."""
    by_short = {d.short: d for d in registry.all_datasets()}

    def cat_of(s: dict) -> str:
        d = by_short.get(s.get("short"))
        return _cat_ko(d.category if d else None)

    ok = [s for s in results if _level(s.get("status")) == "ok"]
    warn = [s for s in results if _level(s.get("status")) == "warn"]
    fail = [s for s in results if _level(s.get("status")) == "fail"]
    total = len(results)
    rows = sum(_rows(s) for s in results)
    stage_ko = STAGE_KO.get(stage, stage)

    color = COLOR_FAIL if fail else (COLOR_WARN if warn else COLOR_OK)
    icon = "❌" if fail else ("⚠️" if warn else "✅")
    title = f"{icon} [commerce] {dag_id} ({stage_ko}) — {total}종 · 성공 {len(ok)}·경고 {len(warn)}·실패 {len(fail)}"

    lines = [f"**{stage_ko} · 전체 {total}종** · ✅성공 {len(ok)} · ⚠️경고 {len(warn)} · ❌실패 {len(fail)} · 총 {rows:,}행"]

    if fail:   # 에러 상세(개별 API)
        lines.append("\n**❌ 실패(에러)**")
        for s in fail[:12]:
            err = redact(str(s.get("error") or "")).splitlines()[0][:110] if s.get("error") else "failed"
            lines.append(f"- `{s.get('short')}` ({cat_of(s)}) — {err}")
        if len(fail) > 12:
            lines.append(f"- … 외 {len(fail) - 12}건")
    if warn:   # 경고 상세(부분 수집)
        lines.append("\n**⚠️ 경고(부분)**")
        for s in warn[:12]:
            lines.append(f"- `{s.get('short')}` ({cat_of(s)}) — {_rows(s):,}/{_total(s):,}행")
        if len(warn) > 12:
            lines.append(f"- … 외 {len(warn) - 12}건")
    empty = [s for s in ok if not _rows(s)]   # 성공이나 0건 수집도 표기
    if empty:
        lines.append("\n**ℹ️ 0건(성공)**: " + ", ".join(f"`{s.get('short')}`" for s in empty[:20])
                     + (f" 외 {len(empty) - 20}" if len(empty) > 20 else ""))

    # category(한글)별 성공/경고/실패 — 이 run 에 등장한 category 전부
    agg: dict[str, dict] = defaultdict(lambda: {"ok": 0, "warn": 0, "fail": 0, "rows": 0})
    for s in results:
        a = agg[cat_of(s)]
        a[_level(s.get("status"))] += 1
        a["rows"] += _rows(s)
    lines.append("\n**API category별(한글) 성공/경고/실패**")
    for cat in sorted(agg):
        a = agg[cat]
        lines.append(f"- {cat}: ✅{a['ok']} ⚠️{a['warn']} ❌{a['fail']} · {a['rows']:,}행")

    occurred = datetime.now(_KST).strftime("%Y-%m-%d %H:%M:%S KST")
    footer = f"run_id={run_id} · observed_date={observed_date} · {occurred}"
    return {"title": title, "description": "\n".join(lines), "footer": footer, "color": color,
            "counts": {"total": total, "ok": len(ok), "warning": len(warn),
                       "error": len(fail), "rows": rows, "empty": len(empty)}}


def send_run_report(*, dag_id: str, run_id: str, observed_date: str,
                    stage: str, results: list[dict]) -> dict:
    """리포트 빌드 + common.discord 전송(best-effort). 반환: counts(로그/테스트용)."""
    rep = build_run_report(dag_id=dag_id, run_id=run_id, observed_date=observed_date,
                           stage=stage, results=results)
    try:
        send_embed(rep["title"], rep["description"], color=rep["color"],
                   footer=rep["footer"], domain=_DOMAIN)
    except Exception as exc:  # noqa: BLE001 — 알림 실패가 DAG 상태를 오염시키지 않게
        log.warning("[commerce] run report 전송 실패(무시): %s", type(exc).__name__)
    log.info("[commerce] run report(%s): %s", stage, rep["counts"])
    return rep["counts"]
