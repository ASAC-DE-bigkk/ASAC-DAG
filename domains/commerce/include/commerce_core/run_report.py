"""DAG 단위 완료 리포트 → 공통 Discord(common.discord, #161) 전송 (#218).

각 commerce DAG(collect/recollect · bronze 적재 · silver 변환)의 finalize 에서 **API(short) 단위**
결과를 모아 한 임베드로 보낸다. 핵심 지표는 **신규 건수(정렬 파일 diff = increment_count / bronze
rows_loaded)** — 전체 API 호출량이 아니라 실제 신규·변경분 위주. 전체 수집량은 병기한다.

구성(§ 요구 반영):
- 대분류(category, 한글) 롤업 표 — **scope 전 API 를 빠짐없이 집계**(합계 행으로 완전성 보증).
- **API 단위 신규 건수** 표(신규순) — 실질 수집분 지표.
- 실패(에러)/경고(부분)/미수집(결과없음) 개별 목록.
- Discord 는 비례폰트라 이름 길이가 다르면 열이 밀린다 → **코드블록(monospace) + CJK 폭 패딩**으로 정렬.

메시지 내용만 도메인 소유, 전송·webhook·redaction 은 common.discord. webhook 미설정=조용히 스킵.

results 각 원소(스테이지 caller 가 정규화): short, status(ok|partial|failed|missing),
new(신규건수), total(전체건수), error.
"""
from __future__ import annotations

import logging
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from common.discord import COLOR_FAIL, COLOR_OK, COLOR_WARN, send_embed
from commerce_core import registry
from security import redact

log = logging.getLogger(__name__)
_KST = timezone(timedelta(hours=9))
_DOMAIN = "commerce"
_MAX_DESC = 3900   # send_embed 4096 한도 밑에서 관리(코드블록 fence/여유 포함)

CATEGORY_KO: dict[str, str] = {
    "food": "식품", "livestock": "축산", "health_medical": "의료", "pharmacy": "약국",
    "animal": "동물", "hygiene_beauty": "위생·미용", "optical_dental": "안경·치과",
    "lodging": "숙박", "culture": "문화", "industry": "산업", "environment": "환경",
}
STAGE_KO: dict[str, str] = {"collect": "수집", "recollect": "재수집",
                            "bronze_load": "bronze 적재", "silver": "silver 변환"}
_LEVELS = ("ok", "warn", "fail", "miss")


def _cat_ko(category: str | None) -> str:
    return CATEGORY_KO.get(category or "", category or "기타")


def _level(status: str | None) -> str:
    if status == "ok":
        return "ok"
    if status == "partial":
        return "warn"
    if status == "missing":
        return "miss"
    return "fail"


def _new(s: dict) -> int:
    return int(s.get("new") or s.get("increment_count") or s.get("rows_loaded") or 0)


def _tot(s: dict) -> int:
    return int(s.get("total") or s.get("rows_total") or s.get("rows_expected")
               or s.get("list_total_count") or 0)


# ── 표 정렬(monospace): CJK/이모지는 2칸 폭 ──────────────────────────────────────
def _dw(s: str) -> int:
    return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in str(s))


def _pad(s: str, width: int, right: bool = False) -> str:
    gap = max(0, width - _dw(s))
    return (" " * gap + str(s)) if right else (str(s) + " " * gap)


def _num(n: int) -> str:
    return f"{int(n):,}"


def build_run_report(*, dag_id: str, run_id: str, observed_date: str, stage: str,
                     results: list[dict], scope_shorts: list[str] | None = None,
                     count_label: str = "신규", show_total: bool = True) -> dict:
    """results(API별) → Discord 임베드 필드 + counts. scope_shorts 로 미수집(결과없음)까지 집계."""
    by_short = {d.short: d for d in registry.all_datasets()}
    results = [dict(s) for s in (results or []) if s]

    # scope 전체 대비 결과 없는 API = 미수집(missing) 로 채워 완전 집계(§ 누락 방지)
    if scope_shorts:
        seen = {s.get("short") for s in results}
        for sh in scope_shorts:
            if sh not in seen:
                results.append({"short": sh, "status": "missing", "new": 0, "total": 0})

    def cat_of(s: dict) -> str:
        d = by_short.get(s.get("short"))
        return _cat_ko(d.category if d else None)

    lv = {s["short"] if s.get("short") else id(s): _level(s.get("status")) for s in results}
    buckets = {L: [s for s in results if _level(s.get("status")) == L] for L in _LEVELS}
    n = {L: len(buckets[L]) for L in _LEVELS}
    total = len(results)
    new_sum = sum(_new(s) for s in results)
    tot_sum = sum(_tot(s) for s in results)
    stage_ko = STAGE_KO.get(stage, stage)

    color = COLOR_FAIL if n["fail"] else (COLOR_WARN if (n["warn"] or n["miss"]) else COLOR_OK)
    icon = "❌" if n["fail"] else ("⚠️" if (n["warn"] or n["miss"]) else "✅")
    title = (f"{icon} [commerce] {dag_id} ({stage_ko}) — {total}종 · "
             f"✅{n['ok']} ⚠️{n['warn']} ❌{n['fail']}" + (f" ⛔{n['miss']}" if n["miss"] else ""))

    head = f"**{count_label} {_num(new_sum)}건**"
    if show_total:
        head += f" · 전체수집 {_num(tot_sum)}건"
    head += f" · 대상 {total}종"
    lines = [head]

    # ── 대분류(category)별 롤업 표 — scope 전체 집계 + 합계 행 ──
    agg: dict[str, dict] = defaultdict(lambda: {"ok": 0, "warn": 0, "fail": 0, "miss": 0, "new": 0})
    for s in results:
        a = agg[cat_of(s)]
        a[_level(s.get("status"))] += 1
        a["new"] += _new(s)
    cats = sorted(agg)
    nw = max([_dw("대분류")] + [_dw(c) for c in cats])
    vw = max(6, len(_num(new_sum)))
    hdr = f"{_pad('대분류', nw)}  성공 경고 실패 미수집  {_pad(count_label, vw, right=True)}"
    tbl = [hdr]
    for c in cats:
        a = agg[c]
        tbl.append(f"{_pad(c, nw)}  {a['ok']:>4} {a['warn']:>4} {a['fail']:>4} {a['miss']:>6}  "
                   f"{_pad(_num(a['new']), vw, right=True)}")
    tbl.append(f"{_pad('합계', nw)}  {n['ok']:>4} {n['warn']:>4} {n['fail']:>4} {n['miss']:>6}  "
               f"{_pad(_num(new_sum), vw, right=True)}")
    lines.append("**대분류별**\n```\n" + "\n".join(tbl) + "\n```")

    # ── 실패/경고/미수집 개별 목록(문제 우선, 전량) ──
    if buckets["fail"]:
        det = []
        for s in buckets["fail"][:15]:
            e = redact(str(s.get("error") or "")).splitlines()[0][:90] if s.get("error") else "failed"
            det.append(f"- `{s.get('short')}` ({cat_of(s)}) — {e}")
        if n["fail"] > 15:
            det.append(f"- … 외 {n['fail'] - 15}건")
        lines.append("**❌ 실패(에러)**\n" + "\n".join(det))
    if buckets["warn"]:
        det = [f"- `{s.get('short')}` ({cat_of(s)}) — {_num(_new(s))}/{_num(_tot(s))}"
               for s in buckets["warn"][:15]]
        if n["warn"] > 15:
            det.append(f"- … 외 {n['warn'] - 15}건")
        lines.append("**⚠️ 경고(부분)**\n" + "\n".join(det))
    if buckets["miss"]:
        ms = [f"`{s.get('short')}`" for s in buckets["miss"][:25]]
        tail = f" … 외 {n['miss'] - 25}" if n["miss"] > 25 else ""
        lines.append(f"**⛔ 미수집(결과없음) {n['miss']}종**\n" + " ".join(ms) + tail)

    # ── API 단위 신규 건수 표(신규순) — 남는 예산만큼 채우고 나머지는 요약 ──
    ranked = sorted(results, key=lambda s: _new(s), reverse=True)
    aw = min(24, max([_dw("API")] + [_dw(str(s.get("short"))) for s in ranked[:60]] or [3]))
    api_hdr = f"{_pad('API', aw)}  {_pad(count_label, vw, right=True)}" + ("      전체" if show_total else "")
    fixed = "\n".join(lines)
    rows_out, shown, shown_new = [], 0, 0
    for s in ranked:
        if _new(s) <= 0 and _level(s.get("status")) == "ok":
            continue   # 신규 0(정상)은 개별 표기 생략 — 롤업/합계에는 이미 포함
        line = f"{_pad(str(s.get('short'))[:aw], aw)}  {_pad(_num(_new(s)), vw, right=True)}"
        if show_total:
            line += f"  {_pad(_num(_tot(s)), 8, right=True)}"
        # 예산 관리: 고정부 + API표가 한도 넘으면 중단
        if _dw(fixed) + len("\n**API별 " + count_label + "**\n```\n" + api_hdr + "\n```")\
           + sum(len(x) + 1 for x in rows_out) + len(line) > _MAX_DESC:
            break
        rows_out.append(line)
        shown += 1
        shown_new += _new(s)
    if rows_out:
        api_block = f"**API별 {count_label}(신규순)**\n```\n" + api_hdr + "\n" + "\n".join(rows_out) + "\n```"
        remain = total - shown
        if remain > 0:
            api_block += f"\n…외 {remain}종(표시분 {count_label} {_num(shown_new)} / 전체 {_num(new_sum)})"
        lines.append(api_block)

    occurred = datetime.now(_KST).strftime("%Y-%m-%d %H:%M:%S KST")
    footer = f"run_id={run_id} · observed_date={observed_date} · {occurred}"
    return {"title": title, "description": "\n".join(lines), "footer": footer, "color": color,
            "counts": {"total": total, "ok": n["ok"], "warning": n["warn"], "error": n["fail"],
                       "missing": n["miss"], "new": new_sum, "total_rows": tot_sum}}


def send_run_report(*, dag_id: str, run_id: str, observed_date: str, stage: str,
                    results: list[dict], scope_shorts: list[str] | None = None,
                    count_label: str = "신규", show_total: bool = True) -> dict:
    """리포트 빌드 + common.discord 전송(best-effort). 반환: counts(로그/테스트용)."""
    rep = build_run_report(dag_id=dag_id, run_id=run_id, observed_date=observed_date, stage=stage,
                           results=results, scope_shorts=scope_shorts,
                           count_label=count_label, show_total=show_total)
    try:
        send_embed(rep["title"], rep["description"], color=rep["color"],
                   footer=rep["footer"], domain=_DOMAIN)
    except Exception as exc:  # noqa: BLE001 — 알림 실패가 DAG 상태를 오염시키지 않게
        log.warning("[commerce] run report 전송 실패(무시): %s", type(exc).__name__)
    log.info("[commerce] run report(%s): %s", stage, rep["counts"])
    return rep["counts"]
