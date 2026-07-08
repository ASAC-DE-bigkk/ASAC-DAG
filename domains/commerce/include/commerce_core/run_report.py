"""DAG 단위 완료 리포트 → 공통 Discord(common.discord, #161) 전송 (#218).

각 commerce DAG(collect/recollect · bronze 적재 · silver 변환)의 finalize 에서 **API(short) 단위**
결과를 모아 한 임베드로 보낸다. 핵심 지표는 **신규 건수(정렬 파일 diff = increment_count / bronze
rows_loaded)** — 전체 API 호출량이 아니라 실제 신규·변경분 위주. 전체 수집량은 병기한다.

구성:
- **대분류(category)별 통계 표** — scope 전 API 를 빠짐없이 집계(합계 행으로 완전성 보증).
- **API 단위 신규 건수** — 대분류로 묶고 `한글(영문)` 라벨, 신규순.
- 실패(에러)/경고(부분)/미수집(결과없음) 개별 목록.

정렬: Discord monospace 코드블록의 CJK 글리프 폭이 ASCII 2배가 아니어서(폰트마다 상이) 한글을
'열' 사이에 넣으면 반드시 밀린다. → **정렬 격자는 ASCII(숫자)만, 한글 이름은 항상 줄 끝(마지막
열)**에 둬 폰트와 무관하게 정렬되게 한다.

메시지 내용만 도메인 소유, 전송·webhook·redaction 은 common.discord. webhook 미설정=조용히 스킵.

results 각 원소(스테이지 caller 가 정규화): short, status(ok|partial|failed|missing),
new(신규건수), total(전체건수), error.
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
_MAX_DESC = 3900   # send_embed 4096 한도 밑에서 관리(코드블록 fence/여유 포함)

CATEGORY_KO: dict[str, str] = {
    "food": "식품", "livestock": "축산", "health_medical": "의료", "pharmacy": "약국",
    "animal": "동물", "hygiene_beauty": "위생·미용", "optical_dental": "안경·치과",
    "lodging": "숙박", "culture": "문화", "industry": "산업", "environment": "환경",
}
STAGE_KO: dict[str, str] = {"collect": "수집", "recollect": "재수집",
                            "bronze_load": "bronze 적재", "silver": "silver 변환"}
_LEVELS = ("ok", "warn", "fail", "miss")


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


def _num(n: int) -> str:
    return f"{int(n):,}"


def build_run_report(*, dag_id: str, run_id: str, observed_date: str, stage: str,
                     results: list[dict], scope_shorts: list[str] | None = None,
                     count_label: str = "신규", show_total: bool = True) -> dict:
    """results(API별) → Discord 임베드 필드 + counts. scope_shorts 로 미수집(결과없음)까지 집계."""
    by_short = {d.short: d for d in registry.all_datasets()}
    results = [dict(s) for s in (results or []) if s]

    # scope 전체 대비 결과 없는 API = 미수집(missing) 으로 채워 완전 집계(누락 방지)
    if scope_shorts:
        seen = {s.get("short") for s in results}
        for sh in scope_shorts:
            if sh not in seen:
                results.append({"short": sh, "status": "missing", "new": 0, "total": 0})

    def cat_en(s: dict) -> str:
        d = by_short.get(s.get("short"))
        return d.category if d and d.category else "기타"

    def cat_lab(en: str) -> str:          # 한글(영문)
        return f"{CATEGORY_KO.get(en, en)}({en})"

    def label(s: dict) -> str:            # API 한글(영문)
        d = by_short.get(s.get("short"))
        return f"{d.name_ko}({s.get('short')})" if d and d.name_ko else str(s.get("short"))

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

    # 대분류 집계
    agg: dict[str, dict] = defaultdict(lambda: {"ok": 0, "warn": 0, "fail": 0, "miss": 0, "new": 0})
    for s in results:
        a = agg[cat_en(s)]
        a[_level(s.get("status"))] += 1
        a["new"] += _new(s)
    cats_by_new = sorted(agg, key=lambda e: agg[e]["new"], reverse=True)

    # ── 대분류별 통계 표: 격자는 ASCII 숫자만, 한글 이름은 줄 끝(마지막 열) ──
    cw = max(2, len(str(max([1, n["ok"], n["warn"], n["fail"], n["miss"]]))))
    vw = max(len("NEW"), len(_num(new_sum)))

    def row(ok, wn, fl, ms, new, lab):
        return f"{ok:>{cw}} {wn:>{cw}} {fl:>{cw}} {ms:>{cw}} {_num(new):>{vw}}  {lab}"

    tbl = [f"{'OK':>{cw}} {'WN':>{cw}} {'FL':>{cw}} {'MS':>{cw}} {'NEW':>{vw}}  대분류(major)"]
    for en in cats_by_new:
        a = agg[en]
        tbl.append(row(a["ok"], a["warn"], a["fail"], a["miss"], a["new"], cat_lab(en)))
    tbl.append(row(n["ok"], n["warn"], n["fail"], n["miss"], new_sum, "합계(total)"))
    lines.append("**대분류별 통계** — OK성공·WN경고·FL실패·MS미수집·NEW신규\n```\n"
                 + "\n".join(tbl) + "\n```")

    # ── 문제(에러/경고/미수집) 개별 목록: 한글(영문) 라벨 ──
    if buckets["fail"]:
        det = []
        for s in buckets["fail"][:12]:
            e = redact(str(s.get("error") or "")).splitlines()[0][:80] if s.get("error") else "failed"
            det.append(f"- {label(s)} — {e}")
        if n["fail"] > 12:
            det.append(f"- …외 {n['fail'] - 12}건")
        lines.append("**❌ 실패(에러)**\n" + "\n".join(det))
    if buckets["warn"]:
        det = [f"- {label(s)} — {_num(_new(s))}/{_num(_tot(s))}" for s in buckets["warn"][:12]]
        if n["warn"] > 12:
            det.append(f"- …외 {n['warn'] - 12}건")
        lines.append("**⚠️ 경고(부분)**\n" + "\n".join(det))
    if buckets["miss"]:
        ms = ", ".join(label(s) for s in buckets["miss"][:20])
        if n["miss"] > 20:
            ms += f" …외 {n['miss'] - 20}"
        lines.append(f"**⛔ 미수집(결과없음) {n['miss']}종**\n{ms}")

    # ── API 단위 신규 건수: 대분류로 묶고 한글(영문), 신규순 (숫자 ASCII 정렬 + 이름 줄 끝) ──
    pos = [s for s in results if _new(s) > 0]
    if pos:
        nw = max(len(_num(_new(s))) for s in pos)
        budget = _MAX_DESC - len("\n".join(lines)) - 60
        body, shown, used = [], 0, 0
        for en in cats_by_new:
            apis = sorted([s for s in pos if cat_en(s) == en], key=_new, reverse=True)
            if not apis:
                continue
            seg = [f"[{cat_lab(en)}] {count_label} {_num(agg[en]['new'])}"]
            seg += [f"{_num(_new(s)):>{nw}}  {label(s)}" for s in apis]
            seg_text = "\n".join(seg)
            if used + len(seg_text) + 1 > budget:
                break
            body.append(seg_text)
            used += len(seg_text) + 1
            shown += len(apis)
        if body:
            blk = f"**API별 · 대분류 묶음({count_label}순)**\n```\n" + "\n".join(body) + "\n```"
            rem = len(pos) - shown
            if rem > 0:
                blk += f"\n…외 {rem}종 생략(전체 {count_label} {_num(new_sum)})"
            lines.append(blk)

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
