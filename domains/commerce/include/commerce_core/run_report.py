"""DAG 단위 완료 리포트 → 공통 Discord(common.discord, #161) 전송 (#218).

각 commerce DAG(collect/recollect · bronze 적재 · silver 변환)의 finalize 에서 API 결과를 모아 한
임베드로 보낸다. 핵심 지표는 **신규 건수(정렬 파일 diff = increment_count / bronze rows_loaded)**
— 전체 API 호출량이 아니라 실제 신규·변경분 위주. 전체 수집량은 병기.

분류 3단(레지스트리 category/sub_category 에서 파생):
- **대분류**: 보건(문화·산업·환경 외 전부) / 문화 / 산업 / 환경.
- **중분류**: 보건 하위=category(식품·축산·의료…), 문화·산업·환경 하위=sub_category(게임·판매·폐기물…).
- **소분류**: API(short). 실수집(신규>0)만 상세 표기, 신규 0(정상)은 '{대분류} 하위 N개 변경내역 없음'.

정렬: Discord monospace 의 CJK 글리프 폭이 ASCII 2배가 아니어서(폰트별 상이) 한글을 열 사이에 넣으면
반드시 밀린다 → **정렬 격자는 ASCII(숫자)만, 한글 이름은 항상 줄 끝**. 소분류 표기는 `한글(영문)`.

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

# 대분류(major) = 문화/산업/환경 외 전부 보건
_OWN_MAJOR = {"culture", "industry", "environment"}
MAJOR_KO = {"health": "보건", "culture": "문화", "industry": "산업", "environment": "환경",
            "etc": "기타"}
# 중분류(mid) 한글 — 보건 하위(category)
CATEGORY_KO = {
    "food": "식품", "livestock": "축산", "health_medical": "의료", "pharmacy": "약국",
    "animal": "동물", "hygiene_beauty": "위생·미용", "optical_dental": "안경·치과", "lodging": "숙박",
}
# 중분류(mid) 한글 — 문화/산업/환경 하위(sub_category)
SUB_KO = {
    # 문화
    "game": "게임제공업", "film_video": "영화·비디오", "performance": "공연", "tourism": "관광",
    "amusement": "유원시설", "culture_arts": "문화예술", "music": "음악", "camping": "야영장",
    "travel": "여행업", "publishing": "출판·인쇄", "sports": "체육시설",
    # 산업
    "sales": "판매업", "timber": "목재", "meter": "계량기", "gas": "가스", "petroleum": "석유",
    "groundwater": "지하수", "emission": "대기배출", "tobacco": "담배", "funeral": "장사",
    "education": "평생교육", "job_agency": "직업소개",
    # 환경
    "manure": "가축분뇨", "sanitation": "위생용품", "waste": "폐기물", "sewage": "물재생·정화",
    "pollution": "수질오염", "env_service": "환경관리업",
}
STAGE_KO = {"collect": "수집", "recollect": "재수집", "bronze_load": "bronze 적재", "silver": "silver 변환"}
_LEVELS = ("ok", "warn", "fail", "miss")

# 분류 단계 표기 — 큰 이모지 대신 작은 텍스트 마커 + 들여쓰기로 뎁스를 식별.
# 대분류 = **볼드**(마커 없음) · 중분류 = • · 소분류 = ◦
MARK_MID, MARK_API = "•", "◦"
_INDENT = "　"   # 전각 공백(들여쓰기, Discord 보존)


def _level(status: str | None) -> str:
    if status == "ok":
        return "ok"
    if status == "partial":
        return "warn"
    if status == "missing":
        return "miss"
    return "fail"


def _major(category: str | None) -> str:
    if not category:
        return "etc"
    return category if category in _OWN_MAJOR else "health"


def _major_lab(mj: str) -> str:
    return f"{MAJOR_KO.get(mj, mj)}({mj})"


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
    """results(API별) → Discord 임베드(대분류>중분류>소분류). scope_shorts 로 미수집까지 집계."""
    by_short = {d.short: d for d in registry.all_datasets()}
    results = [dict(s) for s in (results or []) if s]
    if scope_shorts:
        seen = {s.get("short") for s in results}
        for sh in scope_shorts:
            if sh not in seen:
                results.append({"short": sh, "status": "missing", "new": 0, "total": 0})

    def dset(s):
        return by_short.get(s.get("short"))

    def major_of(s):
        d = dset(s)
        return _major(d.category) if d else "etc"

    def mid_key(s):
        d = dset(s)
        if not d:
            return "etc"
        return d.category if _major(d.category) == "health" else (d.sub_category or "etc")

    def mid_lab(s):
        d = dset(s)
        if not d:
            return "기타(etc)"
        if _major(d.category) == "health":
            return f"{CATEGORY_KO.get(d.category, d.category)}({d.category})"
        k = d.sub_category or "etc"
        return f"{SUB_KO.get(k, k)}({k})"

    def api_lab(s):
        d = dset(s)
        return f"{d.name_ko}({s.get('short')})" if d and d.name_ko else str(s.get("short"))

    buckets = {L: [s for s in results if _level(s.get("status")) == L] for L in _LEVELS}
    n = {L: len(buckets[L]) for L in _LEVELS}
    total = len(results)
    new_sum = sum(_new(s) for s in results)
    tot_sum = sum(_tot(s) for s in results)

    color = COLOR_FAIL if n["fail"] else (COLOR_WARN if (n["warn"] or n["miss"]) else COLOR_OK)
    icon = "❌" if n["fail"] else ("⚠️" if (n["warn"] or n["miss"]) else "✅")
    title = (f"{icon} [commerce] {dag_id} ({STAGE_KO.get(stage, stage)}) — {total}종 · "
             f"✅{n['ok']} ⚠️{n['warn']} ❌{n['fail']}" + (f" ⛔{n['miss']}" if n["miss"] else ""))
    head = f"**{count_label} {_num(new_sum)}건**" + (f" · 전체수집 {_num(tot_sum)}건" if show_total else "")
    head += f" · 대상 {total}종"
    lines = [head]

    # ── 대분류 집계 + 통계 표(ASCII 격자, 한글 이름은 줄 끝) ──
    magg = defaultdict(lambda: {"ok": 0, "warn": 0, "fail": 0, "miss": 0, "new": 0})
    for s in results:
        a = magg[major_of(s)]
        a[_level(s.get("status"))] += 1
        a["new"] += _new(s)
    majors_by_new = sorted(magg, key=lambda m: magg[m]["new"], reverse=True)

    cw = max(2, len(str(max([1, n["ok"], n["warn"], n["fail"], n["miss"]]))))
    vw = max(len("NEW"), len(_num(new_sum)))

    def row(ok, wn, fl, ms, new, lab):
        return f"{ok:>{cw}} {wn:>{cw}} {fl:>{cw}} {ms:>{cw}} {_num(new):>{vw}}  {lab}"

    tbl = [f"{'OK':>{cw}} {'WN':>{cw}} {'FL':>{cw}} {'MS':>{cw}} {'NEW':>{vw}}  대분류(major)"]
    for mj in majors_by_new:
        a = magg[mj]
        tbl.append(row(a["ok"], a["warn"], a["fail"], a["miss"], a["new"], _major_lab(mj)))
    tbl.append(row(n["ok"], n["warn"], n["fail"], n["miss"], new_sum, "합계(total)"))
    lines.append("**대분류별 통계** — OK성공·WN경고·FL실패·MS미수집·NEW신규\n```\n"
                 + "\n".join(tbl) + "\n```")

    # ── 섹션을 상태 그룹으로 모아 그룹 사이에 빈 줄 간격(순서: 에러 › 경고 › 성공) ──
    overview = list(lines)  # head + 대분류 통계 표
    err_sec, warn_sec, succ_sec = [], [], []

    # (에러) 실패 — 어떤 DAG task 였는지 @task 로 명시
    if buckets["fail"]:
        det = []
        for s in buckets["fail"][:12]:
            e = redact(str(s.get("error") or "")).splitlines()[0][:80] if s.get("error") else "failed"
            tk = f" `@{s.get('task')}`" if s.get("task") else ""
            det.append(f"- {api_lab(s)}{tk} — {e}")
        if n["fail"] > 12:
            det.append(f"- …외 {n['fail'] - 12}건")
        err_sec.append("**❌ 실패(에러)**\n" + "\n".join(det))

    # (경고) 부분 수집 + 미수집
    if buckets["warn"]:
        det = [f"- {api_lab(s)} — {_num(_new(s))}/{_num(_tot(s))}" for s in buckets["warn"][:12]]
        if n["warn"] > 12:
            det.append(f"- …외 {n['warn'] - 12}건")
        warn_sec.append("**⚠️ 경고(부분)**\n" + "\n".join(det))
    if buckets["miss"]:
        ms = ", ".join(api_lab(s) for s in buckets["miss"][:20])
        if n["miss"] > 20:
            ms += f" …외 {n['miss'] - 20}"
        warn_sec.append(f"**⛔ 미수집(결과없음) {n['miss']}종**\n{ms}")

    # (성공) 변경내역 없음 요약 + API 신규 상세 — 대분류=볼드 · 중분류=• · 소분류=◦, 들여쓰기로 뎁스
    zero = defaultdict(int)
    for s in results:
        if _new(s) == 0 and _level(s.get("status")) == "ok":
            zero[major_of(s)] += 1
    zero_line = ""
    if zero:
        parts = [f"{MAJOR_KO.get(m, m)} 하위 {c}개" for m, c in sorted(zero.items(), key=lambda x: -x[1])]
        zero_line = "**변경내역 없음(신규 0)**: " + " · ".join(parts)

    pos = [s for s in results if _new(s) > 0]
    if pos:
        fixed = len("\n".join(overview + err_sec + warn_sec)) + len(zero_line)
        budget = _MAX_DESC - fixed - 120
        out, shown, used = [], 0, 0
        for mj in majors_by_new:
            mj_pos = [s for s in pos if major_of(s) == mj]
            if not mj_pos:
                continue
            mids = defaultdict(list)
            for s in mj_pos:
                mids[mid_key(s)].append(s)
            seg = [f"**{_major_lab(mj)}** · {count_label} {_num(magg[mj]['new'])}"]
            for mk in sorted(mids, key=lambda k: sum(_new(x) for x in mids[k]), reverse=True):
                arr = sorted(mids[mk], key=_new, reverse=True)
                seg.append(f"{_INDENT}{MARK_MID} {mid_lab(arr[0])} · {count_label} {_num(sum(_new(x) for x in arr))}")
                seg += [f"{_INDENT}{_INDENT}{MARK_API} {api_lab(s)} · {_num(_new(s))}" for s in arr]
            seg_text = "\n".join(seg)
            if used + len(seg_text) + 1 > budget:
                break
            out.append(seg_text)
            used += len(seg_text) + 1
            shown += len(mj_pos)
        if out:
            head_ln = f"**✅ API별 신규** (볼드=대분류 · {MARK_MID} 중분류 · {MARK_API} 소분류)"
            tail = f"\n…외 {len(pos) - shown}종 생략" if len(pos) - shown > 0 else ""
            succ_sec.append(head_ln + "\n" + "\n".join(out) + tail)
    if zero_line:
        succ_sec.append(zero_line)

    # 그룹 조립: 비어있지 않은 그룹만, 그룹 사이 빈 줄(\n\n)로 간격 → 에러/경고/성공 시각 분리
    groups = [g for g in (overview, err_sec, warn_sec, succ_sec) if g]
    description = "\n\n".join("\n".join(g) for g in groups)

    occurred = datetime.now(_KST).strftime("%Y-%m-%d %H:%M:%S KST")
    footer = f"run_id={run_id} · observed_date={observed_date} · {occurred}"
    return {"title": title, "description": description, "footer": footer, "color": color,
            "counts": {"total": total, "ok": n["ok"], "warning": n["warn"], "error": n["fail"],
                       "missing": n["miss"], "new": new_sum, "total_rows": tot_sum,
                       "majors": {m: magg[m]["new"] for m in majors_by_new}}}


def send_run_report(*, dag_id: str, run_id: str, observed_date: str, stage: str,
                    results: list[dict], scope_shorts: list[str] | None = None,
                    count_label: str = "신규", show_total: bool = True,
                    extra_sections: list[str] | None = None) -> dict:
    """리포트 빌드 + common.discord 전송(best-effort). 반환: counts(로그/테스트용).

    extra_sections: DAG 리포트에 흡수할 task 단위 섹션(예: 유지보수). '메시지 1건 = DAG 1회, task
    산출물은 섹션'(리포트 묶기 원칙). 섹션이 있으면 본문 tail 을 줄여 섹션이 항상 보이게 한다.
    """
    rep = build_run_report(dag_id=dag_id, run_id=run_id, observed_date=observed_date, stage=stage,
                           results=results, scope_shorts=scope_shorts,
                           count_label=count_label, show_total=show_total)
    desc = rep["description"]
    extra = [s for s in (extra_sections or []) if s]
    if extra:
        tail = "\n\n" + "\n\n".join(extra)
        room = 4000 - len(tail)
        if len(desc) > room:
            desc = desc[:room].rstrip() + " …"
        desc += tail
    try:
        send_embed(rep["title"], desc, color=rep["color"], footer=rep["footer"], domain=_DOMAIN)
    except Exception as exc:  # noqa: BLE001 — 알림 실패가 DAG 상태를 오염시키지 않게
        log.warning("[commerce] run report 전송 실패(무시): %s", type(exc).__name__)
    log.info("[commerce] run report(%s): %s", stage, rep["counts"])
    return rep["counts"]


# ── 유지보수(#226) 섹션 렌더 — (테이블,op) 단위 성공/실패 + 자원(elapsed/cpu/peak RAM). GPU=Trino 미사용 N/A ──
def _fmt_ms(ms) -> str:
    return f"{ms / 1000:.1f}s" if ms else "-"


def _fmt_bytes(b) -> str:
    b = float(b or 0)
    for u in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.0f}{u}"
        b /= 1024
    return f"{b:.0f}TB"


def maintenance_section(results: list[dict]) -> str:
    """maintenance.run_table_maintenance 결과 → DAG 리포트용 '🧹 유지보수' 섹션 문자열."""
    if not results:
        return ""
    by_table: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        by_table[r.get("table", "?")].append(r)
    n_ok = sum(1 for r in results if r.get("status") == "ok")
    n_fail = sum(1 for r in results if r.get("status") == "failed")
    icon = "❌" if n_fail else "✅"
    lines = [f"**🧹 유지보수 (Iceberg optimize/expire/orphan)** {icon} op 성공 {n_ok}·실패 {n_fail} "
             "(자원=Trino 쿼리 stats · GPU N/A)"]
    for t, rs in by_table.items():
        skip = next((r for r in rs if r.get("status") == "skipped"), None)
        if skip:
            lines.append(f"- `{t}` — skip({skip.get('reason', '')})")
            continue
        snap = next((r for r in rs if r.get("op") == "snapshots"), None)
        snaptxt = f" 스냅샷 {snap.get('before')}→{snap.get('after')}" if snap else ""
        parts = []
        for r in rs:
            if r.get("op") in ("snapshots", "-"):
                continue
            if r.get("status") == "ok":
                parts.append(f"{r['op']} ✅{_fmt_ms(r.get('elapsed_ms'))}/cpu{_fmt_ms(r.get('cpu_ms'))}"
                             f"/{_fmt_bytes(r.get('peak_mem_bytes'))}")
            else:
                parts.append(f"{r.get('op')} ❌")
        lines.append(f"- `{t}`{snaptxt} — " + " · ".join(parts))
    return "\n".join(lines)
