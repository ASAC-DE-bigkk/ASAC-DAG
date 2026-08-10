# common/serving/pattern_verify.py — export 시점 패턴 검증 스탬프 (Serving#217 후속)
#
# 문제: usage_patterns 를 저작해 게시하면 게이트웨이가 `verified_at` 없는 패턴을 runnable=false
#   로 막아 실행 시 409 를 준다("가져올 수 없는 항목"). verified_at 은 지금까지 별도 스크립트
#   (scripts/verify_usage_patterns.py --apply)가 dbt yml 에 손으로 찍는 단계였고, 저작 후 그 단계를
#   안 돌리면 초안이 계속 미검증으로 남았다.
#
# 조치: **각 도메인 gold→D1 export 라인**에서, 방금 게시한 D1 데이터에 패턴 SQL 을 실제로 돌려
#   통과한 패턴에 verified_at/verified_rows/verified_publication_id 를 **D1 에 직접 스탬프**한다.
#   - 미검증 패턴만 대상(yml 에 verified_at 이 이미 있으면 무접촉 — 손 검증 존중).
#   - 각 도메인 export 는 자기 제품만 다루므로 도메인 간 충돌이 없다(다중 컴퓨터 안전).
#   - 예시값(SQL 주석 `-- :n=10`)으로 바인딩해 실행. 예시 미해결·비-SELECT·실행 실패·0행(단
#     allow_empty 아님)이면 스탬프하지 않고 미검증으로 남긴다(안전망 유지).
#
# yml-소스 원칙과의 관계: yml verified_at(scripts/verify_usage_patterns.py --apply 로 커밋)이 여전히
#   정본이고, 이 스탬프는 그것이 **없는 초안**만 채운다. 나중에 --apply 를 돌리면 yml↔D1 이 수렴한다.

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Callable, Sequence

_PLACEHOLDER_RE = re.compile(r":([a-z][a-z0-9_]*)")


def executable_sql(sql_text: str) -> str:
    """`--` 라인 주석·`/* :y */` 인라인 주석 제거(후자는 값이 이미 상수로 박힌 형)."""
    stripped = re.sub(r"/\*.*?\*/", " ", sql_text or "")
    return "\n".join(re.sub(r"--.*$", "", line) for line in stripped.splitlines())


def resolve_params(sql_text: str, hint_text: str = "") -> tuple[str, dict[str, str], list[str]]:
    """(치환된 실행문, 사용값, 미해결 이름) — 예시값을 sql 본문 주석 → 힌트 순으로 찾아 치환.

    verify_usage_patterns.py 의 동명 함수와 같은 규약(주석 `-- :n=10`, `:gu='성동구'` 형)이다.
    못 찾은 파라미터가 하나라도 있으면 실행을 포기한다(추측값 없음).
    """
    executable = executable_sql(sql_text)
    names = sorted(set(_PLACEHOLDER_RE.findall(executable)), key=len, reverse=True)
    resolved: dict[str, str] = {}
    unresolved: list[str] = []
    for name in names:
        value = None
        for source in (sql_text or "", hint_text or ""):
            for m in re.finditer(rf":{name}(?![a-z0-9_])", source):
                # 예시값은 항상 한 줄(주석 한 행 또는 SQL 한 행) → tail 을 그 줄 끝까지로 잡는다.
                # (60자 고정이면 `:ids=["...","...","..."]` 같은 긴 한 줄 배열이 잘려 미해결로 샜다.)
                rest = source[m.end():]
                nl = rest.find("\n")
                tail = (rest if nl < 0 else rest[:nl])[:600]
                # 🔴 예시값은 **`:이름=값` 꼴이다 — `=` 에 앵커를 건다.** 예전에는 `=` 앞으로
                #   16자까지 아무 문자나 건너뛰며 첫 숫자를 찾았는데, 그 관대함이
                #   `-- :gu=종로구, :from=2026-07-01` 에서 `:gu` 에 **다음 파라미터의 숫자
                #   2026** 을 물려 줬다. 값이 없는 파라미터는 미해결로 남아 스킵되는 게 맞지,
                #   옆칸 숫자를 주워 오면 안 된다.
                eq = re.match(r"\s*=\s*", tail)
                if not eq:
                    continue
                val = tail[eq.end():]
                # ① 따옴표 문자열 (`:gu='성동구'`)
                qm = re.match(r"'(?:[^']|'')*'", val)
                if qm:
                    value = qm.group(0)
                    break
                # ② 한 줄 배열 (`:gus=['a','b']` — json_each(:gus) 검증용, JSON 문자열로 bind)
                am = re.match(r"\[[^\]\n]*\]", val)
                if am:
                    value = "'" + am.group(0).replace("'", "''") + "'"
                    break
                # ③ 숫자 (`:n=10`) — **값 전체가 숫자일 때만.** `2026-07-01` 의 앞 네 자리를
                #   숫자로 삼키면 `event_date BETWEEN 2026 AND 2026` 이 되는데, SQLite 는
                #   TEXT↔INTEGER 를 타입 순서로 비교해 **조용히 0행**(또는 `>= 2026` 이면
                #   반대로 **필터 무력화**)이 된다. 둘 다 검증을 통과하거나 못 하게 만든다.
                num = re.match(r"[0-9]+(?:\.[0-9]+)?(?![0-9A-Za-z_\-./:])", val)
                if num:
                    value = num.group(0)
                    break
                # ④ 따옴표 없는 값 (`:gu=ALL`·`:from=2026-07-01`·`:level=약간 붐빔`) → SQL 문자열로 감싼다
                tm = re.match(r"[^,\n\]]+", val)
                if tm:
                    v = tm.group(0).strip()
                    if v:
                        value = "'" + v.replace("'", "''") + "'"
                        break
            if value is not None:
                break
        if value is None:
            unresolved.append(name)
        else:
            resolved[name] = value
    substituted = executable
    for name in names:
        if name in resolved:
            substituted = re.sub(rf":{name}(?![a-z0-9_])", resolved[name].replace("\\", r"\\"), substituted)
    return substituted.strip(), resolved, unresolved


def _hint_of(row: dict[str, Any]) -> str:
    return "\n".join(str(row.get(k) or "") for k in ("question_ko", "axes", "insight_sample_ko"))


def verify_and_stamp(
    pattern_rows: Sequence[dict[str, Any]],
    *,
    run_sql: Callable[[str], list],
    publication_id: str,
    now_iso: str | None = None,
    param_overrides: dict[str, dict[str, str]] | None = None,
) -> dict[str, list]:
    """미검증 패턴을 실 D1 에 돌려 통과분에 스탬프한다(pattern_rows 를 제자리 수정).

    run_sql(sql) -> list[rows] (실패 시 예외). publication_id = 이 제품의 현재 게시본 id.
    이미 verified_at 이 있는 행은 건드리지 않는다(손 검증 존중). 반환: 처리 요약(로그용).
    """
    now = now_iso or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    overrides = param_overrides or {}
    report: dict[str, list] = {"verified": [], "failed": [], "skipped": []}
    for row in pattern_rows:
        pid = str(row.get("pattern_id"))
        if row.get("verified_at"):                       # 이미 검증(yml 스탬프) — 무접촉
            continue
        if not publication_id:                           # 게시본 id 없음 — 스탬프 근거 없음
            report["skipped"].append((pid, "no publication_id"))
            continue
        sql = row.get("sql") or ""
        substituted, resolved, unresolved = resolve_params(sql, _hint_of(row))
        ov = overrides.get(pid)
        if ov:
            resolved = {**resolved, **ov}
            unresolved = [n for n in unresolved if n not in ov]
            substituted = executable_sql(sql)
            for name in sorted(resolved, key=len, reverse=True):
                substituted = re.sub(rf":{name}(?![a-z0-9_])", resolved[name], substituted)
            substituted = substituted.strip()
        if unresolved:
            report["skipped"].append((pid, f"예시값 미해결 {unresolved}"))
            continue
        head = substituted.lstrip().lower()
        if not (head.startswith("select") or head.startswith("with")):
            report["skipped"].append((pid, "SELECT/WITH 아님"))
            continue
        try:
            rows = run_sql(substituted.rstrip(";") + ";")
        except Exception as exc:                          # noqa: BLE001 — 개별 실패는 남기고 계속
            report["failed"].append((pid, type(exc).__name__))
            continue
        measured = len(rows)
        allow_empty = bool(row.get("allow_empty"))
        if measured == 0 and not allow_empty:             # 예시값 조합은 0행 초과여야 검증(§8)
            report["skipped"].append((pid, "0행(allow_empty 아님)"))
            continue
        row["verified_rows"] = measured
        row["verified_at"] = now
        row["verified_publication_id"] = publication_id
        report["verified"].append(pid)
    return report
