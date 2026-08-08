"""usage_patterns 제공정보 카탈로그 생성 (org 공통) — "무슨 질문에 무슨 정보를 주는가".

**commerce 전용 `generate_pattern_catalog.py` 를 도메인 무관하게 일반화한 판.** commerce 판은
(1) 헤더에 "commerce" 를 하드코딩하고 (2) 테이블 그룹키를 `d1_table` 에서만 얻어 타 도메인은
`?` 로 뭉치며 (3) 제품 id 를 `"commerce_" + t[3:]` 로 고정했다 — 타 도메인에서 전부 깨진다.
이 판은 도메인/제품/테이블을 선언에서 유도한다:
  - 테이블 그룹키 = `pattern.d1_table` → `serving.d1_table`(str) → 모델명
  - 제품 id = `serving.product_id`
  - 도메인 = `--domain` 인자(없으면 product_id 접두)

반환 컬럼 추출(`_returns`)·파라미터 추출(`_params`)은 commerce 판과 동일 — 그 로직은 타 도메인
SQL 에도 그대로 작동함을 실측 확인했다(culture/citydata 반환 컬럼 정확 추출).

실행:
  python generate_pattern_catalog.py --source "domains/culture/models/gold/*.yml" --domain culture --out <md>
  python generate_pattern_catalog.py --source "domains/traffic_weather/models/**/*.yml" --domain traffic_weather --out <md>
"""
from __future__ import annotations

import argparse
import glob as globmod
import re
import sys

import yaml

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

_COMMENT_RE = re.compile(r"--[^\n]*|/\*[\s\S]*?\*/")
_PLACEHOLDER_RE = re.compile(r":([a-z_][a-z0-9_]*)")
_ALIAS_RE = re.compile(r"\bAS\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$", re.I)
_TRAILING_IDENT_RE = re.compile(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*$")


def _params(sql):
    body = _COMMENT_RE.sub(" ", sql or "")
    seen = []
    for name in _PLACEHOLDER_RE.findall(body):
        if name not in seen:
            seen.append(name)
    return seen


def _returns(sql):
    body = _COMMENT_RE.sub(" ", sql or "")
    depth, last_select = 0, -1
    for m in re.finditer(r"[()]|\bselect\b", body, re.I):
        tok = m.group()
        if tok == "(":
            depth += 1
        elif tok == ")":
            depth = max(0, depth - 1)
        elif depth == 0:
            last_select = m.end()
    if last_select < 0:
        return []
    rest = body[last_select:]
    depth, end = 0, len(rest)
    for m in re.finditer(r"[()]|\bfrom\b", rest, re.I):
        tok = m.group()
        if tok == "(":
            depth += 1
        elif tok == ")":
            depth = max(0, depth - 1)
        elif depth == 0:
            end = m.start()
            break
    proj, depth, items, cur = rest[:end], 0, [], ""
    for ch in proj:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            items.append(cur); cur = ""
        else:
            cur += ch
    items.append(cur)
    out = []
    for raw in items:
        item = raw.strip().rstrip(";").strip()
        if not item or item == "*":
            continue
        am = _ALIAS_RE.search(item)
        name = am.group(1) if am else None
        if not name:
            tm = _TRAILING_IDENT_RE.search(item)
            name = tm.group(1) if tm else None
        if name and name.lower() != "distinct":
            out.append(name)
    return out


def _cell(s):
    return (s or "").replace("|", "\\|").replace("\n", " ").strip()


def _serving_of(m):
    return (m.get("config", {}) or {}).get("meta", {}).get("serving", {}) \
        or (m.get("meta", {}) or {}).get("serving", {}) or {}


def render(sources, domain):
    files = []
    for s in sources:
        files.extend(sorted(globmod.glob(s, recursive=True)))
    by_table = {}
    product_of = {}
    for f in files:
        try:
            d = yaml.safe_load(open(f, encoding="utf-8"))
        except Exception:
            continue
        for m in (d.get("models") or []):
            sv = _serving_of(m)
            if not sv:
                continue
            default_table = sv.get("d1_table") if isinstance(sv.get("d1_table"), str) else m.get("name")
            for p in (sv.get("usage_patterns") or []):
                t = p.get("d1_table") or default_table
                by_table.setdefault(t, []).append(p)
                product_of[t] = sv.get("product_id") or t
    total = sum(len(v) for v in by_table.values())
    verified = sum(1 for v in by_table.values() for p in v if p.get("verified_at"))
    out = []
    out.append(f"# {domain} D1 질의 패턴 카탈로그 — 무엇을 물으면 무엇을 주는가")
    out.append("")
    out.append("> **생성물이다. 손으로 고치지 말 것.** 정본은 해당 도메인 gold 모델 yml 의 "
               "`usage_patterns` 이며, `generate_pattern_catalog.py` 로 재생성한다.")
    out.append("")
    out.append(f"패턴 {total}건 / D1 테이블 {len(by_table)}종 / 검증 완료 {verified}건. "
               "각 패턴은 게이트웨이 `run_pattern` 으로 실행하며, `파라미터` 열 이름 전부에 값을 "
               "줘야 한다(모든 파라미터 필수 — 기본값 없음).")
    if verified < total:
        out.append("")
        out.append(f"> ⚠️ 검증 미완 {total - verified}건 — `verified_at` 이 없으면 게이트웨이가 "
                   "실행을 거부(409)한다. 배포 전 검증 필요.")
    out.append("")
    out.append("**질문** = 답하는 물음(`question_ko`), **반환 컬럼** = 받는 결과의 열(SQL 최종 "
               "SELECT 에서 추출), **축** = 집계·랭킹 축(`axes`).")
    out.append("")
    for t in sorted(by_table):
        pats = by_table[t]
        pid = product_of.get(t, t)
        vok = sum(1 for p in pats if p.get("verified_at"))
        badge = "" if vok == len(pats) else f" · ⚠️검증 {vok}/{len(pats)}"
        out.append(f"## {t} (`{pid}`) — {len(pats)}건{badge}")
        out.append("")
        out.append("| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |")
        out.append("|---|---|---|---|---|")
        for p in sorted(pats, key=lambda x: str(x.get("pattern_id"))):
            params = ", ".join(f"`:{n}`" for n in _params(p.get("sql") or "")) or "—"
            returns = ", ".join(f"`{c}`" for c in _returns(p.get("sql") or "")) or "—"
            out.append(f"| `{_cell(str(p.get('pattern_id')))}` | {_cell(p.get('question_ko'))} "
                       f"| {returns} | {params} | {_cell(p.get('axes')) or '—'} |")
        out.append("")
    return "\n".join(out) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", nargs="+", required=True)
    ap.add_argument("--domain", required=True)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    md = render(args.source, args.domain)
    if args.out:
        open(args.out, "w", encoding="utf-8", newline="\n").write(md)
        print(f"written: {args.out}")
    else:
        print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
