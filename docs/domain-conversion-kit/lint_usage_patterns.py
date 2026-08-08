"""usage_patterns 표기·보안 규약 린트 (org 공통) — 도메인 무관 CI 게이트.

**commerce 전용 `dbt/domains/commerce/scripts/lint_usage_patterns.py` 를 일반화한 판.**
commerce 판은 allowlist(declared_tables)를 `meta.serving.d1_table` 에서만 유도했는데, 타
도메인은 `d1_table` 을 선언하지 않고 **모델명이 곧 D1 물리 테이블명**이라(공유 게시기
`common/serving/contract.py:model_name`), commerce 린트를 그대로 돌리면 전 패턴이 E5(선언 밖
테이블)로 오탐한다. 이 판은 테이블을 **`pattern.d1_table` → `serving.d1_table` → 모델명**
순으로 유도해 두 명명 규칙(commerce `d1_*` · 타도메인 `gold_<domain>_*`)을 모두 커버한다.

다중 파일 지원: traffic_weather 처럼 serving 선언이 여러 yml 에 흩어진 도메인을 위해
`--source` 글롭을 받아 allowlist 를 도메인 전체로 합친다(serving_contract validator 와 동형).

검사(ERROR = exit 1):
  E1  pattern_id 슬러그 `^[a-z0-9_]{1,64}$` (하이픈 금지 — Serving#178)
  E2  (table, pattern_id) 유일
  E3  주석 제거 후 SELECT/WITH 단일문
  E4  쓰기/DDL/PRAGMA/ATTACH 금지 토큰
  E5  FROM/JOIN 의 모든 테이블 ⊆ (도메인 게시 테이블 전체 ∪ CTE ∪ json_each)
  E6  값 박힘 표식 `/* :name */` 금지 (Serving#179)
  E7  `LIMIT :name` 은 n/limit/top_n 이름만 (게이트웨이 clamp 대상)
경고(비차단):
  W1  requires 어휘 밖 항목
  W2  파라미터 있는 패턴에 예시값 주석 부재(verify 관용 추출 실패 예상)
  W3  verified_at 부재 (게이트웨이가 실행 거부 = 미배포 상태 — citydata 42건이 이 상태)

실행:
  python lint_usage_patterns.py --source "domains/citydata/models/gold/*.yml"
  python lint_usage_patterns.py --source "domains/traffic_weather/models/**/*.yml"
"""
from __future__ import annotations

import argparse
import glob as globmod
import re
import sys

import yaml

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SLUG_RE = re.compile(r"^[a-z0-9_]{1,64}$")
COMMENT_RE = re.compile(r"--[^\n]*|/\*[\s\S]*?\*/")
BAKED_MARK_RE = re.compile(r"/\*\s*:[a-z_]")
PLACEHOLDER_RE = re.compile(r":([a-z_][a-z0-9_]*)")
ALLOWED_TABLE_FUNCS = {"json_each"}
LIMIT_NAMES = {"n", "limit", "top_n"}
REQUIRES_VOCAB = {"select_columns", "sort", "aggregate", "group_by", "filter_range",
                  "having", "subquery", "window", "join", "filter_set", "filter_null"}
_TOKEN_RE = re.compile(
    r"""(?P<ws>\s+)|(?P<line_comment>--[^\n]*)|(?P<block_comment>/\*[\s\S]*?\*/)
      |(?P<string>'(?:[^']|'')*')|(?P<dquote>"(?:[^"]|"")*")|(?P<bquote>`(?:[^`]|``)*`)
      |(?P<bracket>\[[^\]]*\])|(?P<param>:[a-zA-Z_][a-zA-Z0-9_]*)|(?P<number>\d+\.?\d*)
      |(?P<ident>[a-zA-Z_][a-zA-Z0-9_]*)|(?P<punct>[(),.;])|(?P<other>[^\s])""",
    re.VERBOSE)
FORBIDDEN = {"attach", "detach", "insert", "update", "delete", "drop", "alter",
             "create", "replace", "vacuum", "reindex", "analyze", "load_extension"}
FORBIDDEN_PREFIX = ("pragma",)
BOUNDARY_KW = {"where", "group", "order", "having", "limit", "window", "union",
               "except", "intersect", "on", "using", "returning", "values"}
JOINMOD_KW = {"cross", "inner", "left", "right", "full", "outer", "natural"}


def _tokenize(sql):
    return [(m.lastgroup, m.group()) for m in _TOKEN_RE.finditer(sql or "")
            if m.lastgroup not in ("ws", "line_comment", "block_comment")]


def _strip_ident(v):
    return v.strip('"`[]')


def _cte_names(toks):
    names, depth = set(), 0
    for i, (kind, val) in enumerate(toks):
        if kind == "punct" and val == "(":
            depth += 1
        elif kind == "punct" and val == ")":
            depth = max(0, depth - 1)
        if kind == "ident" and val.lower() == "as" and i >= 2 and i + 1 < len(toks):
            prev, nxt, pp = toks[i - 1], toks[i + 1], toks[i - 2]
            pp_with = pp[0] == "ident" and pp[1].lower() == "with"
            if (prev[0] in ("ident", "dquote", "bquote", "bracket") and nxt == ("punct", "(")
                    and (pp_with or pp == ("punct", ",")) and depth == 0):
                names.add(_strip_ident(prev[1]).lower())
    return names


def _table_refs(toks):
    refs, depth, in_list, list_depth, expect = [], 0, False, 0, False
    i, n = 0, len(toks)
    while i < n:
        kind, val = toks[i]
        low = val.lower() if kind == "ident" else val
        if kind == "punct" and val == "(":
            if expect:
                expect = False
            depth += 1; i += 1; continue
        if kind == "punct" and val == ")":
            depth -= 1
            if in_list and depth < list_depth:
                in_list, expect = False, False
            i += 1; continue
        if kind == "ident" and (low == "from" or low == "join"):
            in_list, list_depth, expect = True, depth, True; i += 1; continue
        if in_list and kind == "punct" and val == "," and depth == list_depth:
            expect = True; i += 1; continue
        if in_list and kind == "ident" and low in BOUNDARY_KW:
            in_list, expect = False, False; i += 1; continue
        if in_list and kind == "ident" and low in JOINMOD_KW:
            i += 1; continue
        if in_list and kind == "ident" and low == "as":
            i += 1; continue
        if expect and kind in ("ident", "dquote", "bquote", "bracket"):
            name, j = _strip_ident(val), i + 1
            while (j + 1 < n and toks[j] == ("punct", ".")
                   and toks[j + 1][0] in ("ident", "dquote", "bquote", "bracket")):
                name += "." + _strip_ident(toks[j + 1][1]); j += 2
            refs.append(name); expect = False; i = j; continue
        i += 1
    return refs


def _serving_of(m):
    return (m.get("config", {}) or {}).get("meta", {}).get("serving", {}) \
        or (m.get("meta", {}) or {}).get("serving", {}) or {}


def _table_of(pattern, serving, model_name):
    """두 명명 규칙 통합: pattern.d1_table → serving.d1_table(str) → model_name."""
    dt = pattern.get("d1_table")
    if dt:
        return str(dt)
    sdt = serving.get("d1_table")
    if isinstance(sdt, str):
        return sdt
    return model_name


def lint(sources):
    files = []
    for s in sources:
        files.extend(sorted(globmod.glob(s, recursive=True)))
    declared_tables = set()
    entries = []   # (table, pattern_id, pattern, serving)
    for f in files:
        try:
            d = yaml.safe_load(open(f, encoding="utf-8"))
        except Exception:
            continue
        for m in (d.get("models") or []):
            sv = _serving_of(m)
            if not sv:
                continue
            # 모든 명명 규칙의 테이블을 allowlist 에 넣는다
            dt = sv.get("d1_table")
            for t in (dt if isinstance(dt, list) else [dt] if dt else [m.get("name")]):
                if t:
                    declared_tables.add(str(t).lower())
            for p in (sv.get("usage_patterns") or []):
                # 패턴 레벨 d1_table override 도 allowlist 에
                if p.get("d1_table"):
                    declared_tables.add(str(p["d1_table"]).lower())
                entries.append((_table_of(p, sv, m.get("name")), str(p.get("pattern_id")), p, sv))

    errors, warns = [], []
    seen = set()
    for table, pid, p, sv in entries:
        where = f"{table}/{pid}"
        if not SLUG_RE.match(pid):
            errors.append(f"E1 {where}: pattern_id 슬러그 위반")
        if (table, pid) in seen:
            errors.append(f"E2 {where}: (table, pattern_id) 중복")
        seen.add((table, pid))
        sql = str(p.get("sql") or "")
        toks = _tokenize(sql)
        if not toks or not (toks[0][0] == "ident" and toks[0][1].lower() in ("select", "with")):
            errors.append(f"E3 {where}: SELECT/WITH 로 시작하지 않음")
        semis = [k for k in range(len(toks)) if toks[k] == ("punct", ";")]
        if semis and not (len(semis) == 1 and semis[0] == len(toks) - 1):
            errors.append(f"E3 {where}: 복수 문장(세미콜론) 의심")
        for kind, val in toks:
            if kind == "ident":
                lw = val.lower()
                if lw in FORBIDDEN or any(lw.startswith(pre) for pre in FORBIDDEN_PREFIX):
                    errors.append(f"E4 {where}: 금지 토큰 '{lw}'")
                    break
        ctes = _cte_names(toks)
        refs = {r.lower() for r in _table_refs(toks)}
        external = sorted(r for r in refs if r not in declared_tables and r not in ctes
                          and r not in ALLOWED_TABLE_FUNCS)
        if external:
            errors.append(f"E5 {where}: 선언 밖 테이블 참조 {external}")
        if BAKED_MARK_RE.search(sql):
            errors.append(f"E6 {where}: 값 박힘 표식(/* :name */)")
        for k in range(len(toks) - 1):
            if toks[k][0] == "ident" and toks[k][1].lower() == "limit" and toks[k + 1][0] == "param":
                if toks[k + 1][1][1:] not in LIMIT_NAMES:
                    errors.append(f"E7 {where}: LIMIT :{toks[k+1][1][1:]} — n/limit/top_n 만 상한 적용")
        extra = [r for r in (p.get("requires") or []) if r and r not in REQUIRES_VOCAB]
        if extra:
            warns.append(f"W1 {where}: requires 어휘 밖 {extra}")
        body = COMMENT_RE.sub(" ", sql)
        params = set(PLACEHOLDER_RE.findall(body))
        if params:
            ct = " ".join(COMMENT_RE.findall(sql))
            missing = sorted(nm for nm in params
                             if not re.search(rf":{nm}\s*=\s*('[^']*'|[0-9])", ct))
            if missing:
                warns.append(f"W2 {where}: 예시값 주석 없는 파라미터 {missing}")
        if not p.get("verified_at"):
            warns.append(f"W3 {where}: verified_at 부재 — 게이트웨이 실행 거부(미배포)")
    return errors, warns, len(entries), len(declared_tables)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", nargs="+", required=True, help="yml 글롭(다중 가능)")
    args = ap.parse_args()
    errors, warns, n, ntab = lint(args.source)
    for w in warns:
        print(f"  warn  {w}")
    for e in errors:
        print(f"  ERROR {e}")
    print(f"lint: patterns={n} tables={ntab} errors={len(errors)} warnings={len(warns)}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
