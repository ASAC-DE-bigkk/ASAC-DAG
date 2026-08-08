"""usage_pattern SQL 정적 보안 감사 (org 공통) — 게시 전 테이블 스코프·문장 형태 게이트.

**이것은 commerce 전용 `dags/domains/commerce/include/gold/pattern_audit.py` 를 도메인
무관하게 일반화한 판이다.** 나중에 `dags/common/serving/pattern_audit.py` 로 배치하고
공유 게시기(`common/serving/publisher.py`)의 패턴 게시 루프에서 호출하면, 커머스가 자체
게시기에서만 받던 감사를 **모든 도메인**이 공유 경로에서 받는다.

배경(commerce 판과 동일한 위협): 게이트웨이(`run_pattern`)는 저장 패턴 SQL 을 주석 제거 →
`SELECT`/`WITH` 확인 → `:name` bind 후 **공유 D1 전체에 verbatim 실행**하며 **어느 테이블을
읽는지 검사하지 않는다**. 같은 DB 에 게이트웨이 내부표(`_keys`=API 키 해시+이메일·`_usage`·
`_burst`·`_ops_*`·`_publication_*`)와 카탈로그·핸드오프 표(`d1_catalog_*`·`d1_usage_patterns`)
가 있다. "이 도메인 패턴은 이 도메인이 게시한 제품 테이블만 읽는다"는 보증을 게시 시점
기계 검사로 만든다.

**commerce 판과의 유일한 차이**: 커머스는 allowlist 를 `SERVING_SPEC`(코드)에서 파생했다.
공통판은 **allowlist 를 인자로 받는다** — 각 도메인의 게시 제품 테이블 집합(= dbt serving
선언 모델명 = D1 물리 테이블명)을 게시기가 만들어 넘긴다. 나머지(토크나이저 테이블 추출,
콤마 조인·서브쿼리·스키마 한정·pragma TVF 방어, 금지 토큰, LIMIT 이름)는 commerce 판과
바이트 동일하다 — 그 코드는 레드팀 검증을 거쳤다(ASAC-DAG#743, self-test 23종).
"""
from __future__ import annotations

import re
from typing import Iterable

_TOKEN_RE = re.compile(
    r"""(?P<ws>\s+)
      | (?P<line_comment>--[^\n]*)
      | (?P<block_comment>/\*[\s\S]*?\*/)
      | (?P<string>'(?:[^']|'')*')
      | (?P<dquote>"(?:[^"]|"")*")
      | (?P<bquote>`(?:[^`]|``)*`)
      | (?P<bracket>\[[^\]]*\])
      | (?P<param>:[a-zA-Z_][a-zA-Z0-9_]*)
      | (?P<number>\d+\.?\d*)
      | (?P<ident>[a-zA-Z_][a-zA-Z0-9_]*)
      | (?P<punct>[(),.;])
      | (?P<other>[^\s])""",
    re.VERBOSE,
)
_ALLOWED_TABLE_FUNCS = frozenset({"json_each"})
_FORBIDDEN = frozenset({
    "attach", "detach", "insert", "update", "delete", "drop", "alter", "create",
    "replace", "vacuum", "reindex", "analyze", "load_extension",
})
_FORBIDDEN_PREFIX = ("pragma",)
_BOUNDARY_KW = frozenset({
    "where", "group", "order", "having", "limit", "window", "union",
    "except", "intersect", "on", "using", "returning", "values",
})
_JOIN_KW = frozenset({"join"})
_JOINMOD_KW = frozenset({"cross", "inner", "left", "right", "full", "outer", "natural"})
_LIMIT_PARAM_RE = re.compile(r"^(n|limit|top_n)$", re.I)


def build_allowlist(product_tables: Iterable[str],
                    cross_domain_sources: Iterable[str] = ()) -> frozenset[str]:
    """도메인 allowlist 를 만든다 — 게시 제품 테이블(모델명=D1 테이블명) ∪ 선언된 크로스도메인
    소스. 게이트웨이 내부표·카탈로그/핸드오프 표는 여기 없으므로 자동 거부된다.

    크로스도메인 소스: 도메인이 dbt published-source 계약으로 명시 소비하는 타 도메인 제품
    (예: weather 가 `commerce_gold.gold_license_dong_summary` 를 source 로 선언). 명시 선언한
    것만 넣는다 — 임의 타 도메인 테이블 읽기는 계속 거부(도메인 격리 유지)."""
    return frozenset(str(t).lower() for t in (*product_tables, *cross_domain_sources))


def _strip_ident(v: str) -> str:
    return v.strip('"`[]')


def _tokenize(sql: str) -> list[tuple[str, str]]:
    return [(m.lastgroup, m.group()) for m in _TOKEN_RE.finditer(sql or "")
            if m.lastgroup not in ("ws", "line_comment", "block_comment")]


def _cte_names(toks: list[tuple[str, str]]) -> set[str]:
    names: set[str] = set()
    depth = 0
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


def _table_refs(toks: list[tuple[str, str]]) -> list[str]:
    refs: list[str] = []
    depth = 0
    in_list = False
    list_depth = 0
    expect = False
    i, n = 0, len(toks)
    while i < n:
        kind, val = toks[i]
        low = val.lower() if kind == "ident" else val
        if kind == "punct" and val == "(":
            if expect:
                expect = False
            depth += 1
            i += 1
            continue
        if kind == "punct" and val == ")":
            depth -= 1
            if in_list and depth < list_depth:
                in_list, expect = False, False
            i += 1
            continue
        if kind == "ident" and (low == "from" or low in _JOIN_KW):
            in_list, list_depth, expect = True, depth, True
            i += 1
            continue
        if in_list and kind == "punct" and val == "," and depth == list_depth:
            expect = True
            i += 1
            continue
        if in_list and kind == "ident" and low in _BOUNDARY_KW:
            in_list, expect = False, False
            i += 1
            continue
        if in_list and kind == "ident" and low in _JOINMOD_KW:
            i += 1
            continue
        if in_list and kind == "ident" and low == "as":
            i += 1
            continue
        if expect and kind in ("ident", "dquote", "bquote", "bracket"):
            name = _strip_ident(val)
            j = i + 1
            while (j + 1 < n and toks[j] == ("punct", ".")
                   and toks[j + 1][0] in ("ident", "dquote", "bquote", "bracket")):
                name += "." + _strip_ident(toks[j + 1][1])
                j += 2
            refs.append(name)
            expect = False
            i = j
            continue
        i += 1
    return refs


def audit_pattern_sql(sql: str, allowed_tables: frozenset[str]) -> list[str]:
    """패턴 SQL 하나를 감사한다. 위반 사유 리스트(빈 리스트 = 통과).

    검사: ① SELECT/WITH 단일문 ② 금지 토큰(쓰기/DDL/PRAGMA/ATTACH) ③ FROM/JOIN 의 모든
    테이블이 allowed_tables ∪ CTE ∪ {json_each} ④ `LIMIT :name` 이름 규약. commerce 판과
    동일 로직 — allowlist 만 인자로 받는다."""
    findings: list[str] = []
    toks = _tokenize(sql)
    if not toks or not (toks[0][0] == "ident" and toks[0][1].lower() in ("select", "with")):
        findings.append("SELECT/WITH 로 시작하지 않음(읽기 전용 아님)")
    semis = [k for k in range(len(toks)) if toks[k] == ("punct", ";")]
    if semis and not (len(semis) == 1 and semis[0] == len(toks) - 1):
        findings.append("세미콜론 내부 등장 — 복수 문장(스택 쿼리) 의심")
    for kind, val in toks:
        if kind == "ident":
            low = val.lower()
            if low in _FORBIDDEN or any(low.startswith(p) for p in _FORBIDDEN_PREFIX):
                findings.append(f"금지 토큰 '{low}' — 쓰기/DDL/PRAGMA/ATTACH 계열")
                break
    ctes = _cte_names(toks)
    refs = [r.lower() for r in _table_refs(toks)]
    external = sorted({r for r in refs
                       if r not in allowed_tables and r not in ctes and r not in _ALLOWED_TABLE_FUNCS})
    if external:
        findings.append(f"allowlist 밖 테이블 참조: {external} "
                        f"(이 도메인 게시 제품 + 선언 크로스도메인 소스만 허용 — "
                        f"내부/미선언 테이블 접근 차단)")
    for k in range(len(toks) - 1):
        if toks[k][0] == "ident" and toks[k][1].lower() == "limit" and toks[k + 1][0] == "param":
            pname = toks[k + 1][1][1:]
            if not _LIMIT_PARAM_RE.match(pname):
                findings.append(f"LIMIT :{pname} — 행수 파라미터는 n/limit/top_n 이름만 "
                                f"상한(5000) 이 걸린다")
    return findings


def audit_patterns(patterns: list[dict], allowed_tables: frozenset[str]) -> dict[str, list[str]]:
    """(pattern_id → 위반 리스트). 위반 없는 패턴은 결과에 넣지 않는다."""
    out: dict[str, list[str]] = {}
    for p in patterns:
        f = audit_pattern_sql(p.get("sql") or "", allowed_tables)
        if f:
            out[str(p.get("pattern_id"))] = f
    return out
