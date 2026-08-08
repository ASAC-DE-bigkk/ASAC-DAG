"""usage_pattern SQL 정적 보안 감사 — 게시 전 테이블 스코프·문장 형태 게이트.

게이트웨이(ASK-Seoul-Serving `handleRunPattern`)는 저장된 패턴 SQL 의 주석을 벗기고
`SELECT`/`WITH` 만 통과시킨 뒤 `:name` 을 prepared bind 한다. 값 인젝션은 이 bind 로
막히지만, **게이트웨이는 그 SQL 이 어느 테이블을 읽는지 검사하지 않는다.** 저장 SQL 은
공유 D1 전체(게이트웨이 내부 `_keys`·`_usage`, 타 도메인 `d1_*`, `d1_catalog_*` 포함)에
verbatim 실행되므로, "commerce 패턴은 commerce 소유 테이블만 읽는다"는 보증은 지금까지
도메인 오너의 사람 리뷰에만 있었다. 이 모듈이 그 보증을 게시 시점 기계 검사로 만든다.

정본 allowlist 는 `serving_export.SERVING_SPEC` 의 d1_table 집합(코드 단일 출처)이다 —
따로 목록을 두지 않는다(드리프트 방지).

**테이블 참조 추출은 토크나이저 기반이다(정규식 아님).** 이전 정규식(`FROM/JOIN` 뒤 첫
식별자만 캡처)은 콤마 조인(`FROM d1_ok, _keys`)의 두 번째 이후 테이블·파생 테이블 뒤
콤마·pragma TVF 를 놓쳤다(레드팀 확증, 2026-08-08 — `_keys` 유출 페이로드 실측 통과).
지금은 FROM/JOIN 절의 **모든** 테이블(콤마 조인·서브쿼리 내부·스키마 한정·테이블값 함수
포함)을 열거해 allowlist∪CTE∪{json_each} 로 검증한다. 파싱이 애매하면 과다 캡처(=게시
거부) 쪽으로 fail-closed 한다.

`serving_export._handoff_rows` 가 게시 직전 각 패턴에 이 감사를 걸어 실패분을 게시에서
제외하고 경보한다. CI/사전 검사는 `scripts/audit_pattern_sql.py` 로 dbt yml 을 훑는다.
"""
from __future__ import annotations

import re

# 토크나이저 — 문자열/주석/인용 식별자를 원자로 보므로 그 안의 `;`·키워드·콤마에 안 속는다.
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

# FROM 자리에 허용하는 테이블값 함수 — 배열 IN 관용구(`IN (SELECT value FROM json_each(:list))`,
# 실 D1 검증 2026-08-08)만. pragma_* 등 다른 테이블값 함수는 여전히 차단된다.
_ALLOWED_TABLE_FUNCS = frozenset({"json_each"})
# 읽기 전용 위반 + 위험 지시. `pragma\w*` — pragma_table_info 같은 TVF 형(밑줄로 경계가
# 사라져 `\bpragma\b` 가 놓치던 것)까지 잡는다. 테이블 워커도 이중으로 잡지만 방어심화.
_FORBIDDEN = frozenset({
    "attach", "detach", "insert", "update", "delete", "drop", "alter", "create",
    "replace", "vacuum", "reindex", "analyze", "load_extension",
})
_FORBIDDEN_PREFIX = ("pragma",)   # pragma / pragma_table_info / pragma_* 전부
# 테이블 리스트를 끝내는 절 키워드(같은 괄호 깊이에서). ON/USING 은 조인 조건 시작 = 리스트 종료.
_BOUNDARY_KW = frozenset({
    "where", "group", "order", "having", "limit", "window", "union",
    "except", "intersect", "on", "using", "returning", "values",
})
_JOIN_KW = frozenset({"join"})
_JOINMOD_KW = frozenset({"cross", "inner", "left", "right", "full", "outer", "natural"})
_LIMIT_PARAM_RE = re.compile(r"^(n|limit|top_n)$", re.I)
_COMMENT_RE = re.compile(r"--[^\n]*|/\*[\s\S]*?\*/")


def commerce_allowlist() -> frozenset[str]:
    """게시 대상 commerce d1_table 집합(정본 = SERVING_SPEC). 순환 임포트 회피 위해 지연."""
    from gold.serving_export import SERVING_SPEC
    return frozenset(s.d1_table for s in SERVING_SPEC)


def _strip_ident(v: str) -> str:
    return v.strip('"`[]')


def _tokenize(sql: str) -> list[tuple[str, str]]:
    toks: list[tuple[str, str]] = []
    for m in _TOKEN_RE.finditer(sql or ""):
        kind = m.lastgroup
        if kind in ("ws", "line_comment", "block_comment"):
            continue
        toks.append((kind, m.group()))
    return toks


def _cte_names(toks: list[tuple[str, str]]) -> set[str]:
    """WITH x AS ( ... ), y AS ( ... ) 의 CTE 이름 — 테이블이 아니라 로컬 별칭.

    (WITH|,) IDENT AS ( 패턴을 깊이 0 에서만 인정한다(중첩 서브쿼리의 별칭 AS 와 구분)."""
    names: set[str] = set()
    depth = 0
    for i, (kind, val) in enumerate(toks):
        if kind == "punct" and val == "(":
            depth += 1
        elif kind == "punct" and val == ")":
            depth = max(0, depth - 1)
        if kind == "ident" and val.lower() == "as" and i >= 2 and i + 1 < len(toks):
            prev = toks[i - 1]
            nxt = toks[i + 1]
            if prev[0] in ("ident", "dquote", "bquote", "bracket") and nxt == ("punct", "("):
                # 앞이 WITH/콤마로 시작한 CTE 정의인지 — i-2 확인(대소문자 무시)
                pp = toks[i - 2]
                pp_is_with = pp[0] == "ident" and pp[1].lower() == "with"
                if (pp_is_with or pp == ("punct", ",")) and depth == 0:
                    names.add(_strip_ident(prev[1]).lower())
    return names


def _table_refs(toks: list[tuple[str, str]]) -> list[str]:
    """FROM/JOIN 절의 모든 테이블 참조를 열거(콤마 조인·서브쿼리·스키마 한정·TVF 포함)."""
    refs: list[str] = []
    depth = 0
    in_list = False       # 현재 테이블 리스트 수집 중인가
    list_depth = 0        # 그 리스트가 있는 괄호 깊이
    expect = False        # 다음 식별자가 테이블 이름인가
    i, n = 0, len(toks)
    while i < n:
        kind, val = toks[i]
        low = val.lower() if kind == "ident" else val
        if kind == "punct" and val == "(":
            if expect:
                expect = False   # 파생 테이블 서브쿼리 — 내부 FROM 은 선형 스캔이 따로 잡는다
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
            i += 1   # 조인 수식어 — 'join' 을 계속 기다림
            continue
        if in_list and kind == "ident" and low == "as":
            i += 1   # 별칭 마커 — 다음 식별자는 별칭(아래 skip 분기)
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


def audit_pattern_sql(sql: str, allowed_tables: frozenset[str] | None = None) -> list[str]:
    """패턴 SQL 하나를 감사한다. 위반 사유 문자열 리스트 반환(빈 리스트 = 통과).

    검사: ① SELECT/WITH 단일문 ② 금지 토큰(쓰기/DDL/PRAGMA/ATTACH) 부재 ③ FROM/JOIN 의
    모든 테이블이 allowlist∪CTE∪{json_each} ④ `LIMIT :name` 의 name 이 상한 적용
    이름(n/limit/top_n)인지(게이트웨이 clamp 대상).
    """
    allowed = allowed_tables if allowed_tables is not None else commerce_allowlist()
    findings: list[str] = []
    toks = _tokenize(sql)
    if not toks or not (toks[0][0] == "ident" and toks[0][1].lower() in ("select", "with")):
        findings.append("SELECT/WITH 로 시작하지 않음(읽기 전용 아님)")
    # 단일 문장: 끝의 세미콜론 하나만 허용. 그 앞의 세미콜론은 스택 쿼리.
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
                       if r not in allowed and r not in ctes and r not in _ALLOWED_TABLE_FUNCS})
    if external:
        findings.append(f"allowlist 밖 테이블 참조: {external} "
                        f"(commerce 소유 d1_* 만 허용 — 내부/타도메인 테이블 접근 차단)")
    # 행수 상한이 상한이려면 파라미터 이름이 n/limit/top_n 여야 게이트웨이가 clamp 한다.
    for k in range(len(toks) - 1):
        if toks[k][0] == "ident" and toks[k][1].lower() == "limit" and toks[k + 1][0] == "param":
            pname = toks[k + 1][1][1:]
            if not _LIMIT_PARAM_RE.match(pname):
                findings.append(f"LIMIT :{pname} — 행수 파라미터는 n/limit/top_n 이름만 "
                                f"상한(5000) 이 걸린다")
    return findings


def audit_patterns(patterns: list[dict], allowed_tables: frozenset[str] | None = None
                   ) -> dict[str, list[str]]:
    """(pattern_id → 위반 리스트). 위반 없는 패턴은 결과에 넣지 않는다."""
    allowed = allowed_tables if allowed_tables is not None else commerce_allowlist()
    out: dict[str, list[str]] = {}
    for p in patterns:
        f = audit_pattern_sql(p.get("sql") or "", allowed)
        if f:
            out[str(p.get("pattern_id"))] = f
    return out
