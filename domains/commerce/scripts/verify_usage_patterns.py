"""usage_patterns 검증 실행 + verified_* 백필(#638 §5-1 — 손 백필 금지, 재실행 실측만).

각 패턴의 `sql`(참조 구현)을 **실제 D1** 에 실행해 반환 행수를 재고, 그 시점의 게시본
식별자(`d1_publish_state.publication_id`)와 함께 dbt yml 에 기록한다:

  verified_rows            — 이번 실행이 실제로 반환한 행수(선언값과 다르면 드리프트 보고 후 갱신)
  verified_at              — 실행 시각(UTC, ISO8601)
  verified_publication_id  — 실행 당시 그 제품의 게시본 id (#638 §2.3)

yml 은 **텍스트 삽입만** 한다(형식·주석 보존 — yaml round-trip 금지, #638 이슈 실측:
`verified_rows` 라인은 275건 전건이 `^ {14}verified_rows: <int>$` 로 기계 정확).
패턴 식별은 (모델의 유효 d1_table, pattern_id) — pattern_id 단독은 3건 충돌.

파라미터: sql 본문의 `:name` 자리(실행문 기준)에, 본문 주석(선행 `-- :n=10` / `예:` /
트레일링 등 7+ 문법)에서 관용 추출한 예시값을 치환한다. 값을 못 찾으면 그 패턴은
건너뛰고 사유를 보고한다 — 추측값 실행은 검증 증거를 오염시킨다.

실행(컨테이너 권장):
  python /opt/airflow/dags/domains/commerce/scripts/verify_usage_patterns.py            # dry-run(파싱·치환만)
  python .../verify_usage_patterns.py --execute                                          # D1 실행 + 보고
  python .../verify_usage_patterns.py --apply                                            # 실행 + yml 기록
로컬 실행은 --env-file <root .env> 로 토큰을 주입한다(값은 어디에도 출력되지 않는다).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))  # dags 루트(common.*, #109)


def _load_env_file(path: str) -> None:
    """root .env 주입(로컬 실행용) — KEY=VALUE 만, 프로세스 env 우선(setdefault). 값 미출력."""
    import os

    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def _bootstrap(env_file: str | None) -> None:
    if env_file:
        _load_env_file(env_file)
    from commerce_core.env import load_commerce_env

    load_commerce_env()
    from security import install_security

    install_security()   # 로그·stdout·예외훅 시크릿 마스킹(스크립트도 엔트리포인트)


# ── yml 텍스트 파서(형식 보존 전제 — 삽입 좌표만 계산) ─────────────────────────
_MODEL_RE = re.compile(r"^  - name: (\S+)\s*$")
_MODEL_D1_RE = re.compile(r"^          d1_table: ([a-zA-Z0-9_]+)\s*(#.*)?$")
_PATTERNS_KEY_RE = re.compile(r"^          usage_patterns:")
_PATTERN_START_RE = re.compile(r'^            - pattern_id: "(.+)"\s*$')
_PATTERN_D1_RE = re.compile(r"^              d1_table: ([a-zA-Z0-9_]+)\s*$")
_VERIFIED_ROWS_RE = re.compile(r"^ {14}verified_rows: ([0-9]+)$")
_SQL_START_RE = re.compile(r"^              sql: \|")
_FIELD_14_RE = re.compile(r"^ {14}\S")
_PLACEHOLDER_RE = re.compile(r":([a-z][a-z0-9_]*)")


@dataclass
class Pattern:
    model: str
    pattern_id: str
    d1_table: str
    verified_rows_line: int          # 0-based line index of the verified_rows line
    declared_rows: int
    sql_lines: list[str] = field(default_factory=list)
    hint_lines: list[str] = field(default_factory=list)   # question_ko/axes/insight — 예시값 폴백
    has_verified_at: bool = False    # 직후 라인에 이미 verified_at 이 있는가(멱등 재실행)

    @property
    def key(self) -> str:
        return f"{self.d1_table}/{self.pattern_id}"

    @property
    def sql_text(self) -> str:
        return "\n".join(self.sql_lines)


def parse_patterns(lines: list[str]) -> list[Pattern]:
    patterns: list[Pattern] = []
    model = ""
    model_d1 = ""
    in_patterns = False
    current: Pattern | None = None
    in_sql = False

    for i, line in enumerate(lines):
        m = _MODEL_RE.match(line)
        if m:
            model, model_d1, in_patterns, current, in_sql = m.group(1), "", False, None, False
            continue
        if not in_patterns:
            md = _MODEL_D1_RE.match(line)
            if md:
                model_d1 = md.group(1)
            if _PATTERNS_KEY_RE.match(line):
                in_patterns = True
            continue
        # usage_patterns 블록 안: dedent(<=10칸 필드/모델 경계)로 블록 종료 판정
        if line.strip() and not line.startswith("            ") and not _MODEL_RE.match(line):
            in_patterns, current, in_sql = False, None, False
            md = _MODEL_D1_RE.match(line)
            if md:
                model_d1 = md.group(1)
            continue
        ps = _PATTERN_START_RE.match(line)
        if ps:
            current = Pattern(model=model, pattern_id=ps.group(1), d1_table=model_d1,
                              verified_rows_line=-1, declared_rows=-1)
            patterns.append(current)
            in_sql = False
            continue
        if current is None:
            continue
        if in_sql:
            if line.startswith("                ") or not line.strip():
                current.sql_lines.append(line[16:])
                continue
            in_sql = False  # 14칸 필드로 복귀
        pd = _PATTERN_D1_RE.match(line)
        if pd:
            current.d1_table = pd.group(1)   # 패턴 레벨 d1_table 이 모델 레벨을 이긴다(geo_grid)
            continue
        if re.match(r"^ {14}(question_ko|axes|insight_sample_ko):", line):
            current.hint_lines.append(line.strip())
            continue
        vr = _VERIFIED_ROWS_RE.match(line)
        if vr:
            current.verified_rows_line = i
            current.declared_rows = int(vr.group(1))
            current.has_verified_at = i + 1 < len(lines) and lines[i + 1].startswith("              verified_at:")
            continue
        if _SQL_START_RE.match(line):
            in_sql = True
    return patterns


# ── 파라미터 치환(관용 추출 — 실패 시 실행 포기가 원칙) ────────────────────────
def _executable_sql(sql_text: str) -> str:
    """`--` 라인 주석과 `/* :y */` 인라인 주석 제거 — 후자는 값이 이미 SQL 상수로 박힌 형."""
    stripped = re.sub(r"/\*.*?\*/", " ", sql_text)
    return "\n".join(re.sub(r"--.*$", "", line) for line in stripped.splitlines())


def resolve_params(sql_text: str, hint_text: str = "") -> tuple[str, dict[str, str], list[str]]:
    """(치환된 실행문, 사용값, 미해결 이름들).

    예시값 탐색 순서: sql 본문(주석 포함) → 힌트(question_ko/axes/insight_sample_ko —
    ':gu='성동구' 실행' / '(:from_status='01' 검증)' 형이 실존). 못 찾으면 실행 포기.
    """
    executable = _executable_sql(sql_text)
    names = sorted(set(_PLACEHOLDER_RE.findall(executable)), key=len, reverse=True)
    resolved: dict[str, str] = {}
    unresolved: list[str] = []
    for name in names:
        value = None
        for source in (sql_text, hint_text):
            for m in re.finditer(rf":{name}(?![a-z0-9_])", source):
                tail = source[m.end():m.end() + 40]
                vm = re.match(r"[^'0-9]{0,16}('(?:[^']|'')*'|[0-9]+(?:\.[0-9]+)?)", tail)
                if vm:
                    value = vm.group(1)
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


# 관용 추출이 못 읽거나 잘못 짚는 주석 문법의 명시 교정 — 값 출처는 각 패턴의 yml 주석·설명
# (범위형 '37.54 ~ 37.57', 두 값 나열 ':gu_a, :gu_b', 산술식 ':lat - 0.005', '||' 연접의 '%',
#  영문 코드 컬럼(축산=livestock·식품=food — D1 실측 대조)). 추측값 없음.
_PARAM_OVERRIDES: dict[str, dict[str, str]] = {
    "d1_geo_grid_detail/bbox_heatmap_slice": {
        "min_lat": "37.54", "max_lat": "37.57", "min_lng": "126.91", "max_lng": "126.94",
        "category": "'food'", "min_cnt": "10",
    },
    "d1_dong_category_matrix/gu_category_cross": {"gu_a": "'강남구'", "gu_b": "'마포구'"},
    "d1_dong_category_matrix/gu_category_concentration": {"category_ko": "'숙박'"},
    "d1_geo_grid_detail/point_area_category_mix": {"lat": "37.4979", "lng": "127.0276"},
    "d1_gu_specialization/category_top_gus": {"category": "'livestock'"},
    "d1_gu_specialization/category_volume_gus": {"category": "'food'"},
    "d1_multi_site/keyword_slice": {"q": "'약국'"},
}


# ── 실행·기록 ────────────────────────────────────────────────────────────────
def run(yml_path: Path, *, execute: bool, apply: bool, only: str | None = None) -> dict:
    import os

    from gold import serving_export as se

    text = yml_path.read_text(encoding="utf-8")
    lines = text.split("\n")
    patterns = parse_patterns(lines)
    if only:   # 부분 재검증(#179) — key(d1_table/pattern_id) 부분 일치만 실행, 나머지는 무접촉
        patterns = [p for p in patterns if only in p.key]
    bad = [p.key for p in patterns if p.verified_rows_line < 0 or not p.d1_table or not p.sql_lines]
    if bad:
        raise SystemExit(f"파싱 실패(수동 확인 필요): {bad[:5]} … {len(bad)}건")

    report: dict = {"total": len(patterns), "verified": 0, "skipped": [], "failed": [],
                    "row_drift": [], "applied": False}

    token = ""
    publication_ids: dict[str, str] = {}
    if execute or apply:
        token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
        if not token:
            raise SystemExit("CLOUDFLARE_API_TOKEN 미설정 — --env-file 또는 프로세스 env 필요")
        state = se._d1("SELECT table_name, publication_id FROM d1_publish_state;", token)
        publication_ids = {str(r.get("table_name")): str(r.get("publication_id") or "")
                           for r in state}

    verified_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    results: dict[str, tuple[int, str]] = {}   # key -> (measured_rows, publication_id)

    for p in patterns:
        overrides = _PARAM_OVERRIDES.get(p.key, {})
        substituted, resolved, unresolved = resolve_params(p.sql_text, "\n".join(p.hint_lines))
        if overrides:
            resolved = {**resolved, **overrides}
            unresolved = [name for name in unresolved if name not in overrides]
            executable = _executable_sql(p.sql_text)
            for name in sorted(resolved, key=len, reverse=True):
                executable = re.sub(rf":{name}(?![a-z0-9_])", resolved[name], executable)
            substituted = executable.strip()
        if unresolved:
            report["skipped"].append({"key": p.key, "reason": f"파라미터 예시값 미해결: {unresolved}"})
            continue
        head = substituted.lstrip().lower()
        if not (head.startswith("select") or head.startswith("with")):
            report["skipped"].append({"key": p.key, "reason": "SELECT/WITH 외 문장 — 실행 거부(§20)"})
            continue
        if not (execute or apply):
            report["verified"] += 1   # dry-run: 실행 가능 판정까지
            continue
        publication_id = publication_ids.get(p.d1_table, "")
        if not publication_id:
            report["skipped"].append({"key": p.key, "reason": "d1_publish_state 에 게시본 id 없음"})
            continue
        try:
            rows = se._d1(substituted.rstrip(";") + ";", token)  # security: allow-sql — 검증된 참조 구현(SELECT 한정), 값 치환은 예시 상수
        except Exception as exc:  # noqa: BLE001 — 개별 실패는 보고하고 계속
            report["failed"].append({"key": p.key, "reason": f"{type(exc).__name__}", "params": resolved})
            continue
        measured = len(rows)
        if measured != p.declared_rows:
            report["row_drift"].append({"key": p.key, "declared": p.declared_rows, "measured": measured})
        results[p.key] = (measured, publication_id)
        report["verified"] += 1

    if apply and results:
        # 아래에서 위로 삽입해 라인 좌표를 보존한다. verified_rows 값도 실측으로 갱신.
        for p in sorted(patterns, key=lambda x: x.verified_rows_line, reverse=True):
            if p.key not in results:
                continue
            measured, publication_id = results[p.key]
            i = p.verified_rows_line
            lines[i] = f"              verified_rows: {measured}"
            stamp = [f'              verified_at: "{verified_at}"',
                     f'              verified_publication_id: "{publication_id}"']
            if p.has_verified_at:   # 재실행 — 기존 두 줄 교체
                lines[i + 1:i + 3] = stamp
            else:
                lines[i + 1:i + 1] = stamp
        yml_path.write_text("\n".join(lines), encoding="utf-8", newline="\n")
        report["applied"] = True

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--yml", default=None, help="commerce gold 모델 yml 경로(기본: $COMMERCE_DBT_PROJECT_DIR)")
    parser.add_argument("--env-file", default=None, help="로컬 실행용 root .env(값 미출력, setdefault)")
    parser.add_argument("--execute", action="store_true", help="D1 실행 + 보고(yml 미기록)")
    parser.add_argument("--apply", action="store_true", help="D1 실행 + yml 에 verified_* 기록")
    parser.add_argument("--only", default=None,
                        help="key(d1_table/pattern_id) 부분 일치 필터 — 바뀐 패턴만 재검증할 때")
    args = parser.parse_args()

    _bootstrap(args.env_file)
    import os

    yml_path = Path(args.yml) if args.yml else (
        Path(os.getenv("COMMERCE_DBT_PROJECT_DIR", "/opt/airflow/dbt/domains/commerce"))
        / "models" / "gold" / "_commerce_gold__models.yml")
    report = run(yml_path, execute=args.execute or args.apply, apply=args.apply, only=args.only)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    ok = not report["failed"] and not report["skipped"]
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
