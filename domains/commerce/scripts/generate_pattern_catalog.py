"""usage_patterns 제공정보 카탈로그 생성 — "이 패턴이 무슨 정보를 주는가" 문서.

dbt yml(정본)을 읽어 D1 테이블별로 패턴 목록을 표로 렌더한다: 질문(question_ko),
제공 정보(provides_ko — 없으면 질문으로 대체), 파라미터(:name 추출), 조합 관용구,
검증 상태. 손으로 고치지 말 것 — yml 을 고치고 재생성한다.

실행:
  python scripts/generate_pattern_catalog.py --yml <path> [--out <md_path>]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import yaml  # noqa: E402

_COMMENT_RE = re.compile(r"--[^\n]*|/\*[\s\S]*?\*/")
_PLACEHOLDER_RE = re.compile(r":([a-z_][a-z0-9_]*)")


def _params(sql: str) -> list[str]:
    body = _COMMENT_RE.sub(" ", sql or "")
    seen: list[str] = []
    for name in _PLACEHOLDER_RE.findall(body):
        if name not in seen:
            seen.append(name)
    return seen


def _cell(s: str | None) -> str:
    return (s or "").replace("|", "\\|").replace("\n", " ").strip()


def render(yml_path: Path) -> str:
    d = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
    by_table: dict[str, list[dict]] = {}
    for m in d.get("models", []):
        sv = m.get("config", {}).get("meta", {}).get("serving", {})
        default_table = sv.get("d1_table")
        for p in (sv.get("usage_patterns") or []):
            t = p.get("d1_table") or (default_table if isinstance(default_table, str) else "?")
            by_table.setdefault(t, []).append(p)

    total = sum(len(v) for v in by_table.values())
    verified = sum(1 for v in by_table.values() for p in v if p.get("verified_at"))
    out: list[str] = []
    out.append("# commerce D1 질의 패턴 카탈로그 — 무엇을 물으면 무엇을 주는가")
    out.append("")
    out.append("> **생성물이다. 손으로 고치지 말 것.** 정본은 "
               "`models/gold/_commerce_gold__models.yml` 의 `usage_patterns` 이며, "
               "`python dags/domains/commerce/scripts/generate_pattern_catalog.py` 로 재생성한다.")
    out.append("")
    out.append(f"패턴 {total}건 / D1 테이블 {len(by_table)}종 / 검증 완료 {verified}건. "
               "각 패턴은 게이트웨이 `run_pattern`(REST `GET /api/v1/patterns/<product>/<pattern>` · "
               "MCP `run_pattern`)으로 실행하며, `파라미터` 열의 이름 전부에 값을 줘야 한다"
               "(모든 파라미터 필수 — 기본값 없음).")
    out.append("")
    for t in sorted(by_table):
        pats = by_table[t]
        product = "commerce_" + t[3:]
        out.append(f"## {t} (`{product}`) — {len(pats)}건")
        out.append("")
        out.append("| pattern_id | 질문 | 제공 정보 | 파라미터 | 관용구 |")
        out.append("|---|---|---|---|---|")
        for p in sorted(pats, key=lambda x: str(x.get("pattern_id"))):
            params = ", ".join(f"`:{n}`" for n in _params(p.get("sql") or "")) or "—"
            idiom = _cell(p.get("composition_idiom")) or "—"
            provides = _cell(p.get("provides_ko")) or _cell(p.get("question_ko"))
            out.append(f"| `{_cell(str(p.get('pattern_id')))}` "
                       f"| {_cell(p.get('question_ko'))} "
                       f"| {provides} | {params} | {idiom} |")
        out.append("")
    return "\n".join(out) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--yml", required=True)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    md = render(Path(args.yml))
    if args.out:
        Path(args.out).write_text(md, encoding="utf-8", newline="\n")
        print(f"written: {args.out}")
    else:
        print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
