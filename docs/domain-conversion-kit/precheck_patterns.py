"""usage_patterns read-only 프리체크 — 실 D1 실행·행수 확인(상태 변경 없음).

**목적**: 도메인 전환 전에 "이 도메인의 패턴들이 실제로 실행되고 행을 반환하는가"를 확인해
나중에 verified_* 스탬핑을 기계적으로 만든다. yml 도 D1 도 **쓰지 않는다**(SELECT 만).

**commerce 코드에서 일반화한 부분**:
- 예시값 추출: commerce `resolve_params` 는 따옴표 문자열/숫자만 읽었는데, 타 도메인은
  `-- :area=성수동, :date=2026-07-30` 처럼 **따옴표 없는 값**을 쓴다. 이 판은 bare 값도
  읽어(숫자가 아니면 문자열로 quote) 치환한다.
- 게이트웨이와 동일하게 주석 제거 후 실행문을 만든다.

실행:
  python precheck_patterns.py --source "domains/citydata/models/gold/*.yml" --env-file <root .env>
"""
from __future__ import annotations

import argparse
import glob as globmod
import io
import json
import os
import re
import sys
import time
from pathlib import Path

import yaml

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

_COMMENT_LINE = re.compile(r"--([^\n]*)")
_BLOCK = re.compile(r"/\*[\s\S]*?\*/")
_ASSIGN = re.compile(r":([a-z_][a-z0-9_]*)\s*=\s*('(?:[^']|'')*'|[^,\s][^,]*?)(?=\s*(?:,|$))", re.I)
_PLACEHOLDER = re.compile(r":([a-z_][a-z0-9_]*)")


def _load_env(path):
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _executable(sql):
    stripped = _BLOCK.sub(" ", sql)
    return "\n".join(re.sub(r"--.*$", "", ln) for ln in stripped.splitlines()).strip()


def _example_values(sql):
    """선행 `-- :a=1, :b=성수동` 주석에서 예시값을 읽는다. bare 값도 처리."""
    vals = {}
    for m in _COMMENT_LINE.finditer(sql):
        for a in _ASSIGN.finditer(m.group(1)):
            vals[a.group(1)] = a.group(2).strip()
    return vals


def _as_literal(v):
    v = v.strip()
    if re.fullmatch(r"-?\d+(\.\d+)?", v):
        return v
    if v.startswith("'") and v.endswith("'"):
        return v
    return "'" + v.replace("'", "''") + "'"


def _substitute(sql, values):
    body = _executable(sql)
    names = set(_PLACEHOLDER.findall(body))
    missing = [n for n in names if n not in values]
    if missing:
        return None, missing
    for n in sorted(names, key=len, reverse=True):
        body = re.sub(rf":{n}(?![a-z0-9_])", _as_literal(values[n]).replace("\\", r"\\"), body)
    return body, []


def _serving_of(m):
    return (m.get("config", {}) or {}).get("meta", {}).get("serving", {}) \
        or (m.get("meta", {}) or {}).get("serving", {}) or {}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", nargs="+", required=True)
    ap.add_argument("--env-file", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    if args.env_file:
        _load_env(args.env_file)

    ROOT = Path("dags")
    sys.path.insert(0, str(ROOT / "domains" / "commerce" / "include"))
    sys.path.insert(0, str(ROOT))
    from commerce_core.env import load_commerce_env
    load_commerce_env()
    from security import install_security
    install_security()
    from gold import serving_export as se
    token = os.environ["CLOUDFLARE_API_TOKEN"]

    files = []
    for s in args.source:
        files.extend(sorted(globmod.glob(s, recursive=True)))
    pats = []
    for f in files:
        try:
            d = yaml.safe_load(open(f, encoding="utf-8"))
        except Exception:
            continue
        for m in (d.get("models") or []):
            sv = _serving_of(m)
            for p in (sv.get("usage_patterns") or []):
                if p.get("sql") and p.get("pattern_id"):
                    pats.append(p)

    results = []
    ok = skip = fail = 0
    for p in pats:
        vals = _example_values(p["sql"])
        body, missing = _substitute(p["sql"], vals)
        if body is None:
            results.append({"pattern_id": p["pattern_id"], "status": "skip",
                            "reason": f"예시값 미해결 {missing}"})
            skip += 1
            continue
        try:
            time.sleep(0.15)
            rows = se._d1(body.rstrip().rstrip(";") + ";", token)
            n = len(rows)
            results.append({"pattern_id": p["pattern_id"], "status": "ok", "rows": n,
                            "declared_rows": p.get("verified_rows")})
            ok += 1
        except Exception as exc:  # noqa: BLE001
            results.append({"pattern_id": p["pattern_id"], "status": "fail",
                            "reason": type(exc).__name__})
            fail += 1
        print(f"  {results[-1]['status']:>4} {p['pattern_id']}"
              f"{' rows=' + str(results[-1].get('rows')) if results[-1]['status']=='ok' else ''}")
    report = {"total": len(pats), "ok": ok, "skip": skip, "fail": fail, "results": results}
    if args.out:
        Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("total", "ok", "skip", "fail")}, ensure_ascii=False))
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
