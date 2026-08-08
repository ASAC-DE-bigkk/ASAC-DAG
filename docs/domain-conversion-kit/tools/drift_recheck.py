"""드리프트 재검증 하네스 (org 공통) — 게시된 패턴이 현재 D1 데이터와 어긋났는지 감지.

#217 [DAG] "드리프트 재검증(데이터 리프레시마다)" 의 사전 작업물. 데이터가 갱신되면 게시된
패턴 SQL 이 스키마/의미와 어긋나 게이트웨이가 500(`pattern execution failed`)을 낼 수 있다.
이 하네스는 **전 도메인 게시 패턴을 실 D1 에 read-only 로 전량 실행**해 (a) 실행 실패(드리프트
후보)와 (b) 선언 행수 대비 실측 행수 변화(row_drift)를 수집한다. **읽기만** 한다 — yml·D1 무변경.

작성은 계정 불필요(#217 B표), 실행은 운영 D1 **읽기** 권한만 필요(쓰기 아님).

예시값: 패턴 SQL 첫 줄 `-- :a=1, :b=성수동` 주석에서 추출(bare 값 포함, precheck 와 동일 규칙).

실행:
  python drift_recheck.py --source "domains/**/*.yml" --env-file <root .env> --out drift.json
종료코드: 실행 실패(드리프트 후보) 있으면 1.
"""
import argparse, glob as gmod, io, json, os, re, sys, time
from pathlib import Path

import yaml

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

_COMMENT_LINE = re.compile(r"--([^\n]*)")
_BLOCK = re.compile(r"/\*[\s\S]*?\*/")
_ASSIGN = re.compile(r":([a-z_][a-z0-9_]*)\s*=\s*('(?:[^']|'')*'|[^,\s][^,]*?)(?=\s*(?:,|$))", re.I)
_PH = re.compile(r":([a-z_][a-z0-9_]*)")


def _load_env(p):
    for raw in Path(p).read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if s and not s.startswith("#") and "=" in s:
            k, _, v = s.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _exec(sql):
    s = _BLOCK.sub(" ", sql)
    return "\n".join(re.sub(r"--.*$", "", ln) for ln in s.splitlines()).strip()


def _vals(sql):
    out = {}
    for m in _COMMENT_LINE.finditer(sql):
        for a in _ASSIGN.finditer(m.group(1)):
            out[a.group(1)] = a.group(2).strip()
    return out


def _lit(v):
    v = v.strip()
    if re.fullmatch(r"-?\d+(\.\d+)?", v):
        return v
    return v if (v.startswith("'") and v.endswith("'")) else "'" + v.replace("'", "''") + "'"


def _sub(sql, vals):
    body = _exec(sql)
    names = set(_PH.findall(body))
    missing = [n for n in names if n not in vals]
    if missing:
        return None, missing
    for n in sorted(names, key=len, reverse=True):
        body = re.sub(rf":{n}(?![a-z0-9_])", _lit(vals[n]).replace("\\", r"\\"), body)
    return body, []


def _sv(m):
    return (m.get("config", {}) or {}).get("meta", {}).get("serving", {}) \
        or (m.get("meta", {}) or {}).get("serving", {}) or {}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", nargs="+", required=True)
    ap.add_argument("--env-file", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--verified-only", action="store_true", help="verified_at 있는 패턴만(게시본)")
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
        files.extend(sorted(gmod.glob(s, recursive=True)))
    # 방어: 실 D1 실행 전 정적 감사 게이트(precheck 와 동일). 게시 테이블 밖(내부표 등) 참조
    # 패턴은 실행하지 않는다 — 이 하네스가 _keys 유출 벡터가 되지 않게.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from pattern_audit import audit_pattern_sql

    pats = []
    allow_tables = set()
    for f in files:
        try:
            d = yaml.safe_load(open(f, encoding="utf-8"))
        except Exception:
            continue
        for m in (d.get("models") or []):
            sv = _sv(m)
            if not sv:
                continue
            dt = sv.get("d1_table")
            for t in (dt if isinstance(dt, list) else [dt] if dt else [m.get("name")]):
                if t:
                    allow_tables.add(str(t).lower())
            for p in (sv.get("usage_patterns") or []):
                if p.get("d1_table"):
                    allow_tables.add(str(p["d1_table"]).lower())
                if p.get("sql") and p.get("pattern_id"):
                    if args.verified_only and not p.get("verified_at"):
                        continue
                    pats.append(p)
    allow = frozenset(allow_tables)

    drift_fail, row_drift, skipped, blocked = [], [], [], []
    ok = 0
    for p in pats:
        guard = audit_pattern_sql(p["sql"], allow)
        if guard:
            blocked.append({"pattern_id": p["pattern_id"], "reason": guard[:2]})
            continue
        body, missing = _sub(p["sql"], _vals(p["sql"]))
        if body is None:
            skipped.append({"pattern_id": p["pattern_id"], "missing": missing})
            continue
        try:
            time.sleep(0.12)
            rows = se._d1(body.rstrip().rstrip(";") + ";", token)
            ok += 1
            dec = p.get("verified_rows")
            if isinstance(dec, int) and dec != len(rows):
                row_drift.append({"pattern_id": p["pattern_id"], "declared": dec, "measured": len(rows)})
        except Exception as exc:  # noqa: BLE001
            drift_fail.append({"pattern_id": p["pattern_id"], "error": type(exc).__name__})
        print(f"  {'FAIL' if drift_fail and drift_fail[-1]['pattern_id']==p['pattern_id'] else 'ok  '} {p['pattern_id']}")
    report = {"total": len(pats), "ok": ok, "drift_fail": drift_fail,
              "row_drift": row_drift, "skipped": skipped, "blocked": blocked}
    if args.out:
        Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"total": len(pats), "ok": ok, "drift_fail": len(drift_fail),
                      "row_drift": len(row_drift), "skipped": len(skipped),
                      "blocked": len(blocked)}, ensure_ascii=False))
    return 1 if (drift_fail or blocked) else 0


if __name__ == "__main__":
    raise SystemExit(main())
