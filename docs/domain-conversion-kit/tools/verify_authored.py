"""저작된 후보 패턴을 ① 정적 보안 감사 ② 실 D1 read-검증 — 도메인별.

각 도메인 candidates/*.json 를 모아 (a) 그 도메인 gold_ 테이블 allowlist 로 pattern_audit,
(b) 감사 통과분을 실 D1 에 combos 로 실행(read-only) 해 0행 초과·실패를 판정한다.
통과·비0행 후보만 accepted.json 으로. yml·D1 쓰기 없음.

실행: python verify_authored.py <domain> --env-file <root .env>
"""
import io, json, os, re, sys, time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
ROOT = Path(r"c:\Users\Dell3571\IdeaProjects\final_seoul\sample")
BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "dags" / "domains" / "commerce" / "include"))
sys.path.insert(0, str(ROOT / "dags"))

PLACEHOLDER = re.compile(r":([a-z_][a-z0-9_]*)")


def load_env(p):
    for raw in Path(p).read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if s and not s.startswith("#") and "=" in s:
            k, _, v = s.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def executable(sql):
    s = re.sub(r"/\*[\s\S]*?\*/", " ", sql)
    return "\n".join(re.sub(r"--.*$", "", ln) for ln in s.splitlines()).strip()


def substitute(sql, combo):
    body = executable(sql)
    names = set(PLACEHOLDER.findall(body))
    missing = [n for n in names if n not in combo]
    if missing:
        raise ValueError(f"missing {missing}")

    def lit(v):
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return str(v)
        return "'" + str(v).replace("'", "''") + "'"
    for n in sorted(names, key=len, reverse=True):
        body = re.sub(rf":{n}(?![a-z0-9_])", lit(combo[n]), body)
    return body


def main():
    dom = sys.argv[1]
    if "--env-file" in sys.argv:
        load_env(sys.argv[sys.argv.index("--env-file") + 1])
    load_from = BASE / dom / "candidates"
    from commerce_core.env import load_commerce_env
    load_commerce_env()
    from security import install_security
    install_security()
    from gold import serving_export as se
    from gold.pattern_audit import audit_pattern_sql
    token = os.environ["CLOUDFLARE_API_TOKEN"]

    # 도메인 allowlist = 이 도메인 전 gold_ 테이블 (덤프 인덱스에서)
    idx = json.loads((BASE / dom / "d1_meta" / "_index.json").read_text(encoding="utf-8"))
    allow = frozenset(str(t).lower() for t in idx.keys())

    accepted, audit_fail, run_fail, zero = [], [], [], []
    files = sorted(load_from.glob("*.json")) if load_from.exists() else []
    for f in files:
        doc = json.loads(f.read_text(encoding="utf-8"))
        for p in doc.get("new_patterns", []):
            key = f"{doc.get('table')}/{p['pattern_id']}"
            findings = audit_pattern_sql(p.get("sql") or "", allow)
            if findings:
                audit_fail.append({"key": key, "findings": findings})
                continue
            combos = p.get("combos") or [{}]
            row_counts = []
            failed = False
            for i, combo in enumerate(combos):
                try:
                    time.sleep(0.12)
                    rows = se._d1(substitute(p["sql"], combo).rstrip(";") + ";", token)
                    row_counts.append(len(rows))
                except Exception as exc:  # noqa: BLE001
                    run_fail.append({"key": key, "combo": i, "error": type(exc).__name__})
                    failed = True
                    break
            if failed:
                continue
            if not row_counts or row_counts[0] == 0:
                zero.append({"key": key, "rows_by_combo": row_counts})
                continue
            accepted.append({**p, "table": doc.get("table"), "product_id": doc.get("product_id"),
                             "real_rows_by_combo": row_counts})
            print(f"  OK {key} rows={row_counts}")
    out = {"domain": dom, "accepted": accepted, "audit_fail": audit_fail,
           "run_fail": run_fail, "zero_first_combo": zero,
           "n_accepted": len(accepted)}
    (BASE / dom / "accepted.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"domain": dom, "accepted": len(accepted), "audit_fail": len(audit_fail),
                      "run_fail": len(run_fail), "zero": len(zero)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
