"""도메인 레플리카(로컬 SQLite)에 후보 패턴 SQL 실행 — 저작 검증용(무제한, 네트워크 없음).

사용: python local_run.py <replica.sqlite> <candidates.json> [--out results.json]
candidates.json: [{"key":"...","sql":"... :y ...","combos":[{"y":"2025"}, ...]}]
게이트웨이 에뮬레이션: 주석 제거, SELECT/WITH 만, 전 :param 필수, 값은 리터럴 치환.
"""
import json, re, sqlite3, sys
from pathlib import Path

PLACEHOLDER = re.compile(r":([a-z_][a-z0-9_]*)")


def executable(sql):
    s = re.sub(r"/\*[\s\S]*?\*/", " ", sql)
    return "\n".join(re.sub(r"--.*$", "", ln) for ln in s.splitlines()).strip()


def substitute(sql, combo):
    names = set(PLACEHOLDER.findall(sql))
    missing = [n for n in names if n not in combo]
    if missing:
        raise ValueError(f"missing params: {missing}")
    extra = [k for k in combo if k not in names]
    if extra:
        raise ValueError(f"undeclared params: {extra}")

    def lit(v):
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return str(v)
        return "'" + str(v).replace("'", "''") + "'"
    for n in sorted(names, key=len, reverse=True):
        sql = re.sub(rf":{n}(?![a-z0-9_])", lit(combo[n]), sql)
    return sql


def main():
    replica = Path(sys.argv[1])
    src = Path(sys.argv[2])
    out = Path(sys.argv[sys.argv.index("--out") + 1]) if "--out" in sys.argv else src.with_suffix(".results.json")
    conn = sqlite3.connect(replica)
    conn.row_factory = sqlite3.Row
    results = []
    for c in json.loads(src.read_text(encoding="utf-8")):
        body = executable(c["sql"])
        if not re.match(r"^(select|with)\b", body, re.I):
            results.append({"key": c["key"], "ok": False, "error": "not SELECT/WITH"})
            continue
        for i, combo in enumerate(c.get("combos") or [{}]):
            e = {"key": c["key"], "combo_index": i, "combo": combo}
            try:
                rows = conn.execute(substitute(body, combo)).fetchall()
                e.update(ok=True, rows=len(rows), first_row=dict(rows[0]) if rows else None)
            except Exception as exc:  # noqa: BLE001
                e.update(ok=False, error=str(exc)[:200])
            results.append(e)
            print(f"{c['key']}[{i}] -> {'OK ' + str(e.get('rows')) if e.get('ok') else 'FAIL: ' + e.get('error','')}")
    out.write_text(json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")
    ok = sum(1 for r in results if r.get("ok") and r.get("rows", 0) > 0)
    print(f"RESULT ok_nonempty={ok} total={len(results)} -> {out}")


if __name__ == "__main__":
    main()
