"""4개 도메인 D1 스키마·데이터 덤프(read-only) + 로컬 SQLite 레플리카 — 패턴 저작용.

commerce 의 d1_dump.py + build_replica.py 를 도메인 무관하게 합친 판. 각 도메인 제품
테이블(gold_<domain>_*)의: PRAGMA 스키마 · 행수 · 샘플 3행 · TEXT 컬럼 카디널리티/distinct ·
수치/시간 컬럼 min/max 를 덤프하고, 로컬 레플리카(작은 표 전량 / 큰 표 표본)를 만든다.
전부 SELECT/PRAGMA — 상태 변경 없음.
"""
import io, json, os, sqlite3, sys, time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
ROOT = Path(r"c:\Users\Dell3571\IdeaProjects\final_seoul\sample")
sys.path.insert(0, str(ROOT / "dags" / "domains" / "commerce" / "include"))
sys.path.insert(0, str(ROOT / "dags"))
for raw in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
    line = raw.strip()
    if line and not line.startswith("#") and "=" in line:
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
from commerce_core.env import load_commerce_env  # noqa: E402
load_commerce_env()
from security import install_security  # noqa: E402
install_security()
from gold import serving_export as se  # noqa: E402

TOKEN = os.environ["CLOUDFLARE_API_TOKEN"]
BASE = Path(__file__).resolve().parent
FACTS = json.loads((BASE.parent / "domain_conversion" / "domain_facts.json").read_text(encoding="utf-8"))


def q(sql, tries=5):
    for a in range(tries):
        try:
            time.sleep(0.15)
            return se._d1(sql, TOKEN)
        except Exception:  # noqa: BLE001
            if a == tries - 1:
                raise
            time.sleep(2.0 * (a + 1))


for dom, f in FACTS.items():
    tables = f["tables"]
    meta_dir = BASE / dom / "d1_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    replica = BASE / dom / "replica.sqlite"
    replica.unlink(missing_ok=True)
    conn = sqlite3.connect(replica)
    index = {}
    for t in tables:
        info = q(f'PRAGMA table_info("{t}");')
        cols = [{"name": r["name"], "type": r["type"]} for r in info]
        if not cols:
            continue
        n = q(f'SELECT COUNT(*) AS n FROM "{t}";')[0]["n"]
        sample = q(f'SELECT * FROM "{t}" LIMIT 3;')
        text_cols = [c["name"] for c in cols if "CHAR" in c["type"].upper() or "TEXT" in c["type"].upper()]
        num_cols = [c["name"] for c in cols if c["name"] not in text_cols]
        card = {}
        if text_cols:
            expr = ", ".join(f'COUNT(DISTINCT "{c}") AS "{c}"' for c in text_cols)
            card = q(f'SELECT {expr} FROM "{t}";')[0]
        distincts = {}
        for c, k in card.items():
            if k is not None and k <= 60:
                rows = q(f'SELECT DISTINCT "{c}" AS v FROM "{t}" WHERE "{c}" IS NOT NULL ORDER BY v LIMIT 60;')
                distincts[c] = [r["v"] for r in rows]
        ranges = {}
        if num_cols:
            expr = ", ".join(f'MIN("{c}") AS "min_{c}", MAX("{c}") AS "max_{c}"' for c in num_cols[:16])
            row = q(f'SELECT {expr} FROM "{t}";')[0]
            for c in num_cols[:16]:
                ranges[c] = [row.get(f"min_{c}"), row.get(f"max_{c}")]
        for c in text_cols:
            if any(tok in c.lower() for tok in ("date", "_y", "ym", "hour", "dow", "time", "month")):
                row = q(f'SELECT MIN("{c}") AS lo, MAX("{c}") AS hi FROM "{t}";')[0]
                ranges[c] = [row["lo"], row["hi"]]
        (meta_dir / f"{t}.json").write_text(json.dumps(
            {"table": t, "row_count": n, "columns": cols, "sample_rows": sample,
             "text_cardinality": card, "distinct_values": distincts, "numeric_ranges": ranges},
            ensure_ascii=False, indent=1), encoding="utf-8")
        # replica
        ddl = ", ".join(f'"{c["name"]}" {c["type"] or "TEXT"}' for c in cols)
        conn.execute(f'CREATE TABLE "{t}" ({ddl})')
        rows = []
        if n <= 5000:
            for off in range(0, n, 1500):
                rows.extend(q(f'SELECT * FROM "{t}" LIMIT 1500 OFFSET {off};'))
        else:
            stride = max(1, n // 4500)
            for r in range(4):
                rows.extend(q(f'SELECT * FROM "{t}" WHERE (rowid % {stride}) = {r} LIMIT 1300;'))
        names = [c["name"] for c in cols]
        ph = ", ".join("?" for _ in names)
        conn.executemany(
            f'INSERT INTO "{t}" ({", ".join(chr(34) + c + chr(34) for c in names)}) VALUES ({ph})',
            [[row.get(c) for c in names] for row in rows])
        conn.commit()
        index[t] = {"rows": n, "cols": len(cols), "replica_rows": len(rows)}
        print(f"{dom}/{t}: {n} rows -> replica {len(rows)}", flush=True)
    (meta_dir / "_index.json").write_text(json.dumps(index, ensure_ascii=False, indent=1), encoding="utf-8")
    conn.close()
print("DONE")
