"""usage_pattern SQL 정적 보안 감사 CLI — dbt yml 전건을 게시 전/CI 에서 검사.

게이트웨이는 저장 패턴 SQL 을 공유 D1 에 verbatim 실행하며 테이블 스코프를 검사하지
않는다(값 bind 만). 이 CLI 는 게시 전에 각 패턴이 commerce 소유 d1_* 만 읽는 읽기 전용
단일문인지 확인한다. 정본 로직·allowlist 는 `gold.pattern_audit`(SERVING_SPEC 파생).

실행:
  python scripts/audit_pattern_sql.py                     # $COMMERCE_DBT_PROJECT_DIR 의 yml
  python scripts/audit_pattern_sql.py --yml <path>
  python scripts/audit_pattern_sql.py --self-test         # 감사기 자체 검증(적대 케이스)
종료코드: 위반 있으면 1(CI 차단), 없으면 0.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):   # Windows 콘솔(cp949)에서도 한글 리포트 출력
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import yaml  # noqa: E402

from gold.pattern_audit import audit_pattern_sql, commerce_allowlist  # noqa: E402


def _iter_patterns(yml_path: Path):
    d = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
    for m in d.get("models", []):
        sv = m.get("config", {}).get("meta", {}).get("serving", {})
        for p in (sv.get("usage_patterns") or []):
            yield m["name"], p


def run(yml_path: Path) -> int:
    allow = commerce_allowlist()
    total = 0
    flagged: list[tuple[str, str, list[str]]] = []
    for model, p in _iter_patterns(yml_path):
        total += 1
        f = audit_pattern_sql(p.get("sql") or "", allow)
        if f:
            flagged.append((model, str(p.get("pattern_id")), f))
    report = {"yml": str(yml_path), "audited": total, "allowlist_size": len(allow),
              "flagged": [{"model": m, "pattern_id": pid, "violations": v} for m, pid, v in flagged]}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if flagged else 0


# ── 감사기 자체 검증(적대 케이스) — 방어가 실제로 막는지 확인 ──────────────────
_SELF_TEST = [
    ("clean_direct", "SELECT * FROM d1_churn_yearly WHERE y = :y", True),
    ("clean_cte", "WITH s AS (SELECT y FROM d1_flow_yearly) SELECT * FROM s", True),
    ("clean_join", "SELECT a.gu_code FROM d1_churn_yearly a JOIN d1_gu_specialization b "
                   "ON a.gu_code = b.gu_code", True),
    ("clean_limit_n", "SELECT gu_code FROM d1_churn_yearly ORDER BY 1 LIMIT :n", True),
    ("param_named_from", "SELECT y FROM d1_churn_yearly WHERE y BETWEEN :from AND :to", True),
    ("json_each_array_in", "SELECT gu_code FROM d1_churn_yearly WHERE gu_code IN "
                           "(SELECT value FROM json_each(:gus))", True),
    ("explicit_join_cte", "WITH pk AS (SELECT dataset FROM d1_seasonality) "
                          "SELECT o.dataset FROM pk o JOIN pk c ON o.dataset=c.dataset", True),
    ("leak_keys", "SELECT key_hash FROM _keys", False),
    # 레드팀 확증 우회(2026-08-08) — 콤마 조인 계열. 전부 차단되어야 한다.
    ("comma_join_keys", "SELECT k.key_hash, k.email FROM d1_gu_specialization c, _keys k", False),
    ("comma_join_usage", "SELECT u.key_hash FROM d1_churn_yearly c, _usage u LIMIT :n", False),
    ("comma_join_other_domain", "SELECT * FROM d1_churn_yearly, d1_weather_daily", False),
    ("comma_join_handoff", "SELECT p.sql FROM d1_churn_yearly c, d1_usage_patterns p", False),
    ("comma_join_pragma_tvf", "SELECT t.name FROM d1_churn_yearly c, "
                              "pragma_table_info('_keys') t", False),
    ("derived_then_comma_keys", "SELECT * FROM (SELECT 1) x, _keys", False),
    ("scalar_subquery_comma", "SELECT c.gu_code, (SELECT k.email FROM d1_flow_yearly x, "
                              "_keys k LIMIT 1) AS leak FROM d1_churn_yearly c", False),
    ("schema_qualified_keys", "SELECT email FROM d1_churn_yearly c, main._keys", False),
    ("leak_other_domain", "SELECT * FROM d1_weather_daily", False),
    ("stacked", "SELECT 1 FROM d1_churn_yearly; DROP TABLE _keys", False),
    ("pragma", "SELECT * FROM pragma_table_info('_keys')", False),
    ("attach", "ATTACH DATABASE 'x' AS y; SELECT 1", False),
    ("comment_hide_join", "SELECT * FROM d1_churn_yearly /* */ JOIN _usage u ON 1=1", False),
    ("bad_limit_name", "SELECT * FROM d1_churn_yearly LIMIT :rows", False),
    ("write", "DELETE FROM d1_churn_yearly", False),
]


def self_test() -> int:
    allow = commerce_allowlist()
    fails = []
    for name, sql, should_pass in _SELF_TEST:
        f = audit_pattern_sql(sql, allow)
        passed = not f
        mark = "OK " if passed == should_pass else "XX "
        if passed != should_pass:
            fails.append((name, should_pass, f))
        print(f"  {mark}{name}: {'통과' if passed else '차단'} "
              f"(기대 {'통과' if should_pass else '차단'}) {f if f else ''}")
    if fails:
        print(f"SELF-TEST FAIL: {len(fails)}건 기대 불일치")
        return 1
    print(f"SELF-TEST OK: {len(_SELF_TEST)}건 전부 기대대로")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--yml", default=None)
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        return self_test()
    yml_path = Path(args.yml) if args.yml else (
        Path(os.getenv("COMMERCE_DBT_PROJECT_DIR", "/opt/airflow/dbt/domains/commerce"))
        / "models" / "gold" / "_commerce_gold__models.yml")
    return run(yml_path)


if __name__ == "__main__":
    raise SystemExit(main())
