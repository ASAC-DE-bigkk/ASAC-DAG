"""citydata 서빙 검증 — D1 게시본 대상 usage_pattern 실측 + gold→D1 중복적재 확인 (#638 verified_*).

각 도메인이 **자체 구현**하기로 한 verified_* 검증 경로(#638 점진 적용 — "미확정 도메인은 NULL")의
citydata 판이다. 공용 Publisher 는 게시 시 verified_* 를 NULL 로 싣고(yml 미백필, #638 optional),
이 검증이 **게시 직후 실측해 채운다**. 손 백필 아님(#638 §5-1 — 재실행 실측만).

자립(매니페스트 불필요): d1_usage_patterns 가 이미 ``sql``·``publication_id`` 를 가지므로 D1 만으로:
  1) 각 pattern.sql 을 실제 D1 에 실행 → 반환 행수 = ``verified_rows`` (부풀면 중복적재 신호)
  2) ``verified_at``(UTC now)·``verified_publication_id``(그 행의 publication_id)로 UPDATE
     — 게시가 INSERT OR REPLACE 로 verified_* 를 NULL 로 덮으므로, 게시 직후 UPDATE 로 갱신
  3) 제품 테이블 ``primary_key_stats``(행수 ≠ distinct PK, 또는 PK NULL)로 **gold→D1 중복적재 직접 판정**

읽기전용 서빙 검증이라 SELECT/WITH 만 실행(임의 SQL 거부). 파라미터는 sql 주석의 예시값(우리 선언).
검증 실행 실패는 fail-open(서빙/수집 무영향). D1 미게시(배포 전)면 대상 0 → no-op.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone

_PLACEHOLDER = re.compile(r":([a-z][a-z0-9_]*)")
_NUMERIC = re.compile(r"^-?\d+(?:\.\d+)?$")


def _strip_comments(sql: str) -> str:
    """`-- ` 라인 주석 제거(예시값은 실행 전에 미리 추출)."""
    return "\n".join(re.sub(r"--.*$", "", line) for line in sql.splitlines())


def resolve_params(sql: str) -> tuple[str | None, list[str]]:
    """sql 주석의 ``-- :name=value`` 예시값으로 :placeholder 치환.

    반환 (치환된 실행문, 미해결 이름들). 숫자는 그대로, 그 외(문자열·날짜)는 작은따옴표.
    미해결이 하나라도 있으면 (None, [미해결]) — 추측 실행 금지(검증 증거 오염 방지).
    """
    resolved: dict[str, str] = {}
    for m in re.finditer(r":([a-z][a-z0-9_]*)\s*=\s*([^,\s]+)", sql):
        name, val = m.group(1), m.group(2)
        resolved[name] = val if _NUMERIC.match(val) else "'" + val.replace("'", "''") + "'"
    body = _strip_comments(sql)
    names = sorted(set(_PLACEHOLDER.findall(body)), key=len, reverse=True)
    unresolved = [n for n in names if n not in resolved]
    if unresolved:
        return None, unresolved
    out = body
    for n in names:
        out = re.sub(rf":{n}(?![a-z0-9_])", resolved[n], out)
    return out.strip(), []


def verify_citydata_serving(target: str = "dev", *, prefix: str = "citydata_") -> dict:
    """게시본 D1 대상으로 (1) usage_pattern 실측→verified_* UPDATE (2) 중복적재 판정.

    반환 리포트: verified(실행 성공 패턴 수)·backfilled(UPDATE 수)·dup_load(중복적재 의심 테이블)·
    skipped(파라미터 미해결·SELECT 외)·failed(실행/0행 오류). D1 클라이언트는 env 로 구성.
    """
    from common.serving.d1_client import sql_literal
    from common.serving.runtime import build_d1_client_from_env

    d1 = build_d1_client_from_env()
    # product_id LIKE 'citydata_%' — '_' 는 LIKE 와일드카드지만, product_id 는 전부
    # 'citydata_<name>' 꼴이라 '_' 가 리터럴 구분자를 매칭하고 'citydata' 로 시작하는
    # product_id 는 모두 citydata 도메인이라 오매칭이 없다(누락도 없음).
    like = sql_literal(prefix + "%")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    report: dict = {"verified": 0, "backfilled": 0, "already_current": 0,
                    "dup_load": [], "skipped": [], "failed": []}

    # ── 1) 패턴 실측 + verified_* 백필 (게시본별 증거) ──────────────────────────
    patterns = d1.execute(
        "SELECT product_id, pattern_id, sql, publication_id, allow_empty, "
        "verified_publication_id, verified_rows "
        f"FROM d1_usage_patterns WHERE product_id LIKE {like}"
    )
    for p in patterns:
        pid, patid = p.get("product_id"), p.get("pattern_id")
        key = f"{pid}/{patid}"
        # 게시본 게이트 — 이미 현재 게시본으로 검증됐으면 스킵(게시당 1회, 낭비 방지).
        # 게시가 INSERT OR REPLACE 로 verified_* 를 NULL 로 덮으므로, verified_rows 가
        # 채워져 있고 verified_publication_id 가 현재 publication_id 와 같으면 = 이번 게시본
        # 은 이미 검증 완료. 다음 게시가 NULL 로 되돌리면 그때 다시 채운다.
        if p.get("verified_rows") is not None and \
                p.get("verified_publication_id") == p.get("publication_id"):
            report["already_current"] += 1
            continue
        stmt, unresolved = resolve_params(p.get("sql") or "")
        if unresolved:
            report["skipped"].append({"key": key, "reason": f"파라미터 예시값 미해결: {unresolved}"})
            continue
        head = stmt.lstrip().lower()
        if not (head.startswith("select") or head.startswith("with")):
            report["skipped"].append({"key": key, "reason": "SELECT/WITH 외 — 실행 거부"})
            continue
        try:
            rows = d1.execute(stmt.rstrip(";"))  # security: allow-sql — 서버 저장 검증 패턴(SELECT 한정)
        except Exception as exc:  # noqa: BLE001 — 개별 실패 보고 후 계속
            report["failed"].append({"key": key, "reason": f"실행: {type(exc).__name__}"})
            continue
        n = len(rows)
        report["verified"] += 1
        if n == 0 and not p.get("allow_empty"):
            report["failed"].append({"key": key, "reason": "0행(allow_empty 아님)"})
        pub = sql_literal(p.get("publication_id") or "")
        d1.execute(
            f"UPDATE d1_usage_patterns SET verified_rows={int(n)}, "
            f"verified_at={sql_literal(now)}, verified_publication_id={pub} "
            f"WHERE product_id={sql_literal(pid)} AND pattern_id={sql_literal(patid)}"
        )
        report["backfilled"] += 1

    # ── 2) gold→D1 중복적재 판정 (행수 ≠ distinct PK, 또는 PK NULL) ─────────────
    exts = d1.execute(
        "SELECT product_id, table_name, primary_key FROM d1_catalog_ext "
        f"WHERE product_id LIKE {like}"
    )
    for e in exts:
        table = e.get("table_name")
        try:
            pk = json.loads(e.get("primary_key") or "[]")
            if not pk:
                continue
            rows, distinct_pk, null_pk = d1.primary_key_stats(table, pk)
            if rows != distinct_pk or null_pk:
                report["dup_load"].append(
                    {"table": table, "rows": rows, "distinct_pk": distinct_pk, "null_pk": null_pk})
        except Exception as exc:  # noqa: BLE001
            report["failed"].append({"key": table, "reason": f"pk_stats: {type(exc).__name__}"})

    return report
