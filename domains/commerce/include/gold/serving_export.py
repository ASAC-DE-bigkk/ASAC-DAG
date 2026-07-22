"""gold(집계) → 공용 D1 서빙 게시 — Serving Contract v1 초안(ASAC-DAG#478) 준거.

계약 분담(#478 수렴 상태):
  dbt yml ``meta.serving``   정적 선언 — enabled/product_id/contract_version/grain/primary_key/
                             publication_mode/zero_policy/partial_policy/refresh/product_question
  이 모듈(export)            실측·게이트·게시 — 행수 실측(선언 금지 합의), 행수 상한(>20,000 스킵),
                             주기(refresh cron 요일) 판정, staging→swap 게시, zero/partial hold,
                             ``_catalog`` upsert + 등록 누락 검증(#477③), ``_publication_log`` 기록
  공용 D1 ``_catalog``       게시 상태 정본(runtime) — citydata_serving_export 와 **동일 스키마**(공용 DB 합승)
  Gateway/Worker             API 계약(path/filter/limit) — 이 모듈 비범위(#476)

게시 주기(공용 D1 무료 쓰기 100k행/일 — **일 단위 경성 한도**, citydata ~72k 사용 중과 합승):
  소형(≤1k행) 11종 = 매일(refresh ``0 6 * * *``, ~2.9k행/일)
  중형(1k~20k) 6종 = 주 1회 — **요일 분산**(월=geo_grid 16k … 토=lifespan 2.6k). 한 요일에 몰면
                     그날 한도를 초과하므로 큰 표부터 월~토에 배치: 일 최대 ≈ 72k+2.9k+16.2k = 91k
  >20,000행    = 게시 스킵 — **선언이 아니라 매 실행 실측으로 판정**(성장 방어; 사용자 지시 2026-07-23)
  첫 게시도 요일 규칙을 따른다(첫 주 스파이크 방지 — 최대 6일 내 전 표 게시됨).
  주기 밀림 자가치유: 직전 게시가 8일 이상 낡으면 요일과 무관하게 게시

전제: env ``CLOUDFLARE_API_TOKEN`` (D1 Edit — 이름 규약상 자동 마스킹). 계정/DB id 는 비밀 아님 —
citydata 와 동일 공용 DB(팀 계정 이관 시 env COMMERCE_D1_ACCOUNT_ID/COMMERCE_D1_DATABASE_ID 로 전환).
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from security import assert_identifier, log_event, redact
from security.netio import http_post

log = logging.getLogger(__name__)

# 공용 서빙 D1 — citydata_serving_export.py 의 상수와 동일 값(계정 이관 시 양쪽 동기).
_DEFAULT_ACCOUNT_ID = "0d39ddce1c07c97df66843ede19f56c4"
_DEFAULT_DATABASE_ID = "9db0e851-558e-489f-9e76-f131d25aa267"

MANIFEST_PATH = os.environ.get(
    "COMMERCE_DBT_MANIFEST", "/opt/airflow/dbt/domains/commerce/target/manifest.json")

D1_ROW_CAP = 20_000            # 사용자 지시(2026-07-23): 초과 시 D1 게시 금지 — 매 실행 실측 판정
PARTIAL_BASELINE_RATIO = 0.8   # 직전 게시 대비 이 비율 미만이면 hold(#478 partial_policy 논의 준거)
STALE_REPUBLISH_DAYS = 8       # 주간 게시가 이보다 낡으면 요일 무관 게시(주기 밀림 자가치유)
_INSERT_BATCH = 100            # D1 HTTP API 요청당 INSERT 행수(요청 크기 제한 여유)

_SQLITE_TYPE = {"integer": "INTEGER", "bigint": "INTEGER", "smallint": "INTEGER",
                "tinyint": "INTEGER", "boolean": "INTEGER", "double": "REAL", "real": "REAL"}
# 자기 도메인 테이블 접두(#478 AC: 자기 도메인 밖에 쓰지 않는다) — 등록 누락 검증도 이 범위만.
_OWNED_PREFIXES = ("gold_license_", "gold_detail_", "gold_env_")


def _d1_api() -> str:
    account = (os.environ.get("COMMERCE_D1_ACCOUNT_ID")
               or os.environ.get("CLOUDFLARE_ACCOUNT_ID") or _DEFAULT_ACCOUNT_ID).strip()
    database = (os.environ.get("COMMERCE_D1_DATABASE_ID") or _DEFAULT_DATABASE_ID).strip()
    return f"https://api.cloudflare.com/client/v4/accounts/{account}/d1/database/{database}/query"


def _sqlite_type(trino_type: str) -> str:
    base = trino_type.split("(")[0]
    return "REAL" if base == "decimal" else _SQLITE_TYPE.get(base, "TEXT")


def _lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _d1(sql: str, token: str) -> list[dict]:
    """D1 HTTP API 실행 — 마지막 statement 의 결과 행 반환. 실패는 마스킹해 예외."""
    resp = http_post(_d1_api(), json={"sql": sql},
                     headers={"Authorization": f"Bearer {token}"}, timeout=120)
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(redact(f"D1 API 실패: {json.dumps(body.get('errors'))[:300]}"))
    result = body.get("result") or []
    return (result[-1].get("results") or []) if result else []


def _load_contracts() -> dict[str, dict]:
    """manifest → {모델명: {serving, description, tests}} (meta.serving 선언 모델만)."""
    manifest = json.loads(open(MANIFEST_PATH, encoding="utf-8").read())
    gates: dict[str, list[str]] = {}
    for node in manifest["nodes"].values():
        if node.get("resource_type") != "test" or not node.get("attached_node"):
            continue
        tm = node.get("test_metadata") or {}
        label = tm.get("name") or node.get("name", "test")
        col = (tm.get("kwargs") or {}).get("column_name")
        gates.setdefault(node["attached_node"], []).append(f"{label}({col})" if col else label)
    out: dict[str, dict] = {}
    for uid, node in manifest["nodes"].items():
        if node.get("resource_type") != "model":
            continue
        serving = (node.get("config", {}).get("meta") or {}).get("serving")
        if serving:
            # manifest 유래 모델명도 SQL 식별자로 쓰기 전 검증한다(§20 — 동적 식별자 게이트)
            out[assert_identifier(node["name"], field="serving model")] = {
                "serving": serving,
                "description": node.get("description", ""),
                "tests": sorted(set(gates.get(uid, []))),
            }
    return out


def _due_today(refresh_cron: str, kst_now: datetime, last_exported_at: str | None) -> bool:
    """refresh cron 의 요일 필드로 이번 run 게시 여부 판정. '*'=매일.

    첫 게시(직전 게시 없음)도 요일 규칙을 따른다 — 전 표가 같은 날 몰리는 스파이크 방지
    (일 쓰기 한도는 평균이 아니라 그날그날 경성 한도다). 지정 요일이 아니어도 직전 게시가
    STALE_REPUBLISH_DAYS 이상 낡으면 게시(주기 밀림 자가치유)."""
    fields = str(refresh_cron or "").split()
    dow = fields[4] if len(fields) == 5 else "*"
    if dow == "*":
        return True
    today = (kst_now.weekday() + 1) % 7          # cron: 0=일 1=월 … 6=토
    if any(part.isdigit() and int(part) % 7 == today for part in dow.split(",")):
        return True
    if last_exported_at:
        try:
            last = datetime.fromisoformat(str(last_exported_at).replace("Z", "+00:00"))
            age_days = (datetime.now(timezone.utc) - last).total_seconds() / 86400
            return age_days >= STALE_REPUBLISH_DAYS
        except ValueError:
            return True                          # 파싱 불가 = 상태 불명 → 게시가 안전
    return False                                 # 첫 게시도 지정 요일을 기다린다(≤6일)


def _publish_snapshot(name: str, col_defs: list[tuple[str, str]], rows: list, token: str) -> int:
    """staging 적재 후 단일 요청 swap — 게시 중에도 직전 스냅샷이 계속 서빙된다(빈 창 최소화).
    반환: 전송 바이트(대략, published_bytes 실측용)."""
    staging = f"{name}__staging"
    ddl_cols = ", ".join(f'"{c}" {_sqlite_type(t)}' for c, t in col_defs)
    _d1(f'DROP TABLE IF EXISTS "{staging}"; CREATE TABLE "{staging}" ({ddl_cols});', token)
    head = f'INSERT INTO "{staging}" ("' + '", "'.join(c for c, _ in col_defs) + '") VALUES\n'
    sent = 0
    for i in range(0, len(rows), _INSERT_BATCH):
        values = ",\n".join("(" + ", ".join(_lit(v) for v in r) + ")"
                            for r in rows[i:i + _INSERT_BATCH])
        payload = head + values + ";"
        _d1(payload, token)
        sent += len(payload.encode("utf-8"))
    _d1(f'DROP TABLE IF EXISTS "{name}"; ALTER TABLE "{staging}" RENAME TO "{name}";', token)
    return sent


def _verify_catalog_registration(published: list[str], token: str) -> list[str]:
    """#477③ — 'D1 엔 올렸는데 _catalog 등록 누락'을 export 끝에서 자동 검증(자기 접두만)."""
    tables = {r["name"] for r in _d1(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '%__staging';", token)}
    catalog = {r["name"] for r in _d1("SELECT name FROM _catalog;", token)}
    owned = {t for t in tables if t.startswith(_OWNED_PREFIXES)}
    return sorted((owned - catalog) | (set(published) - catalog))


def export_serving(source_run_id: str = "manual") -> dict:
    """enabled 계약 전체를 게이트 통과분만 게시하고 요약을 반환한다(리포트/XCom 용)."""
    # strip: env 파일의 공백/개행 오염이 헤더 예외로 이어지는 것을 차단. register_secret:
    # install_security() 밖 실행(단독 스크립트)에서도 예외 마스킹이 토큰을 가리게 한다.
    token = os.environ.get("CLOUDFLARE_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("CLOUDFLARE_API_TOKEN 미설정 — D1 게시 불가(조용한 실패 금지, #477)")
    from security import register_secret
    register_secret(token)

    from bronze.warehouse import _connect, _qualified          # 번들 내부 재사용

    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    cur = conn.cursor()
    contracts = _load_contracts()
    kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
    now_iso = datetime.now(timezone.utc).isoformat()

    # 직전 게시 상태(baseline·주기 자가치유 입력) — 공용 _catalog 를 1회만 조회
    _d1("CREATE TABLE IF NOT EXISTS _catalog (name TEXT PRIMARY KEY, description TEXT, "
        "serving_tier TEXT, tests TEXT, time_axis TEXT, columns TEXT, "
        "row_count INTEGER, exported_at TEXT);", token)
    _d1("CREATE TABLE IF NOT EXISTS _publication_log (publication_id TEXT PRIMARY KEY, "
        "product_id TEXT, source_run_id TEXT, published_at TEXT, serving_status TEXT, "
        "source_row_count INTEGER, published_row_count INTEGER, published_bytes INTEGER, "
        "publication_mode TEXT);", token)
    prev = {r["name"]: r for r in _d1("SELECT name, row_count, exported_at FROM _catalog;", token)}

    published: list[str] = []
    holds: list[str] = []       # 품질 hold(zero/partial) — warning 알림 대상
    skips: list[str] = []       # 정책 스킵(row cap/주기) — info 로그만
    log_rows: list[tuple] = []
    catalog_rows: list[tuple] = []
    total_rows = 0

    for name, meta in sorted(contracts.items()):
        s = meta["serving"]
        if not s.get("enabled"):
            continue
        last = prev.get(name, {})
        if not _due_today(s.get("refresh", "* * * * *"), kst_now, last.get("exported_at")):
            skips.append(f"{name}: 주기 아님({s.get('refresh')})")
            continue

        # 식별자는 _qualified()·_load_contracts() 에서 assert_identifier 로 검증됨
        cur.execute(f"SELECT count(*) FROM {qschema}.{name}")  # security: allow-sql
        source_rows = cur.fetchone()[0]
        status = "published"
        if source_rows == 0:
            status = "held_zero"
            holds.append(f"{name}: 0행 — zero_policy hold(직전 게시 유지)")
        elif source_rows > D1_ROW_CAP:
            status = "skipped_row_cap"
            skips.append(f"{name}: {source_rows:,}행 > 상한 {D1_ROW_CAP:,} — 게시 제외")
        elif last.get("row_count") and source_rows < PARTIAL_BASELINE_RATIO * last["row_count"]:
            status = "held_partial"
            holds.append(f"{name}: {source_rows:,}행 < 직전 {last['row_count']:,}행의 "
                         f"{PARTIAL_BASELINE_RATIO:.0%} — partial_policy hold")

        published_rows = 0
        published_bytes = 0
        if status == "published":
            cur.execute(f"SHOW COLUMNS FROM {qschema}.{name}")  # security: allow-sql
            col_defs = [(r[0], r[1]) for r in cur.fetchall()]
            cur.execute(f"SELECT * FROM {qschema}.{name}")  # security: allow-sql
            rows = cur.fetchall()
            published_bytes = _publish_snapshot(name, col_defs, rows, token)
            got = _d1(f'SELECT count(*) AS c FROM "{name}";', token)
            published_rows = (got[0].get("c") if got else 0) or 0
            if published_rows != len(rows):                     # 게시 검증(스모크)
                status = "verify_mismatch"
                holds.append(f"{name}: D1 {published_rows} ≠ 원본 {len(rows)} — 검증 불일치")
            else:
                published.append(name)
                total_rows += published_rows
                time_axis = next((c for c, t in col_defs
                                  if t.startswith(("timestamp", "date"))), None)
                catalog_rows.append((
                    name, meta["description"], "d1_direct",
                    json.dumps(meta["tests"], ensure_ascii=False), time_axis,
                    json.dumps([{"name": c, "type": t} for c, t in col_defs],
                               ensure_ascii=False), published_rows, now_iso))

        log_rows.append((str(uuid.uuid4()), s.get("product_id", name), source_run_id,
                         now_iso, status, source_rows, published_rows, published_bytes,
                         s.get("publication_mode", "snapshot")))

    # 게시 + 카탈로그 등록은 하나의 Publication(#478) — 게시분만 upsert(미게시 행 보존)
    if catalog_rows:
        _d1("\n".join("INSERT OR REPLACE INTO _catalog VALUES ("
                      + ", ".join(_lit(v) for v in r) + ");" for r in catalog_rows), token)
    if log_rows:
        _d1("\n".join("INSERT OR REPLACE INTO _publication_log VALUES ("
                      + ", ".join(_lit(v) for v in r) + ");" for r in log_rows), token)

    missing = _verify_catalog_registration(published, token)

    summary = {"published": published, "holds": holds, "skips": skips,
               "catalog_missing": missing, "rows_written": total_rows}
    log_event("serving_export_run", level="info", published=len(published),
              holds=len(holds), skips=len(skips), rows_written=total_rows,
              catalog_missing=len(missing))
    for line in skips:
        log.info("[serving export] skip — %s", line)

    # §19.1 그룹 품질 알림 — 파이프라인은 계속, 알려진 품질 이벤트로 통지
    try:
        from commerce_core.notify import notify_quality_event
        if holds:
            notify_quality_event(
                task="commerce_load_gold.serving_export", level="warning",
                title="D1 게시 hold — zero/partial/검증 게이트",
                description="상류 골드가 비었거나 직전 게시 대비 급감·검증 불일치 — 직전 스냅샷 유지",
                metrics={"affected_rows": len(holds), "settled_rows": len(contracts),
                         "affected_ratio_pct": round(100 * len(holds) / max(1, len(contracts)), 1)},
                context={"holds": holds[:10]})
        if missing:
            notify_quality_event(
                task="commerce_load_gold.serving_export", level="error",
                title="_catalog 등록 누락 검증 실패(#477)",
                description="D1 에 존재하나 _catalog 미등록(또는 게시 직후 누락)인 자기 도메인 테이블",
                metrics={"affected_rows": len(missing), "settled_rows": len(published),
                         "affected_ratio_pct": round(100 * len(missing) / max(1, len(published)), 1)},
                context={"missing": missing[:10]})
    except Exception as exc:                                    # noqa: BLE001 — 알림 실패는 게시 판정과 무관
        log.warning("serving export 알림 실패(무시): %s", redact(str(exc)))

    log.info("[serving export] ✓ 게시 %d · hold %d · skip %d · %d행 → 공용 D1",
             len(published), len(holds), len(skips), total_rows)
    return summary
