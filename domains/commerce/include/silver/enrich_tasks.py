"""silver 사전 보강 태스크 — ① 행정동↔법정동 참조 적재 ② 지번주소 결측 Juso 보강.

commerce_localdata_transform DAG 이 dbt run **이전에** 실행한다. 두 테이블 모두
Iceberg `<catalog>.commerce`(bronze 층 — 시각은 UTC(naive), silver 가 KST 변환):

- ``bronze_ref_admin_dong``: 행안부 행정동↔법정동 매핑 최신 스냅샷(전량 교체).
  원천: R2 ``raw/common/admin_dong/load_date=*/ingest_ts=*/page-*.json``
  (공용(common) 수집물 — **읽기 전용**, 이 번들은 적재만 담당). load_date 최대
  → 그 안 ingest_ts 최대 폴더를 채택한다. 원천은 전국이지만 **서울만 적재**
  (Iceberg INSERT 커밋 비용 — ADMIN_DONG_SIDO_FILTER 로 조정).
- ``bronze_address_enrichment``: 도로명→지번 Juso 보강 캐시(road_address_norm 그레인).
  지번(SITEWHLADDR·LOTNO_ADDR 모두)이 없는 도로명만 호출한다. **무엇이 null 이었고
  어떤 패턴으로 몇 회 호출했는지**가 행(pattern_id/attempts/api_calls/status)과
  log_event 요약으로 남는다. filled·not_found(동일 래더판)는 재호출하지 않는다(캐시).
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone

from bronze.warehouse import _connect, _qualified
from commerce_core.notify import notify_completion
from commerce_core.storage import get_storage
from security import log_event
from silver import juso

log = logging.getLogger(__name__)

ADMIN_DONG_RAW_PREFIX = "raw/common/admin_dong/"
REF_TABLE = "bronze_ref_admin_dong"
ENRICH_TABLE = "bronze_address_enrichment"
_INSERT_BATCH = 200
# 적재 필터 — 원천은 전국(~2.1만 행)이나 Iceberg(R2 카탈로그)는 INSERT 커밋 비용이 커서
# 사용 범위(서울, ~700행)만 적재한다. 타 시도가 필요해지면 빈값('')으로 전국 적재.
ADMIN_DONG_SIDO_FILTER = os.getenv("ADMIN_DONG_SIDO_FILTER", "서울특별시")

_REF_COLUMNS = (
    "sido_name", "sgg_name", "sgg_code", "admin_region_code",
    "admin_dong_code", "admin_dong_name", "legal_dong_code", "legal_dong_name",
    "revised_date", "link_no", "source_load_date", "source_ingest_ts", "loaded_at",
)
_ENRICH_COLUMNS = (
    "road_address_norm", "source_road_address", "jibun_address", "matched_road_addr",
    "legal_dong_code", "legal_dong_name", "sgg_name", "si_name",
    "juso_total_count", "pattern_id", "attempts", "api_calls",
    "status", "error_summary", "ladder_version", "requested_at",
)
_TS_COLUMNS = frozenset({"loaded_at", "requested_at"})
_INT_COLUMNS = frozenset({"juso_total_count", "attempts", "api_calls", "ladder_version"})


def _utcnow_ts() -> str:
    """bronze 층 시각 규약 — UTC(naive) 'YYYY-MM-DD HH:MM:SS.ffffff' (silver 가 +9h)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def ensure_enrich_tables() -> None:
    """참조/보강 테이블 IF NOT EXISTS (DDL 은 warehouse 와 동일하게 Trino 로 단일화)."""
    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(  # security: allow-sql — 식별자는 assert_identifier 통과분만
            f"""
            CREATE TABLE IF NOT EXISTS {qschema}.{REF_TABLE} (
                sido_name varchar, sgg_name varchar, sgg_code varchar,
                admin_region_code varchar, admin_dong_code varchar, admin_dong_name varchar,
                legal_dong_code varchar, legal_dong_name varchar,
                revised_date varchar, link_no varchar,
                source_load_date varchar, source_ingest_ts varchar, loaded_at timestamp(6)
            ) WITH (format = 'PARQUET')
            """)
        cur.fetchall()
        cur.execute(  # security: allow-sql
            f"""
            CREATE TABLE IF NOT EXISTS {qschema}.{ENRICH_TABLE} (
                road_address_norm varchar, source_road_address varchar,
                jibun_address varchar, matched_road_addr varchar,
                legal_dong_code varchar, legal_dong_name varchar, sgg_name varchar, si_name varchar,
                juso_total_count integer, pattern_id varchar, attempts integer, api_calls integer,
                status varchar, error_summary varchar, ladder_version integer,
                requested_at timestamp(6)
            ) WITH (format = 'PARQUET')
            """)
        cur.fetchall()
    finally:
        conn.close()


# ── ① 행정동↔법정동 참조 적재 ────────────────────────────────────────────────
_LOAD_DATE_RE = re.compile(r"load_date=(\d{4}-\d{2}-\d{2})/")
_INGEST_TS_RE = re.compile(r"ingest_ts=([0-9TZ]+)/")


def _latest_admin_dong_pages(storage) -> tuple[str, str, list[str]]:
    """최신 load_date → 그 안 최신 ingest_ts 의 page-*.json 키 목록."""
    keys = storage.list_keys(ADMIN_DONG_RAW_PREFIX)
    dated = [(m.group(1), k) for k in keys if (m := _LOAD_DATE_RE.search(k))]
    if not dated:
        raise RuntimeError(f"admin_dong 원천 없음: {ADMIN_DONG_RAW_PREFIX}")
    load_date = max(d for d, _ in dated)
    in_date = [k for d, k in dated if d == load_date]
    stamped = [(m.group(1), k) for k in in_date if (m := _INGEST_TS_RE.search(k))]
    ingest_ts = max(t for t, _ in stamped)
    pages = sorted(k for t, k in stamped
                   if t == ingest_ts and k.rsplit("/", 1)[-1].startswith("page-"))
    return load_date, ingest_ts, pages


def _parse_admin_dong_page(data: bytes) -> list[dict]:
    """page JSON → 매핑 레코드 목록. 코드값은 숫자로 와도 문자열로 고정(자릿수 보존)."""
    payload = json.loads(data.decode("utf-8"))
    rows = []
    for r in payload.get("data") or []:
        if ADMIN_DONG_SIDO_FILTER and (r.get("시도명") or "").strip() != ADMIN_DONG_SIDO_FILTER:
            continue
        legal_code = str(r.get("법정동코드") or "").strip()
        rows.append({
            "sido_name": (r.get("시도명") or "").strip(),
            "sgg_name": (r.get("시군구명") or "").strip(),
            "sgg_code": legal_code[:5],                      # 법정동코드 앞 5자리 = 시군구 코드
            "admin_region_code": str(r.get("행정구역코드") or "").strip(),
            "admin_dong_code": str(r.get("행정동코드") or "").strip(),
            "admin_dong_name": (r.get("행정동명") or "").strip(),
            "legal_dong_code": legal_code,
            "legal_dong_name": (r.get("법정동명") or "").strip(),
            "revised_date": (r.get("개정일자") or "").strip(),
            "link_no": str(r.get("연결번호") or "").strip(),
        })
    return rows


def load_admin_dong_ref() -> dict:
    """R2 raw(common/admin_dong) 최신 스냅샷 → bronze_ref_admin_dong 전량 교체(멱등)."""
    ensure_enrich_tables()
    storage = get_storage()
    load_date, ingest_ts, pages = _latest_admin_dong_pages(storage)
    rows: list[dict] = []
    for key in pages:
        rows.extend(_parse_admin_dong_page(storage.read_bytes(key)))
    if not rows:
        raise RuntimeError(f"admin_dong 페이지 파싱 0행: load_date={load_date} ingest_ts={ingest_ts}")

    loaded_at = _utcnow_ts()
    catalog, schema, qschema = _qualified()
    qtable = f"{qschema}.{REF_TABLE}"
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {qtable}")  # security: allow-sql — 전량 교체(참조 스냅샷)
        cur.fetchall()
        cols = ", ".join(_REF_COLUMNS)
        one = "(" + ", ".join(
            "CAST(? AS timestamp(6))" if c in _TS_COLUMNS else "?" for c in _REF_COLUMNS) + ")"
        total = 0
        for i in range(0, len(rows), _INSERT_BATCH):
            batch = rows[i:i + _INSERT_BATCH]
            params: list = []
            for r in batch:
                params.extend([*(r[c] for c in _REF_COLUMNS[:-3]), load_date, ingest_ts, loaded_at])
            ph = ", ".join(one for _ in batch)
            cur.execute(f"INSERT INTO {qtable} ({cols}) VALUES {ph}", params)  # security: allow-sql
            cur.fetchall()
            total += len(batch)
    finally:
        conn.close()
    seoul = sum(1 for r in rows if r["sido_name"] == "서울특별시")
    summary = log_event("admin_dong_ref_loaded", where="enrich_tasks",
                        source_load_date=load_date, source_ingest_ts=ingest_ts,
                        pages=len(pages), rows=total, seoul_rows=seoul)
    return summary


# ── ② 지번 결측 Juso 보강 ───────────────────────────────────────────────────
# dbt silver 와 동일한 결측/정규화 규칙의 SQL 조각(빈문자→null, 괄호 절단+공백 축약)
_MISSING_ROAD_SQL = """
SELECT road_address_norm, arbitrary(road_address) AS sample_road, count(*) AS row_cnt
FROM (
    SELECT
        nullif(trim(regexp_replace(regexp_replace(
            coalesce(json_extract_scalar(record_json, '$.RDNWHLADDR'), ''),
            '\\(.*$', ''), '\\s+', ' ')), '') AS road_address_norm,
        json_extract_scalar(record_json, '$.RDNWHLADDR') AS road_address,
        nullif(trim(coalesce(json_extract_scalar(record_json, '$.SITEWHLADDR'), '')), '') AS jb,
        nullif(trim(coalesce(json_extract_scalar(record_json, '$.LOTNO_ADDR'), '')), '') AS lot
    FROM {bronze_table}
)
WHERE jb IS NULL AND lot IS NULL AND road_address_norm IS NOT NULL
GROUP BY 1
"""


def _load_cache(cur, qtable: str) -> dict[str, tuple[str, int]]:
    cur.execute(  # security: allow-sql
        f"SELECT road_address_norm, status, ladder_version FROM {qtable}")
    return {r[0]: (r[1], int(r[2] or 0)) for r in cur.fetchall()}


def _should_call(key: str, cache: dict[str, tuple[str, int]]) -> bool:
    """캐시 정책 — filled 는 영구 스킵, not_found 는 같은 래더판이면 스킵, error 는 재시도."""
    hit = cache.get(key)
    if hit is None:
        return True
    status, ladder = hit
    if status == "filled":
        return False
    if status == "not_found" and ladder >= juso.LADDER_VERSION:
        return False
    return True


def fill_jibun_from_road() -> dict:
    """지번 결측 도로명(유니크) → Juso 래더 조회 → enrichment upsert(키 delete-then-insert).

    로그 계약(요청 사항): 지번이 null 이었던 행 규모(null_jibun_rows/distinct_addresses)와
    실제 API 호출 횟수(api_calls)·패턴별 결과를 log_event 요약 + 행 단위로 남긴다.
    """
    ensure_enrich_tables()
    confm_key = juso.get_confm_key()
    juso_url = os.getenv("JUSO_API_URL", juso.JUSO_URL_DEFAULT).strip() or juso.JUSO_URL_DEFAULT
    delay = float(os.getenv("JUSO_REQUEST_DELAY_SECONDS", "0.15") or 0.15)
    raw_cap = (os.getenv("JUSO_MAX_ADDRESSES", "") or "").strip()
    cap = int(raw_cap) if raw_cap.isdigit() and int(raw_cap) > 0 else None

    catalog, schema, qschema = _qualified()
    qtable = f"{qschema}.{ENRICH_TABLE}"
    bronze_table = f"{qschema}.bronze_localdata_license"
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(_MISSING_ROAD_SQL.format(bronze_table=bronze_table))  # security: allow-sql
        missing = cur.fetchall()          # [(norm, sample, row_cnt)]
        cache = _load_cache(cur, qtable)
        null_rows = sum(int(r[2]) for r in missing)
        targets = [r for r in missing if _should_call(r[0], cache)]
        skipped = len(missing) - len(targets)
        if cap is not None:
            targets = targets[:cap]
        log.info("지번 결측: %d행 / 유니크 도로명 %d건 (캐시 스킵 %d, 이번 호출 대상 %d)",
                 null_rows, len(missing), skipped, len(targets))

        cols = ", ".join(_ENRICH_COLUMNS)
        one = "(" + ", ".join(
            "CAST(? AS timestamp(6))" if c in _TS_COLUMNS else "?"
            for c in _ENRICH_COLUMNS) + ")"

        def _flush(chunk: list[dict]) -> None:
            """키 단위 delete-then-insert(멱등). 장기 실행 중 주기 커밋 — 중단돼도 결과 보존."""
            requested_at = _utcnow_ts()
            for i in range(0, len(chunk), _INSERT_BATCH):
                batch = chunk[i:i + _INSERT_BATCH]
                keys = [r["road_address_norm"] for r in batch]
                ph_del = ", ".join("?" for _ in keys)
                cur.execute(  # security: allow-sql
                    f"DELETE FROM {qtable} WHERE road_address_norm IN ({ph_del})", keys)
                cur.fetchall()
                params: list = []
                for r in batch:
                    for c in _ENRICH_COLUMNS:
                        if c == "requested_at":
                            params.append(requested_at)
                        elif c in _INT_COLUMNS:
                            params.append(int(r.get(c) or 0))
                        else:
                            params.append(r.get(c))
                ph = ", ".join(one for _ in batch)
                cur.execute(f"INSERT INTO {qtable} ({cols}) VALUES {ph}", params)  # security: allow-sql
                cur.fetchall()

        results: list[dict] = []
        pending: list[dict] = []
        row_counts: dict[str, int] = {}          # 주소 키 → 결측 행수(미해결 보고용)
        api_calls = 0
        flush_every = max(_INSERT_BATCH, int(os.getenv("JUSO_FLUSH_EVERY", "500") or 500))
        for idx, (norm_key, sample_road, row_cnt) in enumerate(targets, start=1):
            row = juso.fill_one(sample_road or norm_key, confm_key=confm_key,
                                url=juso_url, delay_seconds=delay)
            row["road_address_norm"] = norm_key      # 조인 키는 bronze 정규화 결과로 고정
            row_counts[norm_key] = int(row_cnt)
            api_calls += row["api_calls"]
            results.append(row)
            pending.append(row)
            log.info("지번 보강[%s] %d/%d rows=%s calls=%d pattern=%s: %s",
                     row["status"], idx, len(targets), row_cnt,
                     row["api_calls"], row["pattern_id"], norm_key)
            if len(pending) >= flush_every:
                _flush(pending)
                log.info("중간 플러시: %d건 적재(진행 %d/%d, 누적 호출 %d)",
                         len(pending), idx, len(targets), api_calls)
                pending = []
        if pending:
            _flush(pending)
    finally:
        conn.close()

    filled = sum(1 for r in results if r["status"] == "filled")
    not_found = sum(1 for r in results if r["status"] == "not_found")
    errors = sum(1 for r in results if r["status"] == "error")
    summary = log_event("jibun_fill_run", where="enrich_tasks",
                        null_jibun_rows=null_rows, distinct_addresses=len(missing),
                        cache_skipped=skipped, called_addresses=len(results),
                        api_calls=api_calls, filled=filled, not_found=not_found,
                        errors=errors, ladder_version=juso.LADDER_VERSION,
                        capped=bool(cap is not None and len(missing) - skipped > len(targets)))

    # 미해결(못 채운) 주소 — 수집 시 명시 로그 + 성공/완료 알림 인터페이스로 결과 전달.
    # Juso 전량 실패(비정형 주소 등) 케이스는 여기 남는다. 전체 상세는 enrichment 테이블
    # (status/pattern_id/attempts) — 로그에는 상위 20건 샘플만 싣는다.
    unresolved = [{"road_address_norm": r["road_address_norm"], "status": r["status"],
                   "rows": row_counts.get(r["road_address_norm"], 0)}
                  for r in results if r["status"] != "filled"]
    if unresolved:
        log_event("jibun_fill_unresolved", where="enrich_tasks", level="warning",
                  unresolved_addresses=len(unresolved),
                  unresolved_rows=sum(u["rows"] for u in unresolved),
                  samples=unresolved[:20])
    notify_completion(where="commerce_localdata_transform.enrich_fill_jibun",
                      summary=summary, unresolved=unresolved)
    return summary
