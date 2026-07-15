"""gold 적재(Iceberg) — silver → gold **Iceberg 카탈로그**(서빙 Postgres 폐기, PROJECT.md §4).

구조(재심의 2026-07-14 — 기존 RDB 관계형 모델링을 Iceberg 로 승계):
- **코어**(공통 컬럼): gold_license_entity / gold_license_entity_history / gold_license_dong_summary
  — dbt 모델(정적, commerce_load_gold 의 Cosmos 가 실행). 이 모듈 담당 아님.
- **detail**(API 별 상이 컬럼 평탄화): 카탈로그 구동 — `gold_catalog`(Iceberg) 에 스펙을 두고
  detail 테이블(`commerce_<domain>_detail`)을 DDL ensure + 증분 적재한다. **이 모듈 담당.**
  각 API 의 비공통 필드가 서빙 가능한 단위(테이블·컬럼)로 뽑히는 계층 — record_json 은 silver 정본.

Postgres 시절과의 차이:
- 행이 Python 을 거치지 않는다 — `INSERT INTO gold_detail SELECT json_extract... FROM silver`
  가 Trino 안에서 끝난다(스트리밍·원자 커밋). psycopg2/execute_values 경로 삭제.
- 마커 테이블 없음 — 워터마크는 detail 테이블 자체의 **멤버별 max(collected_at)** 에서 파생.
  INSERT INTO SELECT 는 문장 단위 원자(Iceberg 단일 커밋)라 부분 적재 상태가 없다(재개 표준 §3).
- entity_seq(bigserial) 없음 — **자연키** (dataset, opnsfteamcode, mgtno) (§4.2 D1 특성).
- 뷰 320 미승계(Iceberg REST 뷰 제약) — detail 직접 조회(WHERE dataset=...)로 대체.

메모리: 멤버(dataset)별 1문 실행 — `dataset in (N멤버)` 다중 스캔 팬아웃 OOM 회피(기존 실측 승계).
대형 멤버는 content_hash 버킷 서브청크(스캔당 json 추출 heap 바운드). 문장 사이 pace(heap 회복).
"""
from __future__ import annotations

import logging
import os

from bronze.warehouse import _connect, _qualified
from security.dbio import assert_identifier

log = logging.getLogger(__name__)

CATALOG_TABLE = "gold_catalog"
# detail 키 컬럼(자연키 + 버전 식별) — silver_license_history grain 승계.
DETAIL_KEY_COLUMNS = ("dataset", "opnsfteamcode", "mgtno", "collected_at", "content_hash")
# 멤버 행수가 이를 넘으면 content_hash 버킷 서브청크(json 추출 heap 바운드). env 로 조정.
_BUCKET_ROWS = int(os.getenv("COMMERCE_GOLD_DETAIL_BUCKET_ROWS", "600000"))


def _assert_detail_safe(detail: dict) -> None:
    """detail 의 객체명·payload·member 를 SQL 식별자 게이트에 통과(§20 소비 경계 방어선).

    카탈로그가 DB(gold_catalog)에서 재로드되므로 DB 우회 유입도 여기서 차단한다."""
    assert_identifier(detail["object"], field="gold object")
    for c in detail["payload"]:
        assert_identifier(c, field="gold payload column")
    for m in detail["members"]:
        assert_identifier(m, field="dataset short")


# ── 카탈로그 (Iceberg 테이블 — 측정→규칙 산출 스펙의 정본) ─────────────────────
def ensure_catalog_table(cur, qschema: str) -> None:
    cur.execute(  # security: allow-sql - 검증 식별자 상수 DDL
        f"""
        CREATE TABLE IF NOT EXISTS {qschema}.{CATALOG_TABLE} (
            object varchar, kind varchar, members varchar, n_members integer,
            payload_columns varchar, catalog_version varchar, measured_at timestamp(6)
        ) WITH (format = 'PARQUET')
        """)
    cur.fetchall()


def upsert_catalog(details: list[dict], version: str) -> int:
    """카탈로그 전량 교체(delete+insert — 스펙 스냅샷). 반환: 스펙 수."""
    from datetime import datetime, timezone

    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        ensure_catalog_table(cur, qschema)
        cur.execute(f"DELETE FROM {qschema}.{CATALOG_TABLE}")  # security: allow-sql
        cur.fetchall()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        for i in range(0, len(details), 100):
            batch = details[i:i + 100]
            values = ", ".join("(?, ?, ?, ?, ?, ?, CAST(? AS timestamp(6)))" for _ in batch)
            params: list = []
            for d in batch:
                params += [d["object"], d["kind"], " ".join(d["members"]), len(d["members"]),
                           " ".join(d["payload"]), version, now]
            cur.execute(  # security: allow-sql - 값 전부 바인딩
                f"INSERT INTO {qschema}.{CATALOG_TABLE} "
                f"(object, kind, members, n_members, payload_columns, catalog_version, measured_at) "
                f"VALUES {values}", params)
            cur.fetchall()
    finally:
        conn.close()
    return len(details)


def read_catalog() -> tuple[list[dict], str | None]:
    """gold_catalog → (detail 스펙 목록, 버전). 테이블 부재/빈 → ([], None)."""
    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        try:
            cur.execute(  # security: allow-sql
                f"SELECT object, kind, members, payload_columns, catalog_version "
                f"FROM {qschema}.{CATALOG_TABLE} "
                f"WHERE kind IN ('detail_cluster', 'detail_single') ORDER BY object")
            rows = cur.fetchall()
        except Exception:                 # 테이블 부재(첫 실행) → build_catalog 선행 필요
            return [], None
    finally:
        conn.close()
    details = [{"object": r[0], "kind": r[1], "members": (r[2] or "").split(),
                "payload": (r[3] or "").split()} for r in rows]
    version = rows[0][4] if rows else None
    return details, version


# ── detail DDL / 증분 적재 (Trino 단독 — 행이 Python 을 거치지 않음) ───────────
def detail_ddl(qschema: str, detail: dict) -> str:
    """detail 1테이블 DDL — 자연키+버전 키 + 비공통 payload(varchar)."""
    _assert_detail_safe(detail)
    cols = ", ".join(
        ["dataset varchar", "opnsfteamcode varchar", "mgtno varchar",
         "collected_at timestamp(6)", "content_hash varchar"]
        + [f"{c} varchar" for c in detail["payload"]])
    return (f"CREATE TABLE IF NOT EXISTS {qschema}.{detail['object']} ({cols}) "
            f"WITH (format = 'PARQUET')")


def member_watermark(cur, qschema: str, obj: str, member: str):
    """멤버별 워터마크 **스냅샷** — detail 테이블 자체가 상태(마커 테이블 없음).

    반드시 버킷 루프 **시작 전에 1회** 조회해 전 버킷이 같은 창을 쓰게 한다. correlated
    서브쿼리로 문마다 재평가하면 버킷0 커밋이 워터마크를 전진시켜 버킷1~k 의 행이
    조용히 걸러진다(2026-07-15 실측: mail_order_sale 75%·general_restaurant 50% 유실)."""
    cur.execute(  # security: allow-sql - 검증 식별자, 값 바인딩
        f"SELECT max(collected_at) FROM {qschema}.{obj} WHERE dataset = ?", (member,))
    row = cur.fetchone()
    return row[0] if row else None


def defend_member(cur, qschema: str, obj: str, member: str, wm) -> None:
    """중단 방어(PROJECT.md §3) — 이전 실행이 버킷 도중 죽었을 때 남은 부분 버킷 잔재
    (워터마크 초과분)를 선삭제해 append 멱등을 만든다. wm=None 이면 전량 삭제."""
    if wm is None:
        cur.execute(  # security: allow-sql
            f"DELETE FROM {qschema}.{obj} WHERE dataset = ?", (member,))
    else:
        cur.execute(  # security: allow-sql
            f"DELETE FROM {qschema}.{obj} WHERE dataset = ? AND collected_at > ?", (member, wm))
    cur.fetchall()


def detail_insert_sql(qschema: str, detail: dict, *, bucket: tuple[int, int] | None = None) -> str:
    """멤버 1개 증분 적재문 — silver history 에서 payload 를 json 추출해 append.

    바인딩 순서: (member, wm) — wm 은 member_watermark() **스냅샷**(전 버킷 공통 창).
    bucket=(b,k) 면 content_hash 버킷 서브청크(대형 멤버 heap 바운드)."""
    _assert_detail_safe(detail)
    payload_exprs = "".join(
        f", json_extract_scalar(record_json, '$.{c.upper()}')" for c in detail["payload"])
    cols = ", ".join(DETAIL_KEY_COLUMNS + tuple(detail["payload"]))
    part_pred = ""
    if bucket is not None:
        b, k = int(bucket[0]), int(bucket[1])
        part_pred = f" AND mod(from_base(substr(content_hash, 1, 8), 16), {k}) = {b}"
    return (f"INSERT INTO {qschema}.{detail['object']} ({cols}) "
            f"SELECT dataset, opnsfteamcode, mgtno, collected_at, content_hash{payload_exprs} "
            f"FROM {qschema}.silver_license_history "
            f"WHERE dataset = ? "
            f"AND collected_at > coalesce(CAST(? AS timestamp(6)), timestamp '1970-01-01')"
            f"{part_pred}")


def _member_counts(cur, qschema: str, members: list[str]) -> dict[str, int]:
    vals = ", ".join(f"'{m}'" for m in members)   # 식별자 게이트 통과 값
    cur.execute(  # security: allow-sql
        f"SELECT cast(dataset as varchar), count(*) FROM {qschema}.silver_license_history "
        f"WHERE dataset IN ({vals}) GROUP BY 1")
    return {r[0]: int(r[1]) for r in cur.fetchall()}


def _pace(label: str) -> None:
    from commerce_core import trino_mem

    trino_mem.pace(label)


def run_load_details(details: list[dict], *, force_full: bool = False) -> dict:
    """detail 전체 적재 — DDL ensure → 멤버별 증분 INSERT(대형은 버킷). 반환: {object: rows}.

    force_full=True(refresh 트리거): 각 detail 을 DELETE 후 전량 재적재(워터마크가 epoch 로
    돌아가므로 같은 증분문이 전량을 옮긴다 — 문장 원자성 그대로).
    """
    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    loaded: dict[str, int] = {}
    try:
        cur = conn.cursor()
        for d in details:
            _assert_detail_safe(d)
            cur.execute(detail_ddl(qschema, d))  # security: allow-sql - 검증 식별자 DDL
            cur.fetchall()
            if force_full:
                cur.execute(f"DELETE FROM {qschema}.{d['object']}")  # security: allow-sql
                cur.fetchall()
            counts = _member_counts(cur, qschema, d["members"])
            n = 0
            for m in d["members"]:
                # 워터마크는 멤버당 **1회 스냅샷**(전 버킷 공통 창 — 버킷 간 재평가 금지) 후
                # 중단 방어(이전 부분 버킷 잔재 선삭제) → k개 버킷 INSERT 는 전부 같은 창.
                wm = member_watermark(cur, qschema, d["object"], m)
                defend_member(cur, qschema, d["object"], m, wm)
                wm_param = str(wm) if wm is not None else None
                rows = counts.get(m, 0)
                k = max(1, -(-rows // _BUCKET_ROWS))          # ceil — 대형 멤버만 버킷 분할
                for b in range(k):
                    _pace(f"{d['object']}:{m}" + (f" {b + 1}/{k}" if k > 1 else ""))
                    sql = detail_insert_sql(qschema, d, bucket=(b, k) if k > 1 else None)
                    cur.execute(sql, (m, wm_param))  # security: allow-sql - 식별자 검증·값 바인딩
                    res = cur.fetchall()
                    n += int(res[0][0]) if res and res[0] and res[0][0] is not None else 0
            loaded[d["object"]] = n
            log.info("gold detail %s: %d멤버 %s행%s", d["object"], len(d["members"]),
                     format(n, ","), " (full)" if force_full else "")
    finally:
        conn.close()
    log.info("gold detail DONE: %d객체 %s행", len(loaded), format(sum(loaded.values()), ","))
    return loaded
