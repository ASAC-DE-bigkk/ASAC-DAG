"""silver load DONE marker management.

`silver_license_history` is incremental, but rows in the history table are not a
durable completion marker by themselves. A run is considered complete only after
`dbt test` succeeds; this module records that state in
`silver_load_run_marker`.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from bronze.warehouse import _connect, _qualified

log = logging.getLogger(__name__)

MARKER_TABLE = "silver_load_run_marker"
HISTORY_TABLE = "silver_license_history"


def _utcnow_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def _table_exists(cur, catalog: str, schema: str, table: str) -> bool:
    cur.execute(  # security: allow-sql - catalog is assert_identifier output from _qualified().
        f"""
        SELECT count(*)
        FROM {catalog}.information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """, (schema, table))
    return int(cur.fetchall()[0][0]) > 0


def ensure_silver_marker_table() -> dict:
    """Create marker table and bootstrap DONE markers from existing history.

    Bootstrap is needed when deploying this marker after `silver_license_history`
    already exists. Without it, the first incremental run would treat all old
    publishable bronze runs as unmarked.
    """
    catalog, schema, qschema = _qualified()
    qmarker = f"{qschema}.{MARKER_TABLE}"
    qhistory = f"{qschema}.{HISTORY_TABLE}"
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(  # security: allow-sql
            f"""
            CREATE TABLE IF NOT EXISTS {qmarker} (
                dataset varchar,
                bronze_run_id varchar,
                status varchar,
                marked_at timestamp(6),
                marker_source varchar
            ) WITH (format = 'PARQUET')
            """)
        cur.fetchall()

        bootstrapped = 0
        if _table_exists(cur, catalog, schema, HISTORY_TABLE):
            # 부트스트랩은 **행수 패리티 검증** 후에만 DONE 을 찍는다: history 행수 == bronze
            # publishable 행수인 run 만. "history 에 행이 있으면 완료"라는 옛 가정은 all-or-nothing
            # 빌드 전제였고, 청크/버킷 부분 빌드(seed)가 중단되면 **부분 적재 run 을 DONE 으로
            # 정당화**해 미적재 행이 영구 소실된다(2026-07-13 실측: mail_order_sale 727K 소실 —
            # change-log #60). 인접중복 dedup 으로 행수가 줄어든 run 은 패리티 불일치 → 미마킹으로
            # 남는데, 그 dataset 은 seed 재개가 통째로 재빌드하므로 안전(과소 마킹은 무해, 과대가 유해).
            cur.execute(  # security: allow-sql
                f"""
                INSERT INTO {qmarker} (dataset, bronze_run_id, status, marked_at, marker_source)
                SELECT h.dataset, h.bronze_run_id, 'DONE', CAST(? AS timestamp(6)), 'bootstrap_history'
                FROM (
                    SELECT cast(dataset as varchar) AS dataset,
                           cast(bronze_run_id as varchar) AS bronze_run_id,
                           count(*) AS hist_rows
                    FROM {qhistory}
                    WHERE bronze_run_id IS NOT NULL
                    GROUP BY 1, 2
                ) h
                JOIN (
                    SELECT cast(b.dataset as varchar) AS dataset,
                           cast(b.bronze_run_id as varchar) AS bronze_run_id,
                           count(*) AS bronze_rows
                    FROM {qschema}.bronze_localdata_license b
                    JOIN {qschema}.bronze_collection_run_manifest m
                        ON cast(b.dataset as varchar) = cast(m.dataset as varchar)
                       AND cast(b.bronze_run_id as varchar) = cast(m.bronze_run_id as varchar)
                       AND m.status = 'SUCCESS' AND m.is_publishable
                    GROUP BY 1, 2
                ) p
                    ON p.dataset = h.dataset AND p.bronze_run_id = h.bronze_run_id
                   AND p.bronze_rows = h.hist_rows
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {qmarker} m
                    WHERE m.status = 'DONE'
                      AND m.dataset = h.dataset
                      AND m.bronze_run_id = h.bronze_run_id
                )
                """, (_utcnow_ts(),))
            try:
                rows = cur.fetchall()
                if rows and rows[0]:
                    bootstrapped = int(rows[0][0])
            except Exception:  # noqa: BLE001 - Trino adapters vary on INSERT result shape.
                bootstrapped = -1
    finally:
        conn.close()

    log.info("silver marker 준비 완료: table=%s bootstrapped=%s", qmarker, bootstrapped)
    return {"marker_table": qmarker, "bootstrapped": bootstrapped}


def mark_silver_runs_done(*, processed_cutoff_run_id: str | None = None) -> dict:
    """Mark all successfully tested history run IDs as DONE.

    This task must run after `dbt_test_silver`; therefore DONE means history and
    current passed the project's dbt tests.

    두 소스로 마킹한다:
    1. history 에 행이 남은 run(기존 동작 — 'dbt_test_silver').
    2. **cutoff 이전의 publishable run 전부**('processed_no_rows') — 인접중복 dedup 으로 **행이 하나도
       남지 않은 run**(diff 재유입 전량 제거)은 history 에 없어 1번이 영원히 못 찍고, 그러면 seed_state
       가 영구 미완 판정 → 매 run 재빌드 낭비가 생긴다(2026-07-13 실측, change-log #60 버그6).
       빌드가 시작된 시점(cutoff) 이전의 publishable run 은 이번 빌드가 전부 처리했으므로 DONE 이
       맞다. bronze_run_id 가 수집시각(YYYY-MM-DD_HHMMSS…)을 인코딩하므로 문자열 비교로 race-safe:
       빌드 중 도착한 run(≥cutoff)은 마킹하지 않아 다음 증분이 처리한다. cutoff 미지정 시 1번만(구 동작).
    """
    catalog, schema, qschema = _qualified()
    qmarker = f"{qschema}.{MARKER_TABLE}"
    qhistory = f"{qschema}.{HISTORY_TABLE}"
    conn = _connect(catalog, schema)
    inserted = 0
    inserted_no_rows = 0
    try:
        cur = conn.cursor()
        if not _table_exists(cur, catalog, schema, HISTORY_TABLE):
            raise RuntimeError(f"silver history table not found: {qhistory}")
        cur.execute(  # security: allow-sql
            f"""
            INSERT INTO {qmarker} (dataset, bronze_run_id, status, marked_at, marker_source)
            SELECT h.dataset, h.bronze_run_id, 'DONE', CAST(? AS timestamp(6)), 'dbt_test_silver'
            FROM (
                SELECT DISTINCT cast(dataset as varchar) AS dataset,
                                cast(bronze_run_id as varchar) AS bronze_run_id
                FROM {qhistory}
                WHERE bronze_run_id IS NOT NULL
            ) h
            WHERE NOT EXISTS (
                SELECT 1
                FROM {qmarker} m
                WHERE m.status = 'DONE'
                  AND m.dataset = h.dataset
                  AND m.bronze_run_id = h.bronze_run_id
            )
            """, (_utcnow_ts(),))
        try:
            rows = cur.fetchall()
            if rows and rows[0]:
                inserted = int(rows[0][0])
        except Exception:  # noqa: BLE001
            inserted = -1
        if processed_cutoff_run_id:
            cur.execute(  # security: allow-sql - cutoff 은 바인딩 값
                f"""
                INSERT INTO {qmarker} (dataset, bronze_run_id, status, marked_at, marker_source)
                SELECT p.dataset, p.bronze_run_id, 'DONE', CAST(? AS timestamp(6)), 'processed_no_rows'
                FROM (
                    SELECT DISTINCT cast(b.dataset as varchar) AS dataset,
                                    cast(b.bronze_run_id as varchar) AS bronze_run_id
                    FROM {qschema}.bronze_localdata_license b
                    JOIN {qschema}.bronze_collection_run_manifest m
                        ON cast(b.dataset as varchar) = cast(m.dataset as varchar)
                       AND cast(b.bronze_run_id as varchar) = cast(m.bronze_run_id as varchar)
                       AND m.status = 'SUCCESS' AND m.is_publishable
                ) p
                WHERE p.bronze_run_id < ?
                  AND NOT EXISTS (
                    SELECT 1 FROM {qmarker} m
                    WHERE m.status = 'DONE'
                      AND m.dataset = p.dataset
                      AND m.bronze_run_id = p.bronze_run_id
                )
                """, (_utcnow_ts(), processed_cutoff_run_id))
            try:
                rows = cur.fetchall()
                if rows and rows[0]:
                    inserted_no_rows = int(rows[0][0])
            except Exception:  # noqa: BLE001
                inserted_no_rows = -1
    finally:
        conn.close()

    log.info("silver DONE marker 기록 완료: inserted=%s, no_rows=%s", inserted, inserted_no_rows)
    return {"marker_table": qmarker, "inserted": inserted, "inserted_no_rows": inserted_no_rows}
