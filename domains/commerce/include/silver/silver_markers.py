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
            cur.execute(  # security: allow-sql
                f"""
                INSERT INTO {qmarker} (dataset, bronze_run_id, status, marked_at, marker_source)
                SELECT h.dataset, h.bronze_run_id, 'DONE', CAST(? AS timestamp(6)), 'bootstrap_history'
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
                    bootstrapped = int(rows[0][0])
            except Exception:  # noqa: BLE001 - Trino adapters vary on INSERT result shape.
                bootstrapped = -1
    finally:
        conn.close()

    log.info("silver marker 준비 완료: table=%s bootstrapped=%s", qmarker, bootstrapped)
    return {"marker_table": qmarker, "bootstrapped": bootstrapped}


def mark_silver_runs_done() -> dict:
    """Mark all successfully tested history run IDs as DONE.

    This task must run after `dbt_test_silver`; therefore DONE means history and
    current passed the project's dbt tests.
    """
    catalog, schema, qschema = _qualified()
    qmarker = f"{qschema}.{MARKER_TABLE}"
    qhistory = f"{qschema}.{HISTORY_TABLE}"
    conn = _connect(catalog, schema)
    inserted = 0
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
    finally:
        conn.close()

    log.info("silver DONE marker 기록 완료: inserted=%s", inserted)
    return {"marker_table": qmarker, "inserted": inserted}
