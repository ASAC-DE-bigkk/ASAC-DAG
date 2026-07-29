"""D1 access seam for the common publisher.

``D1Client`` is a small higher-level interface (not raw SQL) so the publisher's
orchestration is exercised against an in-memory fake in tests — no network, no prod
D1. ``HttpD1Client`` is the thin real implementation over the Cloudflare D1 HTTP API;
it reads its token/account/db from the environment and never logs the token.
"""

from __future__ import annotations

import json
from typing import Any, Protocol, Sequence

Column = tuple[str, str]  # (name, trino_type)

_SQLITE_TYPE = {
    "integer": "INTEGER", "bigint": "INTEGER", "smallint": "INTEGER", "tinyint": "INTEGER",
    "boolean": "INTEGER", "double": "REAL", "real": "REAL",
}
# Cloudflare D1 permits a 100,000-byte SQL statement. Leave margin for the
# statement itself and keep API request batches bounded so staging writes stay
# comfortably below the query-duration limit.
MAX_SQL_STATEMENT_BYTES = 80_000
MAX_STATEMENTS_PER_API_BATCH = 4
MAX_API_BATCH_BYTES = 256_000


def sqlite_type(trino_type: str) -> str:
    base = trino_type.split("(")[0].strip().lower()
    return "REAL" if base == "decimal" else _SQLITE_TYPE.get(base, "TEXT")


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _utf8_bytes(value: str) -> int:
    return len(value.encode("utf-8"))


def build_insert_statements(
    name: str,
    columns: Sequence[Column],
    rows: Sequence[dict[str, Any]],
    *,
    replace: bool,
) -> list[str]:
    """Render INSERT statements without exceeding D1's per-statement budget."""

    if not rows:
        return []
    colnames = [column for column, _ in columns]
    verb = "INSERT OR REPLACE INTO" if replace else "INSERT INTO"
    head = f'{verb} "{name}" ("' + '", "'.join(colnames) + '") VALUES\n'
    tail = ";"
    fixed_bytes = _utf8_bytes(head + tail)
    rendered_rows = [
        "(" + ", ".join(sql_literal(row.get(column)) for column in colnames) + ")"
        for row in rows
    ]

    statements: list[str] = []
    current_rows: list[str] = []
    current_bytes = fixed_bytes
    for rendered in rendered_rows:
        rendered_bytes = _utf8_bytes(rendered)
        separator_bytes = _utf8_bytes(",\n") if current_rows else 0
        if fixed_bytes + rendered_bytes > MAX_SQL_STATEMENT_BYTES:
            raise ValueError(
                f"{name}: one rendered row exceeds SQL byte budget "
                f"{MAX_SQL_STATEMENT_BYTES}"
            )
        if current_rows and current_bytes + separator_bytes + rendered_bytes > MAX_SQL_STATEMENT_BYTES:
            statements.append(head + ",\n".join(current_rows) + tail)
            current_rows = []
            current_bytes = fixed_bytes
            separator_bytes = 0
        current_rows.append(rendered)
        current_bytes += separator_bytes + rendered_bytes

    if current_rows:
        statements.append(head + ",\n".join(current_rows) + tail)
    return statements


def _api_batch_bytes(statements: Sequence[str]) -> int:
    body = {"batch": [{"sql": statement} for statement in statements]}
    return _utf8_bytes(json.dumps(body, ensure_ascii=False, separators=(",", ":")))


def group_api_batches(statements: Sequence[str]) -> list[list[str]]:
    """Group already-safe statements into bounded Cloudflare D1 API batches."""

    batches: list[list[str]] = []
    current: list[str] = []
    for statement in statements:
        if _utf8_bytes(statement) > MAX_SQL_STATEMENT_BYTES:
            raise ValueError(f"statement exceeds SQL byte budget {MAX_SQL_STATEMENT_BYTES}")
        candidate = [*current, statement]
        if current and (
            len(candidate) > MAX_STATEMENTS_PER_API_BATCH
            or _api_batch_bytes(candidate) > MAX_API_BATCH_BYTES
        ):
            batches.append(current)
            current = [statement]
        else:
            current = candidate
        if _api_batch_bytes(current) > MAX_API_BATCH_BYTES:
            raise ValueError(f"statement exceeds API batch byte budget {MAX_API_BATCH_BYTES}")
    if current:
        batches.append(current)
    return batches


class D1Client(Protocol):
    def table_row_count(self, name: str) -> int: ...
    def primary_key_stats(self, name: str, primary_key: Sequence[str]) -> tuple[int, int, int]: ...
    def table_max(self, name: str, column: str) -> Any | None: ...
    def catalog_row(self, name: str) -> dict[str, Any] | None: ...
    def ensure_table(self, name: str, columns: Sequence[Column], primary_key: Sequence[str]) -> None: ...
    def replace_table(self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], primary_key: Sequence[str]) -> None: ...
    def restore_replaced_table(self, name: str) -> None: ...
    def finalize_replaced_table(self, name: str) -> None: ...
    def delete_where_gte(self, name: str, column: str, trino_literal: str) -> None: ...
    def insert_rows(self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], *, replace: bool) -> None: ...
    def upsert_catalog(self, catalog_rows: Sequence[dict[str, Any]]) -> None: ...
    def delete_catalog_row(self, name: str) -> None: ...
    def catalog_domain_count(self, model_names: set[str]) -> int: ...
    def append_publication_ledger(self, record: dict[str, Any]) -> None: ...


# ---- catalog schema (single source for both real client and Worker) -----------------

CATALOG_COLUMN_TYPES = (
    ("name", "TEXT PRIMARY KEY"), ("product_id", "TEXT"), ("external", "INTEGER"),
    ("description", "TEXT"), ("product_question", "TEXT"), ("tests", "TEXT"),
    ("time_axis", "TEXT"), ("columns", "TEXT"), ("row_count", "INTEGER"),
    ("serving_status", "TEXT"), ("publication_id", "TEXT"), ("source_run_id", "TEXT"),
    ("published_bytes", "INTEGER"), ("freshness", "TEXT"), ("exported_at", "TEXT"),
)
CATALOG_COLUMNS = tuple(name for name, _ in CATALOG_COLUMN_TYPES)
CATALOG_DDL = "CREATE TABLE IF NOT EXISTS _catalog (" + ", ".join(
    f"{name} {column_type}" for name, column_type in CATALOG_COLUMN_TYPES
) + ");"
PUBLICATION_LEDGER_COLUMN_TYPES = (
    ("publication_id", "TEXT PRIMARY KEY"), ("product_id", "TEXT NOT NULL"),
    ("model_name", "TEXT NOT NULL"), ("source_run_id", "TEXT NOT NULL"),
    ("attempted_at", "TEXT NOT NULL"), ("outcome", "TEXT NOT NULL"),
    ("stage", "TEXT NOT NULL"), ("source_row_count", "INTEGER NOT NULL"),
    ("published_row_count", "INTEGER NOT NULL"), ("d1_row_count", "INTEGER NOT NULL"),
    ("api_smoke_status", "TEXT NOT NULL"), ("rollback_status", "TEXT NOT NULL"),
    ("reason", "TEXT NOT NULL"),
)
PUBLICATION_LEDGER_COLUMNS = tuple(name for name, _ in PUBLICATION_LEDGER_COLUMN_TYPES)
PUBLICATION_LEDGER_DDL = "CREATE TABLE IF NOT EXISTS _publication_ledger (" + ", ".join(
    f"{name} {column_type}" for name, column_type in PUBLICATION_LEDGER_COLUMN_TYPES
) + ");"


class HttpD1Client:
    """Cloudflare D1 HTTP API implementation. Constructed from env by the DAG factory."""

    def __init__(self, api_url: str, token: str) -> None:
        self._api_url = api_url
        self._token = token  # never logged

    def _request(self, body: dict[str, Any]) -> dict[str, Any]:
        import requests  # lazy import so tests never need it

        resp = requests.post(
            self._api_url, json=body,
            headers={"Authorization": f"Bearer {self._token}"}, timeout=120,
        )
        response = resp.json()
        if not response.get("success"):
            # Surface D1 errors without echoing the request (which never carries the token anyway).
            raise RuntimeError(f"D1 API 실패: {json.dumps(response.get('errors'))[:300]}")
        return response

    def _query(self, sql: str) -> list[dict[str, Any]]:
        response = self._request({"sql": sql})
        result = response.get("result") or []
        return (result[-1].get("results") or []) if result else []

    def _query_batch(self, statements: Sequence[str]) -> list[list[dict[str, Any]]]:
        if not statements:
            return []
        response = self._request({"batch": [{"sql": statement} for statement in statements]})
        result = response.get("result") or []
        if len(result) != len(statements) or any(not item.get("success") for item in result):
            raise RuntimeError("D1 API batch 일부 statement 실패")
        return [(item.get("results") or []) for item in result]

    def _create_ddl(
        self,
        name: str,
        columns: Sequence[Column],
        primary_key: Sequence[str] = (),
    ) -> str:
        if not primary_key:
            raise ValueError(f"{name}: primary_key is required for D1 publication")
        cols = ", ".join(f'"{c}" {sqlite_type(t)}' for c, t in columns)
        key_columns = '", "'.join(primary_key)
        cols += f', UNIQUE ("{key_columns}")'
        return f'CREATE TABLE "{name}" ({cols});'

    def _ensure_unique_primary_key(self, name: str, primary_key: Sequence[str]) -> None:
        if not primary_key:
            raise ValueError(f"{name}: primary_key is required for D1 publication")
        columns = '", "'.join(primary_key)
        self._query(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{name}__pk_uq" '
            f'ON "{name}" ("{columns}");'
        )

    def _table_exists(self, name: str) -> bool:
        out = self._query(
            "SELECT name FROM sqlite_master "
            f"WHERE type = 'table' AND name = {sql_literal(name)};"
        )
        return bool(out)

    def _insert_batches(self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], *, replace: bool) -> None:
        statements = build_insert_statements(name, columns, rows, replace=replace)
        for batch in group_api_batches(statements):
            self._query_batch(batch)

    def table_row_count(self, name: str) -> int:
        try:
            out = self._query(f'SELECT count(*) c FROM "{name}";')
        except RuntimeError:
            return 0  # table absent
        return int(out[0].get("c", 0)) if out else 0

    def primary_key_stats(self, name: str, primary_key: Sequence[str]) -> tuple[int, int, int]:
        if not primary_key:
            raise ValueError(f"{name}: primary_key is required for D1 read-back")
        columns = '", "'.join(primary_key)
        null_predicate = " OR ".join(f'"{column}" IS NULL' for column in primary_key)
        out = self._query(
            f'SELECT '
            f'(SELECT count(*) FROM "{name}") AS row_count, '
            f'(SELECT count(*) FROM (SELECT "{columns}" FROM "{name}" GROUP BY "{columns}")) '
            f'AS distinct_primary_key_count, '
            f'(SELECT count(*) FROM "{name}" WHERE {null_predicate}) AS null_primary_key_count;'
        )
        row = out[0] if out else {}
        return (
            int(row.get("row_count", 0)),
            int(row.get("distinct_primary_key_count", 0)),
            int(row.get("null_primary_key_count", 0)),
        )

    def table_max(self, name: str, column: str) -> Any | None:
        try:
            out = self._query(f'SELECT max("{column}") m FROM "{name}";')
        except RuntimeError:
            return None
        return out[0].get("m") if out else None

    def catalog_row(self, name: str) -> dict[str, Any] | None:
        try:
            out = self._query(f"SELECT * FROM _catalog WHERE name = {sql_literal(name)};")
        except RuntimeError:
            return None
        return out[0] if out else None

    def ensure_table(self, name: str, columns: Sequence[Column], primary_key: Sequence[str]) -> None:
        cols = ", ".join(f'"{c}" {sqlite_type(t)}' for c, t in columns)
        self._query(f'CREATE TABLE IF NOT EXISTS "{name}" ({cols});')
        self._ensure_unique_primary_key(name, primary_key)

    def replace_table(
        self,
        name: str,
        columns: Sequence[Column],
        rows: Sequence[dict[str, Any]],
        primary_key: Sequence[str],
    ) -> None:
        # Staging populate first; only swap after a full successful load so a mid-load
        # failure leaves the previous published table (last-known-good) untouched.
        staging = f"{name}__staging"
        self._query(f'DROP TABLE IF EXISTS "{staging}";')
        # A table-level UNIQUE constraint survives the staging rename without a
        # global index-name collision on the next snapshot replacement.
        self._query(self._create_ddl(staging, columns, primary_key))
        self._insert_batches(staging, columns, rows, replace=False)
        previous = f"{name}__previous"
        if self._table_exists(previous):
            raise RuntimeError(f"{name}: unfinished previous snapshot exists; restore or finalize it before publishing")
        if self._table_exists(name):
            self._query(
                f'ALTER TABLE "{name}" RENAME TO "{previous}"; '
                f'ALTER TABLE "{staging}" RENAME TO "{name}";'
            )
        else:
            self._query(f'ALTER TABLE "{staging}" RENAME TO "{name}";')

    def restore_replaced_table(self, name: str) -> None:
        previous = f"{name}__previous"
        if self._table_exists(previous):
            self._query(
                f'DROP TABLE IF EXISTS "{name}"; '
                f'ALTER TABLE "{previous}" RENAME TO "{name}";'
            )
        else:
            self._query(f'DROP TABLE IF EXISTS "{name}";')

    def finalize_replaced_table(self, name: str) -> None:
        self._query(f'DROP TABLE IF EXISTS "{name}__previous";')

    def delete_where_gte(self, name: str, column: str, trino_literal: str) -> None:
        self._query(f'DELETE FROM "{name}" WHERE "{column}" >= {trino_literal};')

    def insert_rows(self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], *, replace: bool) -> None:
        self._insert_batches(name, columns, rows, replace=replace)

    def _catalog_column_names(self) -> set[str]:
        return {str(row["name"]) for row in self._query("PRAGMA table_info(_catalog);")}

    def _ensure_catalog_schema(self) -> None:
        self._query(CATALOG_DDL)
        existing = self._catalog_column_names()
        for name, column_type in CATALOG_COLUMN_TYPES:
            if name in existing:
                continue
            try:
                self._query(f'ALTER TABLE _catalog ADD COLUMN "{name}" {column_type};')
            except RuntimeError:
                # Another publisher may have added the same column between PRAGMA and ALTER.
                if name not in self._catalog_column_names():
                    raise

    def upsert_catalog(self, catalog_rows: Sequence[dict[str, Any]]) -> None:
        self._ensure_catalog_schema()
        column_names = '", "'.join(CATALOG_COLUMNS)
        update_columns = ", ".join(
            f'"{column}" = excluded."{column}"'
            for column in CATALOG_COLUMNS
            if column != "name"
        )
        for row in catalog_rows:
            values = ", ".join(sql_literal(row.get(col)) for col in CATALOG_COLUMNS)
            # Do not use INSERT OR REPLACE: on a legacy catalog that deletes the old
            # row and turns the retained serving_tier field into NULL.
            self._query(
                f'INSERT INTO _catalog ("{column_names}") VALUES ({values}) '
                f'ON CONFLICT("name") DO UPDATE SET {update_columns};'
            )

    def delete_catalog_row(self, name: str) -> None:
        self._ensure_catalog_schema()
        self._query(f"DELETE FROM _catalog WHERE name = {sql_literal(name)};")

    def catalog_domain_count(self, model_names: set[str]) -> int:
        if not model_names:
            return 0
        names = ", ".join(sql_literal(n) for n in sorted(model_names))
        out = self._query(f"SELECT count(*) c FROM _catalog WHERE name IN ({names});")
        return int(out[0].get("c", 0)) if out else 0

    def append_publication_ledger(self, record: dict[str, Any]) -> None:
        self._query(PUBLICATION_LEDGER_DDL)
        columns = '", "'.join(PUBLICATION_LEDGER_COLUMNS)
        values = ", ".join(sql_literal(record.get(column)) for column in PUBLICATION_LEDGER_COLUMNS)
        self._query(f'INSERT INTO _publication_ledger ("{columns}") VALUES ({values});')
