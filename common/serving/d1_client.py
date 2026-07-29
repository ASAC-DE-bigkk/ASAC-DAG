"""D1 access seam for the common publisher.

``D1Client`` is a small higher-level interface (not raw SQL) so the publisher's
orchestration is exercised against an in-memory fake in tests — no network, no prod
D1. ``HttpD1Client`` is the thin real implementation over the Cloudflare D1 HTTP API;
it reads its token/account/db from the environment and never logs the token.
"""

from __future__ import annotations

from typing import Any, Protocol, Sequence

Column = tuple[str, str]  # (name, trino_type)

_SQLITE_TYPE = {
    "integer": "INTEGER", "bigint": "INTEGER", "smallint": "INTEGER", "tinyint": "INTEGER",
    "boolean": "INTEGER", "double": "REAL", "real": "REAL",
}
_INSERT_BATCH = 100


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


class D1Client(Protocol):
    def table_row_count(self, name: str) -> int: ...
    def primary_key_stats(self, name: str, primary_key: Sequence[str]) -> tuple[int, int, int]: ...
    def table_max(self, name: str, column: str) -> Any | None: ...
    def catalog_row(self, name: str) -> dict[str, Any] | None: ...
    def ensure_table(self, name: str, columns: Sequence[Column], primary_key: Sequence[str]) -> None: ...
    def replace_table(self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], primary_key: Sequence[str]) -> None: ...
    def delete_where_gte(self, name: str, column: str, trino_literal: str) -> None: ...
    def insert_rows(self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], *, replace: bool) -> None: ...
    def upsert_catalog(self, catalog_rows: Sequence[dict[str, Any]]) -> None: ...
    def catalog_domain_count(self, model_names: set[str]) -> int: ...


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


class HttpD1Client:
    """Cloudflare D1 HTTP API implementation. Constructed from env by the DAG factory."""

    def __init__(self, api_url: str, token: str) -> None:
        self._api_url = api_url
        self._token = token  # never logged

    def _query(self, sql: str) -> list[dict[str, Any]]:
        import json

        import requests  # lazy import so tests never need it

        resp = requests.post(
            self._api_url, json={"sql": sql},
            headers={"Authorization": f"Bearer {self._token}"}, timeout=120,
        )
        body = resp.json()
        if not body.get("success"):
            # Surface D1 errors without echoing the request (which never carries the token anyway).
            raise RuntimeError(f"D1 API 실패: {json.dumps(body.get('errors'))[:300]}")
        result = body.get("result") or []
        return (result[-1].get("results") or []) if result else []

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

    def _insert_batches(self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], *, replace: bool) -> None:
        colnames = [c for c, _ in columns]
        verb = "INSERT OR REPLACE INTO" if replace else "INSERT INTO"
        head = f'{verb} "{name}" ("' + '", "'.join(colnames) + '") VALUES\n'
        for i in range(0, len(rows), _INSERT_BATCH):
            values = ",\n".join(
                "(" + ", ".join(sql_literal(row.get(c)) for c in colnames) + ")"
                for row in rows[i:i + _INSERT_BATCH]
            )
            self._query(head + values + ";")

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
        self._query(f'DROP TABLE IF EXISTS "{name}"; ALTER TABLE "{staging}" RENAME TO "{name}";')

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

    def catalog_domain_count(self, model_names: set[str]) -> int:
        if not model_names:
            return 0
        names = ", ".join(sql_literal(n) for n in sorted(model_names))
        out = self._query(f"SELECT count(*) c FROM _catalog WHERE name IN ({names});")
        return int(out[0].get("c", 0)) if out else 0
