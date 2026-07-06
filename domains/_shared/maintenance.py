"""Shared Iceberg maintenance helpers for periodic metadata cleanup."""

from __future__ import annotations

from typing import Iterable


def _is_dev_target(target: str | None = None) -> bool:
    if target is not None:
        return target == "dev"
    import os

    return os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"


def _trino_catalog(host_target: str = "dev") -> str:
    import os
    import re

    if host_target != "dev":
        return os.environ.get("TRINO_ICEBERG_CATALOG", "iceberg")

    value = os.environ.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value):
        raise ValueError(f"invalid catalog name: {value}")
    return value


def _ask_seoul_schema() -> str:
    import os
    import re

    value = os.environ.get("ASK_SEOUL_SCHEMA", "ask_seoul")
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value):
        raise ValueError(f"invalid schema name: {value}")
    return value


def _connect_trino():
    import os
    import trino.dbapi

    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=_trino_catalog("dev" if _is_dev_target() else "prod"),
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )


def _table_exists(cursor, schema: str, table: str) -> bool:
    cursor.execute(f"SHOW TABLES FROM {schema}")
    return any(row[0] == table for row in cursor.fetchall())


def run_maintenance(
    target: str = "dev",
    tables: Iterable[str] = (),
    *,
    retention: str = "7d",
    ignore_missing: bool = True,
) -> dict[str, str]:
    """Execute optimize + expire_snapshots + remove_orphan_files for each table."""

    tables = tuple(tables)
    catalog = _trino_catalog(target)
    schema = _ask_seoul_schema()
    fq_schema = f"{catalog}.{schema}"
    cursor = _connect_trino().cursor()
    results: dict[str, str] = {}
    for table in tables:
        try:
            if ignore_missing and not _table_exists(cursor, fq_schema, table):
                results[table] = "skipped (missing)"
                continue
            qualified_table = f"{fq_schema}.{table}"
            for op in (
                "optimize",
                f"expire_snapshots(retention_threshold => '{retention}')",
                f"remove_orphan_files(retention_threshold => '{retention}')",
            ):
                cursor.execute(f"ALTER TABLE {qualified_table} EXECUTE {op}")
                cursor.fetchall()
            results[table] = "ok"
        except Exception as exc:  # noqa: BLE001
            results[table] = f"error: {type(exc).__name__}: {exc}"

    return results


def _normalize_tables(tables: str | Iterable[str] | None) -> tuple[str, ...]:
    if tables is None:
        return tuple()
    if isinstance(tables, str):
        return tuple(t.strip() for t in tables.split(",") if t.strip())
    return tuple(str(t).strip() for t in tables if str(t).strip())
