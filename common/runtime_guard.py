"""Fail-fast checks for the shared traffic/weather runtime contract."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping


class RuntimeTargetError(RuntimeError):
    """Raised when a DAG would use an unsafe or inconsistent runtime target."""


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DOMAIN_SCHEMA_DEFAULTS = {"traffic": "traffic", "weather": "weather"}
_RESERVED_PROD_SCHEMAS = {"ops_smoke", "prod", "production"}
_TARGET_CATALOGS = {
    "dev": ("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev"),
    "prod": ("TRINO_ICEBERG_CATALOG", "iceberg"),
}


def validate_dev_runtime(
    domain: str,
    env: Mapping[str, str] | None = None,
    requested_target: str | None = None,
) -> None:
    """Validate the currently approved dev/prod catalog/schema contract.

    Both ``dev`` and ``prod`` are valid targets. What this still enforces is
    that ``DBT_TARGET``/``ASK_SEOUL_TARGET`` (bronze) and the DAG's
    ``requested_target`` (transform ``--target`` param) all agree, so bronze
    and transform can never silently run against different environments.
    Secret values are intentionally not inspected or included in errors.  The
    check runs as an Airflow task before API collection or dbt execution.
    """

    values = env if env is not None else os.environ
    if domain not in _DOMAIN_SCHEMA_DEFAULTS:
        raise RuntimeTargetError(f"unsupported runtime guard domain: {domain}")

    target_values = [
        (name, str(values[name]).strip().lower())
        for name in ("DBT_TARGET", "ASK_SEOUL_TARGET")
        if str(values.get(name, "")).strip()
    ]
    if not target_values:
        raise RuntimeTargetError("runtime target must be explicitly set to dev or prod")
    if len({value for _, value in target_values}) != 1:
        raise RuntimeTargetError("runtime target aliases disagree")
    target = target_values[0][1]
    if target not in _TARGET_CATALOGS:
        raise RuntimeTargetError("runtime target must be dev or prod")
    if requested_target is not None:
        requested = str(requested_target).strip().lower()
        if requested not in _TARGET_CATALOGS:
            raise RuntimeTargetError("requested runtime target must be dev or prod")
        if requested != target:
            raise RuntimeTargetError("requested runtime target disagrees with environment")

    catalog_env, expected_catalog = _TARGET_CATALOGS[target]
    catalog = str(values.get(catalog_env, "")).strip()
    if catalog != expected_catalog:
        raise RuntimeTargetError(f"{target} catalog must be {expected_catalog}")

    schema_env = f"{domain.upper()}_SCHEMA"
    schema_values = {
        "ASK_SEOUL_SCHEMA": str(values.get("ASK_SEOUL_SCHEMA", "ask_seoul")).strip(),
        schema_env: str(values.get(schema_env, _DOMAIN_SCHEMA_DEFAULTS[domain])).strip(),
    }
    for env_name, schema in schema_values.items():
        if not _IDENTIFIER.fullmatch(schema):
            raise RuntimeTargetError(f"schema identifier is invalid: {env_name}")
        if target == "dev" and schema.lower() in _RESERVED_PROD_SCHEMAS:
            raise RuntimeTargetError(f"schema is reserved for another environment: {env_name}")
