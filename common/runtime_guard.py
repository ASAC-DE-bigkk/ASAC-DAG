"""Fail-fast checks for the shared development runtime contract."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping


class RuntimeTargetError(RuntimeError):
    """Raised when a DAG would use an unsafe or inconsistent runtime target."""


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DOMAIN_SCHEMA_DEFAULTS = {"traffic": "traffic", "weather": "weather"}
_RESERVED_PROD_SCHEMAS = {"ops_smoke", "prod", "production"}


def validate_dev_runtime(
    domain: str,
    env: Mapping[str, str] | None = None,
    requested_target: str | None = None,
) -> None:
    """Validate the currently approved dev-only catalog/schema contract.

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
        raise RuntimeTargetError("runtime target must be explicitly set to dev")
    if len({value for _, value in target_values}) != 1:
        raise RuntimeTargetError("runtime target aliases disagree")
    if target_values[0][1] != "dev":
        raise RuntimeTargetError("runtime target must be dev; prod writes are disabled")
    if requested_target is not None:
        requested = str(requested_target).strip().lower()
        if requested != "dev":
            raise RuntimeTargetError("requested runtime target must be dev")
        if requested != target_values[0][1]:
            raise RuntimeTargetError("requested runtime target disagrees with environment")

    catalog = str(values.get("TRINO_DEV_ICEBERG_CATALOG", "")).strip()
    if catalog != "iceberg_dev":
        raise RuntimeTargetError("dev catalog must be iceberg_dev")

    schema_env = f"{domain.upper()}_SCHEMA"
    schema_values = {
        "ASK_SEOUL_SCHEMA": str(values.get("ASK_SEOUL_SCHEMA", "ask_seoul")).strip(),
        schema_env: str(values.get(schema_env, _DOMAIN_SCHEMA_DEFAULTS[domain])).strip(),
    }
    for env_name, schema in schema_values.items():
        if not _IDENTIFIER.fullmatch(schema):
            raise RuntimeTargetError(f"schema identifier is invalid: {env_name}")
        if schema.lower() in _RESERVED_PROD_SCHEMAS:
            raise RuntimeTargetError(f"schema is reserved for another environment: {env_name}")
