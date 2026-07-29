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
TARGET_CHOICES = ("dev", "prod")
_TARGET_ALIASES = ("ASK_SEOUL_TARGET", "DBT_TARGET")
_R2_TARGETS = {
    "dev": (
        "seoul-dev",
        (
            "R2_DEV_BUCKET_NAME",
            "R2_DEV_ENDPOINT",
            "R2_DEV_ACCESS_KEY_ID",
            "R2_DEV_SECRET_ACCESS_KEY",
        ),
    ),
    "prod": (
        "seoul",
        (
            "R2_BUCKET_NAME",
            "R2_ENDPOINT",
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY",
        ),
    ),
}


def default_target(env: Mapping[str, str] | None = None) -> str:
    """DAG ``target`` Param 의 기본값 — 런타임 env 를 따라간다.

    bronze 는 env(``is_dev_target()``)로, transform 은 DAG param 으로 타깃을 정하던
    이원화(#236)를 없앤다. env 를 prod 로 넘기면 transform param 도 같이 따라오므로
    "bronze=prod / silver=dev" 엇갈림이 생기지 않는다.

    알 수 없는 값은 ``dev`` 로 clamp 한다 — Param ``enum`` 밖의 기본값은 DAG 파싱
    자체를 깨뜨리는데, 그러면 잘못된 값 하나가 도메인 전체를 스케줄에서 지운다.
    실제 거부는 태스크 시점의 :func:`validate_dev_runtime` 이 맡는다.
    """
    values = os.environ if env is None else env
    for name in _TARGET_ALIASES:
        candidate = str(values.get(name, "")).strip().lower()
        if candidate:
            return candidate if candidate in TARGET_CHOICES else "dev"
    return "dev"


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
            raise RuntimeTargetError(
                f"schema is reserved for another environment: {env_name}"
            )

    expected_bucket, required_r2_keys = _R2_TARGETS[target]
    for env_name in required_r2_keys:
        if not str(values.get(env_name, "")).strip():
            raise RuntimeTargetError(
                f"{target} R2 credential is missing: {env_name}"
            )
    bucket_name = str(values[required_r2_keys[0]]).strip()
    if bucket_name != expected_bucket:
        raise RuntimeTargetError(
            f"{target} R2 bucket must be {expected_bucket}"
        )
