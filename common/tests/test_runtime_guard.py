import pytest

from common.runtime_guard import RuntimeTargetError, validate_dev_runtime


def test_accepts_dev_runtime_with_domain_schema_defaults():
    validate_dev_runtime(
        "traffic",
        {
            "DBT_TARGET": "dev",
            "TRINO_DEV_ICEBERG_CATALOG": "iceberg_dev",
        },
    )


def test_rejects_prod_target_even_when_catalog_is_dev():
    with pytest.raises(RuntimeTargetError, match="target"):
        validate_dev_runtime(
            "weather",
            {
                "DBT_TARGET": "prod",
                "TRINO_DEV_ICEBERG_CATALOG": "iceberg_dev",
            },
        )


def test_rejects_requested_transform_target_mismatch():
    with pytest.raises(RuntimeTargetError, match="target"):
        validate_dev_runtime(
            "weather",
            {
                "DBT_TARGET": "dev",
                "TRINO_DEV_ICEBERG_CATALOG": "iceberg_dev",
            },
            requested_target="prod",
        )


def test_rejects_conflicting_target_aliases():
    with pytest.raises(RuntimeTargetError, match="target"):
        validate_dev_runtime(
            "traffic",
            {
                "DBT_TARGET": "dev",
                "ASK_SEOUL_TARGET": "prod",
            },
        )


def test_rejects_non_dev_catalog():
    with pytest.raises(RuntimeTargetError, match="catalog"):
        validate_dev_runtime(
            "traffic",
            {
                "DBT_TARGET": "dev",
                "TRINO_DEV_ICEBERG_CATALOG": "iceberg",
            },
        )


def test_rejects_prod_namespace_in_dev_source_schema():
    with pytest.raises(RuntimeTargetError, match="schema"):
        validate_dev_runtime(
            "traffic",
            {
                "DBT_TARGET": "dev",
                "TRINO_DEV_ICEBERG_CATALOG": "iceberg_dev",
                "ASK_SEOUL_SCHEMA": "ops_smoke",
            },
        )
