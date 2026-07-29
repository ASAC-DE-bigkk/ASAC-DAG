import pytest

from common.runtime_guard import (
    TARGET_CHOICES,
    RuntimeTargetError,
    default_target,
    validate_dev_runtime,
)


def test_accepts_dev_runtime_with_domain_schema_defaults():
    validate_dev_runtime(
        "traffic",
        {
            "DBT_TARGET": "dev",
            "TRINO_DEV_ICEBERG_CATALOG": "iceberg_dev",
            "R2_DEV_BUCKET_NAME": "seoul-dev",
            "R2_DEV_ENDPOINT": "https://dev.invalid",
            "R2_DEV_ACCESS_KEY_ID": "dev-access",
            "R2_DEV_SECRET_ACCESS_KEY": "dev-secret",
        },
    )


def test_accepts_prod_runtime_with_domain_schema_defaults():
    validate_dev_runtime(
        "weather",
        {
            "DBT_TARGET": "prod",
            "TRINO_ICEBERG_CATALOG": "iceberg",
            "R2_BUCKET_NAME": "seoul",
            "R2_ENDPOINT": "https://prod.invalid",
            "R2_ACCESS_KEY_ID": "prod-access",
            "R2_SECRET_ACCESS_KEY": "prod-secret",
        },
    )


def test_accepts_prod_runtime_with_dev_r2_keys_when_prod_tuple_is_valid():
    validate_dev_runtime(
        "weather",
        {
            "DBT_TARGET": "prod",
            "TRINO_ICEBERG_CATALOG": "iceberg",
            "R2_BUCKET_NAME": "seoul",
            "R2_ENDPOINT": "https://prod.invalid",
            "R2_ACCESS_KEY_ID": "prod-access",
            "R2_SECRET_ACCESS_KEY": "prod-secret",
            "R2_DEV_BUCKET_NAME": "seoul-dev",
        },
    )


def test_rejects_dev_runtime_when_dev_r2_tuple_is_incomplete():
    with pytest.raises(RuntimeTargetError, match="R2_DEV_ACCESS_KEY_ID"):
        validate_dev_runtime(
            "traffic",
            {
                "DBT_TARGET": "dev",
                "TRINO_DEV_ICEBERG_CATALOG": "iceberg_dev",
                "R2_DEV_BUCKET_NAME": "seoul-dev",
                "R2_DEV_ENDPOINT": "https://dev.invalid",
                "R2_ACCESS_KEY_ID": "prod-access",
                "R2_DEV_SECRET_ACCESS_KEY": "dev-secret",
            },
        )


def test_rejects_target_bucket_mismatch():
    with pytest.raises(RuntimeTargetError, match="bucket"):
        validate_dev_runtime(
            "traffic",
            {
                "DBT_TARGET": "prod",
                "TRINO_ICEBERG_CATALOG": "iceberg",
                "R2_BUCKET_NAME": "seoul-dev",
                "R2_ENDPOINT": "https://prod.invalid",
                "R2_ACCESS_KEY_ID": "prod-access",
                "R2_SECRET_ACCESS_KEY": "prod-secret",
            },
        )


def test_rejects_prod_target_when_catalog_is_dev():
    with pytest.raises(RuntimeTargetError, match="catalog"):
        validate_dev_runtime(
            "weather",
            {
                "DBT_TARGET": "prod",
                "TRINO_DEV_ICEBERG_CATALOG": "iceberg_dev",
            },
        )


def test_rejects_unsupported_target_value():
    with pytest.raises(RuntimeTargetError, match="target"):
        validate_dev_runtime(
            "weather",
            {
                "DBT_TARGET": "staging",
                "TRINO_ICEBERG_CATALOG": "iceberg",
            },
        )


def test_allows_reserved_schema_name_under_prod_target():
    validate_dev_runtime(
        "traffic",
        {
            "DBT_TARGET": "prod",
            "TRINO_ICEBERG_CATALOG": "iceberg",
            "ASK_SEOUL_SCHEMA": "ops_smoke",
            "R2_BUCKET_NAME": "seoul",
            "R2_ENDPOINT": "https://prod.invalid",
            "R2_ACCESS_KEY_ID": "prod-access",
            "R2_SECRET_ACCESS_KEY": "prod-secret",
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


def test_target_choices_are_dev_and_prod():
    assert TARGET_CHOICES == ("dev", "prod")


def test_default_target_prefers_ask_seoul_target():
    assert default_target({"ASK_SEOUL_TARGET": "prod", "DBT_TARGET": "dev"}) == "prod"


def test_default_target_falls_back_to_dbt_target():
    assert default_target({"DBT_TARGET": "prod"}) == "prod"


def test_default_target_defaults_to_dev_when_unset():
    assert default_target({}) == "dev"


def test_default_target_normalises_case_and_surrounding_space():
    assert default_target({"ASK_SEOUL_TARGET": "  PROD "}) == "prod"


def test_default_target_ignores_blank_alias():
    assert default_target({"ASK_SEOUL_TARGET": "   ", "DBT_TARGET": "prod"}) == "prod"


def test_default_target_clamps_unknown_target_to_dev():
    """DAG 파싱은 절대 죽지 않아야 한다 — 잘못된 값 거부는 validate_dev_runtime 의 몫."""
    assert default_target({"ASK_SEOUL_TARGET": "staging"}) == "dev"
