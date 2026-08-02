"""ASK-Seoul#66 — env 규약 2종에서 R2·Data Catalog·Trino 카탈로그가 어디를 가리키는지.

구 규약(`sample/.env`)은 한 파일에 dev/prod 를 `_DEV_` 접두어로 갈라 담았고,
신 규약(`sample/.env.dev`, `sample/.env.prod`)은 파일 하나가 한 환경이라 접두어 없는
한 벌만 둔다. 접두어를 target 만으로 정하면 신 규약 dev 에서 자격증명이 통째로 비고,
반대로 구 규약에서 접두어 없는 키로 폴백하면 **dev 런이 prod 창고에 쓴다**.

여기서 고정하는 건 그 두 사고가 다시 나지 않는다는 것이다.
"""
from __future__ import annotations

import pytest

from culture_ingest.common.config import (
    build_r2_settings,
    catalog_prefix,
    missing_r2,
    r2_prefix,
    uses_split_dev_keys,
)
from culture_ingest.common.warehouse import build_warehouse_settings

# 구 규약: dev(_DEV_)·prod(무접두어)를 한 파일에. 값은 전부 가짜.
SPLIT_ENV = {
    "R2_DEV_ENDPOINT": "https://dev.example",
    "R2_DEV_ACCESS_KEY_ID": "dev-access",
    "R2_DEV_SECRET_ACCESS_KEY": "dev-secret",
    "R2_DEV_BUCKET_NAME": "seoul-dev",
    "R2_ENDPOINT": "https://prod.example",
    "R2_ACCESS_KEY_ID": "prod-access",
    "R2_SECRET_ACCESS_KEY": "prod-secret",
    "R2_BUCKET_NAME": "seoul",
    "TRINO_ICEBERG_CATALOG": "iceberg",
}

# 신 규약: 접두어 없는 한 벌만. 값이 dev 창고를 가리킨다.
SINGLE_ENV = {
    "R2_ENDPOINT": "https://dev.example",
    "R2_ACCESS_KEY_ID": "dev-access",
    "R2_SECRET_ACCESS_KEY": "dev-secret",
    "R2_BUCKET_NAME": "seoul-dev",
    "TRINO_ICEBERG_CATALOG": "iceberg",
}


@pytest.fixture(autouse=True)
def _clean_process_env(monkeypatch):
    """``pick`` 은 os.environ 을 우선하므로, 실행 환경의 R2_*/TRINO_* 를 걷어낸다."""
    import os

    for key in list(os.environ):
        if key.startswith(("R2_", "TRINO_")):
            monkeypatch.delenv(key, raising=False)


def _load(env: dict[str, str], tmp_path, name: str) -> str:
    path = tmp_path / name
    path.write_text("\n".join(f"{k}={v}" for k, v in env.items()), encoding="utf-8")
    return str(path)


def test_split_env_is_detected_and_single_env_is_not():
    assert uses_split_dev_keys(SPLIT_ENV) is True
    assert uses_split_dev_keys(SINGLE_ENV) is False
    assert uses_split_dev_keys({}) is False


@pytest.mark.parametrize(
    ("env", "target", "expected_r2", "expected_catalog"),
    [
        (SPLIT_ENV, "dev", "R2_DEV_", "R2_DEV_DATA_CATALOG_"),
        (SPLIT_ENV, "prod", "R2_", "R2_DATA_CATALOG_"),
        (SINGLE_ENV, "dev", "R2_", "R2_DATA_CATALOG_"),
        (SINGLE_ENV, "prod", "R2_", "R2_DATA_CATALOG_"),
    ],
)
def test_prefix_follows_env_convention(env, target, expected_r2, expected_catalog):
    assert r2_prefix(target, env) == expected_r2
    assert catalog_prefix(target, env) == expected_catalog


def test_single_env_dev_resolves_credentials_instead_of_failing(tmp_path):
    """신 규약 dev 에서 `R2_DEV_*` 를 찾다가 4키가 비던 회귀(실측)."""
    settings = build_r2_settings("dev", env_file=_load(SINGLE_ENV, tmp_path, ".env.dev"))
    assert missing_r2(settings) == []
    assert settings.bucket == "seoul-dev"
    assert settings.prefix == "R2_"


def test_split_env_dev_still_uses_dev_bucket(tmp_path):
    """구 규약에서 dev 가 prod 버킷으로 새지 않는다 — 되돌리기 어려운 사고 방지."""
    settings = build_r2_settings("dev", env_file=_load(SPLIT_ENV, tmp_path, ".env"))
    assert settings.bucket == "seoul-dev"
    assert settings.prefix == "R2_DEV_"


def test_missing_keys_are_reported_with_the_prefix_actually_used(tmp_path):
    """에러 메시지가 '채워야 할 키'를 가리켜야 한다 — 신 규약이면 무접두어 이름."""
    settings = build_r2_settings("dev", env_file=_load({"TRINO_ICEBERG_CATALOG": "iceberg"}, tmp_path, ".env.dev"))
    assert missing_r2(settings) == [
        "R2_ENDPOINT",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET_NAME",
    ]


def test_trino_catalog_never_falls_back_to_prod_on_split_env():
    """🔴 핵심 안전선: 구 규약 박스에는 iceberg(prod)·iceberg_dev(dev)가 함께 산다.

    dev 가 ``TRINO_ICEBERG_CATALOG``(=iceberg) 로 폴백하면 prod 창고에 쓴다.
    ``TRINO_DEV_ICEBERG_CATALOG`` 미설정이어도 기본 ``iceberg_dev`` 를 유지해야 한다.
    """
    assert build_warehouse_settings("dev", env=SPLIT_ENV).catalog == "iceberg_dev"
    assert build_warehouse_settings("prod", env=SPLIT_ENV).catalog == "iceberg"


def test_trino_catalog_uses_single_name_on_new_env():
    """신 규약은 논리명을 ``iceberg`` 하나로 통일하고, 값이 환경을 가른다."""
    assert build_warehouse_settings("dev", env=SINGLE_ENV).catalog == "iceberg"
    assert build_warehouse_settings("prod", env=SINGLE_ENV).catalog == "iceberg"


def test_split_env_honours_explicit_dev_catalog_override():
    env = {**SPLIT_ENV, "TRINO_ICEBERG_CATALOG": "iceberg_dev_custom"}
    assert build_warehouse_settings("dev", env=env).catalog == "iceberg_dev_custom"
