"""env 키 규약 — 키 이름은 배포 환경을 담지 않는다 (ASK-Seoul#78 `Z-7` · ASAC-DAG#647).

canonical 한 벌(``R2_*``·``R2_DATA_CATALOG_*``·``TRINO_ICEBERG_CATALOG``)만 읽고, 어느
버킷·창고냐는 그 키의 **값**이 정한다. dev 로 되돌릴 때도 키가 아니라 값을 바꾼다.

예전엔 구 규약(`sample/.env` 한 파일에 dev·prod 를 ``R2_DEV_`` 접두어로 갈라 담던 방식)을
함께 지원하는 분기가 있었고, 이 파일은 그 분기를 잠그고 있었다. 호스트 ENV2 개편이 그
키들을 없애 분기는 이미 발동하지 않는 상태였고(#78 실측: 운영 맥미니·로컬 dev 박스 모두
``R2_DEV_BUCKET_NAME`` 미설정), 남겨 두면 누가 그 키를 채우는 순간 같은 날짜 기록이 두
버킷으로 갈린다. 그래서 통로 자체를 없애고, 이 파일이 잠그는 대상도 새 규약으로 바꾼다.
"""
from __future__ import annotations

import pytest

from culture_ingest.common.config import (
    CATALOG_PREFIX,
    R2_PREFIX,
    build_r2_settings,
    missing_r2,
    resolve_target,
)
from culture_ingest.common.warehouse import DEV_CATALOG, PROD_CATALOG, build_warehouse_settings

# 값이 dev 창고를 가리키는 env. 키에는 환경 표시가 없다. 값은 전부 가짜.
DEV_ENV = {
    "R2_ENDPOINT": "https://dev.example",
    "R2_ACCESS_KEY_ID": "dev-access",
    "R2_SECRET_ACCESS_KEY": "dev-secret",
    "R2_BUCKET_NAME": "seoul-dev",
    "TRINO_ICEBERG_CATALOG": DEV_CATALOG,
}

PROD_ENV = {
    "R2_ENDPOINT": "https://prod.example",
    "R2_ACCESS_KEY_ID": "prod-access",
    "R2_SECRET_ACCESS_KEY": "prod-secret",
    "R2_BUCKET_NAME": "seoul",
    "TRINO_ICEBERG_CATALOG": PROD_CATALOG,
}

# 폐지된 키만 채워진 env — 자격증명으로 인정되면 안 된다.
LEGACY_ONLY_ENV = {
    "R2_DEV_ENDPOINT": "https://dev.example",
    "R2_DEV_ACCESS_KEY_ID": "dev-access",
    "R2_DEV_SECRET_ACCESS_KEY": "dev-secret",
    "R2_DEV_BUCKET_NAME": "seoul-dev",
}


@pytest.fixture(autouse=True)
def _clean_process_env(monkeypatch):
    """``pick`` 은 os.environ 을 우선하므로, 실행 환경의 R2_*/TRINO_*/타깃을 걷어낸다."""
    import os

    for key in list(os.environ):
        if key.startswith(("R2_", "TRINO_")) or key in ("ASK_SEOUL_TARGET", "DBT_TARGET"):
            monkeypatch.delenv(key, raising=False)


def _load(env: dict[str, str], tmp_path, name: str) -> str:
    path = tmp_path / name
    path.write_text("\n".join(f"{k}={v}" for k, v in env.items()), encoding="utf-8")
    return str(path)


# ── 자격증명: canonical 한 벌만 ────────────────────────────────────────────────

def test_credentials_come_from_canonical_keys_on_both_targets(tmp_path):
    dev = build_r2_settings("dev", env_file=_load(DEV_ENV, tmp_path, ".env.dev"))
    prod = build_r2_settings("prod", env_file=_load(PROD_ENV, tmp_path, ".env.prod"))
    assert (dev.prefix, prod.prefix) == (R2_PREFIX, R2_PREFIX)
    assert (dev.bucket, prod.bucket) == ("seoul-dev", "seoul")


def test_deprecated_keys_are_not_credentials(tmp_path):
    """``R2_DEV_*`` 만 채워진 박스는 '자격증명 있음'이 아니라 **누락**으로 보고돼야 한다.

    폴백으로 인정하면 그 키를 채운 사람이 어느 버킷을 쓰는지 모른 채 계속 쓰게 된다.
    """
    settings = build_r2_settings("dev", env_file=_load(LEGACY_ONLY_ENV, tmp_path, ".env.dev"))
    assert missing_r2(settings) == [
        "R2_ENDPOINT",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET_NAME",
    ]


def test_catalog_prefix_is_single_set():
    assert (R2_PREFIX, CATALOG_PREFIX) == ("R2_", "R2_DATA_CATALOG_")


# ── 창고: 선언된 '값'이 정한다 ─────────────────────────────────────────────────

def test_warehouse_follows_the_declared_value():
    assert build_warehouse_settings("dev", env=DEV_ENV).catalog == DEV_CATALOG
    assert build_warehouse_settings("prod", env=PROD_ENV).catalog == PROD_CATALOG


def test_warehouse_never_guesses_dev_when_unset():
    """미설정 시 dev 를 추측하면 그게 곧 '코드가 환경을 고르는' 자리다(`Z-7`)."""
    assert build_warehouse_settings("dev", env={}).catalog == PROD_CATALOG


def test_declared_value_wins_even_when_it_contradicts_the_target():
    """⚠️ 값 오설정(target=dev 인데 env 가 prod 값 한 벌)은 여기서 안 막는다.

    키 이름으로 되돌리면 `Z-7` 을 다시 어기는 것이라, 이 조합을 거르는 것은 **값 기반
    게이트**의 몫이다 — 카탈로그 *이름* 이 아니라 창고·버킷 *값* 을 target 과 대조하는
    검사. culture 는 아직 그런 게이트를 안 부른다(#78 에 확인 요청).

    주의: 여기서 `PROD_CATALOG`(=``iceberg``)는 "prod 창고"라는 뜻이 아니라 **canonical
    카탈로그 이름**이다. 실제로 어느 창고냐는 그 카탈로그가 읽는
    ``R2_DATA_CATALOG_WAREHOUSE`` 값이 정한다(dev 박스 실측: ``iceberg`` →
    ``..._seoul-dev``). 이름으로 환경을 읽으려 드는 순간 `Z-7` 을 다시 어긴다.
    """
    assert build_warehouse_settings("dev", env=PROD_ENV).catalog == PROD_CATALOG


# ── target 해석: 모르면 추측하지 않고 배포 선언을 읽는다 ──────────────────────

def test_target_defaults_to_the_runtime_declaration(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "prod")
    assert resolve_target() == "prod"
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    assert resolve_target() == "dev"


def test_explicit_target_still_wins(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "prod")
    assert resolve_target("dev") == "dev"


def test_typo_target_is_rejected():
    with pytest.raises(ValueError):
        resolve_target("prd")
