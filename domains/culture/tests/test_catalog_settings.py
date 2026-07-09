"""#203 — R2 Data Catalog 설정(dev/prod 분기·누락 에러·redaction)과 엔진 디스패치.

pyiceberg 불필요(설정·디스패치는 lazy import 밖) — 로컬에서도 돈다.
"""
from __future__ import annotations

import pytest

from common.security.redaction import get_default_redactor, redact

FAKE_TOKEN = "FAKECATALOGTOKEN1234567890"
FAKE_SECRET = "FAKER2SECRETKEY0987654321"

CATALOG_ENV = {
    "R2_DEV_DATA_CATALOG_URI": "https://catalog.example/dev",
    "R2_DEV_DATA_CATALOG_WAREHOUSE": "acct_seoul-dev",
    "R2_DEV_DATA_CATALOG_TOKEN": FAKE_TOKEN,
    "R2_DEV_ENDPOINT": "https://r2.example",
    "R2_DEV_ACCESS_KEY_ID": "fake-access",
    "R2_DEV_SECRET_ACCESS_KEY": FAKE_SECRET,
    "R2_DEV_BUCKET_NAME": "seoul-dev",
}


@pytest.fixture()
def _dev_env(monkeypatch):
    for k, v in CATALOG_ENV.items():
        monkeypatch.setenv(k, v)
    yield
    red = get_default_redactor()
    for s in (FAKE_TOKEN, FAKE_SECRET):  # 전역 오염 방지
        if s in red._literals:  # noqa: SLF001 -- 테스트 정리 용도
            red._literals.remove(s)


def test_build_catalog_settings_dev_prefix(_dev_env):
    from culture_ingest.common.config import build_catalog_settings
    s = build_catalog_settings("dev")
    assert s.uri == "https://catalog.example/dev"
    assert s.warehouse == "acct_seoul-dev"
    assert s.token == FAKE_TOKEN
    assert s.s3_endpoint == "https://r2.example"
    assert s.s3_access_key_id == "fake-access"
    assert s.s3_secret_access_key == FAKE_SECRET
    assert s.s3_region == "auto"


def test_build_catalog_settings_prod_prefix(monkeypatch):
    monkeypatch.setenv("R2_DATA_CATALOG_URI", "https://catalog.example/prod")
    monkeypatch.setenv("R2_DATA_CATALOG_WAREHOUSE", "acct_seoul")
    monkeypatch.setenv("R2_DATA_CATALOG_TOKEN", "FAKEPRODTOKEN123456")
    monkeypatch.setenv("R2_ENDPOINT", "https://r2.example")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fake")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "FAKEPRODSECRET12345")
    monkeypatch.setenv("R2_BUCKET_NAME", "seoul")
    from culture_ingest.common.config import build_catalog_settings
    s = build_catalog_settings("prod")
    assert s.warehouse == "acct_seoul"
    red = get_default_redactor()
    for v in ("FAKEPRODTOKEN123456", "FAKEPRODSECRET12345"):  # 정리
        if v in red._literals:  # noqa: SLF001
            red._literals.remove(v)


def test_build_catalog_settings_missing_raises(monkeypatch, _dev_env):
    monkeypatch.delenv("R2_DEV_DATA_CATALOG_URI", raising=False)
    from culture_ingest.common.config import build_catalog_settings
    with pytest.raises(RuntimeError, match="R2_DEV_DATA_CATALOG_URI"):
        build_catalog_settings("dev")


def test_build_catalog_settings_registers_secrets(_dev_env):
    """토큰·secret key가 redactor literal 로 등록돼 에러 표면에서 가려진다(#144)."""
    from culture_ingest.common.config import build_catalog_settings
    build_catalog_settings("dev")
    assert FAKE_TOKEN not in redact(f"401 Unauthorized: token={FAKE_TOKEN}")
    assert FAKE_SECRET not in redact(f"s3 error secret={FAKE_SECRET}")


def test_build_catalog_settings_rejects_bad_target(_dev_env):
    from culture_ingest.common.config import build_catalog_settings
    with pytest.raises(ValueError):
        build_catalog_settings("prd")
