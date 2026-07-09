import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest import bronze  # noqa: E402


class RecordingCatalog:
    def __init__(self, name, **properties):
        self.name = name
        self.properties = properties


def _build_catalog(monkeypatch):
    import pyiceberg.catalog.rest as rest

    monkeypatch.setattr(rest, "RestCatalog", RecordingCatalog)
    for name in (
        "R2_DATA_CATALOG_URI",
        "R2_DATA_CATALOG_WAREHOUSE",
        "R2_DATA_CATALOG_TOKEN",
        "R2_ENDPOINT",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
    ):
        monkeypatch.setenv(name, "test-value")
    return bronze._pyiceberg_catalog()


def test_pyiceberg_catalog_relaxes_s3_request_timeout(monkeypatch):
    monkeypatch.delenv("R2_S3_REQUEST_TIMEOUT", raising=False)
    monkeypatch.delenv("R2_DEV_S3_REQUEST_TIMEOUT", raising=False)

    catalog = _build_catalog(monkeypatch)

    assert catalog.properties["s3.request-timeout"] == "10"


def test_pyiceberg_catalog_request_timeout_env_override(monkeypatch):
    monkeypatch.setenv("R2_S3_REQUEST_TIMEOUT", "7.5")
    monkeypatch.setenv("R2_DEV_S3_REQUEST_TIMEOUT", "7.5")

    catalog = _build_catalog(monkeypatch)

    assert catalog.properties["s3.request-timeout"] == "7.5"
