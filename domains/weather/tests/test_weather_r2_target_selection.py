from __future__ import annotations

import sys
from pathlib import Path

import pytest


WEATHER_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEATHER_ROOT))

from weather_ingest.common import runtime as target_runtime  # noqa: E402
from weather_ingest.errors import WeatherBronzeConfigurationError  # noqa: E402
from weather_ingest.reliability import history  # noqa: E402


def _set_both_r2_tuples(monkeypatch: pytest.MonkeyPatch, *, target: str) -> None:
    monkeypatch.setenv("ASK_SEOUL_TARGET", target)
    monkeypatch.setenv("DBT_TARGET", target)
    values = {
        "R2_BUCKET_NAME": "prod-bucket",
        "R2_ENDPOINT": "https://prod.invalid",
        "R2_ACCESS_KEY_ID": "prod-access",
        "R2_SECRET_ACCESS_KEY": "prod-secret",
        "R2_REGION": "prod-region",
        "R2_DEV_BUCKET_NAME": "dev-bucket",
        "R2_DEV_ENDPOINT": "https://dev.invalid",
        "R2_DEV_ACCESS_KEY_ID": "dev-access",
        "R2_DEV_SECRET_ACCESS_KEY": "dev-secret",
        "R2_DEV_REGION": "dev-region",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_prod_resolver_ignores_present_dev_tuple(monkeypatch):
    _set_both_r2_tuples(monkeypatch, target="prod")

    assert target_runtime.r2_env("R2_BUCKET_NAME") == "prod-bucket"
    assert target_runtime.r2_env("R2_ENDPOINT") == "https://prod.invalid"


def test_dev_resolver_never_falls_back_to_prod_tuple(monkeypatch):
    _set_both_r2_tuples(monkeypatch, target="dev")
    monkeypatch.delenv("R2_DEV_ACCESS_KEY_ID")

    with pytest.raises(
        WeatherBronzeConfigurationError, match="R2_DEV_ACCESS_KEY_ID"
    ):
        target_runtime.r2_env("R2_ACCESS_KEY_ID")


def test_reliability_history_uses_prod_tuple_when_dev_tuple_is_present(monkeypatch):
    _set_both_r2_tuples(monkeypatch, target="prod")
    captured: dict[str, object] = {}

    def build_storage(kind: str, **options: object) -> object:
        captured.update(kind=kind, **options)
        return captured

    monkeypatch.setattr("common.storage.build_storage", build_storage)

    history._build_history_storage()

    assert captured == {
        "kind": "r2",
        "bucket": "prod-bucket",
        "endpoint": "https://prod.invalid",
        "key": "prod-access",
        "secret": "prod-secret",
        "region": "prod-region",
    }
