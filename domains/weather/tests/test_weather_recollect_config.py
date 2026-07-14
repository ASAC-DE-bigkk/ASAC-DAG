import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.errors import WeatherInvalidWindowError  # noqa: E402
from weather_ingest.kma import kma_base_datetime_from_conf  # noqa: E402


def test_kma_recollect_conf_is_optional():
    assert kma_base_datetime_from_conf({}) is None
    assert kma_base_datetime_from_conf(None) is None


def test_kma_recollect_conf_accepts_base_datetime():
    assert kma_base_datetime_from_conf(
        {"base_date": "20260703", "base_time": "0800"}
    ) == (
        "20260703",
        "0800",
    )


def test_kma_recollect_conf_requires_date_and_time_together():
    with pytest.raises(WeatherInvalidWindowError, match="base_date and base_time"):
        kma_base_datetime_from_conf({"base_date": "20260703"})


def test_kma_recollect_conf_rejects_invalid_time():
    with pytest.raises(WeatherInvalidWindowError, match="base_time"):
        kma_base_datetime_from_conf({"base_date": "20260703", "base_time": "0900"})
