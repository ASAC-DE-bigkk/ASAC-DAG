from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

import pytest
from airflow.sdk.exceptions import AirflowFailException


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import weather_vilage_fcst_bronze as dag_module  # noqa: E402
import weather_ingest.common.runtime as runtime  # noqa: E402
import weather_ingest.kma as kma  # noqa: E402
from weather_ingest.errors import WeatherBronzeConfigurationError  # noqa: E402


class _Dag:
    dag_id = "weather_vilage_fcst_bronze"


class _EmptyDagRun:
    conf = {}


class _FixedWindowDagRun:
    conf = {"base_date": "20260715", "base_time": "0800"}


class _Ti:
    def xcom_pull(self, **_kwargs):
        return {}


def _open_trino_from_batch(**kwargs):
    return kwargs["ports"].open_trino()


def _install_trino(monkeypatch, connect) -> None:
    trino_module = ModuleType("trino")
    dbapi_module = ModuleType("trino.dbapi")
    dbapi_module.connect = connect
    trino_module.dbapi = dbapi_module
    monkeypatch.setitem(sys.modules, "trino", trino_module)
    monkeypatch.setitem(sys.modules, "trino.dbapi", dbapi_module)


@pytest.mark.parametrize("raw_port", ["not-an-integer", "0", "-1", "65536"])
def test_weather_trino_port_rejects_invalid_configuration(monkeypatch, raw_port):
    monkeypatch.setenv("TRINO_PORT", raw_port)

    with pytest.raises(WeatherBronzeConfigurationError, match="TRINO_PORT"):
        runtime.trino_port()


@pytest.mark.parametrize("raw_port", ["1", "8080", "65535"])
def test_weather_trino_port_accepts_full_tcp_port_range(monkeypatch, raw_port):
    monkeypatch.setenv("TRINO_PORT", raw_port)

    assert runtime.trino_port() == int(raw_port)


def test_weather_live_load_boundary_disables_retry_for_invalid_trino_port(
    monkeypatch,
):
    _install_trino(monkeypatch, lambda **_kwargs: pytest.fail("must not connect"))
    monkeypatch.setenv("TRINO_PORT", "not-an-integer")
    monkeypatch.setattr(
        dag_module,
        "load_kma_bronze_batch",
        _open_trino_from_batch,
    )

    with pytest.raises(AirflowFailException, match="TRINO_PORT"):
        dag_module.load_kma_bronze(
            dag_run=_EmptyDagRun(),
            ti=_Ti(),
            run_id="manual__invalid-trino-port",
        )


def test_weather_live_load_boundary_preserves_trino_connection_failure(monkeypatch):
    error = ConnectionError("Trino connection reset")
    _install_trino(
        monkeypatch,
        lambda **_kwargs: (_ for _ in ()).throw(error),
    )
    monkeypatch.setenv("TRINO_PORT", "8080")
    monkeypatch.setattr(
        dag_module,
        "load_kma_bronze_batch",
        _open_trino_from_batch,
    )

    with pytest.raises(ConnectionError) as raised:
        dag_module.load_kma_bronze(
            dag_run=_EmptyDagRun(),
            ti=_Ti(),
            run_id="manual__trino-reset",
        )

    assert raised.value is error


@pytest.mark.parametrize("raw_page", ["not-an-integer", "0", "-1"])
def test_kma_page_no_rejects_invalid_environment_configuration(
    monkeypatch,
    raw_page,
):
    monkeypatch.setenv("KMA_PAGE_NO", raw_page)

    with pytest.raises(WeatherBronzeConfigurationError, match="KMA_PAGE_NO"):
        kma.kma_page_no()


@pytest.mark.parametrize("raw_delay", ["not-an-integer", "-1"])
def test_kma_publish_delay_rejects_invalid_environment_configuration(
    monkeypatch,
    raw_delay,
):
    monkeypatch.delenv("KMA_BASE_DATE", raising=False)
    monkeypatch.delenv("KMA_BASE_TIME", raising=False)
    monkeypatch.setenv("KMA_PUBLISH_DELAY_MINUTES", raw_delay)

    with pytest.raises(
        WeatherBronzeConfigurationError,
        match="KMA_PUBLISH_DELAY_MINUTES",
    ):
        kma.resolve_kma_base_datetime()


def test_weather_live_landing_boundary_disables_retry_for_invalid_publish_delay(
    monkeypatch,
):
    monkeypatch.delenv("KMA_BASE_DATE", raising=False)
    monkeypatch.delenv("KMA_BASE_TIME", raising=False)
    monkeypatch.setenv("KMA_PUBLISH_DELAY_MINUTES", "not-an-integer")

    with pytest.raises(AirflowFailException, match="KMA_PUBLISH_DELAY_MINUTES"):
        dag_module.land_kma_raw(
            dag=_Dag(),
            dag_run=_EmptyDagRun(),
            run_id="manual__invalid-publish-delay",
        )


def test_missing_kma_grid_file_is_configuration_failure(tmp_path):
    missing = tmp_path / "missing-grids.csv"

    with pytest.raises(WeatherBronzeConfigurationError, match="grid CSV"):
        kma.load_kma_grids(str(missing), expected_grid_count=1)


def test_non_utf8_kma_grid_file_is_configuration_failure(tmp_path):
    grid_path = tmp_path / "grids.csv"
    grid_path.write_bytes(b"place_id,nx,ny\nseoul,\xff,127\n")

    with pytest.raises(WeatherBronzeConfigurationError, match="grid CSV"):
        kma.load_kma_grids(str(grid_path), expected_grid_count=1)


@pytest.mark.parametrize(
    "content",
    [
        "place_id,nx,ny\nseoul,not-an-integer,127\n",
        "place_id,nx\nseoul,60\n",
        'place_id,nx,ny\n"unterminated,60,127\n',
    ],
)
def test_malformed_kma_grid_content_is_configuration_failure(tmp_path, content):
    grid_path = tmp_path / "grids.csv"
    grid_path.write_text(content, encoding="utf-8")

    with pytest.raises(WeatherBronzeConfigurationError, match="grid CSV"):
        kma.load_kma_grids(str(grid_path), expected_grid_count=1)


def test_kma_grid_transient_filesystem_error_is_not_reclassified(monkeypatch):
    error = OSError("temporary grid storage failure")

    def fail_open(*_args, **_kwargs):
        raise error

    monkeypatch.setattr(Path, "open", fail_open)

    with pytest.raises(OSError) as raised:
        kma.load_kma_grids("grids.csv", expected_grid_count=1)

    assert raised.value is error


def test_weather_live_landing_boundary_disables_retry_for_missing_grid_file(
    monkeypatch,
    tmp_path,
):
    missing = tmp_path / "missing-grids.csv"
    monkeypatch.setenv("ASK_SEOUL_KMA_GRID_CSV", str(missing))

    with pytest.raises(AirflowFailException, match="grid CSV"):
        dag_module.land_kma_raw(
            dag=_Dag(),
            dag_run=_FixedWindowDagRun(),
            run_id="manual__missing-grid-file",
        )
