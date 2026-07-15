from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

import pytest
from airflow.sdk.exceptions import AirflowFailException


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_ingest.manual_incident as dag_module  # noqa: E402
import traffic_ingest.common.runtime as runtime  # noqa: E402
import traffic_incident_bronze as scheduled_module  # noqa: E402
from traffic_ingest.errors import TrafficBronzeConfigurationError  # noqa: E402


class _Ti:
    def xcom_pull(self, **_kwargs):
        return {}


def _open_trino_from_batch(**kwargs):
    return kwargs["cursor_factory"]()


def _install_trino(monkeypatch, connect) -> None:
    trino_module = ModuleType("trino")
    dbapi_module = ModuleType("trino.dbapi")
    dbapi_module.connect = connect
    trino_module.dbapi = dbapi_module
    monkeypatch.setitem(sys.modules, "trino", trino_module)
    monkeypatch.setitem(sys.modules, "trino.dbapi", dbapi_module)


@pytest.mark.parametrize("raw_port", ["not-an-integer", "0", "-1", "65536"])
def test_traffic_trino_port_rejects_invalid_configuration(monkeypatch, raw_port):
    monkeypatch.setenv("TRINO_PORT", raw_port)

    with pytest.raises(TrafficBronzeConfigurationError, match="TRINO_PORT"):
        runtime.trino_port()


@pytest.mark.parametrize("raw_port", ["1", "8080", "65535"])
def test_traffic_trino_port_accepts_full_tcp_port_range(monkeypatch, raw_port):
    monkeypatch.setenv("TRINO_PORT", raw_port)

    assert runtime.trino_port() == int(raw_port)


def test_traffic_live_load_boundary_disables_retry_for_invalid_trino_port(
    monkeypatch,
):
    _install_trino(monkeypatch, lambda **_kwargs: pytest.fail("must not connect"))
    monkeypatch.setenv("TRINO_PORT", "not-an-integer")
    monkeypatch.setattr(
        dag_module,
        "load_traffic_bronze_batch",
        _open_trino_from_batch,
    )

    with pytest.raises(AirflowFailException, match="TRINO_PORT"):
        dag_module.load_seoul_traffic_bronze(
            ti=_Ti(),
            run_id="manual__invalid-trino-port",
        )


def test_traffic_live_load_boundary_preserves_trino_connection_failure(monkeypatch):
    error = ConnectionError("Trino connection reset")
    _install_trino(
        monkeypatch,
        lambda **_kwargs: (_ for _ in ()).throw(error),
    )
    monkeypatch.setenv("TRINO_PORT", "8080")
    monkeypatch.setattr(
        dag_module,
        "load_traffic_bronze_batch",
        _open_trino_from_batch,
    )

    with pytest.raises(ConnectionError) as raised:
        dag_module.load_seoul_traffic_bronze(
            ti=_Ti(),
            run_id="manual__trino-reset",
        )

    assert raised.value is error


@pytest.mark.parametrize("raw_limit", ["not-an-integer", "0", "-1"])
def test_traffic_materializer_batch_limit_rejects_invalid_configuration(
    monkeypatch,
    raw_limit,
):
    monkeypatch.setenv("ASK_SEOUL_TRAFFIC_MATERIALIZER_BATCH_SIZE", raw_limit)

    with pytest.raises(
        TrafficBronzeConfigurationError,
        match="ASK_SEOUL_TRAFFIC_MATERIALIZER_BATCH_SIZE",
    ):
        scheduled_module._materializer_batch_limit()
