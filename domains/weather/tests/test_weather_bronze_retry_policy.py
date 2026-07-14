from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from airflow.sdk.exceptions import AirflowFailException


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import weather_vilage_fcst_bronze as dag_module  # noqa: E402
from weather_ingest.errors import (  # noqa: E402
    WeatherBronzeConfigurationError,
    WeatherBronzeDeterministicError,
    WeatherCompletenessError,
    WeatherInvalidWindowError,
    WeatherRawIntegrityError,
    WeatherSourceBusinessError,
    WeatherSourceSchemaError,
)
from weather_ingest.common.runtime import required_env  # noqa: E402
from weather_ingest.bronze_batch import BronzeLoadPorts, load_kma_bronze_batch  # noqa: E402
from weather_ingest.bronze_contract import validate_kma_row_count  # noqa: E402
from weather_ingest.kma import (  # noqa: E402
    kma_num_of_rows,
    normalize_kma_base_datetime,
    parse_kma_response,
)
from weather_ingest.landing import (  # noqa: E402
    KmaLandingIncompleteError,
    RawObjectIntegrityError,
    verify_raw_payload_hash,
)


DETERMINISTIC_ERRORS = (
    WeatherBronzeConfigurationError,
    WeatherSourceBusinessError,
    WeatherSourceSchemaError,
    WeatherRawIntegrityError,
    WeatherCompletenessError,
    WeatherInvalidWindowError,
)


def retry_boundary():
    module = importlib.import_module("weather_ingest.bronze_dag_support")
    decorator = getattr(module, "fail_fast_weather_bronze", None)
    assert callable(decorator), "Weather Bronze retry boundary is missing"
    return decorator


def test_weather_bronze_error_types_share_one_deterministic_base():
    assert all(
        issubclass(error_type, WeatherBronzeDeterministicError)
        for error_type in DETERMINISTIC_ERRORS
    )


@pytest.mark.parametrize("error_type", DETERMINISTIC_ERRORS)
def test_each_weather_deterministic_error_disables_airflow_retry(error_type):
    @retry_boundary()
    def task_callable():
        raise error_type("permanent failure")

    with pytest.raises(AirflowFailException, match="permanent failure"):
        task_callable()


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("timeout"),
        ConnectionError("reset"),
        OSError("R2 unavailable"),
        RuntimeError("Trino unavailable"),
    ],
)
def test_weather_transient_error_remains_retryable(error):
    @retry_boundary()
    def task_callable():
        raise error

    with pytest.raises(type(error)) as raised:
        task_callable()

    assert raised.value is error


def test_weather_invalid_issue_window_uses_deterministic_type():
    with pytest.raises(WeatherInvalidWindowError, match="base_time"):
        normalize_kma_base_datetime("20260715", "0900")


def test_weather_business_code_uses_deterministic_type():
    payload = b'{"response":{"header":{"resultCode":"99","resultMsg":"denied"}}}'

    with pytest.raises(WeatherSourceBusinessError, match="resultCode=99"):
        parse_kma_response(payload)


def test_weather_malformed_json_uses_schema_type():
    with pytest.raises(WeatherSourceSchemaError, match="JSON"):
        parse_kma_response(b"{")


def test_weather_raw_hash_uses_integrity_type_and_legacy_name():
    with pytest.raises(WeatherRawIntegrityError) as raised:
        verify_raw_payload_hash(
            b"actual",
            expected_hash="not-the-hash",
            raw_object_key="raw/weather/page.json",
        )

    assert isinstance(raised.value, RawObjectIntegrityError)


def test_weather_completeness_legacy_name_uses_deterministic_type():
    assert issubclass(KmaLandingIncompleteError, WeatherCompletenessError)


def test_weather_missing_setting_uses_configuration_type(monkeypatch):
    monkeypatch.delenv("KMA_SERVICE_KEY", raising=False)

    with pytest.raises(WeatherBronzeConfigurationError, match="KMA_SERVICE_KEY"):
        required_env("KMA_SERVICE_KEY")


def test_weather_invalid_page_size_setting_uses_configuration_type(monkeypatch):
    monkeypatch.setenv("KMA_NUM_OF_ROWS", "0")

    with pytest.raises(WeatherBronzeConfigurationError, match="KMA_NUM_OF_ROWS"):
        kma_num_of_rows()


def test_weather_non_integer_page_size_setting_uses_configuration_type(monkeypatch):
    monkeypatch.setenv("KMA_NUM_OF_ROWS", "not-an-integer")

    with pytest.raises(WeatherBronzeConfigurationError, match="KMA_NUM_OF_ROWS"):
        kma_num_of_rows()


def test_weather_live_boundary_disables_retry_for_non_integer_page_size(monkeypatch):
    monkeypatch.setenv("KMA_NUM_OF_ROWS", "not-an-integer")
    monkeypatch.setattr(
        dag_module,
        "kma_base_datetime_from_conf",
        lambda _conf: ("20260715", "0800"),
    )
    monkeypatch.setattr(
        dag_module,
        "load_kma_grids",
        lambda: [{"place_id": "first", "nx": 60, "ny": 127}],
    )

    with pytest.raises(AirflowFailException, match="KMA_NUM_OF_ROWS"):
        dag_module.land_kma_raw(
            dag=_Dag(), dag_run=_DagRun(), run_id="manual__bad-page-size"
        )


def test_weather_row_validation_uses_completeness_type():
    with pytest.raises(WeatherCompletenessError, match="no forecast rows"):
        validate_kma_row_count([], {"total_count": 0}, 60, 127)


def test_weather_empty_bronze_batch_uses_completeness_type():
    ports = BronzeLoadPorts(
        open_trino=lambda: pytest.fail("must fail before Trino"),
        ensure_table=lambda *_args: pytest.fail("must fail before Trino"),
        download=lambda *_args: pytest.fail("must fail before R2"),
        append_batches=lambda **_kwargs: pytest.fail("must fail before append"),
    )

    with pytest.raises(WeatherCompletenessError, match="landing result is empty"):
        load_kma_bronze_batch(
            raw_result={},
            dag_run_id="manual__empty",
            allow_partial_pages=False,
            expected_raw_object_count_key="expected_raw_object_count",
            ports=ports,
        )


class _Dag:
    dag_id = "weather_vilage_fcst_bronze"


class _DagRun:
    conf = {"raw_object_keys": ["raw/weather/page.json"]}


class _Ti:
    task_id = "load_kma_bronze"

    def xcom_pull(self, **_kwargs):
        return {
            "raw_objects": [{}],
            "raw_object_keys": ["raw/weather/page.json"],
            "inserted": 1,
            "expected_rows": 1,
            "expected_raw_object_count": 1,
        }


def _raise(error):
    def raiser(*_args, **_kwargs):
        raise error

    return raiser


def test_weather_live_landing_boundary_disables_retry_for_invalid_window(monkeypatch):
    monkeypatch.setattr(
        dag_module,
        "kma_base_datetime_from_conf",
        _raise(WeatherInvalidWindowError("invalid issue window")),
    )

    with pytest.raises(AirflowFailException, match="invalid issue window"):
        dag_module.land_kma_raw(dag=_Dag(), dag_run=_DagRun(), run_id="manual__weather")


def test_weather_backfill_boundary_disables_retry_for_bad_raw_contract(monkeypatch):
    monkeypatch.setattr(
        dag_module,
        "load_kma_grids",
        _raise(WeatherBronzeConfigurationError("invalid grid config")),
    )

    with pytest.raises(AirflowFailException, match="invalid grid config"):
        dag_module.land_kma_raw_object_keys(
            dag=_Dag(), dag_run=_DagRun(), run_id="manual__backfill"
        )


def test_weather_load_boundary_disables_retry_for_completeness_failure(monkeypatch):
    monkeypatch.setattr(
        dag_module,
        "load_kma_bronze_batch",
        _raise(WeatherCompletenessError("incomplete bronze")),
    )

    with pytest.raises(AirflowFailException, match="incomplete bronze"):
        dag_module.load_kma_bronze(
            dag_run=_DagRun(), ti=_Ti(), run_id="manual__weather"
        )


def test_weather_verify_boundary_disables_retry_for_contract_failure(monkeypatch):
    monkeypatch.setattr(
        dag_module,
        "verify_kma_bronze_rows",
        _raise(WeatherCompletenessError("verification mismatch")),
    )

    with pytest.raises(AirflowFailException, match="verification mismatch"):
        dag_module.verify_kma_bronze_runtime(
            dag=_Dag(), ti=_Ti(), run_id="manual__weather"
        )


def test_weather_load_boundary_preserves_transient_error(monkeypatch):
    error = TimeoutError("KMA timeout")
    monkeypatch.setattr(dag_module, "load_kma_bronze_batch", _raise(error))

    with pytest.raises(TimeoutError) as raised:
        dag_module.load_kma_bronze(
            dag_run=_DagRun(), ti=_Ti(), run_id="manual__weather"
        )

    assert raised.value is error
