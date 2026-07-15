from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from airflow.sdk.exceptions import AirflowFailException


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_ingest.manual_incident as dag_module  # noqa: E402
from traffic_ingest.errors import (  # noqa: E402
    TrafficBronzeConfigurationError,
    TrafficBronzeDeterministicError,
    TrafficCompletenessError,
    TrafficInvalidWindowError,
    TrafficRawIntegrityError,
    TrafficSourceBusinessError,
    TrafficSourceSchemaError,
)
from traffic_ingest.acc_info import (  # noqa: E402
    parse_seoul_acc_info_response,
    resolve_acc_info_page_window,
)
from traffic_ingest.bronze import validate_seoul_traffic_row_count  # noqa: E402
from traffic_ingest.bronze_batch import load_traffic_bronze_batch  # noqa: E402
from traffic_ingest.landing import (  # noqa: E402
    RawObjectIntegrityError,
    TrafficLandingIncompleteError,
    TrafficLandingRequest,
    verify_raw_payload_hash,
)
from traffic_ingest.runtime import traffic_api_key  # noqa: E402


DETERMINISTIC_ERRORS = (
    TrafficBronzeConfigurationError,
    TrafficSourceBusinessError,
    TrafficSourceSchemaError,
    TrafficRawIntegrityError,
    TrafficCompletenessError,
    TrafficInvalidWindowError,
)


def retry_boundary():
    module = importlib.import_module("traffic_ingest.bronze_dag_support")
    decorator = getattr(module, "fail_fast_traffic_bronze", None)
    assert callable(decorator), "Traffic Bronze retry boundary is missing"
    return decorator


def test_traffic_bronze_error_types_share_one_deterministic_base():
    assert all(
        issubclass(error_type, TrafficBronzeDeterministicError)
        for error_type in DETERMINISTIC_ERRORS
    )


@pytest.mark.parametrize("error_type", DETERMINISTIC_ERRORS)
def test_each_traffic_deterministic_error_disables_airflow_retry(error_type):
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
def test_traffic_transient_error_remains_retryable(error):
    @retry_boundary()
    def task_callable():
        raise error

    with pytest.raises(type(error)) as raised:
        task_callable()

    assert raised.value is error


def test_traffic_invalid_page_window_uses_deterministic_type():
    with pytest.raises(TrafficInvalidWindowError, match="start_index"):
        resolve_acc_info_page_window({"start_index": 0})


def test_traffic_business_code_uses_deterministic_type():
    payload = b"<RESULT><CODE>ERROR-500</CODE><MESSAGE>denied</MESSAGE></RESULT>"

    with pytest.raises(TrafficSourceBusinessError, match="ERROR-500"):
        parse_seoul_acc_info_response(payload)


def test_traffic_malformed_xml_uses_schema_type():
    with pytest.raises(TrafficSourceSchemaError, match="XML"):
        parse_seoul_acc_info_response(b"<AccInfo>")


def test_traffic_raw_hash_uses_integrity_type_and_legacy_name():
    with pytest.raises(TrafficRawIntegrityError) as raised:
        verify_raw_payload_hash(
            b"actual",
            expected_hash="not-the-hash",
            raw_object_key="raw/traffic/page.xml",
        )

    assert isinstance(raised.value, RawObjectIntegrityError)


def test_traffic_completeness_legacy_name_uses_deterministic_type():
    assert issubclass(TrafficLandingIncompleteError, TrafficCompletenessError)


def test_traffic_missing_api_key_uses_configuration_type(monkeypatch):
    monkeypatch.delenv("SEOUL_OPEN_API_KEY", raising=False)
    monkeypatch.delenv("SEOUL_API_KEY_TRIC", raising=False)

    with pytest.raises(TrafficBronzeConfigurationError, match="SEOUL_OPEN_API_KEY"):
        traffic_api_key()


def test_traffic_invalid_collection_mode_uses_window_type():
    with pytest.raises(TrafficInvalidWindowError, match="unsupported"):
        TrafficLandingRequest(1, 10, 10, mode="unknown")


def test_traffic_row_validation_uses_completeness_type():
    with pytest.raises(TrafficCompletenessError, match="parsed row_count=0"):
        validate_seoul_traffic_row_count([], {"list_total_count": 1})


def test_traffic_empty_bronze_batch_uses_completeness_type():
    with pytest.raises(TrafficCompletenessError, match="landing result is empty"):
        load_traffic_bronze_batch(
            raw_result={},
            dag_run_id="manual__empty",
            cursor_factory=lambda: pytest.fail("must fail before Trino"),
            create_table=lambda *_args: pytest.fail("must fail before Trino"),
            download_raw_object=lambda *_args: pytest.fail("must fail before R2"),
            insert_rows=lambda **_kwargs: pytest.fail("must fail before insert"),
        )


class _Dag:
    dag_id = "traffic_incident_bronze"


class _DagRun:
    conf = {"raw_object_keys": ["raw/traffic/page.xml"]}


class _Ti:
    task_id = "load_seoul_traffic_bronze"

    def xcom_pull(self, **_kwargs):
        return {
            "raw_objects": [{}],
            "raw_object_keys": ["raw/traffic/page.xml"],
            "inserted": 1,
            "list_total_count": 1,
            "expected_rows": 1,
            "page_count": 1,
        }


def _raise(error):
    def raiser(*_args, **_kwargs):
        raise error

    return raiser


def test_traffic_live_landing_boundary_disables_retry_for_invalid_window(monkeypatch):
    monkeypatch.setattr(
        dag_module,
        "resolve_acc_info_page_window",
        _raise(TrafficInvalidWindowError("invalid window")),
    )

    with pytest.raises(AirflowFailException, match="invalid window"):
        dag_module.land_seoul_traffic_raw(
            dag=_Dag(), dag_run=_DagRun(), run_id="manual__traffic"
        )


def test_traffic_backfill_boundary_disables_retry_for_bad_raw_contract(monkeypatch):
    monkeypatch.setattr(dag_module, "build_traffic_landing", lambda: object())
    monkeypatch.setattr(
        dag_module,
        "raw_object_keys_from_conf",
        _raise(TrafficSourceSchemaError("invalid raw keys")),
    )

    with pytest.raises(AirflowFailException, match="invalid raw keys"):
        dag_module.land_seoul_traffic_raw_object_keys(
            dag=_Dag(), dag_run=_DagRun(), run_id="manual__backfill"
        )


def test_traffic_load_boundary_disables_retry_for_completeness_failure(monkeypatch):
    monkeypatch.setattr(
        dag_module,
        "load_traffic_bronze_batch",
        _raise(TrafficCompletenessError("incomplete bronze")),
    )

    with pytest.raises(AirflowFailException, match="incomplete bronze"):
        dag_module.load_seoul_traffic_bronze(ti=_Ti(), run_id="manual__traffic")


def test_traffic_verify_boundary_disables_retry_for_contract_failure(monkeypatch):
    monkeypatch.setattr(
        dag_module,
        "verify_seoul_traffic_bronze_rows",
        _raise(TrafficCompletenessError("verification mismatch")),
    )

    with pytest.raises(AirflowFailException, match="verification mismatch"):
        dag_module.verify_seoul_traffic_bronze_runtime(
            dag=_Dag(), ti=_Ti(), run_id="manual__traffic"
        )


def test_traffic_load_boundary_preserves_transient_error(monkeypatch):
    error = ConnectionError("Trino reset")
    monkeypatch.setattr(dag_module, "load_traffic_bronze_batch", _raise(error))

    with pytest.raises(ConnectionError) as raised:
        dag_module.load_seoul_traffic_bronze(ti=_Ti(), run_id="manual__traffic")

    assert raised.value is error
