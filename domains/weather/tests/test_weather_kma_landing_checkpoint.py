import json
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import weather_vilage_fcst_bronze as dag_module  # noqa: E402


class MissingObjectError(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class DagRun:
    conf = {}


class Dag:
    dag_id = "weather_vilage_fcst_bronze"


def kma_payload() -> bytes:
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": "00", "resultMsg": "OK"},
                "body": {"items": {"item": [{"category": "TMP"}]}, "totalCount": 1},
            }
        }
    ).encode("utf-8")


def invalid_kma_payload() -> bytes:
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": "99", "resultMsg": "SERVICE_BUSY"},
                "body": {"items": {"item": []}, "totalCount": 0},
            }
        }
    ).encode("utf-8")


def test_land_kma_raw_reuses_checkpointed_grid(monkeypatch):
    context = {"dag": Dag(), "dag_run": DagRun(), "run_id": "manual__retry:1"}
    existing_raw_object = {
        "request_id": "request-1",
        "raw_object_key": "raw/weather/kma/first.json",
        "raw_hash": "abc",
        "http_status": 200,
        "collected_at": "2026-07-03T00:00:00+00:00",
        "place_id": "first",
        "base_date": "20260703",
        "base_time": "0800",
        "nx": 56,
        "ny": 130,
    }
    checkpoint_key = dag_module.kma_landing_checkpoint_key(context, "20260703", "0800")
    checkpoint_bytes = json.dumps(
        {"base_date": "20260703", "base_time": "0800", "raw_objects": [existing_raw_object]}
    ).encode("utf-8")
    fetch_calls = []
    uploads = []

    def fake_download_raw_object(object_key, log_label):
        if object_key == checkpoint_key:
            return checkpoint_bytes
        raise MissingObjectError()

    def fake_fetch_url(url, *_args, **_kwargs):
        fetch_calls.append(url)
        return 200, kma_payload()

    def fake_upload_raw_object(**kwargs):
        uploads.append(kwargs)
        return kwargs["object_key"]

    monkeypatch.setattr(dag_module, "resolve_kma_base_datetime", lambda: ("20260703", "0800"))
    monkeypatch.setattr(
        dag_module,
        "load_kma_grids",
        lambda: [
            {"place_id": "first", "nx": 56, "ny": 130},
            {"place_id": "second", "nx": 57, "ny": 130},
        ],
    )
    monkeypatch.setattr(dag_module, "download_raw_object", fake_download_raw_object)
    monkeypatch.setattr(dag_module, "fetch_url", fake_fetch_url)
    monkeypatch.setattr(dag_module, "upload_raw_object", fake_upload_raw_object)
    monkeypatch.setattr(dag_module, "build_kma_url", lambda **kwargs: f"{kwargs['nx']}/{kwargs['ny']}")
    monkeypatch.setattr(dag_module.time, "sleep", lambda _seconds: None)

    result = dag_module.land_kma_raw(**context)

    assert fetch_calls == ["57/130"]
    assert result["raw_object_keys"][0] == existing_raw_object["raw_object_key"]
    assert len(result["raw_object_keys"]) == 2
    assert result["grid_count"] == 2
    assert result["api_call_count"] == 2
    assert result["api_request_count"] == 1
    assert result["reused_raw_object_count"] == 1
    assert [upload["log_label"] for upload in uploads] == ["KMA raw payload", "KMA landing checkpoint"]


def test_land_kma_raw_fails_when_kma_response_result_code_is_not_ok(monkeypatch):
    context = {"dag": Dag(), "dag_run": DagRun(), "run_id": "manual__retry:2"}

    def fake_fetch_url(url, *_args, **_kwargs):
        return 200, invalid_kma_payload()

    def fake_download_raw_object(object_key, log_label):
        raise MissingObjectError()

    monkeypatch.setattr(dag_module, "resolve_kma_base_datetime", lambda: ("20260703", "0800"))
    monkeypatch.setattr(dag_module, "load_kma_grids", lambda: [{"place_id": "first", "nx": 56, "ny": 130}])
    monkeypatch.setattr(dag_module, "download_raw_object", fake_download_raw_object)
    monkeypatch.setattr(dag_module, "fetch_url", fake_fetch_url)
    monkeypatch.setattr(dag_module, "build_kma_url", lambda **kwargs: f"{kwargs['nx']}/{kwargs['ny']}")

    with pytest.raises(RuntimeError, match="KMA API returned resultCode=99"):
        dag_module.land_kma_raw(**context)
