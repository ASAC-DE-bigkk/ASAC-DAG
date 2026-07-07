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


class TaskInstance:
    def __init__(self, raw_result):
        self.raw_result = raw_result

    def xcom_pull(self, task_ids):
        assert task_ids == "land_kma_raw"
        return self.raw_result


def kma_payload(total_count: int = 1, item_count: int = 1, page_no: int = 1, num_of_rows: int = 1000) -> bytes:
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": "00", "resultMsg": "OK"},
                "body": {
                    "items": {"item": [{"category": "TMP", "seq": seq} for seq in range(item_count)]},
                    "pageNo": page_no,
                    "numOfRows": num_of_rows,
                    "totalCount": total_count,
                },
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
        if object_key == existing_raw_object["raw_object_key"]:
            return kma_payload()
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


def test_land_kma_raw_fetches_all_pages_when_total_count_exceeds_page_size(monkeypatch):
    context = {"dag": Dag(), "dag_run": DagRun(), "run_id": "manual__pagination:1"}
    fetch_calls = []
    uploads = []

    def fake_download_raw_object(object_key, log_label):
        raise MissingObjectError()

    def fake_fetch_url(url, *_args, **_kwargs):
        fetch_calls.append(url)
        if url.endswith("page-1"):
            return 200, kma_payload(total_count=1001, item_count=1000)
        return 200, kma_payload(total_count=1001, item_count=1)

    def fake_upload_raw_object(**kwargs):
        uploads.append(kwargs)
        return kwargs["object_key"]

    monkeypatch.setattr(dag_module, "resolve_kma_base_datetime", lambda: ("20260705", "1700"))
    monkeypatch.setattr(dag_module, "load_kma_grids", lambda: [{"place_id": "first", "nx": 56, "ny": 130}])
    monkeypatch.setattr(dag_module, "download_raw_object", fake_download_raw_object)
    monkeypatch.setattr(dag_module, "fetch_url", fake_fetch_url)
    monkeypatch.setattr(dag_module, "upload_raw_object", fake_upload_raw_object)
    monkeypatch.setattr(
        dag_module,
        "build_kma_url",
        lambda **kwargs: f"{kwargs['nx']}/{kwargs['ny']}/page-{kwargs['page_no']}",
    )
    monkeypatch.setattr(dag_module, "kma_num_of_rows", lambda: 1000)
    monkeypatch.setattr(dag_module.time, "sleep", lambda _seconds: None)

    result = dag_module.land_kma_raw(**context)

    assert fetch_calls == ["56/130/page-1", "56/130/page-2"]
    assert [item["page_no"] for item in result["raw_objects"]] == [1, 2]
    assert [item["row_count"] for item in result["raw_objects"]] == [1000, 1]
    assert [item["total_count"] for item in result["raw_objects"]] == [1001, 1001]
    assert result["raw_page_count"] == 2
    assert result["api_request_count"] == 2
    assert [upload["log_label"] for upload in uploads].count("KMA raw payload") == 2


def test_land_kma_raw_object_keys_rebuilds_loader_input(monkeypatch):
    raw_key = (
        "raw/weather_forecast/kma_vilage_fcst/load_date=2026-07-05/"
        "nx=56/ny=130/20260705T082000KST_base-202607050800_request-1.json"
    )

    class BackfillDagRun:
        conf = {"raw_object_keys": [raw_key]}

    monkeypatch.setattr(dag_module, "load_kma_grids", lambda: [{"place_id": "first", "nx": 56, "ny": 130}])
    monkeypatch.setattr(
        dag_module,
        "download_raw_object",
        lambda object_key, _log_label: kma_payload(total_count=1001, item_count=1000, page_no=1, num_of_rows=1000),
    )

    result = dag_module.land_kma_raw_object_keys(
        dag=Dag(),
        dag_run=BackfillDagRun(),
        run_id="manual__backfill",
    )

    assert result["api_request_count"] == 0
    assert result["expected_raw_object_count"] == 1
    assert result["raw_objects"][0]["request_id"] == "request-1"
    assert result["raw_objects"][0]["place_id"] == "first"
    assert result["raw_objects"][0]["page_no"] == 1
    assert result["raw_objects"][0]["num_of_rows"] == 1000


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


def test_load_kma_bronze_fails_before_insert_when_expected_page_is_missing(monkeypatch):
    raw_object = {
        "request_id": "request-page-1",
        "raw_object_key": "raw/weather/kma/page-1.json",
        "raw_hash": "abc",
        "http_status": 200,
        "collected_at": "2026-07-05T08:20:00+00:00",
        "place_id": "first",
        "base_date": "20260705",
        "base_time": "1700",
        "nx": 56,
        "ny": 130,
        "page_no": 1,
        "num_of_rows": 1000,
    }
    raw_result = {
        "raw_objects": [raw_object],
        "grid_count": 1,
        "api_call_count": 1,
        "base_date": "20260705",
        "base_time": "1700",
    }
    insert_calls = []

    monkeypatch.setattr(dag_module, "trino_cursor", lambda: (object(), "iceberg_dev", "dev"))
    monkeypatch.setattr(dag_module, "create_kma_bronze_table", lambda *_args: "iceberg_dev.dev.bronze")
    monkeypatch.setattr(
        dag_module,
        "download_raw_object",
        lambda _object_key, _log_label: kma_payload(total_count=1001, item_count=1000),
    )
    monkeypatch.setattr(
        dag_module,
        "append_kma_bronze_row_batches_pyiceberg",
        lambda **kwargs: insert_calls.append(kwargs),
    )

    with pytest.raises(RuntimeError, match="KMA bronze pagination incomplete"):
        dag_module.load_kma_bronze(ti=TaskInstance(raw_result), run_id="manual__load:missing-page")

    assert insert_calls == []


def test_load_kma_bronze_inserts_pages_after_aggregate_count_matches(monkeypatch):
    raw_objects = [
        {
            "request_id": "request-page-1",
            "raw_object_key": "raw/weather/kma/page-1.json",
            "raw_hash": "abc",
            "http_status": 200,
            "collected_at": "2026-07-05T08:20:00+00:00",
            "place_id": "first",
            "base_date": "20260705",
            "base_time": "1700",
            "nx": 56,
            "ny": 130,
            "page_no": 1,
            "num_of_rows": 1000,
        },
        {
            "request_id": "request-page-2",
            "raw_object_key": "raw/weather/kma/page-2.json",
            "raw_hash": "def",
            "http_status": 200,
            "collected_at": "2026-07-05T08:20:01+00:00",
            "place_id": "first",
            "base_date": "20260705",
            "base_time": "1700",
            "nx": 56,
            "ny": 130,
            "page_no": 2,
            "num_of_rows": 1000,
        },
    ]
    raw_result = {
        "raw_objects": raw_objects,
        "grid_count": 1,
        "api_call_count": 2,
        "api_request_count": 2,
        "reused_raw_object_count": 0,
        "base_date": "20260705",
        "base_time": "1700",
    }
    insert_calls = []

    def fake_download_raw_object(object_key, _log_label):
        if object_key.endswith("page-1.json"):
            return kma_payload(total_count=1001, item_count=1000)
        return kma_payload(total_count=1001, item_count=1)

    def fake_insert_kma_bronze_row_batches(**kwargs):
        row_batches = kwargs["row_batches"]
        insert_calls.append(
            {
                "dag_run_id": kwargs["dag_run_id"],
                "delete_existing": kwargs["delete_existing"],
                "batch_count": len(row_batches),
                "row_counts": [len(batch["rows"]) for batch in row_batches],
                "page_nos": [batch["page_no"] for batch in row_batches],
            }
        )
        return sum(len(batch["rows"]) for batch in row_batches)

    monkeypatch.setattr(dag_module, "trino_cursor", lambda: (object(), "iceberg_dev", "dev"))
    monkeypatch.setattr(dag_module, "create_kma_bronze_table", lambda *_args: "iceberg_dev.dev.bronze")
    monkeypatch.setattr(dag_module, "download_raw_object", fake_download_raw_object)
    monkeypatch.setattr(dag_module, "append_kma_bronze_row_batches_pyiceberg", fake_insert_kma_bronze_row_batches)

    result = dag_module.load_kma_bronze(ti=TaskInstance(raw_result), run_id="manual__load:all-pages")

    assert len(insert_calls) == 1
    assert insert_calls[0]["dag_run_id"] == "manual__load:all-pages"
    assert insert_calls[0]["delete_existing"] is True
    assert insert_calls[0]["batch_count"] == 2
    assert insert_calls[0]["row_counts"] == [1000, 1]
    assert insert_calls[0]["page_nos"] == [1, 2]
    assert result["inserted"] == 1001
    assert result["expected_rows"] == 1001
    assert result["expected_raw_object_count"] == 2
    assert result["raw_page_count"] == 2
