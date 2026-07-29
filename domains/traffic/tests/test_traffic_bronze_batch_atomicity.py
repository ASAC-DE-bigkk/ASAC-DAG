from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.bronze_batch import (  # noqa: E402
    load_traffic_bronze_batch,
    load_traffic_bronze_batches,
)
from common.raw_manifest import build_raw_manifest  # noqa: E402
from traffic_ingest.errors import (  # noqa: E402
    TrafficCompletenessError,
    TrafficRawIntegrityError,
    TrafficSourceSchemaError,
)


def _payload(total_count: int, incident_id: str) -> bytes:
    return (
        "<AccInfo>"
        f"<list_total_count>{total_count}</list_total_count>"
        "<RESULT><CODE>INFO-000</CODE><MESSAGE>OK</MESSAGE></RESULT>"
        f"<row><acc_id>{incident_id}</acc_id></row>"
        "</AccInfo>"
    ).encode()


def _valid_batch() -> tuple[dict, dict[str, bytes]]:
    payloads = {
        "raw/traffic/page-1.xml": _payload(2, "A1"),
        "raw/traffic/page-2.xml": _payload(2, "A2"),
    }
    raw_objects = [
        {
            "request_id": f"request-{index}",
            "raw_object_key": key,
            "raw_hash": hashlib.sha256(payload).hexdigest(),
            "http_status": 200,
            "collected_at": f"2026-07-15T00:2{index}:00+00:00",
            "start_index": index,
            "end_index": index,
            "row_count": 1,
            "total_count": 2,
        }
        for index, (key, payload) in enumerate(payloads.items(), start=1)
    ]
    return (
        {
            "raw_objects": raw_objects,
            "raw_object_keys": list(payloads),
            "manifest_key": "raw/traffic/_manifest.json",
            "result_code": "INFO-000",
            "list_total_count": 2,
            "parsed_rows": 2,
            "expected_rows": 2,
            "collection_mode": "full_snapshot",
            "is_publishable": True,
            "page_count": 2,
            "requested_end_index": 2,
            "pages": [
                {
                    "start_index": item["start_index"],
                    "end_index": item["end_index"],
                    "row_count": item["row_count"],
                    "list_total_count": item["total_count"],
                    "raw_object_key": item["raw_object_key"],
                }
                for item in raw_objects
            ],
        },
        payloads,
    )


def _manifest_bytes(raw_result: dict, dag_run_id: str) -> bytes:
    return json.dumps(
        build_raw_manifest(
            run_id=dag_run_id,
            dataset="seoul_traffic_incident",
            load_date="2026-07-15",
            object_keys=raw_result["raw_object_keys"],
            expected_count=len(raw_result["raw_object_keys"]),
            actual_count=len(raw_result["raw_object_keys"]),
            completed_at="2026-07-15T00:30:00+00:00",
        )
    ).encode()


def _ports(events: list[str], payloads: dict[str, bytes], raw_result: dict, dag_run_id: str) -> dict:
    def cursor_factory():
        events.append("cursor")
        return object(), "iceberg_dev", "ask_seoul"

    def create_table(_cursor, _catalog, _schema):
        events.append("create-table")
        return "iceberg_dev.ask_seoul.bronze_seoul_traffic_incident"

    def download_raw_object(key, _description):
        events.append(f"download:{key}")
        if key == raw_result["manifest_key"]:
            return _manifest_bytes(raw_result, dag_run_id)
        return payloads[key]

    def insert_rows(**kwargs):
        events.append(f"insert:{kwargs['raw_object_key']}")
        return len(kwargs["rows"])

    return {
        "cursor_factory": cursor_factory,
        "create_table": create_table,
        "download_raw_object": download_raw_object,
        "insert_rows": insert_rows,
    }


def test_traffic_batch_prepares_every_raw_page_before_opening_trino():
    raw_result, payloads = _valid_batch()
    events: list[str] = []

    result = load_traffic_bronze_batch(
        raw_result=raw_result,
        dag_run_id="manual__atomic",
        **_ports(events, payloads, raw_result, "manual__atomic"),
    )

    assert result["inserted"] == 2
    assert events == [
        "download:raw/traffic/_manifest.json",
        "download:raw/traffic/page-1.xml",
        "download:raw/traffic/page-2.xml",
        "cursor",
        "create-table",
        "insert:raw/traffic/page-1.xml",
        "insert:raw/traffic/page-2.xml",
    ]


@pytest.mark.parametrize(
    ("failure", "expected_error"),
    [
        ("raw-contract", TrafficSourceSchemaError),
        ("hash", TrafficRawIntegrityError),
        ("parse", TrafficSourceSchemaError),
        ("metadata", TrafficCompletenessError),
        ("page", TrafficCompletenessError),
        ("aggregate", TrafficCompletenessError),
    ],
)
def test_incomplete_traffic_batch_performs_zero_database_mutations(
    failure, expected_error
):
    raw_result, payloads = _valid_batch()
    raw_result = deepcopy(raw_result)
    payloads = dict(payloads)
    second = raw_result["raw_objects"][1]
    if failure == "raw-contract":
        del second["request_id"]
    elif failure == "hash":
        second["raw_hash"] = hashlib.sha256(b"different").hexdigest()
    elif failure == "parse":
        payloads[second["raw_object_key"]] = b"<AccInfo>"
        second["raw_hash"] = hashlib.sha256(
            payloads[second["raw_object_key"]]
        ).hexdigest()
    elif failure == "metadata":
        second["row_count"] = 2
    elif failure == "page":
        second["start_index"] = 3
        second["end_index"] = 3
    elif failure == "aggregate":
        raw_result["expected_rows"] = 3

    events: list[str] = []
    with pytest.raises(expected_error):
        load_traffic_bronze_batch(
            raw_result=raw_result,
            dag_run_id="manual__invalid",
            **_ports(events, payloads, raw_result, "manual__invalid"),
        )

    assert not any(
        event in {"cursor", "create-table"} or event.startswith("insert:")
        for event in events
    )


def test_traffic_batch_missing_manifest_performs_zero_database_mutations():
    raw_result, payloads = _valid_batch()
    raw_result.pop("manifest_key")
    events: list[str] = []

    with pytest.raises(TrafficCompletenessError, match="manifest is missing"):
        load_traffic_bronze_batch(
            raw_result=raw_result,
            dag_run_id="manual__missing-manifest",
            **_ports(events, payloads, {"manifest_key": "unused"}, "manual__missing-manifest"),
        )

    assert events == []


def test_multiple_receipts_are_prepared_before_one_set_based_database_mutation():
    first_result, first_payloads = _valid_batch()
    second_result, second_payloads = _valid_batch()
    second_result = deepcopy(second_result)
    second_result["manifest_key"] = "raw/traffic/second/_manifest.json"
    for raw_object in second_result["raw_objects"]:
        original_key = raw_object["raw_object_key"]
        raw_object["raw_object_key"] = original_key.replace(
            "raw/traffic/", "raw/traffic/second/"
        )
    second_result["raw_object_keys"] = [
        raw_object["raw_object_key"] for raw_object in second_result["raw_objects"]
    ]
    for page, raw_object in zip(second_result["pages"], second_result["raw_objects"]):
        page["raw_object_key"] = raw_object["raw_object_key"]
    second_payloads = {
        key.replace("raw/traffic/", "raw/traffic/second/"): payload
        for key, payload in second_payloads.items()
    }
    payloads = {**first_payloads, **second_payloads}
    raw_results = {
        "scheduled__first": first_result,
        "scheduled__second": second_result,
    }
    events: list[str] = []
    replaced: list[list[dict]] = []

    def cursor_factory():
        events.append("cursor")
        return object(), "iceberg_dev", "ask_seoul"

    def create_table(_cursor, _catalog, _schema):
        events.append("create-table")
        return "iceberg_dev.ask_seoul.bronze_seoul_traffic_incident"

    def download_raw_object(key, _description):
        events.append(f"download:{key}")
        if key == first_result["manifest_key"]:
            return _manifest_bytes(first_result, "scheduled__first")
        if key == second_result["manifest_key"]:
            return _manifest_bytes(second_result, "scheduled__second")
        return payloads[key]

    def replace_snapshots(**kwargs):
        events.append("replace")
        replaced.append(kwargs["snapshots"])
        return {
            snapshot["dag_run_id"]: sum(
                len(page["rows"]) for page in snapshot["pages"]
            )
            for snapshot in kwargs["snapshots"]
        }

    result = load_traffic_bronze_batches(
        raw_results=raw_results,
        cursor_factory=cursor_factory,
        create_table=create_table,
        download_raw_object=download_raw_object,
        replace_snapshots=replace_snapshots,
    )

    assert result["scheduled__first"]["inserted"] == 2
    assert result["scheduled__second"]["inserted"] == 2
    assert events[-3:] == ["cursor", "create-table", "replace"]
    assert events.index("cursor") > max(
        index
        for index, event in enumerate(events)
        if event.startswith("download:")
    )
    assert len(replaced) == 1
    assert [snapshot["dag_run_id"] for snapshot in replaced[0]] == [
        "scheduled__first",
        "scheduled__second",
    ]


def test_invalid_receipt_prevents_all_set_based_database_mutations():
    first_result, first_payloads = _valid_batch()
    second_result, second_payloads = _valid_batch()
    second_result = deepcopy(second_result)
    second_result["expected_rows"] = 3
    events: list[str] = []

    def download_raw_object(key, _description):
        if key == first_result["manifest_key"]:
            return _manifest_bytes(first_result, "scheduled__first")
        if key == second_result["manifest_key"]:
            return _manifest_bytes(second_result, "scheduled__second")
        return {**first_payloads, **second_payloads}[key]

    with pytest.raises(TrafficCompletenessError):
        load_traffic_bronze_batches(
            raw_results={
                "scheduled__first": first_result,
                "scheduled__second": second_result,
            },
            cursor_factory=lambda: events.append("cursor"),
            create_table=lambda *_args: events.append("create-table"),
            download_raw_object=download_raw_object,
            replace_snapshots=lambda **_kwargs: events.append("replace"),
        )

    assert events == []
