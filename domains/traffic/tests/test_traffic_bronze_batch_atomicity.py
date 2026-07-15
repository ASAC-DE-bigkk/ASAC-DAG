from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path
import sys

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.bronze_batch import load_traffic_bronze_batch  # noqa: E402
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


def _ports(events: list[str], payloads: dict[str, bytes]) -> dict:
    def cursor_factory():
        events.append("cursor")
        return object(), "iceberg_dev", "ask_seoul"

    def create_table(_cursor, _catalog, _schema):
        events.append("create-table")
        return "iceberg_dev.ask_seoul.bronze_seoul_traffic_incident"

    def download_raw_object(key, _description):
        events.append(f"download:{key}")
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
        **_ports(events, payloads),
    )

    assert result["inserted"] == 2
    assert events == [
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
            **_ports(events, payloads),
        )

    assert not any(
        event in {"cursor", "create-table"} or event.startswith("insert:")
        for event in events
    )
