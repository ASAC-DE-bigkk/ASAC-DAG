from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.errors import TrafficSourceSchemaError  # noqa: E402
from traffic_ingest.landing import (  # noqa: E402
    RunIdentity,
    TrafficLanding,
    TrafficLandingRequest,
)


class UnusedSource:
    def __init__(self) -> None:
        self.requests = []

    def fetch_page(self, start_index: int, end_index: int):
        self.requests.append((start_index, end_index))
        raise AssertionError("malformed checkpoint must fail before TOPIS")


class MemoryRawObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def exists(self, key: str) -> bool:
        return key in self.objects

    def read_bytes(self, key: str) -> bytes:
        return self.objects[key]

    def write_bytes(self, key: str, payload: bytes, _content_type: str) -> None:
        self.objects[key] = payload

    def write_bytes_if_absent(
        self, key: str, payload: bytes, content_type: str
    ) -> bool:
        if key in self.objects:
            return False
        self.write_bytes(key, payload, content_type)
        return True


def landing_for(source, store) -> TrafficLanding:
    return TrafficLanding(
        source=source,
        raw_store=store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "unused",
    )


@pytest.mark.parametrize(
    "checkpoint_payload",
    [
        b"\xff",
        b"{",
        json.dumps(
            {
                "request": {
                    "start_index": 1,
                    "end_index": 1000,
                    "page_size": 1000,
                    "mode": "full_snapshot",
                },
                "raw_objects": [{}],
                "result_code": "INFO-000",
                "total_count": 1,
                "parsed_rows": 1,
            }
        ).encode(),
        json.dumps(
            {
                "request": {
                    "start_index": 1,
                    "end_index": 1000,
                    "page_size": 1000,
                    "mode": "full_snapshot",
                },
                "raw_objects": "not-a-list",
                "result_code": "INFO-000",
                "total_count": 1,
                "parsed_rows": 1,
            }
        ).encode(),
        json.dumps(
            {
                "request": {
                    "start_index": 1,
                    "end_index": 1000,
                    "page_size": 1000,
                    "mode": "full_snapshot",
                },
                "raw_objects": [
                    {
                        "request_id": "request-1",
                        "raw_object_key": "raw/traffic/page.xml",
                        "payload_hash": "hash",
                        "http_status": 200,
                        "collected_at": "2026-07-14T00:20:00+00:00",
                        "start_index": [],
                        "end_index": 1000,
                        "row_count": 1,
                        "total_count": 1,
                    }
                ],
                "result_code": "INFO-000",
                "total_count": 1,
                "parsed_rows": 1,
            }
        ).encode(),
    ],
    ids=("unicode", "json", "missing-key", "wrong-type", "field-type"),
)
def test_collect_translates_malformed_checkpoint_to_source_schema(
    checkpoint_payload,
):
    source = UnusedSource()
    raw_store = MemoryRawObjectStore()
    raw_store.objects[
        "raw/_checkpoints/seoul_traffic_incident/"
        "dag_id=traffic_incident_bronze/"
        "run_id=manual__malformed-checkpoint/landing.json"
    ] = checkpoint_payload

    with pytest.raises(TrafficSourceSchemaError, match="checkpoint"):
        landing_for(source, raw_store).collect(
            RunIdentity("traffic_incident_bronze", "manual__malformed-checkpoint"),
            TrafficLandingRequest(1, 1000, 1000),
        )

    assert source.requests == []


def test_collect_preserves_transient_checkpoint_read_error():
    class UnavailableStore(MemoryRawObjectStore):
        def exists(self, _key: str) -> bool:
            return True

        def read_bytes(self, _key: str) -> bytes:
            raise OSError("R2 unavailable")

    with pytest.raises(OSError, match="R2 unavailable"):
        landing_for(UnusedSource(), UnavailableStore()).collect(
            RunIdentity("traffic_incident_bronze", "manual__r2-down"),
            TrafficLandingRequest(1, 1000, 1000),
        )
