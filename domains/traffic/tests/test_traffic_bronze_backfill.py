import json
import sys
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from traffic_ingest.landing import (  # noqa: E402
    TrafficCollectionMode,
    TrafficLanding,
    TrafficLandingIncompleteError,
)
from traffic_ingest.landing_contracts import RunIdentity  # noqa: E402


class ReplayOnlySource:
    def fetch_page(self, *_args):
        raise AssertionError("backfill replay must not call TOPIS")


class MemoryRawObjectStore:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = dict(objects)

    def exists(self, key: str) -> bool:
        return key in self.objects

    def read_bytes(self, key: str) -> bytes:
        return self.objects[key]

    def write_bytes(self, key: str, payload: bytes, _content_type: str) -> None:
        self.objects[key] = payload


def acc_info_payload() -> bytes:
    return b"""
<AccInfo>
  <list_total_count>1</list_total_count>
  <RESULT><CODE>INFO-000</CODE><MESSAGE>OK</MESSAGE></RESULT>
  <row>
    <acc_id>A1</acc_id>
    <occr_date>20260705</occr_date>
    <occr_time>0820</occr_time>
    <acc_type>test</acc_type>
  </row>
</AccInfo>
"""


def test_land_seoul_traffic_raw_object_keys_rebuilds_loader_input():
    raw_key = (
        "raw/traffic_incident/seoul_traffic_incident/load_date=2026-07-05/"
        "20260705T082000KST_AccInfo-1-1000_request-1.xml"
    )

    landing = TrafficLanding(
        source=ReplayOnlySource(),
        raw_store=MemoryRawObjectStore({raw_key: acc_info_payload()}),
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 5, tzinfo=timezone.utc),
        request_id=lambda: "unused",
    )

    result = landing.replay(
        [raw_key],
        run=RunIdentity("traffic_incident_backfill", "manual__replay"),
    ).to_xcom()

    assert result["raw_object_keys"] == [raw_key]
    assert result["result_code"] == "INFO-000"
    assert result["list_total_count"] == 1
    assert result["page_count"] == 1
    assert result["collection_mode"] == TrafficCollectionMode.BACKFILL.value
    assert result["is_publishable"] is True
    assert result["raw_objects"][0]["request_id"] == "request-1"
    assert result["raw_objects"][0]["start_index"] == 1
    assert result["raw_objects"][0]["end_index"] == 1000


def test_replay_writes_a_manifest_for_the_original_snapshot_run():
    raw_key = (
        "raw/traffic_incident/seoul_traffic_incident/load_date=2026-07-05/"
        "20260705T082000KST_AccInfo-1-1000_request-1.xml"
    )
    raw_store = MemoryRawObjectStore({raw_key: acc_info_payload()})
    landing = TrafficLanding(
        source=ReplayOnlySource(),
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 5, tzinfo=timezone.utc),
        request_id=lambda: "unused",
    )

    result = landing.replay(
        [raw_key],
        run=RunIdentity("traffic_incident_landing", "legacy-snapshot"),
    )

    assert result.manifest_key == (
        "raw/traffic_incident/seoul_traffic_incident/load_date=2026-07-05/"
        "run_id=legacy-snapshot/_manifest.json"
    )
    assert json.loads(raw_store.read_bytes(result.manifest_key))["run_id"] == "legacy-snapshot"


def test_backfill_rejects_a_raw_set_that_does_not_start_at_one():
    raw_key = (
        "raw/traffic_incident/seoul_traffic_incident/load_date=2026-07-05/"
        "20260705T082000KST_AccInfo-2-2_request-1.xml"
    )
    landing = TrafficLanding(
        source=ReplayOnlySource(),
        raw_store=MemoryRawObjectStore({raw_key: acc_info_payload()}),
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 5, tzinfo=timezone.utc),
        request_id=lambda: "unused",
    )

    with pytest.raises(TrafficLandingIncompleteError, match="start_index=1"):
        landing.replay(
            [raw_key],
            run=RunIdentity("traffic_incident_backfill", "manual__replay"),
        )
