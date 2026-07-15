from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.landing import (  # noqa: E402
    RunIdentity,
    TrafficLanding,
    TrafficLandingBatch,
    TrafficLandingIncompleteError,
    TrafficLandingRequest,
)


def acc_info_payload(*, total_count: int, incident_ids: tuple[str, ...]) -> bytes:
    rows = "".join(
        f"<row><acc_id>{incident_id}</acc_id><occr_date>20260714</occr_date>"
        f"<occr_time>0820</occr_time><acc_type>test</acc_type></row>"
        for incident_id in incident_ids
    )
    return (
        "<AccInfo>"
        f"<list_total_count>{total_count}</list_total_count>"
        "<RESULT><CODE>INFO-000</CODE><MESSAGE>OK</MESSAGE></RESULT>"
        f"{rows}</AccInfo>"
    ).encode("utf-8")


class ScriptedTopisSource:
    def __init__(self, pages: dict[tuple[int, int], bytes]) -> None:
        self.pages = pages
        self.requests: list[tuple[int, int]] = []

    def fetch_page(self, start_index: int, end_index: int) -> tuple[int, bytes]:
        self.requests.append((start_index, end_index))
        return 200, self.pages[(start_index, end_index)]


class SequentialTopisSource:
    def __init__(self, payloads: list[bytes]) -> None:
        self.payloads = payloads
        self.requests: list[tuple[int, int]] = []

    def fetch_page(self, start_index: int, end_index: int) -> tuple[int, bytes]:
        self.requests.append((start_index, end_index))
        return 200, self.payloads.pop(0)


class MemoryRawObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}

    def exists(self, key: str) -> bool:
        return key in self.objects

    def read_bytes(self, key: str) -> bytes:
        return self.objects[key][0]

    def write_bytes(self, key: str, payload: bytes, content_type: str) -> None:
        self.objects[key] = (payload, content_type)


def test_collect_preserves_raw_lineage_for_one_successful_page():
    payload = acc_info_payload(total_count=1, incident_ids=("A1",))
    raw_store = MemoryRawObjectStore()
    landing = TrafficLanding(
        source=ScriptedTopisSource({(1, 1000): payload}),
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "request-1",
    )

    batch = landing.collect(
        RunIdentity(
            dag_id="traffic_incident_bronze", run_id="scheduled__2026-07-14T00:20:00Z"
        ),
        TrafficLandingRequest(start_index=1, end_index=1000, page_size=1000),
    )

    assert batch.result_code == "INFO-000"
    assert batch.total_count == 1
    assert batch.parsed_rows == 1
    assert len(batch.raw_objects) == 1
    raw_object = batch.raw_objects[0]
    assert raw_object.request_id == "request-1"
    assert raw_object.start_index == 1
    assert raw_object.end_index == 1000
    assert raw_object.payload_hash == hashlib.sha256(payload).hexdigest()
    assert raw_store.read_bytes(raw_object.raw_object_key) == payload
    assert (
        raw_store.objects[raw_object.raw_object_key][1]
        == "application/xml; charset=utf-8"
    )
    assert "SEOUL_API_KEY" not in raw_object.raw_object_key


def test_collect_fetches_every_page_required_by_topis_total_count():
    source = ScriptedTopisSource(
        {
            (1, 2): acc_info_payload(total_count=3, incident_ids=("A1", "A2")),
            (3, 3): acc_info_payload(total_count=3, incident_ids=("A3",)),
        }
    )
    request_ids = iter(("request-1", "request-2"))
    landing = TrafficLanding(
        source=source,
        raw_store=MemoryRawObjectStore(),
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: next(request_ids),
    )

    batch = landing.collect(
        RunIdentity(dag_id="traffic_incident_bronze", run_id="manual__pagination"),
        TrafficLandingRequest(start_index=1, end_index=2, page_size=2),
    )

    assert source.requests == [(1, 2), (3, 3)]
    assert [(item.start_index, item.end_index) for item in batch.raw_objects] == [
        (1, 2),
        (3, 3),
    ]
    assert batch.total_count == 3
    assert batch.parsed_rows == 3


def test_collect_fails_loudly_when_landed_pages_do_not_cover_reported_total():
    source = ScriptedTopisSource(
        {
            (1, 2): acc_info_payload(total_count=3, incident_ids=("A1",)),
            (3, 3): acc_info_payload(total_count=3, incident_ids=()),
        }
    )
    request_ids = iter(("request-1", "request-2", "request-3", "request-4"))
    landing = TrafficLanding(
        source=source,
        raw_store=MemoryRawObjectStore(),
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: next(request_ids),
    )

    with pytest.raises(
        TrafficLandingIncompleteError,
        match="total_count=3, parsed_rows=1",
    ):
        landing.collect(
            RunIdentity(dag_id="traffic_incident_bronze", run_id="manual__incomplete"),
            TrafficLandingRequest(start_index=1, end_index=2, page_size=2),
        )
    assert source.requests == [(1, 2), (3, 3), (1, 2), (3, 3)]


def test_collect_refetches_once_after_source_count_exceeds_metadata():
    source = SequentialTopisSource(
        [
            acc_info_payload(total_count=1, incident_ids=("A1", "A2")),
            acc_info_payload(total_count=1, incident_ids=("B1",)),
        ]
    )
    raw_store = MemoryRawObjectStore()
    request_ids = iter(("request-1", "request-2"))
    landing = TrafficLanding(
        source=source,
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: next(request_ids),
    )

    batch = landing.collect(
        RunIdentity(dag_id="traffic_incident_bronze", run_id="manual__fresh-retry"),
        TrafficLandingRequest(start_index=1, end_index=1000, page_size=1000),
    )

    assert source.requests == [(1, 1000), (1, 1000)]
    assert batch.total_count == 1
    assert batch.parsed_rows == 1
    assert batch.raw_objects[0].request_id == "request-2"
    assert len([key for key in raw_store.objects if key.endswith(".xml")]) == 2


def test_collect_reuses_same_run_checkpoint_without_duplicate_source_request():
    source = ScriptedTopisSource(
        {(1, 1000): acc_info_payload(total_count=1, incident_ids=("A1",))}
    )
    raw_store = MemoryRawObjectStore()
    request_ids = iter(("request-1",))
    landing = TrafficLanding(
        source=source,
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: next(request_ids),
    )
    run = RunIdentity(dag_id="traffic_incident_bronze", run_id="manual__same-run")
    request = TrafficLandingRequest(start_index=1, end_index=1000, page_size=1000)

    first = landing.collect(run, request)
    source.pages.clear()
    source.requests.clear()
    second = landing.collect(run, request)

    assert second == first
    assert source.requests == []
    assert (
        len([key for key in raw_store.objects if not key.endswith("landing.json")]) == 1
    )
    checkpoint_key = next(
        key for key in raw_store.objects if key.endswith("landing.json")
    )
    assert json.loads(raw_store.read_bytes(checkpoint_key))["complete"] is True


@pytest.mark.parametrize("mutation", ["missing", "corrupt"])
def test_collect_recollects_checkpoint_page_when_raw_object_is_not_trustworthy(
    mutation,
):
    original = acc_info_payload(total_count=1, incident_ids=("A1",))
    replacement = acc_info_payload(total_count=1, incident_ids=("A2",))
    raw_store = MemoryRawObjectStore()
    run = RunIdentity("traffic_incident_bronze", "manual__repair-checkpoint")
    request = TrafficLandingRequest(1, 1000, 1000)
    first = TrafficLanding(
        source=ScriptedTopisSource({(1, 1000): original}),
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "request-1",
    ).collect(run, request)
    raw_key = first.raw_objects[0].raw_object_key
    if mutation == "missing":
        del raw_store.objects[raw_key]
    else:
        raw_store.objects[raw_key] = (b"corrupt", "application/xml")

    retry_source = ScriptedTopisSource({(1, 1000): replacement})
    repaired = TrafficLanding(
        source=retry_source,
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 21, tzinfo=timezone.utc),
        request_id=lambda: "request-2",
    ).collect(run, request)

    assert retry_source.requests == [(1, 1000)]
    assert repaired.raw_objects[0].request_id == "request-2"
    assert (
        repaired.raw_objects[0].payload_hash == hashlib.sha256(replacement).hexdigest()
    )


def test_full_snapshot_rejects_non_one_start_before_source_request():
    source = ScriptedTopisSource({})
    landing = TrafficLanding(
        source=source,
        raw_store=MemoryRawObjectStore(),
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, tzinfo=timezone.utc),
        request_id=lambda: "unused",
    )

    with pytest.raises(ValueError, match="full_snapshot.*start_index=1"):
        landing.collect(
            RunIdentity("traffic_incident_bronze", "manual__invalid-window"),
            TrafficLandingRequest(5, 15, 11),
        )

    assert source.requests == []


def test_window_collection_uses_window_count_and_is_never_publishable():
    payload = acc_info_payload(
        total_count=20, incident_ids=tuple(f"A{i}" for i in range(5, 16))
    )
    source = ScriptedTopisSource({(5, 15): payload})
    landing = TrafficLanding(
        source=source,
        raw_store=MemoryRawObjectStore(),
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, tzinfo=timezone.utc),
        request_id=lambda: "request-window",
    )

    batch = landing.collect(
        RunIdentity("traffic_incident_recollect", "manual__window"),
        TrafficLandingRequest(
            5,
            15,
            11,
            mode="window",
        ),
    )

    assert source.requests == [(5, 15)]
    assert batch.expected_rows == 11
    assert batch.parsed_rows == 11
    assert batch.is_publishable is False
    assert batch.to_xcom()["collection_mode"] == "window"


def test_collect_resumes_from_partial_page_checkpoint_after_failure():
    first_page = acc_info_payload(total_count=2, incident_ids=("A1",))
    second_page = acc_info_payload(total_count=2, incident_ids=("A2",))
    raw_store = MemoryRawObjectStore()
    run = RunIdentity(dag_id="traffic_incident_bronze", run_id="manual__partial")
    request = TrafficLandingRequest(start_index=1, end_index=1, page_size=1)

    first_attempt = TrafficLanding(
        source=ScriptedTopisSource({(1, 1): first_page}),
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=iter(("request-1", "request-failed")).__next__,
    )
    with pytest.raises(KeyError):
        first_attempt.collect(run, request)

    checkpoint_key = next(
        key for key in raw_store.objects if key.endswith("landing.json")
    )
    partial = json.loads(raw_store.read_bytes(checkpoint_key))
    assert partial["complete"] is False
    assert len(partial["raw_objects"]) == 1

    retry_source = ScriptedTopisSource({(2, 2): second_page})
    batch = TrafficLanding(
        source=retry_source,
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 21, tzinfo=timezone.utc),
        request_id=lambda: "request-2",
    ).collect(run, request)

    assert retry_source.requests == [(2, 2)]
    assert [item.request_id for item in batch.raw_objects] == [
        "request-1",
        "request-2",
    ]
    assert json.loads(raw_store.read_bytes(checkpoint_key))["complete"] is True


def test_collect_does_not_reuse_checkpoint_for_a_different_request():
    source = ScriptedTopisSource(
        {
            (1, 1000): acc_info_payload(total_count=1, incident_ids=("A1",)),
            (1, 500): acc_info_payload(total_count=1, incident_ids=("A1",)),
        }
    )
    landing = TrafficLanding(
        source=source,
        raw_store=MemoryRawObjectStore(),
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "request-1",
    )
    run = RunIdentity(
        dag_id="traffic_incident_bronze",
        run_id="manual__request-change",
    )

    landing.collect(
        run,
        TrafficLandingRequest(start_index=1, end_index=1000, page_size=1000),
    )
    source.requests.clear()
    landing.collect(
        run,
        TrafficLandingRequest(start_index=1, end_index=500, page_size=500),
    )

    assert source.requests == [(1, 500)]


def test_replay_deduplicates_raw_keys_and_rebuilds_lineage_without_source_request():
    raw_key = (
        "raw/traffic_incident/seoul_traffic_incident/load_date=2026-07-14/"
        "20260714T092000KST_AccInfo-1-1000_request-1.xml"
    )
    payload = acc_info_payload(total_count=1, incident_ids=("A1",))
    raw_store = MemoryRawObjectStore()
    raw_store.write_bytes(raw_key, payload, "application/xml; charset=utf-8")
    source = ScriptedTopisSource({})
    landing = TrafficLanding(
        source=source,
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "must-not-be-used",
    )

    batch = landing.replay([raw_key, raw_key])

    assert source.requests == []
    assert len(batch.raw_objects) == 1
    assert batch.raw_objects[0].request_id == "request-1"
    assert batch.raw_objects[0].raw_object_key == raw_key
    assert batch.raw_objects[0].start_index == 1
    assert batch.raw_objects[0].end_index == 1000
    assert batch.total_count == 1
    assert batch.parsed_rows == 1


def test_replay_rejects_duplicate_page_ranges_that_mask_missing_rows():
    first_key = (
        "raw/traffic_incident/seoul_traffic_incident/load_date=2026-07-14/"
        "20260714T092000KST_AccInfo-1-1_request-1.xml"
    )
    duplicate_key = (
        "raw/traffic_incident/seoul_traffic_incident/load_date=2026-07-14/"
        "20260714T092001KST_AccInfo-1-1_request-2.xml"
    )
    payload = acc_info_payload(total_count=2, incident_ids=("A1",))
    raw_store = MemoryRawObjectStore()
    raw_store.write_bytes(first_key, payload, "application/xml")
    raw_store.write_bytes(duplicate_key, payload, "application/xml")
    landing = TrafficLanding(
        source=ScriptedTopisSource({}),
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, tzinfo=timezone.utc),
        request_id=lambda: "unused",
    )

    with pytest.raises(TrafficLandingIncompleteError, match="duplicate page range"):
        landing.replay([first_key, duplicate_key])


def test_landing_batch_round_trips_through_airflow_xcom_mapping():
    landing = TrafficLanding(
        source=ScriptedTopisSource(
            {(1, 1000): acc_info_payload(total_count=1, incident_ids=("A1",))}
        ),
        raw_store=MemoryRawObjectStore(),
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "request-1",
    )
    batch = landing.collect(
        RunIdentity(dag_id="traffic_incident_bronze", run_id="manual__xcom"),
        TrafficLandingRequest(start_index=1, end_index=1000, page_size=1000),
    )

    document = batch.to_xcom()

    assert document["source_id"] == "seoul_traffic_incident"
    assert document["raw_objects"][0]["raw_hash"] == batch.raw_objects[0].payload_hash
    assert document["raw_object_keys"] == [batch.raw_objects[0].raw_object_key]
    assert document["list_total_count"] == 1
    assert document["page_count"] == 1
    assert TrafficLandingBatch.from_xcom(document) == batch
