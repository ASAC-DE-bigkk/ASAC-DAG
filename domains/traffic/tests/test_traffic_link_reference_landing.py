import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.errors import TrafficBronzeConfigurationError  # noqa: E402
from traffic_ingest.link_reference_info import (  # noqa: E402
    LINK_INFO_SERVICE,
    LINK_VERTEX_SERVICE,
)
from traffic_ingest.link_reference_landing import (  # noqa: E402
    TrafficLinkReferenceLanding,
)


LINK_INFO_PAYLOAD = """<LinkInfo>
  <list_total_count>1</list_total_count>
  <RESULT><CODE>INFO-000</CODE><MESSAGE>ok</MESSAGE></RESULT>
  <row><LINK_ID>1220003800</LINK_ID><ROAD_NAME>테스트로</ROAD_NAME></row>
</LinkInfo>""".encode("utf-8")

LINK_VERTEX_PAYLOAD = b"""<LinkVerInfo>
  <list_total_count>2</list_total_count>
  <RESULT><CODE>INFO-000</CODE><MESSAGE>ok</MESSAGE></RESULT>
  <row><LINK_ID>1220003800</LINK_ID><VER_SEQ>1</VER_SEQ><GRS80TM_X>194000</GRS80TM_X><GRS80TM_Y>451000</GRS80TM_Y></row>
  <row><LINK_ID>1220003800</LINK_ID><VER_SEQ>2</VER_SEQ><GRS80TM_X>194001</GRS80TM_X><GRS80TM_Y>451001</GRS80TM_Y></row>
</LinkVerInfo>"""


class Store:
    def __init__(self):
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.write_order: list[str] = []

    def write_bytes(self, key, payload, content_type):
        self.objects[key] = (payload, content_type)
        self.write_order.append(key)


def _landing(store: Store, calls: list[tuple[str, str]]):
    def fetch_service(service_name: str, link_id: str):
        calls.append((service_name, link_id))
        payload = (
            LINK_INFO_PAYLOAD
            if service_name == LINK_INFO_SERVICE
            else LINK_VERTEX_PAYLOAD
        )
        return 200, payload

    return TrafficLinkReferenceLanding(
        raw_store=store,
        fetch_service=fetch_service,
        clock=lambda: datetime(2026, 8, 9, 15, 1, 2, tzinfo=timezone.utc),
    )


def test_landing_writes_two_raw_objects_then_one_complete_manifest_per_link():
    store = Store()
    calls: list[tuple[str, str]] = []

    result = _landing(store, calls).collect(
        link_ids=["1220003800"],
        dag_run_id="manual__link-ref",
        landing_load_date="2026-08-10",
    )

    assert calls == [
        ("LinkInfo", "1220003800"),
        ("LinkVerInfo", "1220003800"),
    ]
    assert [item["request_id"] for item in result["raw_objects"]] == [
        "eee4f118aec25a3dc2decfbd1409d34b",
        "eb9a84544f7bf71f52484e48b74a2bae",
    ]
    assert [item["service_name"] for item in result["raw_objects"]] == [
        "LinkInfo",
        "LinkVerInfo",
    ]
    assert {item["load_date"] for item in result["raw_objects"]} == {
        "2026-08-10"
    }
    assert result["requested_link_ids"] == ["1220003800"]
    assert result["expected_raw_objects"] == 2
    assert result["parsed_rows"] == 3
    assert store.objects[result["raw_object_keys"][0]][0] == LINK_INFO_PAYLOAD
    assert store.objects[result["raw_object_keys"][1]][0] == LINK_VERTEX_PAYLOAD
    assert store.write_order[-1] == result["manifest_key"]
    manifest = json.loads(store.objects[result["manifest_key"]][0])
    assert manifest == {
        "run_id": "manual__link-ref",
        "dataset": "seoul_traffic_link_reference",
        "load_date": "2026-08-10",
        "object_keys": result["raw_object_keys"],
        "expected_count": 2,
        "actual_count": 2,
        "completed_at": "2026-08-09T15:01:02+00:00",
        "status": "complete",
    }
    assert all(
        "fixture-secret" not in json.dumps(item)
        and "api_key" not in json.dumps(item).lower()
        for item in result["raw_objects"]
    )


def test_landing_empty_cache_miss_set_is_a_noop_without_clock_api_or_manifest():
    calls: list[tuple[str, str]] = []

    result = TrafficLinkReferenceLanding(
        raw_store=Store(),
        fetch_service=lambda service, link: calls.append((service, link)),
        clock=lambda: pytest.fail("empty cache miss set must not read the clock"),
    ).collect(link_ids=[], dag_run_id="manual__cached")

    assert result == {
        "source_id": "seoul_traffic_link_reference",
        "requested_link_ids": [],
        "raw_objects": [],
        "raw_object_keys": [],
        "parsed_rows": 0,
        "expected_raw_objects": 0,
        "is_publishable": True,
        "manifest_key": None,
        "landing_load_date": None,
    }
    assert calls == []


def test_invalid_explicit_partition_fails_before_static_api_call():
    calls: list[tuple[str, str]] = []
    landing = _landing(Store(), calls)

    with pytest.raises(TrafficBronzeConfigurationError, match="landing_load_date"):
        landing.collect(
            link_ids=["1220003800"],
            dag_run_id="manual__link-ref",
            landing_load_date="not-a-date",
        )

    assert calls == []
