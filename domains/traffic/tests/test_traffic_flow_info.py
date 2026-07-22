import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.errors import (
    TrafficBronzeConfigurationError,
    TrafficSourceBusinessError,
    TrafficSourceEmptyResponseError,
    TrafficSourceSchemaError,
)
from traffic_ingest.flow_info import (
    build_api_url,
    normalize_link_ids,
    parse_traffic_info_response,
    request_params_json,
    resolve_flow_link_ids,
)
from traffic_ingest.flow_landing import TrafficFlowLanding


def _payload(code="INFO-000", rows=None):
    return json.dumps(
        {
            "TrafficInfo": {
                "list_total_count": len(rows or []),
                "RESULT": {"CODE": code, "MESSAGE": "ok"},
                "row": rows or [],
            }
        }
    ).encode("utf-8")


def test_parse_traffic_info_response_extracts_source_fields():
    metadata, rows = parse_traffic_info_response(
        _payload(
            rows=[
                {
                    "LINK_ID": "1220003800",
                    "PRCS_SPD": "31.2",
                    "PRCS_TRV_TIME": "42",
                }
            ]
        )
    )

    assert metadata == {
        "result_code": "INFO-000",
        "result_msg": "ok",
        "list_total_count": 1,
        "row_count": 1,
    }
    assert rows == [
        {
            "link_id": "1220003800",
            "prcs_spd": "31.2",
            "prcs_trv_time": "42",
        }
    ]


def test_parse_traffic_info_response_accepts_official_xml_response():
    payload = b"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
    <TrafficInfo>
      <list_total_count>1</list_total_count>
      <RESULT><CODE>INFO-000</CODE><MESSAGE>ok</MESSAGE></RESULT>
      <row><LINK_ID>1220003800</LINK_ID><PRCS_SPD>31.2</PRCS_SPD><PRCS_TRV_TIME>42</PRCS_TRV_TIME></row>
    </TrafficInfo>"""

    metadata, rows = parse_traffic_info_response(payload)

    assert metadata["result_code"] == "INFO-000"
    assert rows[0]["link_id"] == "1220003800"


def test_info_200_is_a_valid_zero_row_response():
    metadata, rows = parse_traffic_info_response(_payload("INFO-200"))

    assert metadata["result_code"] == "INFO-200"
    assert rows == []


def test_non_success_traffic_info_response_fails_loudly():
    with pytest.raises(TrafficSourceBusinessError, match="ERROR-310"):
        parse_traffic_info_response(_payload("ERROR-310"))


def test_empty_traffic_info_response_is_a_known_transient_error():
    with pytest.raises(TrafficSourceEmptyResponseError):
        parse_traffic_info_response(b"")


def test_non_empty_garbled_traffic_info_response_still_fails_loudly():
    with pytest.raises(TrafficSourceSchemaError):
        parse_traffic_info_response(b"not json and not xml")


def test_link_ids_are_deduplicated_and_request_metadata_has_no_api_key():
    assert normalize_link_ids(["b", "a", "b"]) == ["b", "a"]
    request = request_params_json("1220003800")
    assert "1220003800" in request
    assert "api_key" not in request
    assert "secret" not in request.lower()

    with pytest.raises(TrafficBronzeConfigurationError):
        normalize_link_ids(["bad/link"])


def test_build_api_url_uses_link_id_suffix():
    url = build_api_url("1220003800", api_key="test-key")
    assert url.endswith("/xml/TrafficInfo/1/1/1220003800/")
    assert "test-key" in url


def test_flow_landing_preserves_raw_json_and_is_stable_for_same_run_link():
    class Store:
        def __init__(self):
            self.objects = {}

        def write_bytes(self, key, payload, content_type):
            self.objects[key] = (payload, content_type)

    store = Store()
    landing = TrafficFlowLanding(
        raw_store=store,
        fetch_page=lambda _link: (200, _payload(rows=[{"LINK_ID": "1220003800"}])),
        clock=lambda: datetime(2026, 7, 15, 1, 2, 3, tzinfo=timezone.utc),
    )

    result = landing.collect(link_ids=["1220003800"], dag_run_id="manual__flow")
    descriptor = result["raw_objects"][0]

    assert result["expected_rows"] == 1
    assert descriptor["raw_object_key"] in store.objects
    assert store.objects[descriptor["raw_object_key"]][0] == _payload(
        rows=[{"LINK_ID": "1220003800"}]
    )
    assert "manual__flow" in descriptor["raw_object_key"]
    assert descriptor["raw_object_key"].endswith(".xml")


def test_flow_link_resolution_is_pinned_to_exact_incident_snapshot():
    class Cursor:
        def __init__(self):
            self.statement = ""

        def execute(self, statement):
            self.statement = " ".join(statement.split())

        def fetchall(self):
            return [("1220003800",), ("1220003900",)]

    cursor = Cursor()

    result = resolve_flow_link_ids(
        incident_run_id="scheduled__incident-42",
        cursor_factory=lambda: (cursor, "iceberg_dev", "ask_seoul"),
        environ={"SEOUL_TRAFFIC_FLOW_MAX_LINKS": "1000"},
    )

    assert result == ["1220003800", "1220003900"]
    assert "incident.dag_run_id = 'scheduled__incident-42'" in cursor.statement
    assert "bronze_collection_run_manifest" not in cursor.statement
    assert "ORDER BY event_at" not in cursor.statement


def test_flow_link_resolution_requires_parent_when_links_are_not_explicit():
    with pytest.raises(TrafficBronzeConfigurationError, match="incident_run_id"):
        resolve_flow_link_ids(
            cursor_factory=lambda: pytest.fail("missing parent must fail before SQL"),
            environ={},
        )
