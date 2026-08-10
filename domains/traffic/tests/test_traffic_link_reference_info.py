import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.errors import (  # noqa: E402
    TrafficBronzeConfigurationError,
    TrafficSourceBusinessError,
    TrafficSourceEmptyResponseError,
    TrafficSourceSchemaError,
)
from traffic_ingest.link_reference_info import (  # noqa: E402
    LINK_INFO_SERVICE,
    LINK_VERTEX_SERVICE,
    build_link_reference_api_url,
    build_link_reference_raw_object_key,
    parse_link_reference_response,
    request_params_json,
)


def _link_vertex_xml(rows: list[tuple[object, object, object]]) -> bytes:
    row_xml = "".join(
        "<row>"
        "<LINK_ID>1220003800</LINK_ID>"
        f"<VER_SEQ>{sequence}</VER_SEQ>"
        f"<GRS80TM_X>{x}</GRS80TM_X>"
        f"<GRS80TM_Y>{y}</GRS80TM_Y>"
        "</row>"
        for sequence, x, y in rows
    )
    return (
        "<LinkVerInfo>"
        f"<list_total_count>{len(rows)}</list_total_count>"
        "<RESULT><CODE>INFO-000</CODE><MESSAGE>정상 처리되었습니다.</MESSAGE></RESULT>"
        f"{row_xml}"
        "</LinkVerInfo>"
    ).encode("utf-8")


def test_link_info_xml_extracts_native_road_contract():
    payload = """<LinkInfo>
      <list_total_count>1</list_total_count>
      <RESULT><CODE>INFO-000</CODE><MESSAGE>정상 처리되었습니다.</MESSAGE></RESULT>
      <row>
        <LINK_ID>1220003800</LINK_ID><ROAD_NAME>테스트로</ROAD_NAME>
        <ST_NODE_NM>시점</ST_NODE_NM><ED_NODE_NM>종점</ED_NODE_NM>
        <MAP_DIST>182.3</MAP_DIST><REG_CD>11000</REG_CD>
      </row>
    </LinkInfo>""".encode("utf-8")

    metadata, rows = parse_link_reference_response(
        LINK_INFO_SERVICE,
        payload,
        requested_link_id="1220003800",
    )

    assert metadata == {
        "service_name": "LinkInfo",
        "result_code": "INFO-000",
        "result_msg": "정상 처리되었습니다.",
        "list_total_count": 1,
        "row_count": 1,
    }
    assert rows == [
        {
            "link_id": "1220003800",
            "road_name": "테스트로",
            "start_node_name": "시점",
            "end_node_name": "종점",
            "map_distance": "182.3",
            "region_code": "11000",
        }
    ]


def test_link_vertex_json_preserves_ordered_grs80_values_as_source_strings():
    payload = json.dumps(
        {
            "LinkVerInfo": {
                "list_total_count": 2,
                "RESULT": {"CODE": "INFO-000", "MESSAGE": "ok"},
                "row": [
                    {
                        "LINK_ID": "1220003800",
                        "VER_SEQ": "1",
                        "GRS80TM_X": "194000.25",
                        "GRS80TM_Y": "451000.5",
                    },
                    {
                        "LINK_ID": "1220003800",
                        "VER_SEQ": "2",
                        "GRS80TM_X": "194001.25",
                        "GRS80TM_Y": "451001.5",
                    },
                ],
            }
        },
        ensure_ascii=False,
    ).encode("utf-8")

    metadata, rows = parse_link_reference_response(
        LINK_VERTEX_SERVICE,
        payload,
        requested_link_id="1220003800",
    )

    assert metadata["row_count"] == 2
    assert rows == [
        {
            "link_id": "1220003800",
            "vertex_sequence": "1",
            "grs80tm_x": "194000.25",
            "grs80tm_y": "451000.5",
        },
        {
            "link_id": "1220003800",
            "vertex_sequence": "2",
            "grs80tm_x": "194001.25",
            "grs80tm_y": "451001.5",
        },
    ]


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([(1, "194000", "451000"), (1, "194001", "451001")], "vertex_sequence"),
        ([("one", "194000", "451000")], "vertex_sequence"),
        ([(1, "not-a-number", "451000")], "GRS80TM"),
    ],
)
def test_link_vertex_rejects_ambiguous_or_non_numeric_geometry(rows, message):
    with pytest.raises(TrafficSourceSchemaError, match=message):
        parse_link_reference_response(
            LINK_VERTEX_SERVICE,
            _link_vertex_xml(rows),
            requested_link_id="1220003800",
        )


def test_link_reference_rejects_rows_for_a_different_requested_link():
    payload = _link_vertex_xml([(1, "194000", "451000")]).replace(
        b"1220003800", b"1220003900"
    )

    with pytest.raises(TrafficSourceSchemaError, match="requested link_id"):
        parse_link_reference_response(
            LINK_VERTEX_SERVICE,
            payload,
            requested_link_id="1220003800",
        )


def test_link_info_requires_exactly_one_success_row():
    payload = b"""<LinkInfo>
      <list_total_count>0</list_total_count>
      <RESULT><CODE>INFO-000</CODE><MESSAGE>ok</MESSAGE></RESULT>
    </LinkInfo>"""

    with pytest.raises(TrafficSourceSchemaError, match="exactly one row"):
        parse_link_reference_response(
            LINK_INFO_SERVICE,
            payload,
            requested_link_id="1220003800",
        )


def test_link_info_requires_a_usable_road_name():
    payload = b"""<LinkInfo>
      <list_total_count>1</list_total_count>
      <RESULT><CODE>INFO-000</CODE><MESSAGE>ok</MESSAGE></RESULT>
      <row><LINK_ID>1220003800</LINK_ID><ROAD_NAME> </ROAD_NAME></row>
    </LinkInfo>"""

    with pytest.raises(TrafficSourceSchemaError, match="ROAD_NAME"):
        parse_link_reference_response(
            LINK_INFO_SERVICE,
            payload,
            requested_link_id="1220003800",
        )


def test_link_vertex_requires_source_total_count_to_match_rows():
    payload = _link_vertex_xml([(1, "194000", "451000")]).replace(
        b"list_total_count>1", b"list_total_count>2"
    )

    with pytest.raises(TrafficSourceSchemaError, match="list_total_count"):
        parse_link_reference_response(
            LINK_VERTEX_SERVICE,
            payload,
            requested_link_id="1220003800",
        )


def test_info_200_is_a_valid_empty_reference_response():
    payload = b"""<LinkInfo>
      <list_total_count>0</list_total_count>
      <RESULT><CODE>INFO-200</CODE><MESSAGE>no data</MESSAGE></RESULT>
    </LinkInfo>"""

    metadata, rows = parse_link_reference_response(
        LINK_INFO_SERVICE,
        payload,
        requested_link_id="1220003800",
    )

    assert metadata["result_code"] == "INFO-200"
    assert rows == []


def test_non_success_reference_response_and_empty_payload_fail_loudly():
    payload = b"""<LinkInfo>
      <list_total_count>0</list_total_count>
      <RESULT><CODE>ERROR-310</CODE><MESSAGE>invalid</MESSAGE></RESULT>
    </LinkInfo>"""

    with pytest.raises(TrafficSourceBusinessError, match="ERROR-310"):
        parse_link_reference_response(
            LINK_INFO_SERVICE,
            payload,
            requested_link_id="1220003800",
        )
    with pytest.raises(TrafficSourceEmptyResponseError):
        parse_link_reference_response(
            LINK_INFO_SERVICE,
            b"",
            requested_link_id="1220003800",
        )


def test_service_specific_urls_and_request_metadata_do_not_expose_the_key():
    info_url = build_link_reference_api_url(
        LINK_INFO_SERVICE,
        "1220003800",
        api_key="fixture-secret",
    )
    vertex_url = build_link_reference_api_url(
        LINK_VERTEX_SERVICE,
        "1220003800",
        api_key="fixture-secret",
    )

    assert info_url.endswith("/xml/LinkInfo/1/1/1220003800/")
    assert vertex_url.endswith("/xml/LinkVerInfo/1/1000/1220003800/")
    assert "fixture-secret" in info_url
    params = request_params_json(LINK_VERTEX_SERVICE, "1220003800")
    assert json.loads(params) == {
        "api": "LinkVerInfo",
        "end_index": 1000,
        "format": "xml",
        "link_id": "1220003800",
        "start_index": 1,
    }
    assert "fixture-secret" not in params


def test_raw_key_uses_explicit_partition_and_rejects_unknown_service():
    key = build_link_reference_raw_object_key(
        datetime(2026, 8, 9, 15, 1, 2, tzinfo=timezone.utc),
        "manual__link-ref",
        LINK_INFO_SERVICE,
        "1220003800",
        landing_load_date="2026-08-10",
    )

    assert key == (
        "raw/traffic/seoul_traffic_link_reference/load_date=2026-08-10/"
        "run_id=manual__link-ref/"
        "20260810T000102KST_LinkInfo-link_id=1220003800.xml"
    )
    with pytest.raises(TrafficBronzeConfigurationError, match="service_name"):
        build_link_reference_api_url(
            "UnknownInfo",
            "1220003800",
            api_key="fixture-secret",
        )
