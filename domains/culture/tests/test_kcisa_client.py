"""#196 — KcisaClient(area2 페이징) + kcisa XML 파싱 테스트."""
from __future__ import annotations

from culture_ingest.common.records import parse_records

_KCISA_XML = (
    b"<response><body><items>"
    b"<item><serviceName>\xec\xa0\x84\xec\x8b\x9c</serviceName><seq>1</seq>"
    b"<title>A</title><place>P</place><gpsX>127.0</gpsX><gpsY>37.5</gpsY></item>"
    b"<item><serviceName>\xea\xb3\xb5\xec\x97\xb0</serviceName><seq>2</seq>"
    b"<title>B</title><place>Q</place></item>"
    b"</items><totalCount>2</totalCount></body></response>"
)


def test_parse_kcisa_items():
    recs = parse_records("kcisa", _KCISA_XML, "item", "area2")
    assert len(recs) == 2
    assert recs[0]["seq"] == "1" and recs[0]["title"] == "A"
    assert recs[0]["gpsX"] == "127.0"
    assert recs[1]["seq"] == "2"


def test_parse_kcisa_empty():
    body = b"<response><body><items></items><totalCount>0</totalCount></body></response>"
    assert parse_records("kcisa", body, "item", "area2") == []
