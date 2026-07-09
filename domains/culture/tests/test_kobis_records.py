"""#197 — parse_records 의 kobis 분기(JSON boxOfficeResult.dailyBoxOfficeList).

KOBIS 는 JSON 이고 배열 경로가 서울(.row)과 달라 전용 분기다.
"""
from __future__ import annotations

import json

from culture_ingest.common.records import parse_records

_ENDPOINT = "searchDailyBoxOfficeList"

_SAMPLE = json.dumps({
    "boxOfficeResult": {
        "boxofficeType": "일별 박스오피스",
        "showRange": "20260707~20260707",
        "dailyBoxOfficeList": [
            {"rank": "1", "movieCd": "20260001", "movieNm": "영화A", "audiCnt": "44464"},
            {"rank": "2", "movieCd": "20260002", "movieNm": "영화B", "audiCnt": "20000"},
        ],
    }
}).encode("utf-8")


def test_parse_kobis_returns_daily_list():
    recs = parse_records("kobis", _SAMPLE, "item", _ENDPOINT)
    assert len(recs) == 2
    assert recs[0]["movieCd"] == "20260001"
    assert recs[0]["movieNm"] == "영화A"
    assert recs[1]["rank"] == "2"


def test_parse_kobis_corrupt_body_returns_empty():
    assert parse_records("kobis", b"not json", "item", _ENDPOINT) == []


def test_parse_kobis_missing_container_returns_empty():
    body = json.dumps({"faultInfo": {"message": "bad"}}).encode("utf-8")
    assert parse_records("kobis", body, "item", _ENDPOINT) == []
