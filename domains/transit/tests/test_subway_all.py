"""지하철 도착 일괄(ALL) 수집(#369) 단위 테스트.

계약: SUBWAY_STATIONS=ALL 이면 경로형 일괄 URL 로 **1콜**, 역별 루프 없음.
(실측 2026-07-15: 경로형 /ALL 은 전량 2,954행, start/end 형은 1000행 캡 → 경로형 필수.)
"""
import sys
from pathlib import Path

import pytest

_TRANSIT = Path(__file__).resolve().parents[1]        # domains/transit
_DAGS = Path(__file__).resolve().parents[3]           # dags 루트
for p in (str(_DAGS), str(_TRANSIT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from seoul_transit import config, subway  # noqa: E402
from seoul_transit.api import SeoulApiError, subway_all_url  # noqa: E402


def _all_payload(n=3):
    return {
        "errorMessage": {"code": "INFO-000", "total": n},
        "realtimeArrivalList": [
            {"statnNm": f"역{i}", "recptnDt": f"2026-07-15 12:00:0{i}"} for i in range(n)
        ],
    }


def test_subway_all_url_is_path_form():
    url = subway_all_url("KEY")
    assert url.endswith("/KEY/json/realtimeStationArrival/ALL")
    assert "/0/" not in url  # start/end 형은 1000행 캡 — 경로형 강제


def test_collect_all_single_call(monkeypatch):
    calls = []
    def fake_get(url, timeout=20):
        calls.append((url, timeout))
        return _all_payload()
    monkeypatch.setattr(subway, "get", fake_get)
    monkeypatch.setattr(config, "SUBWAY_STATIONS", ["ALL"])

    res = subway.collect_subway("KEY", "subway_arrival")

    assert len(calls) == 1                       # 전 역 = 1콜 (역별 루프 없음)
    assert calls[0][0].endswith("/ALL") and calls[0][1] == 60
    assert len(res["raws"]) == 1                 # 행 파싱은 loader 소관 — raws 만 반환
    raw = res["raws"][0]
    assert raw["rows"] == 3
    assert raw["request_params"] == {"target": "ALL", "rows": None}
    assert raw["ts_collected"]                   # loader 마커용


def test_collect_all_error_envelope_raises(monkeypatch):
    # 200 + 에러 엔벨로프(쿼터·키 만료)는 실패로 — 0행 마스킹 금지(#229 유지).
    monkeypatch.setattr(subway, "get", lambda url, timeout=20: {
        "status": 500, "code": "ERROR-337", "message": "일별 트래픽 초과", "total": 0,
    })
    monkeypatch.setattr(config, "SUBWAY_STATIONS", ["ALL"])
    with pytest.raises(SeoulApiError, match="ERROR-337"):
        subway.collect_subway("KEY", "subway_arrival")


def test_station_list_fallback_loops_per_station(monkeypatch):
    calls = []
    def fake_get(url, timeout=20):
        calls.append(url)
        return _all_payload(1)
    monkeypatch.setattr(subway, "get", fake_get)
    monkeypatch.setattr(config, "SUBWAY_STATIONS", ["강남", "잠실"])

    res = subway.collect_subway("KEY", "subway_arrival")

    assert len(calls) == 2                       # 역별 폴백 유지(부분 수집·롤백용)
    assert all("/ALL" not in u for u in calls)
    assert len(res["raws"]) == 2
    assert all(r["ts_collected"] for r in res["raws"])
