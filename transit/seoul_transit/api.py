"""서울 교통 실시간 API 호출 헬퍼 (표준 라이브러리만 사용)."""

import json
import urllib.parse
import urllib.request

SUBWAY_BASE = "http://swopenapi.seoul.go.kr/api/subway"  # 지하철 도착·위치
OPENAPI_BASE = "http://openapi.seoul.go.kr:8088"          # 주차·citydata(도로)


def get(url: str, timeout: int = 20) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "asac-transit-collector/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def subway_url(key: str, service: str, rows: int, target: str) -> str:
    """realtimeStationArrival / realtimePosition 공통 URL 빌더."""
    return f"{SUBWAY_BASE}/{key}/json/{service}/0/{rows}/{urllib.parse.quote(target)}"


def openapi_url(key: str, service: str, start: int, end: int, *path: str) -> str:
    """openapi.seoul.go.kr:8088 공통 빌더 (주차·도로 확장용).

    GetParkingInfo: openapi_url(key,"GetParkingInfo",1,1000)        → .../1/1000/
    citydata:       openapi_url(key,"citydata",1,5,"강남역")        → .../1/5/강남역
    """
    url = f"{OPENAPI_BASE}/{key}/json/{service}/{start}/{end}/"
    if path:
        url += "/".join(urllib.parse.quote(p) for p in path)
    return url
