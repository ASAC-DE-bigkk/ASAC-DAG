"""서울 교통 실시간 API 호출 헬퍼 — 공통 HTTP 클라이언트(#78) 경유.

외부 API 일시 오류(5xx·429·네트워크·timeout) 재시도는 HttpCore 가 수행한다.
기존(#29) 지수 백오프 정책 — 2·4·8s, 최초 1회 + 재시도 3회 — 을
max_attempts/backoff_base 로 보존(HttpCore 는 여기에 jitter·Retry-After 존중 추가).
영구 오류(4xx: 잘못된 키/파라미터 등)는 재시도 없이 즉시 HttpProblemError 로 올린다.
"""

import json
import urllib.parse

from common.http import HttpCore

SUBWAY_BASE = "http://swopenapi.seoul.go.kr/api/subway"  # 지하철 도착·위치 (JSON)
OPENAPI_BASE = "http://openapi.seoul.go.kr:8088"          # 주차·citydata(도로) (JSON)
BUS_BASE = "http://ws.bus.go.kr/api/rest"                 # 서울 TOPIS 버스 도착·위치 (XML)

_MAX_ATTEMPTS = 4       # 기존 _RETRIES=3(추가 시도) + 최초 1회
_BACKOFF_BASE = 2.0     # 기존 _BACKOFF=2.0 → 2, 4, 8s

_CORE = HttpCore(
    source="seoul_openapi",
    max_attempts=_MAX_ATTEMPTS,
    backoff_base=_BACKOFF_BASE,
    user_agent="asac-transit-collector/1.0",
    # 기존 코드는 호출 간 지연이 없었다 — 코드 기본 5req/s 를 받지 않는다(#78 리뷰).
    # 지하철·버스·주차 3개 호스트가 한 버킷으로 묶이는 것도 방지. 스로틀 도입은 별도 결정.
    rate_limit=None,
)


def _read(url: str, timeout: int) -> str:
    """원본 응답 텍스트. 일시 오류 재시도·소진 시 HttpProblemError 는 HttpCore 소관.

    URL 에 키가 박혀 있어도(서울식 경로 키) 로그·예외에서는 redact 된다.
    """
    return _CORE.get(url, timeout=timeout).text


def get(url: str, timeout: int = 20) -> dict:
    """JSON 응답 (지하철·citydata). 일시 오류 재시도 포함."""
    return json.loads(_read(url, timeout))


def get_text(url: str, timeout: int = 20) -> str:
    """원본 텍스트 응답 (버스 XML — 파싱 없이 원본 보존). 일시 오류 재시도 포함."""
    return _read(url, timeout)


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


def bus_url(key_enc: str, path: str, **params: str) -> str:
    """서울 TOPIS 버스 빌더. key_enc 는 이미 URL 인코딩된 서비스키.

    bus_url(enc, "arrive/getArrInfoByRouteAll", busRouteId="100100025")
    """
    qs = "".join(f"&{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
    return f"{BUS_BASE}/{path}?serviceKey={key_enc}{qs}"
