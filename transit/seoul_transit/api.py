"""서울 교통 실시간 API 호출 헬퍼 (표준 라이브러리만 사용).

외부 API 일시 오류(5xx·429·네트워크·timeout)에 지수 백오프로 재시도한다.
예: ws.bus.go.kr 가 간헐적으로 HTTP 503 → 한 번의 일시 오류로 태스크가 죽지 않게.
영구 오류(4xx: 잘못된 키/파라미터 등)는 재시도해도 의미 없어 즉시 올린다.
"""

import json
import time
import urllib.error
import urllib.parse
import urllib.request

SUBWAY_BASE = "http://swopenapi.seoul.go.kr/api/subway"  # 지하철 도착·위치 (JSON)
OPENAPI_BASE = "http://openapi.seoul.go.kr:8088"          # 주차·citydata(도로) (JSON)
BUS_BASE = "http://ws.bus.go.kr/api/rest"                 # 서울 TOPIS 버스 도착·위치 (XML)

_RETRIES = 3            # 일시 오류 시 추가 시도 횟수
_BACKOFF = 2.0         # 백오프 기준(초) → 2, 4, 8s

_HEADERS = {"User-Agent": "asac-transit-collector/1.0"}


def _read(url: str, timeout: int) -> str:
    """원본 응답 텍스트. 일시 오류(5xx·429·URLError·timeout)에 지수 백오프 재시도, 4xx 는 즉시 실패."""
    last = None
    for attempt in range(_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            last = e
            if e.code < 500 and e.code != 429:  # 4xx(429 제외) = 재시도 무의미 → 즉시 실패
                raise
        except (urllib.error.URLError, TimeoutError) as e:  # 연결 실패/타임아웃
            last = e
        if attempt < _RETRIES:
            time.sleep(_BACKOFF * (2 ** attempt))
    raise last  # 모든 재시도 소진 → 마지막 예외를 올림(태스크 실패로 드러냄)


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
