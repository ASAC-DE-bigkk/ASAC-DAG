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


class SeoulApiError(RuntimeError):
    """서울 OpenAPI 가 비정상 결과 코드를 반환(HTTP 200 + 에러 엔벨로프) — #229.

    HttpCore 는 HTTP 4xx/5xx 만 실패로 본다. 서울 API 는 인증오류·쿼터초과를
    **200 본문의 결과 코드**로 알리므로, 코드를 검사해 이 예외로 올려 태스크를
    실패시킨다(→ #77 콜백·재시도·#161 Discord). 그러지 않으면 0행 success 로 마스킹돼
    실시간 이력에 영구 공백이 남는다.
    """


# INFO-000 = 정상, INFO-200 = 데이터 없음(정상 종료로 간주). culture 선례와 동일.
_OK_RESULT_CODES = frozenset({"INFO-000", "INFO-200"})


def result_code(payload: dict, service: str) -> str | None:
    """응답에서 결과 코드를 뽑는다 — 서울의 두 엔벨로프 형태를 모두 지원.

    - swopenapi(지하철): 정상은 ``errorMessage.code``(예 INFO-000), 인증오류 시엔
      errorMessage 필드가 **top-level 로 flatten**되어 ``code``/``message`` 로 온다.
    - openapi.seoul(주차): 정상은 ``payload[service].RESULT.CODE``, 인증/요청 오류 시엔
      **top-level ``RESULT``** 만 오기도 한다(culture clients 선례).

    코드가 어디에도 없으면 None(구형/예외 응답 — 여기서 raise 하지 않고 0행 경보에 맡긴다).
    """
    if not isinstance(payload, dict):
        return None
    em = payload.get("errorMessage")
    if isinstance(em, dict) and em.get("code"):
        return em.get("code")
    # 지하철 인증오류: errorMessage 가 top-level 로 flatten (service 키·리스트 없음).
    if payload.get("code") and payload.get("message") is not None and service not in payload:
        return payload.get("code")
    svc = payload.get(service)
    if isinstance(svc, dict) and isinstance(svc.get("RESULT"), dict):
        return svc["RESULT"].get("CODE")
    if isinstance(payload.get("RESULT"), dict):
        return payload["RESULT"].get("CODE")
    return None


def raise_for_result(payload: dict, service: str, target: str = "") -> None:
    """비정상 결과 코드면 SeoulApiError. 정상(INFO-000/200)·코드없음이면 통과."""
    code = result_code(payload, service)
    if code and code not in _OK_RESULT_CODES:
        where = f" target={target}" if target else ""
        raise SeoulApiError(f"{service} 응답 오류 code={code}{where}")


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
