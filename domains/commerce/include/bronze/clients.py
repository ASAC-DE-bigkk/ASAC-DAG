"""서울 열린데이터광장 OpenAPI 클라이언트.

요청: GET {base}/{KEY}/json/{SERVICE}/{START_INDEX}/{END_INDEX}/   (1회 ≤1000건)
응답 봉투:
    {"<SERVICE>": {"list_total_count": N, "RESULT": {"CODE","MESSAGE"}, "row": [...]}}
인증/요청 오류 시 최상위 RESULT 만 오기도 한다.

- 원본 응답 바이트(raw)를 그대로 반환 → bronze 는 가공 없이 저장.
- 인증키는 로그/예외/경로에 절대 남기지 않는다.
`requests` 는 메서드 안에서 지연 임포트 → parse_page(순수 함수)는 requests 없이 테스트 가능.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from common.http import HttpCore                 # 공통 재시도·redaction·rate limit·typed 예외 (#78)
from common.http.contract import TransportResponse
from common.http.errors import HttpProblemError
from security import netio, redact   # 시크릿 마스킹 + HTTP 정책 래퍼(timeout/TLS/예외 마스킹)

log = logging.getLogger(__name__)

CODE_OK = "INFO-000"
CODE_NO_DATA = "INFO-200"        # 데이터 없음(페이지 끝)
AUTH_ERROR_CODES = {"INFO-100", "INFO-300", "ERROR-500", "ERROR-600", "ERROR-601"}


class SeoulApiError(RuntimeError):
    def __init__(self, code: str, message: str, service: str):
        super().__init__(f"[{service}] {code}: {message}")
        self.code = code
        self.message = message
        self.service = service


class SeoulAuthError(SeoulApiError):
    """인증키 문제 — 전체에 영향. 빠르게 전체 실패시킨다."""


@dataclass
class Page:
    raw_bytes: bytes
    code: str
    message: str
    rows: list[dict]
    total_count: int


def _raise_for_code(code: str, msg: str, service: str) -> None:
    if code in AUTH_ERROR_CODES:
        raise SeoulAuthError(code, msg, service)


def parse_page(raw: bytes, service: str) -> Page:
    """원본 바이트 → Page. 순수 함수(네트워크 불필요) — 단위 테스트 대상."""
    try:
        doc = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SeoulApiError("ERROR-PARSE", f"invalid JSON: {exc}", service)

    block = doc.get(service)
    if block is None:  # 최상위 RESULT(인증/요청 오류) 또는 예상 밖 구조
        result = doc.get("RESULT", {})
        code = result.get("CODE", "ERROR-UNKNOWN")
        msg = result.get("MESSAGE", "unexpected response shape")
        _raise_for_code(code, msg, service)
        raise SeoulApiError(code, msg, service)

    result = block.get("RESULT", {})
    code = result.get("CODE", "ERROR-UNKNOWN")
    msg = result.get("MESSAGE", "")
    total = int(block.get("list_total_count", 0) or 0)
    if code == CODE_NO_DATA:
        return Page(raw, code, msg, [], total)
    if code != CODE_OK:
        _raise_for_code(code, msg, service)
        raise SeoulApiError(code, msg, service)
    return Page(raw, code, msg, block.get("row", []) or [], total)


class _NetioTransport:
    """HttpCore 의 Transport 를 commerce 보안 게이트(netio)로 구현 — §20 유지 (#78).

    netio.http_request 가 timeout 주입·TLS 검증 강제·예외 args 마스킹을 담당하고,
    그 위에 HttpCore 가 통합 재시도(429/5xx+연결오류)·redaction 로깅·typed 예외를
    얹는다(합성). requests.Session 은 그대로 재사용해 연결 풀링을 유지한다.
    """

    def __init__(self, session) -> None:
        self._session = session

    def send(self, method: str, url: str, *, params, headers, timeout) -> TransportResponse:
        resp = netio.http_request(method, url, session=self._session, timeout=timeout,
                                  params=dict(params) if params else None,
                                  headers=dict(headers) if headers else None)
        return TransportResponse(status=resp.status_code, content=resp.content,
                                 headers=dict(resp.headers))


class SeoulOpenApiClient:
    def __init__(self, key: str, base_url: str, *, timeout: int = 30,
                 max_attempts: int = 3, backoff_seconds: float = 2.0) -> None:
        if not key:
            raise SeoulAuthError("INFO-100", "SEOUL_API_KEY_COMM 미설정", "<config>")
        import requests  # 지연 임포트

        self._key = key
        self._base = base_url.rstrip("/")
        # rate_limit 은 None — commerce 는 기존 SEOUL_REQUEST_DELAY_SECONDS 간격을
        # 그대로 유지한다(HttpCore rate limit 과 이중 지연 방지, 처리량 동작 보존).
        self._core = HttpCore(
            source="seoul_openapi",
            transport=_NetioTransport(requests.Session()),
            timeout=float(timeout),
            max_attempts=max_attempts,
            backoff_base=backoff_seconds,
            rate_limit=None,
        )

    def _url(self, service: str, start: int, end: int) -> str:
        return f"{self._base}/{self._key}/json/{service}/{start}/{end}/"

    def fetch_page(self, service: str, start: int, end: int) -> Page:
        # 전송·재시도·URL redaction 로깅은 HttpCore 소관(URL 경로의 키는 로그/예외에서 마스킹).
        # 업무 오류(INFO-100 등) 분류는 parse_page 가 담당 — HTTP 200 응답 본문에서 판정한다.
        try:
            response = self._core.get(self._url(service, start, end))
        except HttpProblemError as exc:
            # 재시도 소진/HTTP 오류. 이 메시지는 bronze 마커(error 필드)로 영구 저장되므로 마스킹.
            raise SeoulApiError("ERROR-NETWORK",
                                f"max retries exceeded: {redact(str(exc))}", service)
        return parse_page(response.content, service)
