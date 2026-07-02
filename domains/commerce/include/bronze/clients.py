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
import time
from dataclasses import dataclass

from security import redact   # 시크릿 마스킹(로그/예외 메시지 → 마커 저장 시 키 누출 차단)

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


class SeoulOpenApiClient:
    def __init__(self, key: str, base_url: str, *, timeout: int = 30,
                 max_attempts: int = 3, backoff_seconds: float = 2.0) -> None:
        if not key:
            raise SeoulAuthError("INFO-100", "SEOUL_API_KEY_COMM 미설정", "<config>")
        import requests  # 지연 임포트

        self._key = key
        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._backoff = backoff_seconds
        self._session = requests.Session()

    def _url(self, service: str, start: int, end: int) -> str:
        return f"{self._base}/{self._key}/json/{service}/{start}/{end}/"

    def _safe_url(self, service: str, start: int, end: int) -> str:
        return f"{self._base}/***/json/{service}/{start}/{end}/"  # 로그용(키 마스킹)

    def fetch_page(self, service: str, start: int, end: int) -> Page:
        import requests  # 지연 임포트

        last_exc: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                resp = self._session.get(self._url(service, start, end),
                                         timeout=self._timeout)
                resp.raise_for_status()
                return parse_page(resp.content, service)
            except SeoulApiError:
                raise  # API 레벨 오류는 재시도 안 함
            except (requests.RequestException, ValueError) as exc:
                last_exc = exc
                # requests 예외 메시지엔 인증키가 박힌 URL 이 들어갈 수 있어 마스킹 후 기록.
                log.warning("fetch attempt %d/%d failed for %s: %s", attempt,
                            self._max_attempts, self._safe_url(service, start, end),
                            redact(str(exc)))
                if attempt < self._max_attempts:
                    time.sleep(self._backoff * attempt)
        # 이 메시지는 bronze 마커(error 필드)로 영구 저장되므로 반드시 마스킹한다.
        raise SeoulApiError("ERROR-NETWORK",
                            f"max retries exceeded: {redact(str(last_exc))}", service)
