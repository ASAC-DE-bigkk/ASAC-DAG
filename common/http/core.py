"""HttpCore — 소스 API 호출의 단일 통로 (#78). 구체 클래스, 상속 금지·합성 사용.

강제 사항(우회 불가 — 기획 '보안 요구'):
1. timeout 필수: 기본값 존재, None 을 넘기면 즉시 ValueError.
2. TLS 검증 비활성 불가: verify 파라미터 자체를 노출하지 않는다.
3. 예외·로그의 URL 은 항상 redact 후 노출(서울식 경로 키 대응).
4. 재시도는 기본 멱등 GET 에만: 429/5xx·연결/타임아웃 오류에 지수 백오프+jitter,
   `Retry-After` 헤더 존중. 소진 시 HttpProblemError(typed, #77 연계).
5. 소스별 rate limit(초당 호출 수) — limits.resolve_rate_limit 3단 계층.

소스별 클라이언트는 이 클래스를 상속하지 말고 주입받아 사용한다(has-a).
응답 파싱(JSON/XML)은 core 밖(어댑터 소관) — core 는 bytes/text 까지만.
"""
from __future__ import annotations

import logging
import random
import time
from typing import Callable, Mapping

from common.http import auth as auth_strategies
from common.http.contract import Transport, TransportResponse
from common.http.errors import HttpProblemError
from common.errors import types as error_types
from common.security import redact

LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30.0
DEFAULT_USER_AGENT = "asac-dag-http/1.0"
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD"})


class RequestsTransport:
    """기본 전송 — requests (#78 Q1 합의: 이미지 포함·다수 사용). lazy import."""

    def send(self, method: str, url: str, *, params: Mapping[str, str] | None,
             headers: Mapping[str, str] | None, timeout: float) -> TransportResponse:
        import requests

        # verify 미노출 — 항상 기본(TLS 검증 ON). timeout 은 HttpCore 가 보장.
        resp = requests.request(method, url, params=params, headers=headers,
                                timeout=timeout)
        return TransportResponse(status=resp.status_code, content=resp.content,
                                 headers=dict(resp.headers))


class HttpCore:
    def __init__(self, *, source: str = "default",
                 transport: Transport | None = None,
                 timeout: float = DEFAULT_TIMEOUT,
                 max_attempts: int = 3,
                 backoff_base: float = 0.5,
                 backoff_max: float = 30.0,
                 rate_limit: float | None = ...,
                 user_agent: str = DEFAULT_USER_AGENT,
                 sleep: Callable[[float], None] = time.sleep,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        from common.http.limits import resolve_rate_limit

        if timeout is None:
            raise ValueError("timeout=None 금지 — 기본값을 쓰거나 양수를 지정")
        self.source = source
        self._transport = transport or RequestsTransport()
        self._timeout = float(timeout)
        self._max_attempts = max(1, int(max_attempts))
        self._backoff_base = backoff_base
        self._backoff_max = backoff_max
        self._rate_limit = resolve_rate_limit(source, rate_limit)
        self._user_agent = user_agent
        self._sleep = sleep
        self._monotonic = monotonic
        self._last_request_at: float | None = None

    # ── 공개 API ─────────────────────────────────────────────────────────────
    def get(self, url: str, *, params: Mapping[str, str] | None = None,
            headers: Mapping[str, str] | None = None,
            auth: "auth_strategies.NoAuth | auth_strategies.QueryKey | auth_strategies.PathKey | auth_strategies.HeaderKey | None" = None,
            timeout: float | None = None,
            expected_status: tuple[int, ...] = (200,)) -> TransportResponse:
        return self.request("GET", url, params=params, headers=headers, auth=auth,
                            timeout=timeout, expected_status=expected_status)

    def request(self, method: str, url: str, *, params: Mapping[str, str] | None = None,
                headers: Mapping[str, str] | None = None, auth=None,
                timeout: float | None = None, retry: bool | None = None,
                expected_status: tuple[int, ...] = (200,)) -> TransportResponse:
        if timeout is None:
            timeout = self._timeout
        if timeout is None or timeout <= 0:
            raise ValueError("timeout 은 양수여야 함 (None/0 금지)")

        method = method.upper()
        applied = (auth or auth_strategies.NoAuth()).apply(url, params, headers)
        send_headers = {"User-Agent": self._user_agent, **applied.headers}
        retryable = retry if retry is not None else (method in _IDEMPOTENT_METHODS)
        attempts = self._max_attempts if retryable else 1

        last_status: int | None = None
        last_error: BaseException | None = None
        for attempt in range(1, attempts + 1):
            self._respect_rate_limit()
            try:
                response = self._transport.send(
                    method, applied.url, params=applied.params,
                    headers=send_headers, timeout=timeout)
            except Exception as exc:                      # 연결/타임아웃 계열
                last_error, last_status = exc, None
                LOGGER.warning("[http:%s] %s %s 실패(%s) attempt=%d/%d",
                               self.source, method, redact(applied.url),
                               type(exc).__name__, attempt, attempts)
                if attempt < attempts:
                    self._sleep(self._backoff_delay(attempt, None))
                continue

            if response.status in expected_status:
                return response
            last_status, last_error = response.status, None
            LOGGER.warning("[http:%s] %s %s → status=%d attempt=%d/%d",
                           self.source, method, redact(applied.url),
                           response.status, attempt, attempts)
            if response.status not in _RETRYABLE_STATUS or attempt >= attempts:
                break
            self._sleep(self._backoff_delay(attempt, response))

        raise self._problem_error(method, applied.url, last_status, last_error, attempts)

    # ── 내부 ────────────────────────────────────────────────────────────────
    def _respect_rate_limit(self) -> None:
        if not self._rate_limit:
            return
        min_interval = 1.0 / self._rate_limit
        now = self._monotonic()
        if self._last_request_at is not None:
            wait = min_interval - (now - self._last_request_at)
            if wait > 0:
                self._sleep(wait)
                now = self._monotonic()
        self._last_request_at = now

    def _backoff_delay(self, attempt: int, response: TransportResponse | None) -> float:
        if response is not None:
            retry_after = response.header("Retry-After")
            if retry_after:
                try:
                    return min(float(retry_after), self._backoff_max)
                except ValueError:
                    pass                                  # HTTP-date 형식은 백오프로 후퇴
        delay = min(self._backoff_base * (2 ** (attempt - 1)), self._backoff_max)
        return delay * (0.5 + random.random() / 2)        # jitter: 50~100%

    def _problem_error(self, method: str, url: str, status: int | None,
                       error: BaseException | None, attempts: int) -> HttpProblemError:
        if status is not None:
            error_type, detail = error_types.HTTP_ERROR, f"status={status}"
        else:
            error_type = error_types.classify_exception(error) if error else error_types.CONNECTION_ERROR
            if error_type is error_types.UNHANDLED:       # 전송 오류는 연결 계열로 수렴
                error_type = error_types.CONNECTION_ERROR
            detail = f"{type(error).__name__}: {error}" if error else None
        return HttpProblemError(error_type, method=method, url=url, status=status,
                                detail=detail, attempts=attempts, source_system=self.source)
