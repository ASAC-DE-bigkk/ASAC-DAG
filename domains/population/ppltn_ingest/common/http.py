"""소스 클라이언트 공용 HTTP 헬퍼 — 공통 HTTP 클라이언트(#78) 위임 shim.

낮은 수준의 GET 실행과 timeout 기본값만 제공한다(이슈 #16의 얇은 ``common/http``).
URL 생성·성공 판정·파싱은 각 소스(``source``)의 몫이다.

실제 전송은 ``dags/common/http`` 의 ``HttpCore`` 가 담당한다(#78):
timeout 강제, 429/5xx 지수 백오프+jitter(Retry-After 존중), 소스별 rate limit,
로그/예외 URL redaction. 시도 소진 시 ``HttpProblemError`` (typed, #77 연계)가
던져지며 -- 예외 메시지의 URL 은 이미 redact 되어 있다. 기존과 동일하게
호출자는 예외 메시지를 ``redact_secret`` 으로 한 번 더 마스킹해도 무해하다.

``fetch`` 의 시그니처·반환 타입(``HttpResult``)은 기존과 동일하다 -- 호출측
(``source/client.py``)은 수정 없이 그대로 쓴다(re-export shim 원칙).
"""

from __future__ import annotations

from dataclasses import dataclass

from common.http import HttpCore

DEFAULT_TIMEOUT = 30
DEFAULT_USER_AGENT = "ask-seoul-bronze/1.0"

# 기존 urllib 구현은 호출당 1회 시도였다(자체 재시도 루프 없음 — 재시도는
# Airflow 태스크 retries 몫). 동작 보존을 위해 HttpCore 도 1회로 고정한다.
MAX_ATTEMPTS = 1

# urllib.urlopen 은 2xx 응답을 성공으로 반환했다 — 성공 기준을 그대로 보존.
_OK_STATUS = tuple(range(200, 300))

_CORE: HttpCore | None = None


def _core() -> HttpCore:
    """모듈 단일 HttpCore (lazy) — rate limit 상태를 호출 간 공유한다."""
    global _CORE
    if _CORE is None:
        _CORE = HttpCore(
            source="seoul_openapi",
            timeout=float(DEFAULT_TIMEOUT),
            max_attempts=MAX_ATTEMPTS,
            user_agent=DEFAULT_USER_AGENT,
        )
    return _CORE


@dataclass(frozen=True)
class HttpResult:
    """단일 HTTP 응답의 원본 결과 (파싱 전)."""

    status: int
    body: bytes


def fetch(url: str, *, timeout: int = DEFAULT_TIMEOUT, user_agent: str = DEFAULT_USER_AGENT) -> HttpResult:
    """URL 1건을 GET해 상태코드와 원본 bytes를 반환한다(파싱 없음).

    ⚠️ ``url``에 시크릿이 포함될 수 있으므로, 호출자는 예외를 잡을 때
    ``redact_secret``으로 마스킹한 메시지만 로그/메타데이터에 남겨야 한다.
    (HttpCore 가 던지는 ``HttpProblemError`` 는 이미 redact 되어 있다.)
    """
    response = _core().get(
        url,
        headers={"User-Agent": user_agent},
        timeout=float(timeout),
        expected_status=_OK_STATUS,
    )
    return HttpResult(status=response.status, body=response.content)
