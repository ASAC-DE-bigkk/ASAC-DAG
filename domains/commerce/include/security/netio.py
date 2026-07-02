"""network IO 가드 — HTTP 호출의 보안 정책을 **한 곳**에서 강제하는 래퍼.

정책(호출측이 잊어도 여기서 걸린다):
  1. `timeout` 필수 — 미지정이면 기본값 주입(자원 고갈/무한 대기 차단, 기본 30s,
     env `SECURITY_HTTP_TIMEOUT` 으로 조정).
  2. TLS 인증서 검증 비활성(verify 를 False 로 전달) **차단** — InsecureRequestBlocked.
  3. 예외 마스킹 — 전송 실패 예외의 args 를 체인까지 스크럽(scrub_exception)한 뒤
     **같은 타입 그대로** 재전파 → 호출측 except 절/재시도 로직을 깨지 않으면서,
     예외가 로그·마커·알림 어디로 흘러가도 str(exc) 에 시크릿이 없다.

`requests` 는 지연 임포트(세션을 넘기면 임포트조차 안 함) — 패키지 자체는 stdlib only 유지.
사용:
    from security import http_get, http_request
    resp = http_get(url)                          # 세션 없이(1회성)
    resp = http_request("GET", url, session=s, timeout=10)   # 기존 세션 재사용
"""
from __future__ import annotations

import os
from typing import Any

from security.redaction import redact, scrub_exception


class InsecureRequestBlocked(ValueError):
    """TLS 검증 비활성 시도 차단(CLAUDE.md §20: verify 비활성 금지)."""


def default_timeout() -> float:
    try:
        return float(os.getenv("SECURITY_HTTP_TIMEOUT", "30"))
    except ValueError:
        return 30.0


def http_request(method: str, url: str, *, session: Any = None,
                 timeout: float | tuple | None = None, **kwargs) -> Any:
    """정책 강제 HTTP 호출. session 미지정 시 requests 모듈로 직접 호출.

    kwargs 는 requests 와 동일(params/headers/json/...). 반환도 requests.Response.
    """
    if kwargs.get("verify") is False:
        raise InsecureRequestBlocked(
            "TLS certificate verification must not be disabled (security policy)")
    if timeout is None:
        timeout = default_timeout()
    if session is None:
        import requests  # 지연 임포트
        caller = requests
    else:
        caller = session
    try:
        return caller.request(method, url, timeout=timeout, **kwargs)
    except Exception as exc:
        raise scrub_exception(exc)   # 같은 타입 유지 + 메시지 체인 마스킹


def http_get(url: str, **kwargs) -> Any:
    """GET (정책 강제: timeout 기본 주입 · TLS 검증 비활성 차단 · 예외 마스킹)."""
    return http_request("GET", url, **kwargs)


def http_post(url: str, **kwargs) -> Any:
    """POST (정책 강제: timeout 기본 주입 · TLS 검증 비활성 차단 · 예외 마스킹)."""
    return http_request("POST", url, **kwargs)


def safe_url(url: str) -> str:
    """로그/저장용 URL 마스킹(literal + structural). 원본 url 은 변경하지 않는다."""
    return redact(url)
