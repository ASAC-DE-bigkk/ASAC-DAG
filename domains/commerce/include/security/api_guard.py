"""API 요청/응답 가드 — 호출 기록(receipt)과 응답 요약을 **시크릿 없이** 남기는 헬퍼.

리니지(§2.1)를 위해 "무엇을 호출했고 무엇이 왔는지"는 남겨야 하지만, URL 경로·쿼리·헤더에
인증 정보가 박히는 API 가 많다(서울 OpenAPI 는 키가 URL 경로에 들어간다). 이 모듈은
저장/로그/알림에 넣어도 안전한 형태로 요청·응답 메타를 만든다.

- `scrub_url(url)`      : 경로/쿼리의 시크릿 마스킹(literal+structural+쿼리 이름 규칙).
- `scrub_headers(h)`    : Authorization/Cookie/X-Api-Key 등 민감 헤더 이름 기반 마스킹.
- `scrub_params(p)`     : 쿼리/폼 파라미터 — 이름이 시크릿 규칙(KEY/TOKEN/…)이면 값 마스킹.
- `api_receipt(...)`    : 호출 1건의 **저장 가능한 영수증**(메서드·마스킹 URL·상태·시각·소요).
- `response_summary(...)`: 응답의 **저장 가능한 요약**(상태·길이·content_hash·마스킹 샘플).
  원문 보존(bronze)은 그대로 두고, 로그/마커/알림에는 이 요약을 쓴다.

stdlib only — 특정 HTTP 라이브러리에 비종속(값만 넘기면 된다).
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from security.redaction import PLACEHOLDER, _SECRET_NAME_RE, redact

# 이름만으로 민감으로 간주하는 헤더(값 전체 마스킹). 소문자 비교.
_SENSITIVE_HEADERS = frozenset({
    "authorization", "proxy-authorization", "cookie", "set-cookie",
    "x-api-key", "x-auth-token", "x-amz-security-token", "api-key", "apikey",
    "x-access-token", "x-secret", "x-session-id",
})


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def scrub_url(url: str) -> str:
    """URL 마스킹 — 쿼리 값(이름이 시크릿 규칙) → 경로/전체(literal+structural) 순서로."""
    try:
        parts = urlsplit(url)
        if parts.query:
            pairs = [(k, PLACEHOLDER if _SECRET_NAME_RE.search(k) else v)
                     for k, v in parse_qsl(parts.query, keep_blank_values=True)]
            url = urlunsplit(parts._replace(query=urlencode(pairs)))
    except ValueError:
        pass                      # 파싱 불가 문자열도 아래 redact 는 통과시킨다
    return redact(url)


def scrub_headers(headers: Mapping[str, Any] | None) -> dict:
    """민감 헤더(이름 기반)는 값 전체 마스킹, 그 외 값은 redact() 통과."""
    out: dict = {}
    for name, value in (headers or {}).items():
        if str(name).lower() in _SENSITIVE_HEADERS or _SECRET_NAME_RE.search(str(name)):
            out[str(name)] = PLACEHOLDER
        else:
            out[str(name)] = redact(value if isinstance(value, str) else str(value))
    return out


def scrub_params(params: Mapping[str, Any] | None) -> dict:
    """요청 파라미터 — 이름이 시크릿 규칙(KEY/SECRET/TOKEN/…)이면 값 마스킹."""
    out: dict = {}
    for name, value in (params or {}).items():
        if _SECRET_NAME_RE.search(str(name)):
            out[str(name)] = PLACEHOLDER
        else:
            out[str(name)] = redact(value)
    return out


def api_receipt(*, method: str, url: str, service: str | None = None,
                params: Mapping[str, Any] | None = None,
                headers: Mapping[str, Any] | None = None,
                status: int | None = None, ok: bool | None = None,
                elapsed_ms: float | None = None, request_id: str | None = None,
                extra: Mapping[str, Any] | None = None) -> dict:
    """API 호출 1건의 저장 가능한 영수증(dict) — 그대로 마커/로그/알림에 넣어도 안전."""
    receipt: dict = {
        "kind": "api_receipt", "requested_at": _utcnow_iso(),
        "method": str(method).upper(), "url": scrub_url(url),
    }
    if service is not None:
        receipt["service"] = redact(str(service))
    if params is not None:
        receipt["params"] = scrub_params(params)
    if headers is not None:
        receipt["headers"] = scrub_headers(headers)
    if status is not None:
        receipt["status"] = int(status)
    if ok is not None:
        receipt["ok"] = bool(ok)
    if elapsed_ms is not None:
        receipt["elapsed_ms"] = round(float(elapsed_ms), 3)
    if request_id is not None:
        receipt["request_id"] = redact(str(request_id))
    if extra:
        receipt["extra"] = redact(dict(extra))
    return receipt


def response_summary(*, status: int | None = None,
                     headers: Mapping[str, Any] | None = None,
                     body: bytes | str | None = None, max_body: int = 256) -> dict:
    """응답의 저장 가능한 요약 — 원문 대신 길이·sha256·마스킹 샘플만 남긴다.

    원문 보존이 필요한 레이어(bronze)는 원본 바이트를 그대로 저장하고,
    로그/마커/알림에는 이 요약을 쓴다(원문 = 데이터, 요약 = 관측 메타).
    """
    summary: dict = {"kind": "api_response"}
    if status is not None:
        summary["status"] = int(status)
    if headers is not None:
        summary["headers"] = scrub_headers(headers)
    if body is not None:
        raw = body.encode("utf-8", errors="replace") if isinstance(body, str) else bytes(body)
        summary["content_length"] = len(raw)
        summary["content_hash"] = hashlib.sha256(raw).hexdigest()
        sample = raw[:max_body].decode("utf-8", errors="replace")
        summary["body_sample"] = redact(sample)
    return summary
