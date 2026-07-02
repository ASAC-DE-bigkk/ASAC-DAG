"""network IO 가드 — HTTP 호출의 보안 정책을 **한 곳**에서 강제하는 래퍼.

정책(호출측이 잊어도 여기서 걸린다):
  1. `timeout` 필수 — 미지정이면 기본값 주입(자원 고갈/무한 대기 차단, 기본 30s,
     env `SECURITY_HTTP_TIMEOUT` 으로 조정).
  2. TLS 인증서 검증 비활성(verify 를 False 로 전달) **차단** — InsecureRequestBlocked.
  3. 예외 마스킹 — 전송 실패 예외의 args 를 체인까지 스크럽(scrub_exception)한 뒤
     **같은 타입 그대로** 재전파 → 호출측 except 절/재시도 로직을 깨지 않으면서,
     예외가 로그·마커·알림 어디로 흘러가도 str(exc) 에 시크릿이 없다.
  4. (opt-in) SSRF 가드 — `url_check=True` 면 스킴/사용자정보/사설·메타데이터 IP 차단
     (OWASP A01:2025 에 통합된 SSRF, CWE-918). 기본 off — 기존 호출 동작 불변.
  5. (opt-in) 응답 크기 상한 — `max_response_bytes=` 지정 시 스트리밍 카운터로 강제
     (CWE-770 자원 무제한 할당 차단; Content-Length 헤더는 힌트일 뿐 신뢰하지 않는다).

`requests` 는 지연 임포트(세션을 넘기면 임포트조차 안 함) — 패키지 자체는 stdlib only 유지.
사용:
    from security import http_get, http_request, assert_url_allowed
    resp = http_get(url)                          # 세션 없이(1회성)
    resp = http_request("GET", url, session=s, timeout=10)   # 기존 세션 재사용
    resp = http_request("GET", user_url, url_check=True)     # 사용자 입력 URL(SSRF 가드)
"""
from __future__ import annotations

import ipaddress
import os
import socket
from typing import Any, Collection
from urllib.parse import urlsplit

from security.redaction import redact, scrub_exception


class InsecureRequestBlocked(ValueError):
    """TLS 검증 비활성 시도 차단(CLAUDE.md §20: verify 비활성 금지)."""


class UnsafeURLBlocked(ValueError):
    """SSRF 가드 차단 — 허용되지 않은 스킴/호스트/사설·메타데이터 IP."""


class ResponseTooLarge(ValueError):
    """응답 크기 상한(max_response_bytes) 초과 — 자원 고갈(CWE-770) 차단."""


def default_timeout() -> float:
    try:
        return float(os.getenv("SECURITY_HTTP_TIMEOUT", "30"))
    except ValueError:
        return 30.0


# ── SSRF 가드(CWE-918 / OWASP A01:2025) ────────────────────────────────────────
# 명시적 CIDR 차단 리스트 — ipaddress.is_private/is_global 에 의존하지 않는다
# (CVE-2024-4032: 구버전 파이썬의 오분류). 클라우드 메타데이터(IMDS) 대역 포함.
_BLOCKED_V4 = tuple(ipaddress.ip_network(n) for n in (
    "0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16",
    "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.168.0.0/16", "198.18.0.0/15",
    "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
    "255.255.255.255/32", "100.100.100.200/32",   # 마지막: Alibaba Cloud IMDS
))
_BLOCKED_V6 = tuple(ipaddress.ip_network(n) for n in (
    "::/128", "::1/128", "::ffff:0:0/96",          # v4-mapped 는 아래에서 v4 로도 재검사
    "64:ff9b::/96", "64:ff9b:1::/48",              # NAT64 변환 프리픽스(정책 우회 차단)
    "100::/64", "2001:db8::/32", "2002::/16", "fc00::/7", "fe80::/10", "ff00::/8",
))


def _ip_blocked(ip: "ipaddress.IPv4Address | ipaddress.IPv6Address") -> bool:
    if isinstance(ip, ipaddress.IPv6Address):
        mapped = ip.ipv4_mapped
        if mapped is not None and _ip_blocked(mapped):
            return True
        return any(ip in net for net in _BLOCKED_V6)
    return any(ip in net for net in _BLOCKED_V4)


def assert_url_allowed(url: str, *, schemes: tuple = ("http", "https"),
                       allowed_hosts: "Collection[str] | None" = None,
                       block_private: bool = True, resolve_dns: bool = False) -> str:
    """URL 이 SSRF 정책을 통과하면 그대로 반환, 아니면 UnsafeURLBlocked.

    차단: 비허용 스킴(file/ftp/gopher 등) · userinfo(user:pass@) 포함 · 빈 호스트 ·
    (block_private) IP 리터럴이 사설/루프백/링크로컬/메타데이터 대역 ·
    (allowed_hosts 지정 시) 목록 밖 호스트 · (resolve_dns) DNS 응답 IP 전수 검사.

    한계(문서화): resolve_dns 검증 후 실제 요청이 **재해석**되는 DNS 리바인딩(TOCTOU)은
    stdlib 래퍼로 못 막는다 — 완전 방어가 필요하면 검증된 IP 고정(커스텀 어댑터) +
    리다이렉트 비허용을 함께 써야 한다. url_check=True 인 http_request 는 기본으로
    allow_redirects=False 를 적용한다(각 hop 재검증은 호출측 책임).
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        raise UnsafeURLBlocked(f"unparsable url: {redact(str(url))!r}")
    if parts.scheme.lower() not in schemes:
        raise UnsafeURLBlocked(f"scheme not allowed: {parts.scheme!r}")
    if parts.username is not None or parts.password is not None:
        raise UnsafeURLBlocked("userinfo(user:pass@) in url is not allowed")
    host = parts.hostname
    if not host:
        raise UnsafeURLBlocked("empty host")
    if allowed_hosts is not None and host.lower() not in {h.lower() for h in allowed_hosts}:
        raise UnsafeURLBlocked(f"host not in allowlist: {host!r}")
    if block_private:
        try:
            ips = [ipaddress.ip_address(host)]           # IP 리터럴
        except ValueError:
            ips = []
            if resolve_dns:                              # 호스트명 → 모든 A/AAAA 응답 검사
                try:
                    infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
                    ips = [ipaddress.ip_address(info[4][0]) for info in infos]
                except socket.gaierror:
                    raise UnsafeURLBlocked(f"dns resolution failed: {host!r}")
        for ip in ips:
            if _ip_blocked(ip):
                raise UnsafeURLBlocked(f"blocked ip range: {host!r} -> {ip}")
    return url


def is_url_allowed(url: str, **kwargs) -> bool:
    """assert_url_allowed 의 bool 버전(예외 대신 True/False)."""
    try:
        assert_url_allowed(url, **kwargs)
        return True
    except UnsafeURLBlocked:
        return False


# ── HTTP 래퍼 ───────────────────────────────────────────────────────────────────
def http_request(method: str, url: str, *, session: Any = None,
                 timeout: "float | tuple | None" = None,
                 url_check: bool = False,
                 allowed_hosts: "Collection[str] | None" = None,
                 resolve_dns: bool = True,
                 max_response_bytes: "int | None" = None, **kwargs) -> Any:
    """정책 강제 HTTP 호출. session 미지정 시 requests 모듈로 직접 호출.

    kwargs 는 requests 와 동일(params/headers/json/...). 반환도 requests.Response.
    url_check=True: SSRF 가드 적용(**기본 resolve_dns=True** — 호스트명을 DNS 해석해 모든
    응답 IP 를 사설/메타데이터 대역과 대조; 실제 요청 직전이라 어차피 해석되므로 안전한 기본).
    allow_redirects 는 기본 False(명시 전달 시 존중 — 리다이렉트 hop 의 가드 우회 차단).
    max_response_bytes: 스트리밍 카운터로 응답 크기 상한 강제(초과 시 ResponseTooLarge).
    """
    if kwargs.get("verify") is False:
        raise InsecureRequestBlocked(
            "TLS certificate verification must not be disabled (security policy)")
    if url_check:
        # resolve_dns=True 라야 호스트명·숫자표기 IP(십진/8진/16진/후행점)도 실제 해석해 검사한다
        # (IP 리터럴만 막으면 metadata.google.internal, 2130706433 등이 그대로 통과).
        assert_url_allowed(url, allowed_hosts=allowed_hosts, resolve_dns=resolve_dns)
        kwargs.setdefault("allow_redirects", False)   # 리다이렉트 hop 의 가드 우회 차단
    if timeout is None:
        timeout = default_timeout()
    if max_response_bytes is not None:
        kwargs["stream"] = True   # 크기 검사를 위해 스트리밍 강제(stream=False 로 상한 무력화 방지)
    if session is None:
        import requests  # 지연 임포트
        caller = requests
    else:
        caller = session
    try:
        resp = caller.request(method, url, timeout=timeout, **kwargs)
    except Exception as exc:
        raise scrub_exception(exc)   # 같은 타입 유지 + 메시지 체인 마스킹
    if max_response_bytes is not None:
        _enforce_response_cap(resp, max_response_bytes, url)
    return resp


def _enforce_response_cap(resp: Any, cap: int, url: str) -> None:
    """응답 크기 상한 강제 — 헤더는 힌트, 실제 수신 바이트 카운터가 최종 판정."""
    declared = None
    try:
        declared = int(resp.headers.get("Content-Length", ""))
    except (AttributeError, TypeError, ValueError):
        pass
    if declared is not None and declared > cap:
        _close_quietly(resp)
        raise ResponseTooLarge(f"declared {declared}B > cap {cap}B: {redact(url)}")
    total, chunks = 0, []
    for chunk in resp.iter_content(65536):
        total += len(chunk)
        if total > cap:
            _close_quietly(resp)
            raise ResponseTooLarge(f"body exceeded cap {cap}B: {redact(url)}")
        chunks.append(chunk)
    resp._content = b"".join(chunks)   # 이후 .content/.text 가 평소처럼 동작


def _close_quietly(resp: Any) -> None:
    try:
        resp.close()
    except Exception:
        pass


def http_get(url: str, **kwargs) -> Any:
    """GET (정책 강제: timeout 기본 주입 · TLS 검증 비활성 차단 · 예외 마스킹)."""
    return http_request("GET", url, **kwargs)


def http_post(url: str, **kwargs) -> Any:
    """POST (정책 강제: timeout 기본 주입 · TLS 검증 비활성 차단 · 예외 마스킹)."""
    return http_request("POST", url, **kwargs)


def safe_url(url: str) -> str:
    """로그/저장용 URL 마스킹(literal + structural + userinfo). 원본은 변경하지 않는다."""
    return redact(url)
