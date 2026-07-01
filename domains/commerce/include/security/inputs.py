"""입력 검증 — 사용자 입력(DAG params 등)이 저장 경로/식별자로 흘러가기 전 안전성 점검.

경로 주입(path traversal) 방어: `observed_date` 같은 파라미터가 `../`, 절대경로, 제어문자,
구분자(`/`,`\\`)를 담은 채 silver 파티션 경로(`observed_date=<...>`)에 들어가면 의도치 않은
위치에 쓰기/덮어쓰기가 가능하다. 경계에서 막는다(stdlib 만).
"""
from __future__ import annotations

import re

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# 경로 세그먼트로 허용할 문자: 영숫자·점·밑줄·하이픈·등호(파티션 표기 a=b 허용). '/'·'..'·제어문자 불가.
_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._=\-]+$")


def is_iso_date(value: str) -> bool:
    """YYYY-MM-DD 형태인지(달력 유효성까지는 보지 않음 — 형식/주입만)."""
    return bool(value) and bool(_ISO_DATE_RE.match(value))


def is_safe_segment(value: str) -> bool:
    """단일 경로 세그먼트로 안전한지. traversal/구분자/제어문자/선행 하이픈 거부."""
    if not value or value in (".", "..") or value.startswith("-"):
        return False
    if "/" in value or "\\" in value or "\x00" in value:
        return False
    return bool(_SAFE_SEGMENT_RE.match(value))


def assert_safe_segment(value: str, *, field: str = "value") -> str:
    """안전하면 그대로 반환, 아니면 ValueError. 경로로 쓰기 직전 사용."""
    if not is_safe_segment(value):
        raise ValueError(f"unsafe {field} (path-injection guard): {value!r}")
    return value


def assert_iso_date(value: str, *, field: str = "observed_date") -> str:
    """YYYY-MM-DD 강제(파티션 키 계약). 아니면 ValueError."""
    if not is_iso_date(value):
        raise ValueError(f"{field} must be YYYY-MM-DD, got {value!r}")
    return value
