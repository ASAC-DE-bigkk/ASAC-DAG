"""암호 유틸 — CSPRNG 토큰 · 상수시간 비교 · PBKDF2 비밀번호 해시(stdlib only).

가이드라인: ASVS 5.0 V6/V11, OWASP Password Storage Cheat Sheet, NIST SP 800-63B.
- 토큰/세션 ID 는 반드시 CSPRNG(`secrets`) — `random` 모듈 금지(CWE-330/338,
  audit 의 `insecure_random` 점검이 잡는다). 기본 32바이트(=256비트) ≫ 최소 64비트 권고.
- 비밀 비교는 상수시간(`hmac.compare_digest`) — 타이밍 부채널(CWE-208) 차단.
- 비밀번호 저장: **Argon2id 가 1순위**(외부 의존성 허용 시 argon2-cffi 권장,
  m=19456KiB/t=2/p=1). 이 모듈은 의존성 0 이 필요한 환경의 **의도된 stdlib/FIPS 폴백**:
  PBKDF2-HMAC-SHA256, 기본 반복 600,000회(치트시트 권고치), salt 16바이트 랜덤.

인코딩은 자기서술(`pbkdf2_sha256$<iterations>$<b64salt>$<b64hash>`)이라 반복수 상향 시
`needs_rehash()` 로 로그인 시점 투명 업그레이드가 가능하다.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import unicodedata

_ALGO = "pbkdf2_sha256"
DEFAULT_ITERATIONS = 600_000     # OWASP Password Storage cheat sheet (PBKDF2-HMAC-SHA256)
_MIN_ITERATIONS = 210_000        # 이 밑으로는 조용한 약화 방지를 위해 거부(FIPS 최저선 근사)
_SALT_BYTES = 16


def generate_token(nbytes: int = 32) -> str:
    """URL-safe CSPRNG 토큰(기본 256비트). 세션/API 토큰·nonce 용."""
    return secrets.token_urlsafe(nbytes)


def generate_hex_token(nbytes: int = 32) -> str:
    """hex CSPRNG 토큰(기본 256비트)."""
    return secrets.token_hex(nbytes)


def constant_time_equals(a: "str | bytes", b: "str | bytes") -> bool:
    """상수시간 비교(CWE-208 타이밍 부채널 차단). 토큰/서명/해시 비교에 사용."""
    if isinstance(a, str):
        a = a.encode("utf-8")
    if isinstance(b, str):
        b = b.encode("utf-8")
    return hmac.compare_digest(a, b)


def _normalize(password: str) -> bytes:
    """NFKC 정규화 후 UTF-8 인코딩(NIST SP 800-63B §5.1.1.2 — 동치 문자열 wrong-reject 방지).

    hash 와 verify 가 **동일 정규화**를 써야 일관된다(한쪽만 하면 검증 불일치).
    """
    return unicodedata.normalize("NFKC", password).encode("utf-8")


def hash_password(password: str, *, iterations: int = DEFAULT_ITERATIONS) -> str:
    """비밀번호 → 자기서술 PBKDF2 해시 문자열. 평문은 어디에도 저장/로그하지 않는다."""
    if not isinstance(password, str) or not password:
        raise ValueError("password must be a non-empty str")
    if iterations < _MIN_ITERATIONS:
        raise ValueError(f"iterations must be >= {_MIN_ITERATIONS} (got {iterations})")
    salt = secrets.token_bytes(_SALT_BYTES)
    dk = hashlib.pbkdf2_hmac("sha256", _normalize(password), salt, iterations)
    return "$".join((_ALGO, str(iterations),
                     base64.b64encode(salt).decode("ascii"),
                     base64.b64encode(dk).decode("ascii")))


def verify_password(password: str, encoded: str) -> bool:
    """저장된 해시로 비밀번호 검증(상수시간). 파라미터는 저장값에서만 읽는다.

    손상/악의적 저장값(None·비문자열·잘못된 알고리즘·반복수 0/음수·base64 오류)은 예외 대신
    **깨끗한 거부**(False)로 처리한다 — 검증 경로가 예외로 죽지 않게(로그인 DoS 방지;
    DB NULL 이 None 으로 들어와도 크래시 없이 거부).
    """
    if not isinstance(encoded, str):     # None/bytes 등 비문자열 저장값 → 크래시 대신 거부
        return False
    try:
        algo, iter_s, salt_b64, hash_b64 = encoded.split("$")
        if algo != _ALGO:
            return False
        iterations = int(iter_s)
        if iterations < 1:                 # pbkdf2_hmac 는 iterations<1 에서 ValueError → 사전 거부
            return False
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(hash_b64)
        dk = hashlib.pbkdf2_hmac("sha256", _normalize(password), salt, iterations)
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(dk, expected)


def needs_rehash(encoded: str, *, min_iterations: int = DEFAULT_ITERATIONS) -> bool:
    """저장 해시가 현행 정책(알고리즘/반복수) 미달이면 True — 로그인 성공 시 재해시 유도.

    비문자열/손상 저장값은 재해시 필요(True)로 본다 — 크래시 없이 안전한 방향으로 판정.
    """
    if not isinstance(encoded, str):
        return True
    try:
        algo, iter_s, _salt, _hash = encoded.split("$")
        return algo != _ALGO or int(iter_s) < min_iterations
    except (ValueError, TypeError):
        return True
