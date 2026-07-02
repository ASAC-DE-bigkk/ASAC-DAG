"""security — 이식 가능한 보안 대응 패키지(시크릿 마스킹 · 입력검증 · 종합검증).

stdlib 만 사용하므로 어느 Airflow 번들의 `include/` 에 그대로 떨어뜨려 쓸 수 있다.
공개 API 는 아래 한 곳에서 모두 import 한다.

- 마스킹:   `redact`, `register_secret`, `refresh_env_secrets`, `Redactor`, `PLACEHOLDER`
- 로깅설치: `install_log_redaction`, `SecretRedactingFilter`
- 입력검증: `assert_safe_segment`, `assert_iso_date`, `is_safe_segment`, `is_iso_date`
- 종합검증: `run_security_verification`(단일 포인트), `assert_secure`, `SecurityReport`
"""
from __future__ import annotations

from security.inputs import (
    assert_iso_date, assert_safe_segment, is_iso_date, is_safe_segment,
)
from security.log_filter import (
    SecretRedactingFilter, install_log_redaction, is_log_redaction_installed,
)
from security.redaction import (
    PLACEHOLDER, Redactor, get_default_redactor, redact, refresh_env_secrets,
    register_secret,
)
from security.verify import (
    SecurityError, SecurityReport, assert_secure, run_security_verification,
)

__all__ = [
    "redact", "register_secret", "refresh_env_secrets", "Redactor", "PLACEHOLDER",
    "get_default_redactor",
    "install_log_redaction", "SecretRedactingFilter", "is_log_redaction_installed",
    "assert_safe_segment", "assert_iso_date", "is_safe_segment", "is_iso_date",
    "run_security_verification", "assert_secure", "SecurityReport", "SecurityError",
]
