"""security — 이식 가능한 **통합 보안 플러그인**(시크릿 마스킹 · IO 가드 · 입력검증 · 종합검증).

stdlib 만 사용하므로 어느 프로젝트(Airflow 번들/서버/스크립트)의 import 루트에 그대로
떨어뜨려 쓸 수 있다. 원샷 설치 한 줄이면 런타임 가드(로그·stdout·예외훅)가 전부 켜진다:

    from security import install_security
    install_security()        # env 적재 직후, 엔트리포인트당 1회

공개 API 는 아래 한 곳에서 모두 import 한다.

- 부트스트랩: `install_security`, `security_status`, `is_security_installed`
- 마스킹:     `redact`, `scrub_exception`, `register_secret`, `refresh_env_secrets`,
              `Redactor`, `PLACEHOLDER`
- 로깅설치:   `install_log_redaction`, `SecretRedactingFilter`
- stdio/훅:   `install_stdout_redaction`, `install_excepthook_redaction`
- 입력검증:   `assert_safe_segment`, `assert_iso_date`, `is_safe_segment`, `is_iso_date`
- network IO: `http_request`, `http_get`, `http_post`, `safe_url` (모듈: security.netio)
- file IO:    `safe_key`, `safe_join`, `write_json_redacted`, `write_text_redacted`
- API 가드:   `api_receipt`, `response_summary`, `scrub_url`, `scrub_headers`, `scrub_params`
- 이벤트:     `log_event`, `log_exception`, `event_record`, `exception_record`
- 종합검증:   `run_security_verification`(단일 포인트), `assert_secure`, `SecurityReport`
"""
from __future__ import annotations

from security.api_guard import (
    api_receipt, response_summary, scrub_headers, scrub_params, scrub_url,
)
from security.archive import UnsafeArchiveError, safe_extract_tar, safe_extract_zip
from security.bootstrap import install_security, is_security_installed, security_status
from security.crypto import (
    constant_time_equals, generate_hex_token, generate_token, hash_password,
    needs_rehash, verify_password,
)
from security.events import event_record, exception_record, log_event, log_exception
from security.fileio import safe_join, safe_key, write_json_redacted, write_text_redacted
from security.inputs import (
    assert_iso_date, assert_safe_segment, is_iso_date, is_safe_segment,
)
from security.log_filter import (
    SecretRedactingFilter, install_log_redaction, is_log_redaction_installed,
)
from security.netio import (
    InsecureRequestBlocked, ResponseTooLarge, UnsafeURLBlocked, assert_url_allowed,
    http_get, http_post, http_request, is_url_allowed, safe_url,
)
from security.redaction import (
    PLACEHOLDER, Redactor, get_default_redactor, redact, refresh_env_secrets,
    register_secret, sanitize_log_value, scrub_exception,
)
from security.stdio_guard import (
    install_excepthook_redaction, install_stdout_redaction,
    is_excepthook_redaction_installed, is_stdout_redaction_installed,
)
from security.verify import (
    SecurityError, SecurityReport, assert_secure, run_security_verification,
)

__all__ = [
    # 부트스트랩(원샷)
    "install_security", "security_status", "is_security_installed",
    # 마스킹 · 로그 인젝션 무력화
    "redact", "scrub_exception", "register_secret", "refresh_env_secrets",
    "sanitize_log_value", "Redactor", "PLACEHOLDER", "get_default_redactor",
    # 로깅/스트림/훅 가드
    "install_log_redaction", "SecretRedactingFilter", "is_log_redaction_installed",
    "install_stdout_redaction", "is_stdout_redaction_installed",
    "install_excepthook_redaction", "is_excepthook_redaction_installed",
    # 입력검증
    "assert_safe_segment", "assert_iso_date", "is_safe_segment", "is_iso_date",
    # network IO (+SSRF 가드 · 응답 상한)
    "http_request", "http_get", "http_post", "safe_url",
    "assert_url_allowed", "is_url_allowed",
    "InsecureRequestBlocked", "UnsafeURLBlocked", "ResponseTooLarge",
    # file IO · 아카이브
    "safe_key", "safe_join", "write_json_redacted", "write_text_redacted",
    "safe_extract_zip", "safe_extract_tar", "UnsafeArchiveError",
    # API 가드
    "api_receipt", "response_summary", "scrub_url", "scrub_headers", "scrub_params",
    # 이벤트(분석 가능 로그)
    "log_event", "log_exception", "event_record", "exception_record",
    # 암호 유틸(CSPRNG/상수시간/비밀번호)
    "generate_token", "generate_hex_token", "constant_time_equals",
    "hash_password", "verify_password", "needs_rehash",
    # 종합검증
    "run_security_verification", "assert_secure", "SecurityReport", "SecurityError",
]
