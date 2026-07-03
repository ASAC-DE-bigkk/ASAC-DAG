"""원샷 부트스트랩 — `install_security()` **한 줄**로 런타임 보안 가드 전체 설치.

다른 프로젝트는 이 함수 하나만 호출하면 된다(받아쓰기 수준 이식 — docs/security/adoption.md):

    from security import install_security
    install_security()        # env 적재 직후, 엔트리포인트(DAG/스크립트/서버)당 1회

설치 내용(모두 idempotent · 실패해도 예외를 던지지 않아 엔트리포인트 기동을 막지 않음):
  1. env 시크릿 적재      — 이름 규칙(KEY/SECRET/TOKEN/…) 값들을 기본 redactor 에 등록.
  2. 로그 마스킹          — 루트/airflow 로거·핸들러에 SecretRedactingFilter.
  3. stdout/stderr 마스킹 — print()/서드파티 직접 출력 경로.
  4. 미처리 예외 훅 마스킹 — sys.excepthook / threading.excepthook 트레이스백.

이후에도 남는 수동 지점(가드가 대신 못 하는 것): 저장(at-rest) 전 redact(),
사용자 입력 경로 검증(assert_*), HTTP 는 netio 래퍼 사용 — CLAUDE.md §20 트리거 참조.
"""
from __future__ import annotations

import logging
from typing import Iterable

from security.log_filter import install_log_redaction, is_log_redaction_installed
from security.redaction import refresh_env_secrets, register_secret
from security.stdio_guard import (
    install_excepthook_redaction, install_stdout_redaction,
    is_excepthook_redaction_installed, is_stdout_redaction_installed,
)

log = logging.getLogger(__name__)


def install_security(*, logs: bool = True, stdio: bool = True, excepthooks: bool = True,
                     refresh_env: bool = True, extra_secrets: Iterable[str] = (),
                     neutralize_log_controls: bool = False) -> dict:
    """런타임 보안 가드 일괄 설치. 설치 상태 dict 반환(진단용 — 시크릿 값은 없음).

    Args:
        logs / stdio / excepthooks: 개별 가드 on/off(기본 전부 on).
        refresh_env: env 의 시크릿 값(이름 규칙)을 기본 redactor 에 (재)등록.
        extra_secrets: 이름 규칙 밖의 시크릿 값 추가 등록(예: 파일에서 읽은 토큰).
        neutralize_log_controls: 로그 msg/args 의 제어문자 무력화(CWE-117, opt-in —
            여러 줄 메시지가 한 줄로 펴지므로 기본 off. 트레이스백(exc_text)은 보존).
    """
    status: dict = {}
    try:
        if refresh_env:
            status["env_secrets_registered"] = refresh_env_secrets()
        for value in extra_secrets:
            register_secret(value)
        if logs:
            install_log_redaction(refresh_env=False,   # env 는 위에서 이미 적재
                                  neutralize_controls=neutralize_log_controls)
        if stdio:
            install_stdout_redaction()
        if excepthooks:
            install_excepthook_redaction()
    except Exception:   # 보안 설치 실패가 기동을 막지 않게 — 상태로만 노출
        log.warning("install_security 부분 실패(무시) — security_status() 로 확인 가능")
    status.update(security_status())
    return status


def security_status() -> dict:
    """가드별 설치 여부(런타임 점검·audit 에서 사용)."""
    return {
        "log_redaction": is_log_redaction_installed(),
        "stdout_redaction": is_stdout_redaction_installed(),
        "excepthook_redaction": is_excepthook_redaction_installed(),
    }


def is_security_installed() -> bool:
    """핵심 가드(로그·stdout·예외훅)가 모두 설치돼 있으면 True."""
    return all(security_status().values())
