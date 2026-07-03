"""로깅 시크릿 마스킹 — 모든 로그 레코드(메시지/인자/트레이스백)를 출력 전에 마스킹한다.

Python logging 주의점: 필터를 *로거* 에 달면 그 로거로 *직접* 들어온 레코드만 거른다(자식
로거에서 전파된 레코드는 안 거름). 반면 *핸들러* 에 달면 그 핸들러에 도달하는 모든 레코드를
거른다. 그래서 `install_log_redaction()` 은 루트/airflow 로거 **및 그 핸들러들**에 모두 단다.

한계: install 이후 새로 추가되는 핸들러에는 자동으로 안 붙는다. 그래서 진짜 위험한 곳
(예외→마커 저장 등)은 호출측에서 redact()로도 가린다(이중 방어). docs/security/security.md 참고.
"""
from __future__ import annotations

import logging
from typing import Iterable

from security.redaction import (
    Redactor, get_default_redactor, refresh_env_secrets, sanitize_log_value,
)

_FILTER_ATTR = "_commerce_secret_redactor"
_SCRUBBED_ATTR = "_commerce_scrubbed"      # 레코드 1회 처리 표식(로거+핸들러 이중 통과 방지)
_DEFAULT_LOGGER_NAMES = ("", "airflow", "airflow.task", "airflow.processor")


class SecretRedactingFilter(logging.Filter):
    """레코드의 msg/args/exc 를 마스킹하는 logging.Filter. 항상 True(레코드는 통과).

    neutralize_controls=True(opt-in)면 msg/args 의 제어문자도 무력화한다(CWE-117 —
    위조 로그라인/ANSI 이스케이프 차단). 단 여러 줄 메시지가 한 줄로 펴지므로,
    exc_info 트레이스백을 직접 메시지로 찍는 핸들러에는 켜지 않는 것을 권장
    (exc_text 는 건드리지 않는다 — 트레이스백 줄바꿈은 보존).
    """

    def __init__(self, redactor: Redactor | None = None, *,
                 neutralize_controls: bool = False) -> None:
        super().__init__()
        self.redactor = redactor or get_default_redactor()
        self.neutralize_controls = neutralize_controls

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003 (logging API)
        # 같은 필터 인스턴스가 로거+핸들러 양쪽에 달려 레코드가 두 번 통과할 수 있다.
        # 센티넬로 1회만 처리(중복 마스킹은 무해하나 sanitize 는 백슬래시를 중복 이스케이프함).
        if getattr(record, _SCRUBBED_ATTR, False):
            return True
        try:
            if record.args:
                # record.msg 는 아직 %-치환 전 **포맷 문자열**이다. 여기에 마스킹을 걸면
                # `secret=%s` 의 `%s` 가 소비돼 이후 포매팅이 깨진다(로그 라인 유실).
                # → 먼저 완전히 렌더한 뒤 마스킹하고 args 를 비운다(핸들러 재치환 방지).
                rendered = record.getMessage()
                record.msg = self._finish(self.redactor.redact_text(rendered))
                record.args = ()
            elif isinstance(record.msg, str):
                record.msg = self._finish(self.redactor.redact_text(record.msg))
            if record.exc_info or record.exc_text:
                # 포맷된 트레이스백을 마스킹해 exc_text 로 캐시 → 포매터가 이 값을 쓴다.
                record.exc_text = self.redactor.redact_text(self._format_exc(record))
        except Exception:   # 마스킹 실패가 로깅 자체를 막지 않게(로그는 흘려보냄)
            pass
        setattr(record, _SCRUBBED_ATTR, True)
        return True

    def _finish(self, text: str) -> str:
        return sanitize_log_value(text) if self.neutralize_controls else text

    def _format_exc(self, record: logging.LogRecord) -> str:
        if record.exc_text:
            return record.exc_text
        return logging.Formatter().formatException(record.exc_info)


def _attach(target: logging.Logger | logging.Handler, flt: SecretRedactingFilter) -> bool:
    """필터를 idempotent 하게 부착(이미 있으면 skip). 부착했으면 True."""
    if any(getattr(f, _FILTER_ATTR, False) for f in target.filters):
        return False
    target.addFilter(flt)
    return True


def install_log_redaction(redactor: Redactor | None = None, *,
                          logger_names: Iterable[str] = _DEFAULT_LOGGER_NAMES,
                          refresh_env: bool = True,
                          neutralize_controls: bool = False) -> SecretRedactingFilter:
    """로깅 마스킹 설치 — 지정 로거 + 그 핸들러 + 루트 핸들러에 필터를 단다(idempotent).

    DAG 임포트 시 `load_commerce_env()` 직후 1회 호출하면, 이후 모든 commerce 로그에서
    시크릿이 자동 마스킹된다. 실패해도 예외를 던지지 않는다(DAG 임포트를 막지 않음).
    neutralize_controls=True 면 msg/args 의 제어문자도 무력화(CWE-117, opt-in).
    """
    red = redactor or get_default_redactor()
    if refresh_env:
        red.load_env_secrets()
    flt = SecretRedactingFilter(red, neutralize_controls=neutralize_controls)
    setattr(flt, _FILTER_ATTR, True)
    try:
        seen_handlers: set[int] = set()
        for name in logger_names:
            lg = logging.getLogger(name)
            _attach(lg, flt)
            for h in lg.handlers:
                if id(h) not in seen_handlers:
                    _attach(h, flt)
                    seen_handlers.add(id(h))
        for h in logging.getLogger().handlers:   # 루트 핸들러도 보강
            if id(h) not in seen_handlers:
                _attach(h, flt)
                seen_handlers.add(id(h))
    except Exception:
        logging.getLogger(__name__).warning("install_log_redaction 부분 실패(무시)")
    return flt


def is_log_redaction_installed(logger_names: Iterable[str] = _DEFAULT_LOGGER_NAMES) -> bool:
    """지정 로거 중 하나라도 마스킹 필터가 달려 있으면 True(검증용)."""
    for name in logger_names:
        lg = logging.getLogger(name)
        if any(getattr(f, _FILTER_ATTR, False) for f in lg.filters):
            return True
        if any(getattr(f, _FILTER_ATTR, False) for h in lg.handlers for f in h.filters):
            return True
    return False
