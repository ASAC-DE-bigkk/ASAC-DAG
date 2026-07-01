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

from security.redaction import Redactor, get_default_redactor, refresh_env_secrets

_FILTER_ATTR = "_commerce_secret_redactor"
_DEFAULT_LOGGER_NAMES = ("", "airflow", "airflow.task", "airflow.processor")


class SecretRedactingFilter(logging.Filter):
    """레코드의 msg/args/exc 를 마스킹하는 logging.Filter. 항상 True(레코드는 통과)."""

    def __init__(self, redactor: Redactor | None = None) -> None:
        super().__init__()
        self.redactor = redactor or get_default_redactor()

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003 (logging API)
        try:
            if isinstance(record.msg, str):
                record.msg = self.redactor.redact_text(record.msg)
            if record.args:
                if isinstance(record.args, dict):
                    record.args = {k: self._scrub(v) for k, v in record.args.items()}
                else:
                    record.args = tuple(self._scrub(a) for a in record.args)
            if record.exc_info:
                # 포맷된 트레이스백을 마스킹해 exc_text 로 캐시 → 포매터가 이 값을 쓴다.
                record.exc_text = self.redactor.redact_text(self._format_exc(record))
        except Exception:   # 마스킹 실패가 로깅 자체를 막지 않게(로그는 흘려보냄)
            pass
        return True

    def _scrub(self, value):
        if isinstance(value, str):
            return self.redactor.redact_text(value)
        if isinstance(value, (dict, list, tuple)):
            return self.redactor.redact(value)
        return value

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
                          refresh_env: bool = True) -> SecretRedactingFilter:
    """로깅 마스킹 설치 — 지정 로거 + 그 핸들러 + 루트 핸들러에 필터를 단다(idempotent).

    DAG 임포트 시 `load_commerce_env()` 직후 1회 호출하면, 이후 모든 commerce 로그에서
    시크릿이 자동 마스킹된다. 실패해도 예외를 던지지 않는다(DAG 임포트를 막지 않음).
    """
    red = redactor or get_default_redactor()
    if refresh_env:
        red.load_env_secrets()
    flt = SecretRedactingFilter(red)
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
