"""stdout/stderr · 미처리 예외 훅 마스킹 — logging 을 거치지 않는 출력 경로의 시크릿 차단.

logging 필터(log_filter)는 로거를 거친 레코드만 가린다. 그런데 실무에서는
  - `print()` / 서드파티 라이브러리의 직접 stdout·stderr 출력
  - 미처리 예외(sys.excepthook / threading.excepthook)가 찍는 트레이스백
으로도 시크릿이 샌다(Airflow 는 task 의 stdout/stderr 를 로그 파일로 흡수한다).
이 모듈은 그 두 경로를 감싼다(stdlib only · idempotent · 실패해도 예외를 던지지 않음).

한계: write() 호출 1회 단위로 마스킹한다 — 시크릿이 여러 write 로 쪼개져 나오는 극단적
경우는 못 잡는다(일반적인 print/traceback 은 한 write 안에 온전히 들어온다).
"""
from __future__ import annotations

import sys
import threading
import traceback
from typing import Any

from security.redaction import Redactor, get_default_redactor

_GUARD_ATTR = "_security_redacting"


class RedactingStream:
    """텍스트 스트림 프록시 — write 시점에 마스킹 후 원본 스트림에 위임."""

    def __init__(self, target: Any, redactor: Redactor | None = None) -> None:
        self._target = target
        self._redactor = redactor or get_default_redactor()
        setattr(self, _GUARD_ATTR, True)

    def write(self, s: str) -> int:
        try:
            s = self._redactor.redact_text(s) if isinstance(s, str) else s
        except Exception:   # 마스킹 실패가 출력 자체를 막지 않게
            pass
        return self._target.write(s)

    def writelines(self, lines) -> None:
        for line in lines:
            self.write(line)

    def __getattr__(self, name: str):   # flush/close/isatty/encoding/buffer 등 위임
        return getattr(self._target, name)


def install_stdout_redaction(redactor: Redactor | None = None) -> bool:
    """sys.stdout / sys.stderr 를 마스킹 프록시로 감싼다(idempotent). 감쌌으면 True."""
    changed = False
    try:
        if not getattr(sys.stdout, _GUARD_ATTR, False):
            sys.stdout = RedactingStream(sys.stdout, redactor)
            changed = True
        if not getattr(sys.stderr, _GUARD_ATTR, False):
            sys.stderr = RedactingStream(sys.stderr, redactor)
            changed = True
    except Exception:
        pass
    return changed


def uninstall_stdout_redaction() -> None:
    """프록시 해제(주로 테스트용) — 감싸져 있을 때만 원본으로 되돌린다."""
    if getattr(sys.stdout, _GUARD_ATTR, False):
        sys.stdout = sys.stdout._target
    if getattr(sys.stderr, _GUARD_ATTR, False):
        sys.stderr = sys.stderr._target


def is_stdout_redaction_installed() -> bool:
    return bool(getattr(sys.stdout, _GUARD_ATTR, False)
                and getattr(sys.stderr, _GUARD_ATTR, False))


# ── 미처리 예외 훅 ───────────────────────────────────────────────────────────────
_orig_excepthook = None
_orig_threading_excepthook = None


def _format_redacted(redactor: Redactor, exc_type, exc, tb) -> str:
    lines = traceback.format_exception(exc_type, exc, tb)
    return redactor.redact_text("".join(lines))


def install_excepthook_redaction(redactor: Redactor | None = None) -> bool:
    """미처리 예외 트레이스백을 마스킹해 stderr 로 출력(sys/threading 훅, idempotent)."""
    global _orig_excepthook, _orig_threading_excepthook
    red = redactor or get_default_redactor()
    changed = False
    try:
        if not getattr(sys.excepthook, _GUARD_ATTR, False):
            _orig_excepthook = sys.excepthook

            def _hook(exc_type, exc, tb):
                try:
                    sys.stderr.write(_format_redacted(red, exc_type, exc, tb))
                except Exception:        # 훅 실패 시 원본 훅으로 폴백(출력은 보장)
                    _orig_excepthook(exc_type, exc, tb)

            setattr(_hook, _GUARD_ATTR, True)
            sys.excepthook = _hook
            changed = True

        if not getattr(threading.excepthook, _GUARD_ATTR, False):
            _orig_threading_excepthook = threading.excepthook

            def _thread_hook(args):
                try:
                    name = getattr(args.thread, "name", "?")
                    sys.stderr.write(f"Exception in thread {name}:\n" + _format_redacted(
                        red, args.exc_type, args.exc_value, args.exc_traceback))
                except Exception:
                    _orig_threading_excepthook(args)

            setattr(_thread_hook, _GUARD_ATTR, True)
            threading.excepthook = _thread_hook
            changed = True
    except Exception:
        pass
    return changed


def uninstall_excepthook_redaction() -> None:
    """훅 해제(주로 테스트용)."""
    global _orig_excepthook, _orig_threading_excepthook
    if getattr(sys.excepthook, _GUARD_ATTR, False) and _orig_excepthook is not None:
        sys.excepthook = _orig_excepthook
        _orig_excepthook = None
    if (getattr(threading.excepthook, _GUARD_ATTR, False)
            and _orig_threading_excepthook is not None):
        threading.excepthook = _orig_threading_excepthook
        _orig_threading_excepthook = None


def is_excepthook_redaction_installed() -> bool:
    return bool(getattr(sys.excepthook, _GUARD_ATTR, False)
                and getattr(threading.excepthook, _GUARD_ATTR, False))
