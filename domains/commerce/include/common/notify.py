"""알림 인터페이스 — 예외 발생 시 **로그 내용을 알림 메시지로 전송**할 수 있는 추상 인터페이스.

⚠️ 아직 **실제 전송은 하지 않는다**(기본 `NoopNotifier`). 또한 파이프라인에 아직 와이어링하지
않았다 — 인터페이스만 제공한다. Slack/Email/Webhook 등 구현체를 나중에 추가하고, 그때
`set_notifier()` 로 주입(또는 `COMMERCE_NOTIFIER` 같은 env 로 팩토리 분기)하면 된다.

사용(향후, 예):
    from common.notify import notify_exception
    try:
        ...수집...
    except Exception as exc:
        notify_exception(exc, where="ingest_one:general_restaurant")  # 기본은 no-op
        raise

시크릿(SEOUL_API_KEY_COMM/R2 토큰)은 메시지에 넣지 않는다 — 호출측에서 마스킹/제외(CLAUDE.md §2.5).
"""
from __future__ import annotations

import logging
import traceback
from abc import ABC, abstractmethod

from security import redact   # 외부 전송 전 메시지/컨텍스트의 시크릿 마스킹

log = logging.getLogger(__name__)

LEVELS = ("info", "warning", "error", "critical")


class Notifier(ABC):
    """알림 채널 추상 인터페이스. 구현체는 send() 하나만 채우면 된다."""

    @abstractmethod
    def send(self, *, subject: str, message: str, level: str = "error",
             context: dict | None = None) -> None:
        """알림 1건 전송. level ∈ LEVELS. context 는 부가 메타(dag_id/run_id/short 등)."""


class NoopNotifier(Notifier):
    """기본 구현 — **실제 전송 안 함**(아직 실행 X). 로그만 남긴다."""

    def send(self, *, subject: str, message: str, level: str = "error",
             context: dict | None = None) -> None:
        log.info("[notify:noop] level=%s subject=%s context=%s (전송 비활성 — 인터페이스만)",
                 level, subject, context or {})


# 향후 구현 예시(미사용 — 참고용):
#
# class WebhookNotifier(Notifier):
#     def __init__(self, url: str): self.url = url
#     def send(self, *, subject, message, level="error", context=None):
#         import requests
#         requests.post(self.url, json={"subject": subject, "text": message,
#                                       "level": level, "context": context or {}}, timeout=10)


_notifier: Notifier | None = None


def get_notifier() -> Notifier:
    """현재 알림 채널. 미설정이면 NoopNotifier(전송 안 함)."""
    global _notifier
    if _notifier is None:
        _notifier = NoopNotifier()
    return _notifier


def set_notifier(notifier: Notifier) -> None:
    """알림 채널 주입(예: 운영에서 WebhookNotifier 로 교체)."""
    global _notifier
    _notifier = notifier


def notify_exception(exc: BaseException, *, where: str, context: dict | None = None,
                     log_tail: str | None = None) -> None:
    """예외 + (선택)로그 꼬리를 알림으로 전송. 기본은 no-op이라 안전하게 호출 가능.

    Args:
        exc: 발생한 예외.
        where: 발생 위치 라벨(예: "ingest_one:general_restaurant").
        context: 부가 메타(dag_id/run_id/bronze_run_id/short 등). 시크릿 금지.
        log_tail: 알림에 덧붙일 최근 로그 텍스트(선택).
    """
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    message = f"where={where}\nerror={exc}\n\n{tb}"
    if log_tail:
        message += f"\n--- log tail ---\n{log_tail}"
    message = redact(message)               # 외부 채널로 나가기 전 마스킹(§2.5)
    safe_context = redact(context or {})
    try:
        get_notifier().send(subject=f"[commerce] 예외: {where}", message=message,
                            level="error", context=safe_context)
    except Exception:  # 알림 실패가 본 파이프라인을 막지 않게(best-effort)
        log.exception("notify_exception 전송 실패(무시): where=%s", where)
