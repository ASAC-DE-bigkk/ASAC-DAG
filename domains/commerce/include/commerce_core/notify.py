"""알림 인터페이스 — 예외/완료 이벤트를 알림 채널로 전송하는 추상 인터페이스 + 웹훅 구현체.

기본은 `NoopNotifier`(전송 안 함, 로그만). env `COMMERCE_NOTIFY_WEBHOOK_URL` 을 설정하면
`WebhookNotifier` 로 자동 전환된다(`COMMERCE_NOTIFY_WEBHOOK_KIND=discord|generic`, 기본 discord).
테스트/특수 채널은 `set_notifier()` 주입으로 교체.

사용:
    from commerce_core.notify import notify_completion, notify_exception
    notify_completion(where="enrich_fill_jibun", summary={...}, unresolved=[...])  # 성공/완료 알림
    try:
        ...수집...
    except Exception as exc:
        notify_exception(exc, where="ingest_one:general_restaurant")
        raise

시크릿(SEOUL_API_KEY_COMM/R2 토큰)은 메시지에 넣지 않는다 — 전송 직전 redact() 이중 방어
+ 웹훅 URL 자체(토큰 포함)는 register_secret 으로 마스킹 등록(CLAUDE.md §2.5).
"""
from __future__ import annotations

import json
import logging
import os
import traceback
from abc import ABC, abstractmethod

from security import redact, register_secret   # 외부 전송 전 시크릿 마스킹 + URL 시크릿 등록
from security.netio import http_post           # timeout 주입·TLS 강제·예외 마스킹

log = logging.getLogger(__name__)

LEVELS = ("info", "warning", "error", "critical")
_DISCORD_CONTENT_LIMIT = 1800   # discord content 2000자 제한 — 여유 두고 절단


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


class WebhookNotifier(Notifier):
    """웹훅 전송 구현체 — kind='discord'(content 포맷) 또는 'generic'(JSON 통짜).

    URL 은 토큰을 포함하므로 생성 시 register_secret 으로 마스킹 등록하고 절대 로그하지 않는다.
    전송은 보안 래퍼(http_post: timeout·TLS 강제·예외 마스킹)로만 나간다.
    """

    def __init__(self, url: str, *, kind: str = "discord", timeout: float = 10.0) -> None:
        self.url = url
        self.kind = kind if kind in ("discord", "generic") else "discord"
        self.timeout = timeout
        register_secret(url)          # 웹훅 URL(토큰 포함)이 로그/예외에 새지 않게

    def send(self, *, subject: str, message: str, level: str = "error",
             context: dict | None = None) -> None:
        if self.kind == "discord":
            body = f"**{subject}** [{level}]\n{message}"
            if context:
                body += f"\ncontext: {json.dumps(context, ensure_ascii=False, default=str)}"
            payload = {"content": redact(body)[:_DISCORD_CONTENT_LIMIT]}
        else:
            payload = {"subject": redact(subject), "text": redact(message),
                       "level": level, "context": redact(context or {})}
        resp = http_post(self.url, json=payload, timeout=self.timeout)
        resp.raise_for_status()


_notifier: Notifier | None = None


def _from_env() -> Notifier:
    """env 팩토리 — COMMERCE_NOTIFY_WEBHOOK_URL 이 있으면 웹훅, 없으면 no-op."""
    url = os.getenv("COMMERCE_NOTIFY_WEBHOOK_URL", "").strip()
    if url:
        return WebhookNotifier(url, kind=os.getenv("COMMERCE_NOTIFY_WEBHOOK_KIND", "discord").strip() or "discord")
    return NoopNotifier()


def get_notifier() -> Notifier:
    """현재 알림 채널. 명시 주입 > env(COMMERCE_NOTIFY_WEBHOOK_URL) > NoopNotifier."""
    global _notifier
    if _notifier is None:
        _notifier = _from_env()
    return _notifier


def set_notifier(notifier: Notifier | None) -> None:
    """알림 채널 주입(테스트/특수 채널). None 이면 다음 호출 때 env 로 재결정."""
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


def notify_completion(*, where: str, summary: dict, unresolved: list[dict] | None = None,
                      level: str | None = None) -> None:
    """성공/완료 알림 — 처리 결과 값(summary)과 미해결 샘플을 알림 채널로 전송(best-effort).

    level 미지정 시 자동: 미해결(unresolved)이 있으면 warning, 없으면 info(성공/완료).
    unresolved 는 [{"road_address_norm": ..., "status": ..., "rows": ...}] 형태 권장 —
    메시지에는 최대 10건만 싣고 전체 규모는 summary 카운트로 전달한다.
    """
    unresolved = unresolved or []
    lvl = level or ("warning" if unresolved else "info")
    lines = [f"{k}={v}" for k, v in summary.items()
             if k not in ("ts", "level", "event", "where")]
    message = "처리 결과: " + " · ".join(lines)
    if unresolved:
        message += f"\n미해결 {len(unresolved)}건 (상위 10):"
        for u in unresolved[:10]:
            message += (f"\n- [{u.get('status', '?')}] {u.get('road_address_norm', '?')}"
                        f" (rows={u.get('rows', '?')})")
    try:
        get_notifier().send(subject=f"[commerce] 완료: {where}", message=redact(message),
                            level=lvl, context=redact({"where": where}))
    except Exception:  # 알림 실패가 본 파이프라인을 막지 않게(best-effort)
        log.exception("notify_completion 전송 실패(무시): where=%s", where)


def notify_quality_event(*, task: str, level: str, title: str, description: str,
                         metrics: dict, context: dict | None = None) -> None:
    """사전 인지 품질 이슈를 [작업>레벨] 단위로 묶어 알림 채널로 전송한다."""
    lvl = level if level in LEVELS else "warning"
    metric_lines = [f"- {k}={v}" for k, v in metrics.items()]
    message = (
        f"작업: {task}\n"
        f"레벨: {lvl}\n"
        f"설명: {description}\n\n"
        "지표:\n" + "\n".join(metric_lines)
    )
    safe_context = redact({"task": task, **(context or {})})
    try:
        get_notifier().send(
            subject=f"[commerce][{task}>{lvl}] {title}",
            message=redact(message),
            level=lvl,
            context=safe_context,
        )
    except Exception:  # 알림 실패가 본 파이프라인을 막지 않게(best-effort)
        log.exception("notify_quality_event 전송 실패(무시): task=%s level=%s", task, lvl)
