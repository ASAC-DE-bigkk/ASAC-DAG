"""culture raw 적재 완료 Discord 알림.

`report` 태스크의 run_report 를 Discord 임베드로 포맷(build_report_payload)하고
webhook 으로 전송(DiscordWebhookNotifier)한다. env CULTURE_DISCORD_WEBHOOK_URL 이 없으면
NoopNotifier(전송 안 함) 라 코드만으로 안전하게 머지된다.

시크릿(웹훅 URL)은 메시지·로그에 절대 넣지 않는다. 전송 실패는 삼켜 파이프라인을 막지 않는다.
"""
from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

from culture_ingest.source.datasets import BY_NAME

log = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
WEBHOOK_ENV = "CULTURE_DISCORD_WEBHOOK_URL"
COLOR_PASS = 3066993   # 0x2ECC71
COLOR_FAIL = 15158332  # 0xE74C3C


class Notifier(ABC):
    @abstractmethod
    def send(self, payload: dict) -> None:
        """Discord webhook payload 1건 전송."""


class NoopNotifier(Notifier):
    def send(self, payload: dict) -> None:
        title = ((payload.get("embeds") or [{}])[0]).get("title", "")
        log.info("[notify:noop] %s (전송 비활성 — URL 미설정)", title)


class DiscordWebhookNotifier(Notifier):
    def __init__(self, url: str, timeout: float = 10.0):
        self._url = url
        self._timeout = timeout

    def send(self, payload: dict) -> None:
        try:
            requests.post(self._url, json=payload, timeout=self._timeout)
        except Exception:  # noqa: BLE001 -- best-effort. URL 로그 금지.
            log.exception("[notify] Discord 전송 실패(무시)")


def notifier_from_env(env: dict | None = None) -> Notifier:
    env = os.environ if env is None else env
    url = (env.get(WEBHOOK_ENV) or "").strip()
    return DiscordWebhookNotifier(url) if url else NoopNotifier()
