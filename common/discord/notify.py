"""공통 Discord 전송 모듈 (#161) — 전송·env·안전장치만 공통, 메시지 내용은 도메인 소유.

culture `Notifier`(domains/culture/culture_ingest/common/notify.py) 패턴의 승격판.
설계 합의(#161 댓글 통합):

* **webhook 폴백 체인** — `<DOMAIN>_DISCORD_WEBHOOK_URL`(도메인별 채널, 있으면 우선)
  → `DISCORD_WEBHOOK_URL`(공통 폴백). 도메인 채널 유지 의견(3표)과 단일 수렴
  절충안(culture)을 모두 수용한다. 마이그레이션 무중단(기존 env 그대로 동작).
* **best-effort** — 전송 실패는 삼킨다(예외 타입명만 로그). 알림 실패가 태스크
  상태·다른 콜백을 오염시키지 않는다. URL 은 어떤 로그에도 남기지 않는다.
* **전송 직전 redaction** — `refresh_env_secrets()` → `redact()` (sink.py:81 선례).
  webhook URL 은 `_SECRET_NAME_DENY`(`_URL$`)로 자동 등록에서 빠지므로
  `register_secret()` 으로 명시 등록한다(culture 검토 의견).
* **stdlib urllib** — 외부 의존성 없음. Discord 제한에 맞춰 truncate 내장
  (title 256 / description 4096 / footer 2048).
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request

from common.security import redact, refresh_env_secrets, register_secret

LOGGER = logging.getLogger(__name__)

# 표준 색 (culture COLOR_PASS/FAIL 승격 + WARN 추가)
COLOR_OK = 3066993     # 0x2ECC71 green
COLOR_FAIL = 15158332  # 0xE74C3C red
COLOR_WARN = 15844367  # 0xF1C40F yellow

FALLBACK_WEBHOOK_ENV = "DISCORD_WEBHOOK_URL"

# Discord 필드 길이 제한 (https://discord.com/developers/docs/resources/webhook)
_MAX_TITLE = 256
_MAX_DESCRIPTION = 4096
_MAX_FOOTER = 2048
_MAX_CONTENT = 2000

_TIMEOUT_SECONDS = 5.0


def resolve_webhook(domain: str | None = None, *, env: dict | None = None) -> str:
    """도메인별 env 우선, 없으면 공통 `DISCORD_WEBHOOK_URL` 폴백. 둘 다 없으면 ""."""
    environ = os.environ if env is None else env
    if domain:
        domain_env = f"{domain.upper()}_DISCORD_WEBHOOK_URL"
        url = (environ.get(domain_env) or "").strip()
        if url:
            return url
    return (environ.get(FALLBACK_WEBHOOK_ENV) or "").strip()


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _post(webhook: str, payload: dict) -> bool:
    """payload 를 webhook 으로 POST. 실패는 삼키고 False (URL 비로그)."""
    register_secret(webhook)  # 전송 실패 로그 등 어떤 경로로도 URL 이 안 새게 명시 등록
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        webhook,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS):
            pass
        return True
    except Exception as exc:  # noqa: BLE001 -- best-effort: 알림 실패가 run 을 못 막게
        LOGGER.warning("[discord] 전송 실패(무시): %s", type(exc).__name__)
        return False


def send_embed(title: str, description: str, *, color: int = COLOR_OK,
               footer: str | None = None, domain: str | None = None,
               webhook: str | None = None) -> bool:
    """embed 1건 전송. webhook 미해석(env 미설정) 시 조용히 스킵하고 False.

    반환값은 "전송 성공 여부" — 호출측이 분기하라는 뜻이 아니라 테스트/로그용이다.
    """
    url = webhook or resolve_webhook(domain)
    if not url:
        LOGGER.info("[discord] webhook 미설정 — 전송 스킵: %s", _truncate(title, 80))
        return False
    refresh_env_secrets()
    embed: dict = {
        "title": _truncate(redact(title), _MAX_TITLE),
        "description": _truncate(redact(description), _MAX_DESCRIPTION),
        "color": color,
    }
    if footer:
        embed["footer"] = {"text": _truncate(redact(footer), _MAX_FOOTER)}
    return _post(url, {"embeds": [embed]})


def send_text(content: str, *, domain: str | None = None,
              webhook: str | None = None) -> bool:
    """평문 메시지 1건 전송 (population 일일 리포트 등 기존 평문 사용처 이전용)."""
    url = webhook or resolve_webhook(domain)
    if not url:
        LOGGER.info("[discord] webhook 미설정 — 전송 스킵")
        return False
    refresh_env_secrets()
    return _post(url, {"content": _truncate(redact(content), _MAX_CONTENT)})
