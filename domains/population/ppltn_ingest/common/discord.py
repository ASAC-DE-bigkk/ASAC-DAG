"""Discord webhook 전송 helper (도메인 무관).

webhook URL은 환경변수 ``DISCORD_WEBHOOK_URL``에서 온다(시크릿 -- 커밋 금지).
표준 라이브러리 ``urllib``만 쓴다.
"""

from __future__ import annotations

import json
import urllib.request

from .config import load_env_file, pick

DISCORD_WEBHOOK_ENV = "DISCORD_WEBHOOK_URL"

# Discord 메시지 content 최대 길이(초과 시 잘림 방지용).
MAX_CONTENT = 1900


def webhook_url(env_file: str | None = None) -> str:
    """환경변수(+선택적 .env)에서 Discord webhook URL을 읽는다."""
    return pick(DISCORD_WEBHOOK_ENV, load_env_file(env_file))


def post_message(url: str, content: str) -> int:
    """webhook으로 텍스트 메시지를 보낸다. 성공 시 HTTP 상태(보통 204) 반환."""
    body = json.dumps({"content": content[:MAX_CONTENT]}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "ask-seoul-report/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return response.status
