"""소스 클라이언트 공용 HTTP 헬퍼.

낮은 수준의 GET 실행과 timeout 기본값만 제공한다(이슈 #16의 얇은 ``common/http``).
URL 생성·성공 판정·파싱은 각 소스(``source``)의 몫이다.

표준 라이브러리 ``urllib``만 쓴다 -- 메인 Airflow 이미지에 ``requests``가 없을 수
있으므로 의존성을 늘리지 않는다. 시크릿이 URL 경로에 포함되는 소스가 있으므로,
예외 메시지는 항상 호출자가 ``redact_secret``으로 마스킹해서 로그에 남긴다.
"""

from __future__ import annotations

import urllib.request
from dataclasses import dataclass

DEFAULT_TIMEOUT = 30
DEFAULT_USER_AGENT = "ask-seoul-bronze/1.0"


@dataclass(frozen=True)
class HttpResult:
    """단일 HTTP 응답의 원본 결과 (파싱 전)."""

    status: int
    body: bytes


def fetch(url: str, *, timeout: int = DEFAULT_TIMEOUT, user_agent: str = DEFAULT_USER_AGENT) -> HttpResult:
    """URL 1건을 GET해 상태코드와 원본 bytes를 반환한다(파싱 없음).

    ⚠️ ``url``에 시크릿이 포함될 수 있으므로, 호출자는 예외를 잡을 때
    ``redact_secret``으로 마스킹한 메시지만 로그/메타데이터에 남겨야 한다.
    """
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return HttpResult(status=response.status, body=response.read())
