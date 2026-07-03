"""서울 열린데이터광장 공용 어댑터 — 5개 도메인이 공유하는 가장 큰 중복 (#78 Q3 합의).

HttpCore 를 **상속하지 않고 주입**받아 쓴다(합성). 키는 env 에서 로드해
서비스/도메인별로 생성 시 지정한다(Exisign 보완: 서비스별 key 분리 —
인스턴스를 키별로 만들거나 fetch 호출에 key override 를 준다).

URL 규약: http://openapi.seoul.go.kr:8088/<KEY>/<json|xml>/<SERVICE>/<start>/<end>/
응답 파싱(JSON envelope/XML)은 도메인 소관 — 이 어댑터는 bytes/JSON dict 까지만.
"""
from __future__ import annotations

import json
import os
from typing import Any

from common.http.auth import PathKey
from common.http.contract import TransportResponse
from common.http.core import HttpCore

DEFAULT_BASE_URL = "http://openapi.seoul.go.kr:8088"
SOURCE = "seoul_openapi"


def _env_base_url() -> str:
    """base URL 해석: 인자 > env > 코드 안전 기본값(값이 없어도 깨지지 않게).

    env 이름이 도메인마다 갈라져 있어 둘 다 읽는다 — 루트 .env 는
    SEOUL_OPEN_API_BASE_URL(population), commerce 번들은 SEOUL_OPENAPI_BASE_URL.
    이름 통일은 후속 과제(기획 문서 '열어둔 질문' 참고).
    """
    return (os.environ.get("SEOUL_OPEN_API_BASE_URL")
            or os.environ.get("SEOUL_OPENAPI_BASE_URL")
            or DEFAULT_BASE_URL)


class SeoulOpenApiClient:
    def __init__(self, core: HttpCore, key: str, *,
                 base_url: str | None = None) -> None:
        self._core = core                                  # 합성 — 부모 아님
        self._key = key
        self._base_url = (base_url or _env_base_url()).rstrip("/")

    def url_for(self, service: str, start: int, end: int, *, fmt: str = "json") -> str:
        """{api_key} 자리표시자를 남긴 템플릿 — 실제 키 치환은 PathKey(요청 직전)가 한다."""
        return f"{self._base_url}/{{api_key}}/{fmt}/{service}/{start}/{end}/"

    def fetch_bytes(self, service: str, start: int, end: int, *, fmt: str = "json",
                    key: str | None = None) -> TransportResponse:
        """1페이지 원본 응답. key 로 호출 단위 키 override 가능(서비스별 키 분리)."""
        return self._core.get(
            self.url_for(service, start, end, fmt=fmt),
            auth=PathKey(key or self._key, encode=False))  # 서울 키는 URL-safe 40자

    def fetch_json(self, service: str, start: int, end: int, *,
                   key: str | None = None) -> dict[str, Any]:
        response = self.fetch_bytes(service, start, end, fmt="json", key=key)
        return json.loads(response.content.decode("utf-8"))
