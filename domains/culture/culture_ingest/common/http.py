"""소스 클라이언트가 공용으로 쓰는 HTTP 헬퍼."""

from __future__ import annotations

from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_session(total_retries: int = 3, backoff: float = 0.5) -> requests.Session:
    """일시적 HTTP 오류에 재시도/백오프가 걸린 requests 세션을 만든다."""
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


@dataclass
class Page:
    """받아온 원본 응답 bytes 한 페이지/윈도우 (소스 무관 공통 형태)."""

    index: int  # 1부터 시작하는 페이지(KOPIS cpage) 또는 윈도우 시작값(서울)
    body: bytes
    row_count: int
    ext: str  # "xml" | "json"
