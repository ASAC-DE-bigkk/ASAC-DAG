"""소스 클라이언트가 공용으로 쓰는 HTTP 자료형.

전송 계층(세션/재시도/rate limit)은 #152 부터 루트 `common.http`(#78) 소관 —
여기엔 도메인 페이징 결과의 공통 형태(:class:`Page`)만 남는다.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Page:
    """받아온 원본 응답 bytes 한 페이지/윈도우 (소스 무관 공통 형태)."""

    index: int  # 1부터 시작하는 페이지(KOPIS cpage) 또는 윈도우 시작값(서울)
    body: bytes
    row_count: int
    ext: str  # "xml" | "json"
