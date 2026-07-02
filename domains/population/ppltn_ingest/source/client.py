"""서울 citydata_ppltn HTTP 클라이언트.

원본 응답 bytes를 *받아오고*, 페이징/성공 판정에 필요한 최소한만 응답을 들여다본다
-- 업무 필드(area_nm, congest_lvl 등)는 파싱하지 않는다(파싱은 silver/dbt 몫).

시크릿(API key)은 요청 URL 경로에 포함되므로, 예외를 로그로 남길 때는 반드시
``client.redact(...)`` 로 마스킹한다(이슈 #16: secret은 log/path/metadata에 원문 금지).
"""

from __future__ import annotations

import json
import urllib.parse
from dataclasses import dataclass

from ..common.config import redact_secret
from ..common.http import fetch
from . import config as source_config


@dataclass
class FetchResult:
    """장소 1건 조회 원본 결과 (필드 분해 없음)."""

    area_nm: str
    status: int
    raw_body: bytes          # 전체 응답(래퍼 포함) -- R2 raw 아카이브용
    record: dict | None      # citydata_ppltn[0] 레코드 -- bronze payload용
    result_code: str | None
    result_msg: str | None
    row_count: int

    @property
    def ok(self) -> bool:
        """소스 성공 기준: 정상 코드 + 레코드 1건 이상."""
        return self.result_code in source_config.SEOUL_OK_CODES and self.record is not None


class SeoulPpltnClient:
    """서울 실시간 도시데이터 인구혼잡도(citydata_ppltn) 클라이언트."""

    def __init__(self, api_key: str, timeout: int = 30):
        self.api_key = api_key
        self.timeout = timeout

    def build_url(self, area_nm: str) -> str:
        """장소명을 경로에 넣은 요청 URL. ⚠️ 경로에 API key 포함 -- redact 대상."""
        key = urllib.parse.quote(self.api_key, safe="")
        area = urllib.parse.quote(area_nm, safe="")
        base = source_config.SEOUL_OPEN_API_BASE_URL.rstrip("/")
        return f"{base}/{key}/json/citydata_ppltn/1/5/{area}"

    def redact(self, text: str) -> str:
        """메시지에서 API key를 마스킹한다(예외 로깅용)."""
        return redact_secret(text, self.api_key)

    def fetch_area(self, area_nm: str) -> FetchResult:
        """장소 1건을 조회해 원본 bytes + 최소 메타데이터를 반환한다(파싱X)."""
        result = fetch(self.build_url(area_nm), timeout=self.timeout)
        data = json.loads(result.body)
        container = data.get(source_config.PPLTN_CONTAINER_KEY)
        meta = data.get("RESULT") or {}
        code = meta.get("RESULT.CODE") or meta.get("CODE")
        msg = meta.get("RESULT.MESSAGE") or meta.get("MESSAGE")
        rows = container if isinstance(container, list) else []
        record = rows[0] if rows else None
        return FetchResult(
            area_nm=area_nm,
            status=result.status,
            raw_body=result.body,
            record=record,
            result_code=code,
            result_msg=msg,
            row_count=len(rows),
        )
