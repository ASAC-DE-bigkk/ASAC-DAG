"""서울 citydata(통합 도시데이터) HTTP 클라이언트 + 블록 파서 (#192).

``citydata_ppltn``(인구 블록 전용)의 상위 통합 API. 장소 1곳 응답에 인구·상권·
승하차·따릉이·날씨/대기질 등 ~20개 블록이 담긴다(실측 ~177KB). 원본 bytes 를
받아오고, 성공 판정과 **블록 단위 분해**에 필요한 최소한만 들여다본다 --
블록 내부 필드 파싱은 silver/dbt 몫(citydata_ppltn 과 동일한 schema-on-read 원칙).

시크릿(API key)은 요청 URL 경로에 포함되므로 예외 로깅 시 ``client.redact()`` 필수.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from ..common.config import redact_secret
from ..common.http import fetch
from . import config as source_config

# 추적 메타데이터 source_id -- raw path 와 bronze row 양쪽에 쓰인다.
CITYDATA_SOURCE_ID = "seoul_citydata"

# bronze 에 기본 적재하는 블록 allowlist (#192 합의 범위: 신규 가치 + 인구).
# 겹침 블록(도로/주차/도착정보/문화행사 등)은 raw(.json.gz)에만 보존 --
# 각 도메인 전용 원천이 canonical, citydata 본은 교차검증용.
DEFAULT_BRONZE_BLOCKS = (
    "LIVE_PPLTN_STTS",   # 인구 (citydata_ppltn 과 동일 블록 -- 통합안 검증용)
    "LIVE_CMRCL_STTS",   # 실시간 상권(신한카드) -- 팀 미보유
    "LIVE_SUB_PPLTN",    # 지하철 승하차 인원 -- transit 미보유
    "LIVE_BUS_PPLTN",    # 버스 승하차 인원 -- transit 미보유
    "SBIKE_STTS",        # 따릉이 현황 -- 팀 미보유
    "WEATHER_STTS",      # 날씨 실황 + 대기질(PM2.5/PM10) -- weather 는 예보만 보유
    # 팀 미보유 신규 가치(#192 원칙, 겹침 도메인 없음 확인). 재난문자·뉴스는 이벤트성(평소 0).
    "CHARGER_STTS",      # 전기차 충전소(장소별 충전소·충전기 상태) -- 아무 도메인도 미보유
    "LIVE_DST_MESSAGE",  # 긴급재난문자(재해구분·긴급단계·메시지·생성일시) -- 이벤트성
    "LIVE_YNA_NEWS",     # 연합뉴스(기사구분·제목·내용·일자·출처) -- 이벤트성
)

# CITYDATA 안에서 블록이 아닌 스칼라 메타 키.
_META_KEYS = ("AREA_NM", "AREA_CD")


@dataclass
class ParsedCitydata:
    """응답 body 에서 뽑은 최소 메타데이터 + 블록별 원본(직렬화 전 객체)."""

    area_nm: str | None
    area_cd: str | None
    blocks: dict          # {블록명: 원본 객체(dict|list)} -- 파싱 안 함
    result_code: str | None
    result_msg: str | None

    @property
    def ok(self) -> bool:
        return self.result_code in source_config.SEOUL_OK_CODES and bool(self.blocks)


def parse_citydata_body(body: bytes) -> ParsedCitydata:
    """응답 bytes 에서 결과코드와 CITYDATA 블록들을 꺼낸다(블록 내부 파싱 X)."""
    data = json.loads(body)
    meta = data.get("RESULT") or {}
    city = data.get("CITYDATA") or {}
    blocks = {k: v for k, v in city.items() if k not in _META_KEYS and v not in (None, "", [])}
    return ParsedCitydata(
        area_nm=(city.get("AREA_NM") or "").strip() or None,
        area_cd=(city.get("AREA_CD") or "").strip() or None,
        blocks=blocks,
        result_code=meta.get("RESULT.CODE") or meta.get("CODE"),
        result_msg=meta.get("RESULT.MESSAGE") or meta.get("MESSAGE"),
    )


@dataclass
class CitydataFetchResult:
    """장소 1건 조회 원본 결과."""

    area_nm: str
    status: int
    raw_body: bytes           # 전체 응답(전 블록) -- gzip 후 R2 raw 아카이브용
    parsed: ParsedCitydata

    @property
    def ok(self) -> bool:
        return self.parsed.ok


class SeoulCitydataClient:
    """서울 실시간 도시데이터 통합(citydata) 클라이언트."""

    def __init__(self, api_key: str, timeout: int = 60):
        # 응답이 커서(~177KB) ppltn(30s)보다 여유 있는 타임아웃.
        self.api_key = api_key
        self.timeout = timeout

    def build_url(self, area_nm: str) -> str:
        """⚠️ 경로에 API key 포함 -- redact 대상."""
        import urllib.parse

        key = urllib.parse.quote(self.api_key, safe="")
        area = urllib.parse.quote(area_nm, safe="")
        base = source_config.SEOUL_OPEN_API_BASE_URL.rstrip("/")
        return f"{base}/{key}/json/citydata/1/5/{area}"

    def redact(self, text: str) -> str:
        return redact_secret(text, self.api_key)

    def fetch_area(self, area_nm: str) -> CitydataFetchResult:
        result = fetch(self.build_url(area_nm), timeout=self.timeout)
        return CitydataFetchResult(
            area_nm=area_nm,
            status=result.status,
            raw_body=result.body,
            parsed=parse_citydata_body(result.body),
        )
