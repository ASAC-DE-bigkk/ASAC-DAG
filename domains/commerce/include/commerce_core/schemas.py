"""공통 스키마/상수 — bronze·silver 가 공유.

LOCALDATA 인허가 표준의 **공통 19컬럼**(도메인 5종 실호출 교집합으로 검증).
silver 정규화는 이 컬럼을 기준 스키마로 삼고, 그 외는 optional 로 둔다.
자세한 근거: docs/pipeline/common_info.md
"""
from __future__ import annotations

from dataclasses import dataclass

# 저장 경로 안정성을 위한 도메인 상수(범용 DOMAIN env 와 독립)
DOMAIN = "commerce"
SOURCE_SYSTEM = "seoul_open_data_plaza"

# 모든 인허가 API 공통(검증된 19컬럼) — silver 정규화 기준 스키마
COMMON_COLUMNS: tuple[str, ...] = (
    "OPNSFTEAMCODE", "MGTNO", "BPLCNM",
    "APVPERMYMD", "DCBYMD",
    "TRDSTATEGBN", "TRDSTATENM", "DTLSTATEGBN", "DTLSTATENM",
    "SITETEL", "SITEWHLADDR", "RDNWHLADDR", "SITEPOSTNO", "RDNPOSTNO",
    "LASTMODTS", "UPDATEGBN", "UPDATEDT",
    "X", "Y",
)

# 대부분 제공하나 일부 업종군 누락 → optional
NEAR_COMMON_COLUMNS: tuple[str, ...] = (
    "SITEAREA", "APVCANCELYMD", "CLGSTDT", "CLGENDDT", "UPTAENM",
)

# 수집 task 상태(ingest 반환). ok → <short>.completed 마커, 그 외 → <short>.incomplete 마커.
STATUS_OK = "ok"
STATUS_PARTIAL = "partial"
STATUS_FAILED = "failed"


@dataclass(frozen=True)
class Dataset:
    oa_id: str                 # 서울 열린데이터광장 데이터셋 ID (source-native)
    name_ko: str               # 원본 데이터셋명
    short: str                 # 안정적 영문 축약(저장 경로/파일명/마커 키)
    category: str              # 분류(food/livestock/...)
    schedule: str              # daily | monthly | irregular
    service_name: str | None   # 서울 OpenAPI 서비스명. None 이면 수집 제외
