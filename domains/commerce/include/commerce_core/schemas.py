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
    category: str              # **대분류**(food/livestock/health_medical/.../culture/industry)
    schedule: str              # daily | monthly | irregular
    service_name: str | None   # 서울 OpenAPI 서비스명. None 이면 수집 제외
    sub_category: str | None = None  # **명칭에 따른 분류**(대분류 하위 세분류). 미지정 가능
    fmt: str = "v1"            # 응답 컬럼 표준: v1(구형 MGTNO…) | v2(신형 MNG_NO…). 등록값 = 감시 기준


# ── LOCALDATA 컬럼 표준 2종(v1 구형 · v2 신형) 별칭 계약 ─────────────────────────
# 같은 개념이 표준마다 다른 이름으로 온다. **정본=v1**, v2 는 별칭으로 매핑해 silver/bronze 가
# 동일 값으로 정규화한다. (env 13종이 v2 — MNG_NO/OGDP_INST_CD/SALS_STTS_CD… 로 응답.)
# canonical(v1) -> (v2 별칭들…). 조회는 canonical_get(row, name) 로 v1·v2 모두 대응.
COLUMN_ALIASES_V2: dict[str, str] = {
    "MGTNO": "MNG_NO", "OPNSFTEAMCODE": "OGDP_INST_CD", "BPLCNM": "BPLC_NM",
    "TRDSTATEGBN": "SALS_STTS_CD", "TRDSTATENM": "SALS_STTS_NM",
    "DTLSTATEGBN": "DTL_SALS_STTS_CD", "DTLSTATENM": "DTL_SALS_STTS_NM",
    "UPDATEDT": "DATA_UPDT_YMD", "LASTMODTS": "LAST_MDFCN_YMD",
    "RDNWHLADDR": "ROAD_NM_ADDR", "SITEWHLADDR": "LOTNO_ADDR",
    "X": "XCRD", "Y": "YCRD", "APVPERMYMD": "LCPMT_YMD", "DCBYMD": "CLSBIZ_YMD",
    # 업태명 — 2026-08-04 mail_order_sale v2 전환 실측에서 확인된 짝. gold detail payload 가
    # 원문 키를 직접 뽑으므로(정규화 컬럼 아님) 이 표에 없으면 v2 행의 uptaenm 이 통째로 빈다.
    "UPTAENM": "BZSTAT_SE_NM",
}


def canonical_get(row: dict, canonical: str):
    """row 에서 정본(v1) 키 우선, 없으면 v2 별칭으로 값 조회 → 표준 정규화."""
    if canonical in row:
        return row[canonical]
    alias = COLUMN_ALIASES_V2.get(canonical)
    return row.get(alias) if alias else None


def detect_row_format(row_keys) -> str:
    """응답 row 의 컬럼 표준 판별. 식별키 기준: MGTNO=v1 · MNG_NO=v2 · 둘 다 없으면 unknown."""
    keys = set(row_keys)
    if "MGTNO" in keys:
        return "v1"
    if "MNG_NO" in keys:
        return "v2"
    return "unknown"
