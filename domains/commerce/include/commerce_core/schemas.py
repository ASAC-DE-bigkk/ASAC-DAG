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
    canonical_fmt: str | None = None  # Raw 비교·저장 정본. None이면 응답 형식(fmt)을 그대로 보존


# ── LOCALDATA 컬럼 표준 2종(v1 구형 · v2 신형) 별칭 계약 ─────────────────────────
# 같은 개념이 표준마다 다른 이름으로 온다. **정본=v1**, v2 는 별칭으로 매핑해 silver/bronze 가
# 동일 값으로 정규화한다. (env 13종이 v2 — MNG_NO/OGDP_INST_CD/SALS_STTS_CD… 로 응답.)
# canonical(v1) -> (v2 별칭들…). 조회는 canonical_get(row, name) 로 v1·v2 모두 대응.
COLUMN_ALIASES_V2: dict[str, str] = {
    "MGTNO": "MNG_NO", "OPNSFTEAMCODE": "OGDP_INST_CD", "BPLCNM": "BPLC_NM",
    "APVCANCELYMD": "LCPMT_RTRCN_YMD", "CLGSTDT": "TCBIZ_BGNG_YMD",
    "CLGENDDT": "TCBIZ_END_YMD", "ROPNYMD": "ROBIZ_YMD",
    "TRDSTATEGBN": "SALS_STTS_CD", "TRDSTATENM": "SALS_STTS_NM",
    "DTLSTATEGBN": "DTL_SALS_STTS_CD", "DTLSTATENM": "DTL_SALS_STTS_NM",
    "UPDATEDT": "DATA_UPDT_YMD", "LASTMODTS": "LAST_MDFCN_YMD",
    "RDNWHLADDR": "ROAD_NM_ADDR", "SITEWHLADDR": "LOTNO_ADDR",
    "X": "XCRD", "Y": "YCRD", "APVPERMYMD": "LCPMT_YMD", "DCBYMD": "CLSBIZ_YMD",
    "SILMETNM": "NTSL_MTH_NM", "SITEPOSTNO": "LCTN_ZIP", "RDNPOSTNO": "ROAD_NM_ZIP",
    "SITETEL": "TELNO", "UPDATEGBN": "DATA_UPDT_SE",
    # 업태명 — 2026-08-04 mail_order_sale v2 전환 실측에서 확인된 짝. gold detail payload 가
    # 원문 키를 직접 뽑으므로(정규화 컬럼 아님) 이 표에 없으면 v2 행의 uptaenm 이 통째로 빈다.
    "UPTAENM": "BZSTAT_SE_NM",
}

# ── 데이터셋 스코프 도메인 별칭(v1 정본 <- v2) ────────────────────────────────────
# 2026-08-04 전환은 공통 표준 25필드만이 아니라 **업종 고유 필드까지** 개명했다. v2 쪽 같은
# 이름이 데이터셋마다 다른 v1 이름에서 오므로(CAPT→CPTL vs CAPTSCALE→CPTL) 전역 역표가
# 성립하지 않는다 — 데이터셋 단위로 위 전역표에 얹는다.
#
# 짝은 손 추정이 아니라 **데이터에서 유도**했다(2026-08-07 실측):
#   시대 분리(collected_at < 2026-08-03 12:00Z = 구형) → 한쪽 시대에만 있는 필드 diff →
#   같은 업소(mgtno)의 구·신 최신값 전 조합 대조 → **비공란 값 일치율 99% 이상만** 채택.
#   46쌍이 99.994~100% 로 실증됐다(빈값끼리 일치는 증거로 안 쳤다 — emission_repair 에서
#   이름 추정 짝이 교차로 틀린 것을 이 방법이 잡았다).
# `# 소거법` 표시 8필드는 값이 전부 비어 증거가 원리적으로 불가능한 짝 — 개명 집합이 닫혀
# 있고(구·신 개수가 전 데이터셋 대칭) 남은 후보가 유일해서 채택했다. 현재 전 행이 빈값이라
# 오짝이어도 데이터 영향이 없고, 값이 들어오기 시작하면 재검증한다.
DATASET_COLUMN_ALIASES_V2: dict[str, dict[str, str]] = {
    "animal_sale": {
        "RGTMBDSNO": "RGHT_MNBD_SN", "SITEAREA": "LCTN_AREA",
    },
    "caregiver_academy": {
        "DELETEYMD": "DEL_YMD",                     # 소거법(1:1) — 전 행 빈값
    },
    "distribution_sale": {
        "BDNGOWNSENM": "BLDG_PSN_SE_NM", "FACILTOTSCP": "FCLT_WHOL_SCL",
        "FCTYOWKEPCNT": "FCTRY_OFJB_EMPLE_NUMBR", "FCTYPDTJOBEPCNT": "FCTRY_PROWR_EMPLE_NUMBR",
        "FCTYSILJOBEPCNT": "FCTRY_SAWOR_EMPLE_NUMBR", "HOFFEPCNT": "HDOFC_EMPLE_NUMBR",
        "HOMEPAGE": "HMPG", "ISREAM": "DEPOT", "LVSENM": "GRD_SE_NM",
        "MANEIPCNT": "ML_PRCTR_NUMBR", "MONAM": "MRNT", "MULTUSNUPSOYN": "MLT_UTZTN_COMM_YN",
        "SITEAREA": "LCTN_AREA", "SNTUPTAENM": "SNTT_BZSTAT_NM",
        "TRDPJUBNSENM": "TRDP_NEARE_SE_NM", "WMEIPCNT": "FML_PRCTR_NUMBR",
        "WTRSPLYFACILSENM": "WSP_FCLT_SE_NM",
        "JTUPSOASGNNO": "TRADI_COMM_DSGN_NO",       # 소거법(2:2) — 지정번호↔DSGN_NO 토큰
        "JTUPSOMAINEDF": "TRADI_COMM_FD",           # 소거법(2:2) — 주력분야↔FD 토큰
    },
    "door_to_door_sale": {
        "ASETSCP": "AST_SCL", "BCTOTAM": "LBLT_GRAMT", "CAPT": "CPTL",
    },
    "emission_repair_agent": {
        "COBGBNNM": "TPBIZ_SE_NM",                  # 값 실증 — 이름 추정(→CTGRY_NM)은 교차 오짝이었다
        "ENVBSNSENM": "ENVM_TASK_SE_NM",
        "JONGGBNNM": "CTGRY_NM",                    # 소거법(1:1) — 전 행 빈값
    },
    "feed_manufacturing": {
        "LINDJOBGBNNM": "LIVEK_TASK_SE_NM", "LINDPRCBGBNNM": "LIVEK_PROCE_SE_NM",
        "RGTMBDSNO": "RGHT_MNBD_SN", "SITEAREA": "LCTN_AREA",
        "LINDSEQNO": "LIVEK_SN",                    # 소거법(1:1) — 전 행 빈값
    },
    "free_job_agency": {
        "BUPGBNNM": "CORP_SE_NM", "SENM": "SE_NM",
    },
    "funeral_director_academy": {
        "DELETEYMD": "DEL_YMD",                     # 소거법(1:1) — 전 행 빈값
    },
    "groundwater_construction": {
        "CAPT": "CPTL", "FACILEQI": "FCLT_EQPMNT", "OTRORGTRANSYN": "OINST_BFR_YN",
        "SPECMPWTOTNUM": "TELGM_MNPW_NUMBR",
    },
    "groundwater_purification": {
        "CAPT": "CPTL", "FACILEQI": "FCLT_EQPMNT", "OTRORGTRANSYN": "OINST_BFR_YN",
        "SPECMPWTOTNUM": "TELGM_MNPW_NUMBR",
    },
    "groundwater_survey": {
        "FACILEQI": "FCLT_EQPMNT", "OTRORGTRANSYN": "OINST_BFR_YN",
        "SPECMPWTOTNUM": "TELGM_MNPW_NUMBR",
    },
    "mutual_aid_funeral": {
        "CAPTSCALE": "CPTL", "DEBTPAYISRE": "DBT_GIVE_GRNTE", "DEPBANKNM": "DEPST_INST",
        "DEPUCORGETCNM": "DDC_CTRT_ETC_NM", "DEPUCORGNM": "DDC_CTRT_NM",
        "INSUR": "INSRNC",                          # 소거법(2:2) — INSUR↔INSRNC 토큰
        "MWSRNM": "CVLCPT_KND_NM",                  # 소거법(2:2) — 잔여 유일 후보
    },
}

CANONICAL_MAPPING_VERSION = "localdata-v2-to-v1-2026-08-04.2"   # .2: 도메인 고유 필드 54쌍 추가


class SchemaCanonicalizationError(ValueError):
    """가역성을 보장할 수 없는 LOCALDATA 컬럼 정본화 요청."""


#: v1 정본 <- v2 별칭 역방향 표. 키 단위 치환에 쓴다.
_V1_BY_V2: dict[str, str] = {v2: v1 for v1, v2 in COLUMN_ALIASES_V2.items()}


def canonicalize_row(row: dict, *, source_format: str, canonical_format: str,
                     extra_aliases: dict[str, str] | None = None) -> dict:
    """LOCALDATA row를 선언된 정본 형식으로 가역 변환한다 — **키 단위 1:1 치환**.

    값은 건드리지 않는다. 별칭표에 있는 v2 키만 v1 이름으로 바꾸고, **그 밖의 필드는 그대로
    통과**시킨다. 데이터셋마다 공통 필드 구성이 다르고(예: 판매방식 `NTSL_MTH_NM` 은 판매업만
    가진다) 업종 고유 필드(`LCTN_AREA`·`CPTL`·`DEL_YMD` …)가 따로 있어서, 25필드 정확 일치를
    요구하면 12종 중 한 종도 통과하지 못한다(2026-08-06 실측 — 초판이 그랬다).

    ``extra_aliases`` 는 데이터셋 스코프 도메인 별칭(v1<-v2, `DATASET_COLUMN_ALIASES_V2`) —
    전역표 위에 얹는다. 전역에 못 넣는 이유는 v2 이름 충돌(CAPT→CPTL vs CAPTSCALE→CPTL).

    실패시키는 경우는 **가역성이 깨지는 것 하나**다: v1·v2 이름이 한 행에 같이 있고 값이 다르면
    어느 쪽이 정본인지 정할 수 없다. 값이 같으면 중복일 뿐이라 통과시킨다.

    멱등: 이미 v1 인 행은 바꿀 키가 없어 그대로 나온다(재처리 안전).
    """
    if source_format == canonical_format:
        return dict(row)
    if (source_format, canonical_format) != ("v2", "v1"):
        raise SchemaCanonicalizationError(
            f"지원하지 않는 정본 변환: {source_format}->{canonical_format}")

    v1_by_v2 = _V1_BY_V2
    if extra_aliases:
        v1_by_v2 = {**_V1_BY_V2, **{v2: v1 for v1, v2 in extra_aliases.items()}}
    conflicts = []
    out: dict = {}
    for key, value in row.items():
        canonical = v1_by_v2.get(key)
        if canonical is None:
            out[key] = value            # 별칭표 밖(고유 필드·이미 v1) — 원문 그대로
            continue
        if canonical in row and str(row[canonical]).strip() != str(value).strip():
            conflicts.append(f"{canonical}={row[canonical]!r} vs {key}={value!r}")
            continue
        out[canonical] = value
    if conflicts:
        raise SchemaCanonicalizationError(
            "v1·v2 키가 한 행에 공존하고 값이 다릅니다(정본 판정 불가): " + "; ".join(conflicts[:5]))
    return out


def canonicalize_dataset_row(dataset: Dataset, row: dict) -> dict:
    """registry의 source/canonical 형식 계약 + 데이터셋 스코프 도메인 별칭을 적용한다."""
    return canonicalize_row(
        row, source_format=dataset.fmt,
        canonical_format=dataset.canonical_fmt or dataset.fmt,
        extra_aliases=DATASET_COLUMN_ALIASES_V2.get(dataset.short))


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
