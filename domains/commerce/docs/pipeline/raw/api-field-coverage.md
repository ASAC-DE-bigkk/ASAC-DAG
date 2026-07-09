# API 응답 필드 커버리지 — 공통/비공통 분리 · 식별값 유효성 (152종)

> 실측일 2026-07-09 · **152종 전량 라이브 샘플**(각 `service_name` 1/5 호출)로 응답 `row` 필드셋을
> 수집·집계. 목적: (1) 수집 정상 여부, (2) 공통 vs API별 비공통 필드 분리, (3) 기존 bronze/silver
> 라인의 **식별값**이 152종 전체에 그대로 유효한지 검증. 근거 컬럼 계약: [../common_info.md](../common_info.md).
> 분류 체계(대분류/중분류/소분류): [../../PROJECT.md](../../PROJECT.md).

## 0. 결론 (요약)

- **수집 정상**: **152 / 152 OK**, 실패 0. (v1 139종 + v2 13종=환경.)
- **식별값 전량 유효(canonical)**: `OPNSFTEAMCODE·MGTNO·UPDATEDT·LASTMODTS = **152/152**`. v2(환경)는
  컬럼명이 달라도(§6) `schemas.py`의 v1/v2 별칭으로 흡수되어 식별값이 전 종 유효하다. 즉 bronze 정렬/식별키
  `(UPDATEDT → LASTMODTS → OPNSFTEAMCODE → MGTNO)` 와 silver 그레인 `(dataset, opnsfteamcode, mgtno)`
  **그대로 사용 가능**. `content_hash` 는 레코드 전체 해시라 스키마 무관.
- **v1 공통 14 + 준공통 5 + 비공통(대다수)**: 139종 v1 전 종 공통은 **14컬럼**. 5개는 소수 데이터셋에서
  빠져 **준공통**. 나머지는 업종별 시설/설비 컬럼(비공통). **v2(환경 13종)은 별도 컬럼셋(§6).**
  silver 는 `record_json` **schema-on-read**(있으면 파싱, 없으면 null)라 비공통/누락은 **무손실**.

## 1. 수집 정상 여부

`bronze.clients.SeoulOpenApiClient.fetch_page(svc, 1, 5)` 로 152종 호출 → **전부 OK**(응답 row 반환).
재현: §5.

## 2. 공통 영역 (v1 139종 전 종 공통 = 14컬럼)

139종(v1) 응답 `row` 키의 **교집합**:

```text
OPNSFTEAMCODE, MGTNO, BPLCNM, APVPERMYMD,
TRDSTATEGBN, DTLSTATEGBN, DTLSTATENM,
RDNWHLADDR, RDNPOSTNO, LASTMODTS, UPDATEGBN, UPDATEDT, X, Y
```

### 준공통 (전 종은 아님 — 소수 데이터셋 결측, optional 처리)

| 컬럼 | 커버리지(v1) | 비고 |
|---|---:|---|
| `SITEWHLADDR` | 138/139 | 지번주소. 결측 시 `LOTNO_ADDR`(숙박 등)→Juso 폴백(silver 처리됨) |
| `TRDSTATENM` | 138/139 | 상태**명**. 코드 `TRDSTATEGBN` 는 139/139 → 매핑으로 복원 |
| `SITETEL` | 134/139 | 소재지 전화 |
| `DCBYMD` | 131/139 | 폐업일자 |
| `SITEPOSTNO` | 127/139 | 지번 우편번호 |

→ 모두 **optional(있으면 파싱, 없으면 null)**. `schemas.py`의 `COMMON_COLUMNS(19)`는 정규화 기준
스키마일 뿐 존재 보장이 아니다(`NEAR_COMMON_COLUMNS` 로 준공통 처리).

## 3. 비공통 영역 (v1 API별 시설·설비 컬럼)

공통 14 + 준공통 5 를 뺀 나머지는 업종별 컬럼. **대분류/중분류(업종군)별 비공통 필드 수**(공통·준공통 밖):

| 대분류 | category | 비공통 개별필드 수 | 대표 컬럼 |
|---|---|---:|---|
| 문화 | culture | **82** | 시설 `FACILAR`/`*NUMLAY`, 관광 `INSUR*DT`/`SHP*`, 체육 `LDERCNT`/`MEMCOLL*`, `CULPHYEDCOBNM` |
| 산업 | industry | **74** | `CAPT`, `ROPNYMD`, 판매/계량/가스/석유/담배/장사/직업소개 등 업종 특화 다수 |
| 보건·식품 | food | **49** | 종업원 `HOFFEPCNT`/`MANEIPCNT`/`WMEIPCNT`, 업태 `TRDPJUBNSENM`/`SNTUPTAENM`, `BDNGOWNSENM`, `MONAM` |
| 보건·의료 | health_medical | **42** | `HSTRMNUM`, `MEDEXTRITEMSCN(NM)`, `SICBNUM`, `TOTAR`, `METR*`, 산후조리 `*AR`/`NURSECNT` |
| 보건·위생미용 | hygiene_beauty | **35** | `SITEAREA`, 층수 `USE*FLR`, 인력 `MANEIPCNT`/`WMEIPCNT`, 조건부허가 `CNDPERM*` |
| 보건·안경치과 | optical_dental | **28** | 렌즈/검안 `PUPILDISTMEASNUM`/`DEFPTRFRTGGNUM`, 치과기공 `CRFTUSE*`, `TOTAR` |
| 보건·숙박 | lodging | **20** | 한실/양실, 층수 `BDNG*FLRCNT`, `CNDPERM*`, `LOTNO_ADDR` |
| 보건·축산 | livestock | **10** | `LIND*`, `RGTMBDSNO`, `ROPNYMD`, `SITEAREA` |
| 보건·동물 | animal | **9** | `RGTMBDSNO`, `ROPNYMD`, `CFRMGBNCTN` |
| 보건·약국 | pharmacy | **5** | `PHARMTRDAR`, `ASGNYMD` |

- **준공통 다음 빈도(여러 업종군 공유)**: `CLGSTDT`/`CLGENDDT`(휴업), `APVCANCELYMD`(인허가취소),
  `SITEAREA`(면적), `UPTAENM`(업태) — `schemas.py`의 `NEAR_COMMON_COLUMNS` 로 optional 처리 중.
- 전체 per-dataset 필드 목록은 §5 방법으로 재현(레포에 원자료는 두지 않음). 보건 대분류의 상세 추출은
  dbt `silver_license_detail_health`(#80)로 컬럼화됨 — [dbt dataset-columns.md](../../../../../../dbt/domains/commerce/docs/dataset-columns.md).

## 4. 식별값 유효성 — 기존 bronze/silver 라인 그대로 사용 가능

| 용도 | 사용 값 | 152종 커버리지 | 결론 |
|---|---|---:|---|
| **업소 식별(중복·이력)** | `OPNSFTEAMCODE` + `MGTNO` | 152/152 | ✅ (v2=OGDP_INST_CD+MNG_NO 별칭 흡수. MGTNO 는 발급 자치단체 안 유니크 → OPNSFTEAMCODE 필수, #198) |
| **버전 정렬(증분 diff)** | `UPDATEDT` → `LASTMODTS` | 152/152 | ✅ (v2=DATA_UPDT_YMD+LAST_MDFCN_YMD. UPDATEDT None 폴백 #193 유효) |
| **내용 변경 감지** | `content_hash`(레코드 canonical sha256) | 스키마 무관 | ✅ 비공통 포함 전체 해시 |
| **영업상태 추적** | `TRDSTATEGBN`(코드)/`DTLSTATEGBN` | 139/139(v1) | ✅ v2=SALS_STTS_CD/DTL_SALS_STTS_CD 별칭 |
| **주소·행정동** | `RDNWHLADDR`/`SITEWHLADDR`·`LOTNO_ADDR` | 138(+폴백) | ✅ silver 지번 폴백 처리 |
| **좌표** | `X`,`Y`(EPSG:5174) | 139/139(v1) | ✅ v2=XCRD/YCRD 별칭. silver 좌표 변환 그대로 |

**silver 무손실 근거**: silver 모델은 `json_extract_scalar(record_json,'$.<FIELD>') + nullif(trim(...),'')`
(v1/v2 는 `lf()` 매크로로 정본 우선·별칭 폴백) → **없는 키는 null**, 비공통은 record_json 원본 보존.

## 5. 재현 방법

```bash
# 152종 응답 필드셋 + 공통/비공통 + 식별값 커버리지 프로브(컨테이너)
docker exec <scheduler> python -u -c '
import sys; sys.path.insert(0,"/opt/airflow/dags/domains/commerce/include"); sys.path.insert(0,"/opt/airflow/dags")
from commerce_core.env import load_commerce_env; load_commerce_env()
from commerce_core import registry
from commerce_core.settings import get_settings
from commerce_core.schemas import canonical_get
from bronze.clients import SeoulOpenApiClient
import time
s=get_settings(); c=SeoulOpenApiClient(key=s.seoul_openapi_key, base_url=s.seoul_openapi_base_url)
for d in registry.all_datasets():
    page=c.fetch_page(d.service_name,1,5)          # row 키 = 필드셋
    time.sleep(0.12)
'   # 각 dataset row 키의 교집합=공통(v1), 합집합-공통=비공통, canonical_get 로 식별값 카운트
```

인증키는 클라이언트가 URL·로그에서 마스킹하며(CLAUDE.md §2.5), 본 분석 산출물에도 키/URL 미포함.

## 6. v2(환경) 컬럼셋 — 신형 표준 (13종)

환경 대분류(13종)는 **v2 신형 컬럼명**을 쓴다(구형 v1과 이름만 다르고 개념은 대응). 응답 키셋:

```text
MNG_NO, OGDP_INST_CD, BPLC_NM, BPLC_SE_NM,
SALS_STTS_CD, SALS_STTS_NM, DTL_SALS_STTS_CD, DTL_SALS_STTS_NM,
ROAD_NM_ADDR, ROAD_NM_ZIP, LOTNO_ADDR, LCTN_ZIP, TELNO,
DATA_UPDT_YMD, DATA_UPDT_SE, LAST_MDFCN_YMD, LCPMT_YMD, CLSBIZ_YMD,
TCBIZ_BGNG_YMD, TCBIZ_END_YMD, ROBIZ_YMD, XCRD, YCRD,
CTGRY_NM, BZSTAT_SE_NM, ENVM_TASK_SE_NM, TPBIZ_SE_NM
```

### v1↔v2 별칭 (식별/공통 대응 — `schemas.py COLUMN_ALIASES_V2`)

| 개념 | v1 | v2 |
|---|---|---|
| 관리번호 | `MGTNO` | `MNG_NO` |
| 개방자치단체코드 | `OPNSFTEAMCODE` | `OGDP_INST_CD` |
| 사업장명 | `BPLCNM` | `BPLC_NM` |
| 영업상태 코드/명 | `TRDSTATEGBN`/`TRDSTATENM` | `SALS_STTS_CD`/`SALS_STTS_NM` |
| 상세상태 코드/명 | `DTLSTATEGBN`/`DTLSTATENM` | `DTL_SALS_STTS_CD`/`DTL_SALS_STTS_NM` |
| 도로명/지번 주소 | `RDNWHLADDR`/`SITEWHLADDR` | `ROAD_NM_ADDR`/`LOTNO_ADDR` |
| 데이터갱신일/최종수정 | `UPDATEDT`/`LASTMODTS` | `DATA_UPDT_YMD`/`LAST_MDFCN_YMD` |
| 인허가/폐업일 | `APVPERMYMD`/`DCBYMD` | `LCPMT_YMD`/`CLSBIZ_YMD` |
| 좌표 | `X`/`Y` | `XCRD`/`YCRD` |

- **비공통(환경 고유)**: `BPLC_SE_NM`(사업장구분), `CTGRY_NM`(분류), `ENVM_TASK_SE_NM`(환경업무구분),
  `TPBIZ_SE_NM`/`BZSTAT_SE_NM`, `TCBIZ_*`(휴업), `ROBIZ_YMD`(재개업) 등. silver 는 `lf('MGTNO','MNG_NO')`
  식으로 공통축을 흡수하고 비공통은 record_json 보존(무손실).
