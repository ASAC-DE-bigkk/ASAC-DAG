# API 응답 필드 커버리지 — 공통/비공통 분리 · 식별값 유효성 (107종)

> 실측일 2026-07-08 · **107종 전량 라이브 샘플**(각 `LOCALDATA_*` 1/5 호출)로 응답 `row` 필드셋을
> 수집·집계. 목적: (1) 수집 정상 여부, (2) 공통 vs API별 비공통 필드 분리, (3) 기존 bronze/silver
> 라인의 **식별값**이 107종 전체에 그대로 유효한지 검증. 근거 컬럼 계약: [../common_info.md](../common_info.md).

## 0. 결론 (요약)

- **수집 정상**: 107 / 107 **OK**, 실패 0. (모든 `service_name` 응답 `INFO-000`.)
- **식별값 전량 유효**: **OPNSFTEAMCODE·MGTNO·UPDATEDT·LASTMODTS = 107/107**. 즉 기존
  bronze 정렬/식별키 `(UPDATEDT → LASTMODTS → OPNSFTEAMCODE → MGTNO)` 와 silver 그레인
  `(dataset, opnsfteamcode, mgtno)` **그대로 사용 가능**. `content_hash` 는 레코드 전체 해시라
  스키마 무관하게 동작. 영업상태 `TRDSTATEGBN` 도 107/107.
- **공통 14 + 준공통 5 + 비공통(대다수)**: 진짜 전 종 공통은 14컬럼. 기존 "공통 19"는 5개가
  1~2개 데이터셋에서 빠져 **준공통**. 그 외는 업종별 시설/설비 컬럼(비공통, 45개 스키마 변형).
  silver 는 `record_json` **schema-on-read**(있으면 파싱, 없으면 null)라 비공통/누락은 **무손실**.

## 1. 수집 정상 여부

`bronze.clients.SeoulOpenApiClient.fetch_page(svc, 1, 5)` 로 107종 호출 → **전부 OK**(`RESULT.CODE=INFO-000`,
`list_total_count` 반환). 재현: `python -m bronze.resolve verify` (PYTHONPATH=include, 컨테이너).

## 2. 공통 영역 (전 종 공통 = 14컬럼)

107종 응답 `row` 키의 **교집합**:

```text
OPNSFTEAMCODE, MGTNO, BPLCNM, APVPERMYMD,
TRDSTATEGBN, DTLSTATEGBN, DTLSTATENM,
RDNWHLADDR, RDNPOSTNO, LASTMODTS, UPDATEGBN, UPDATEDT, X, Y
```

### 준공통 (기존 "공통 19" 중 전 종은 아님 — 1~2개 데이터셋 결측)

| 컬럼 | 커버리지 | 결측 데이터셋 | 비고 |
|---|---:|---|---|
| `TRDSTATENM` | 106/107 | full_amusement_park | 상태**명**. 코드 `TRDSTATEGBN` 는 107/107 → 매핑으로 복원 가능 |
| `SITETEL` | 106/107 | lodging | 전화 |
| `SITEWHLADDR` | 106/107 | lodging | 지번주소. lodging 은 `LOTNO_ADDR` 제공 → **silver 이미 폴백 처리** |
| `DCBYMD` | 105/107 | lodging, culture_arts_corporation | 폐업일 |
| `SITEPOSTNO` | 105/107 | culture_arts_corporation, outdoor_advertising | 지번우편번호 |

→ 모두 **optional(있으면 파싱, 없으면 null)** 로 다뤄야 하며, 실제로 silver 는 그렇게 처리한다
(§4). `schemas.py` 의 `COMMON_COLUMNS(19)` 는 정규화 기준 스키마일 뿐 존재 보장이 아니다.

## 3. 비공통 영역 (API별 시설·설비 컬럼)

공통 14를 뺀 나머지는 업종별 컬럼으로, **45개 서로 다른 스키마 변형**이 존재. 빈도 상위(거의 준공통)
→ 하위(업종 특화)로 갈수록 특정 업종에만 등장. 대표 군집:

| 업종군(category) | 대표 비공통 컬럼 | 예 데이터셋 |
|---|---|---|
| 식품·제조 (food) | `FCTY*EPCNT`, `WMEIPCNT`, `MANEIPCNT`, `WTRSPLYFACILSENM`, `SITEAREA`, `SNTUPTAENM` | bakery, food_mfg, hfood_general_sale, contract_meal_service |
| 문화 시설 (culture) | `CULPHYEDCOBNM`, `FACILAR`, `JISGNUMLAY`/`UNDERNUMLAY`, `NEARENVNM`, `REGNSENM`, `TOTNUMLAY` | cinema, video_*, game_*, performance_hall, convention_*, amusement_* |
| 여행·관광 (culture) | `INSUR*DT`, `SHPTOTTONS`, `SHPCNT`, `ENGSTNTRNM*`, `MEETSAMTIMESYGSTF` | *_travel_agency, tour_cruise, tourism_operator |
| 체육·회원제 (culture) | `BUPNM`, `LDERCNT`, `MEMCOLLTOTSTFNUM`, `PUPRSENM`, `INSURJNYNCODE`, `BDNGYAREA` | golf_course, dance_hall, billiard_hall, fitness_center, martial_arts_gym |
| 위생·미용 (hygiene_beauty) | `SNTUPTAENM`, `USEJISG*FLR`/`USEUNDER*FLR`, `MANEIPCNT`, `CNDPERM*` | barber_shop, beauty_shop, laundry, bathhouse |
| 의료 (health_medical) | `HSTRMNUM`, `MEDEXTRITEMSCN(NM)`, `SICBNUM`, `TOTAR`, `METR*` | hospital, clinic, affiliated_medical, medical_similar, medical_corporation |
| 축산·동물 (livestock/animal) | `LIND*`, `RGTMBDSNO`, `ROPNYMD`, `SITEAREA`, `CFRMGBNCTN` | livestock_*, animal_* |
| 특화 1-종 | optical(`LENSCUTNUM`/`EYEPYONUM`…), dental(`CRFTUSE*`/`DENTIUSEPRESSNUM`…), disinfection(살포기 `*SPRAYNUM`), postpartum(`NURSECNT`/`BABYRGLSTNUM`…) | optical_shop, dental_lab, disinfection, postpartum_care |

- **준공통 다음 빈도**: `CLGSTDT`/`CLGENDDT`(휴업 82/81), `APVCANCELYMD`(인허가취소 80), `SITEAREA`(40),
  `UPTAENM`(33) — 이미 `schemas.py`의 `NEAR_COMMON_COLUMNS` 로 optional 처리 중.
- 특이: lodging 은 `LOTNO_ADDR`·`SNTTN_BZSTAT_NM`(1/107) 등 고유 컬럼. outdoor_advertising 은
  `TRDCTN`(거래내용) 등 최소 스키마(22컬럼). 전체 컬럼폭은 22~50.

전체 per-dataset 필드 목록은 §5 방법으로 재현 가능(레포에 원자료는 두지 않음).

## 4. 식별값 유효성 — 기존 bronze/silver 라인 그대로 사용 가능

| 용도 | 사용 값 | 107종 커버리지 | 결론 |
|---|---|---:|---|
| **업소 식별(중복·이력)** | `OPNSFTEAMCODE` + `MGTNO` | 107/107 · 107/107 | ✅ 그대로 (MGTNO 는 발급 자치단체 안에서만 유니크 → OPNSFTEAMCODE 필수, #198) |
| **버전 정렬(증분 diff)** | `UPDATEDT` → `LASTMODTS` | 107/107 · 107/107 | ✅ 그대로 (UPDATEDT None 폴백 #193 유효) |
| **내용 변경 감지** | `content_hash`(레코드 canonical sha256) | 스키마 무관 | ✅ 비공통 컬럼 포함 전체 해시 |
| **영업상태 추적** | `TRDSTATEGBN`(코드) / `DTLSTATEGBN` | 107/107 | ✅ 상태명 `TRDSTATENM` 은 106 → 코드→명 매핑 |
| **주소·행정동** | `RDNWHLADDR`(도로명) / `SITEWHLADDR`(지번)·`LOTNO_ADDR` | 107 / 106(+lodging 폴백) | ✅ silver 지번 폴백 이미 처리 |
| **좌표** | `X`, `Y`(EPSG:5174) | 107/107 · 107/107 | ✅ silver 좌표 변환 그대로 |
| **인허가/수집 계보** | `APVPERMYMD`, `LASTMODTS`, run/observed 메타 | 107/107 | ✅ |

**silver 무손실 근거**: silver 모델([silver_license_history.sql](../../../../../dbt/domains/commerce/models/silver/silver_license_history.sql))은
`json_extract_scalar(record_json, '$.<FIELD>')` + `nullif(trim(...), '')` 로 필드를 뽑는다 →
**없는 키는 null**, 비공통 컬럼은 무시(record_json 원본 보존). 따라서 45개 스키마 변형이 섞여도
silver 정형은 **동일 규칙으로 안전**하며, 신규 업종 특화 컬럼이 필요해지면 그때 파생만 추가한다.

## 5. 재현 방법

```bash
# 107종 수집 정상 여부(OK/FAIL + total)
docker compose run --rm --no-deps -T \
  -e PYTHONPATH=/opt/airflow/dags/domains/commerce/include:/opt/airflow/dags \
  airflow-scheduler python -m bronze.resolve verify

# 필드셋/공통·비공통/식별값 커버리지 (probe 스크립트를 stdin 으로: registry 전 종 1/5 샘플 → 집계)
#   각 dataset 응답 row 키의 교집합=공통, 합집합-공통=비공통, 4개 식별필드 보유 카운트.
```

인증키는 클라이언트가 URL·로그에서 마스킹하며(CLAUDE.md §2.5), 본 분석 산출물에도 키/URL 미포함.
