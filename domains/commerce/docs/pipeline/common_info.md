# commerce — API 공통 정보 (common_info)

서울 인허가(LOCALDATA) 데이터셋 39종을 **commerce** 도메인으로 수집한다. 모든 대상은
서울 열린데이터광장 OpenAPI 의 LOCALDATA 표준(지방행정 인허가데이터 표준)을 따르므로
**응답 컬럼 구조가 업종군에 걸쳐 거의 동일**하다. 이 문서는 실호출로 검증한 공통 컬럼·
`UPDATEDT` 제공 여부·저장/상태 관리 계약을 정리한다.

- 코드(자립 패키지): [dags/domains/commerce/include/](../../include/) (`common`·`bronze`·`silver`)
- DAG: `commerce_collect_raw` — [dags/domains/commerce/commerce_raw.py](../../commerce_raw.py)
- 데이터셋 레지스트리(단일 진실 공급원): [dags/domains/commerce/config/dataset_registry.yaml](../../config/dataset_registry.yaml)
- 실행 인자/환경변수(SEOUL_API_KEY_COMM 등): [configuration.md](../configuration/configuration.md)

---

## 1. 저장 구조 (DB·외부 매니페스트 없음)

**DAG 실행 1회 = `run_id` 폴더 1개**. `{prefix}`(=`COMMERCE_STORAGE_PREFIX`)·bucket 접두는
스토리지 백엔드가 자동 부착. 자세히: [../architecture/storage.md](../architecture/storage.md).

```text
{prefix}/raw/commerce/<YYYY>/<MM>/<DD>/run_id=<YYYY-MM-DD_HHMMSS_mmm>/<short>.jsonl       # API당 1파일(원본 페이지 NDJSON)
{prefix}/raw/commerce/<YYYY>/<MM>/<DD>/run_id=<...>/_markers/<short>.completed | .incomplete  # API별 수집 결과 마커(리니지 JSON)
{prefix}/raw/commerce/<YYYY>/<MM>/<DD>/run_id=<...>/_markers/_RUN.completed | .incomplete      # 실행 전체 마커
```

- `<short>` = API 축약단어 = 데이터셋 `slug`(예: `general_restaurant`, `lodging`, `beauty_shop`).
- 한 API 의 모든 페이지를 **줄단위 NDJSON 1파일**(줄 1개 = 원본 응답 1페이지).
- **bronze 는 이 `run_id` 폴더 안에서만** 파일을 만든다(외부 경로에 상태 파일 없음).
- 중복 제어: bronze 는 매 실행 전체 수집, **중복 제거는 silver 가 `MGTNO` 로**(아래 §4).
- 인증키는 경로/마커/로그에 절대 남기지 않는다(CLAUDE.md §2.5).

---

## 2. 공통 응답 컬럼

도메인 대표 5종(식품접객 `072404`, 식품판매 `072208`, 의료 `010101`, 동물 `020301`,
공중위생 `030601`)의 응답 행 키를 실호출로 교집합한 결과 — **모든 API 공통 19컬럼**:

| 컬럼 | 의미 |
|---|---|
| `OPNSFTEAMCODE` | 개방자치단체코드 |
| `MGTNO` | 관리번호(인허가 단위 식별자) |
| `BPLCNM` | 사업장명 |
| `APVPERMYMD` | 인허가일자 |
| `DCBYMD` | 폐업일자 |
| `TRDSTATEGBN` / `TRDSTATENM` | 영업상태 구분코드 / 상태명 |
| `DTLSTATEGBN` / `DTLSTATENM` | 상세영업상태 코드 / 상태명 |
| `SITETEL` | 소재지 전화 |
| `SITEWHLADDR` | 지번 주소 |
| `RDNWHLADDR` | 도로명 주소 |
| `SITEPOSTNO` / `RDNPOSTNO` | 지번 / 도로명 우편번호 |
| `LASTMODTS` | 최종수정시점(타임스탬프) |
| `UPDATEGBN` | 데이터갱신 구분(I=등록, U=수정) |
| **`UPDATEDT`** | **데이터갱신일자** |
| `X` / `Y` | 좌표정보(X, Y) |

### 준공통(대부분 제공하나 일부 업종군에서 누락 — optional 처리)

| 컬럼 | 의미 | 누락되는 군(샘플 기준) |
|---|---|---|
| `SITEAREA` | 소재지 면적 | 공중위생 |
| `APVCANCELYMD` | 인허가취소일자 | 식품(접객·판매) |
| `CLGSTDT` / `CLGENDDT` | 휴업 시작 / 종료일 | 식품(접객·판매) |
| `UPTAENM` | 업태명 | 동물·공중위생(빈 값) |

> 그 외 업종군 고유 컬럼(예: 식품접객의 좌석수·시설규모, 의료의 진료과목 등)은 군마다 추가된다.
> 전체 컬럼 수(샘플): 식품접객 39 · 식품판매 39 · 의료 24 · 동물 25 · 공중위생 33.
> 안정적인 다운스트림 스키마는 **위 공통 19컬럼**을 기준으로 잡고 나머지는 optional 로 다룬다.

---

## 3. `UPDATEDT` (데이터갱신일자) 제공 여부

**검증 결과: 대표 5개 업종군 전부에서 `UPDATEDT` 가 응답된다.** LOCALDATA 표준 필드이므로
39종 전체에 공통 제공된다고 본다(미해석 서비스명은 §5 절차로 채운 뒤 `verify` 로 재확인).

- 갱신 추적 권장 조합: `UPDATEDT`(일자) + `LASTMODTS`(시점) + `UPDATEGBN`(I/U).
- 증분/변경 감지 로직은 이 셋을 키로 설계한다.

검증 재현(`bronze.resolve` 는 실행 시 `.env.commerce` 를 자동 적재하므로, 거기에
`SEOUL_API_KEY_COMM` 가 있으면 아래 인라인 지정은 생략 가능):

```bash
# 도메인 대표 코드의 응답 컬럼 교집합 + UPDATEDT 존재 확인
SEOUL_API_KEY_COMM=... python -m bronze.resolve probe 072404 072208 010101 020301 030601
# 또는 .env.commerce 설정 후:  python -m bronze.resolve probe 072404 ...
```

---

## 4. 수집 상태/중복/재수집 (run_id 폴더의 마커)

DB·외부 매니페스트 없음 — 상태/이력은 **각 `run_id` 폴더의 마커**가 전부. **API당 마커
1개**(상호배타, 타입이 곧 상태):

| 마커 | 의미 |
|---|---|
| `_markers/<short>.completed` | cap 없이 끝까지 + 건수 일치(status=ok) |
| `_markers/<short>.incomplete` | 건수 불일치/부분(cap)/오류(status=partial\|failed) |
| (마커 없음) | 이번 실행 미시도 |
| `_markers/_RUN.completed\|.incomplete` | 실행 전체 요약(datasets_ok/incomplete/rows…) |

마커 JSON 에 리니지+요약(`pages_written`·`rows_total`·`list_total_count`·`complete`·`bronze_key`·
`pages[].content_hash`)이 들어 있어, 어디까지 받았는지 그 폴더만 보면 안다.

**이력**: 실행마다 `run_id` 폴더가 쌓이므로 폴더 목록 자체가 수집 이력이다(중앙 파일 불필요).

**중복/재수집**:
- bronze 는 **매 실행 전체 수집**(같은 날 스킵 없음). 같은 업장이 여러 run 에 중복될 수 있으나
  **중복 제거는 silver 가 `MGTNO` 로** 흡수한다([bronze/pagination-ordering.md](bronze/pagination-ordering.md)).
- 특정 실행을 무효화하려면 그 `run_id` 폴더를 삭제(다른 run 에 영향 없음).
- `incomplete` 로 끝난 API 는 다음 실행에서 자연히 다시 수집된다(전체 수집이므로).

> 경쟁 안전: 마커는 각 ingest 태스크가 **자기 API 것 1개**만 쓰고(겹치지 않음), `_RUN` 마커는
> `finalize_run` 1곳에서만 쓴다. DAG 는 `max_active_runs=1`.

---

## 4-1. 페이지네이션 · 완전성 점검 · backfill

서울 OpenAPI 는 **1회 조회 최대 1000건**(`START_INDEX`/`END_INDEX` 윈도우). 수집은 이
윈도우를 `page_no` 로 밀며 **마지막 페이지까지 순회**한다.

- 1회 조회 건수 = `SEOUL_PAGE_SIZE`(기본 1000, 서울 상한 1000으로 캡). 설정으로 변경 가능.
- `SEOUL_MAX_PAGES` = **비움/미설정(기본) → 무제한, 끝까지 순회**(일반 API 호출 횟수 제한 없음) / >0 → 부분(샘플, 개발용).
- 끝 판정: `END_INDEX >= list_total_count` 또는 마지막(부분/빈) 페이지.

**완전성 점검(점검 게이트)** — 순회 종료 후:

| 조건 | status | 마커 | 다음 실행 |
|---|---|---|---|
| cap 없이 끝까지 + 수집건수 ≥ `list_total_count` | `ok` | `<short>.completed` | — |
| 끝까지 갔으나 건수 불일치 | `partial` | `<short>.incomplete` | **재수집** |
| `SEOUL_MAX_PAGES` 로 의도적 부분수집 | `partial` | `<short>.incomplete` | **재수집** |
| 순회 중 오류(네트워크/파싱) | `failed` | `<short>.incomplete` | **재수집** |

→ `completed` 마커만 "수집 완료". 미완료는 `incomplete` 마커로 남고, **매 실행이 전체 수집**이라
다음 실행에서 자연히 다시 받는다. 마커 JSON 의 `list_total_count`·`complete`·`rows_total` 로
어디까지 받았는지 점검한다.

**재수집/backfill 실행 방법**:

```bash
# 1) 평소 스케줄/수동 실행이 곧 전체 수집(= 자동 재수집). 별도 force 불필요.
airflow dags trigger commerce_collect_raw

# 2) 특정 논리일로(silver 파티션 override)
airflow dags trigger commerce_collect_raw -c '{"observed_date": "2026-06-01"}'
#    날짜 범위:
airflow dags backfill commerce_collect_raw -s 2026-06-01 -e 2026-06-07
```

> 인허가는 스냅샷 데이터라 과거일 backfill 도 "현재 전체 스냅샷"을 그 논리일 파티션(silver)으로
> 다시 받는 것이다(과거 시점 스냅샷이 API 에 없음). bronze 는 `run_id`(실행시각) 폴더로 쌓인다.

---

## 5. 데이터셋 카탈로그 (39종)

`service_name`(LOCALDATA 코드)이 채워진 것만 수집 대상이다 — 현재 **39종 전부 채워져 전 종
수집**(미해석 0). **job 단위 = API 단위**(`ingest_one[<short>]` 1 인스턴스 = API 1개 = 데이터셋 1개,
39 job). 코드 출처: 식품군 자동해석 · 의료/동물 13종 BPLCNM 확인 · 나머지 14종 포털 정본 코드(§6).
<!-- 표는 dags/domains/commerce/config/dataset_registry.yaml 기준. YAML 변경 시 갱신. -->
<!-- 인허가 외(위치정보/현황) 2종은 격리: docs/pipeline/non-license-datasets.md -->
<!-- food_hygiene_status(OA-13663)·medical_location(OA-20337) 는 더 이상 수집 대상 아님. -->

#### food

| short (slug) | oa_id | 데이터셋 | 주기 | service_name |
|---|---|---|---|---|
| `bakery` | OA-16084 | 서울시 제과점영업 인허가 정보 | daily | `LOCALDATA_072218` |
| `container_pkg_mfg` | OA-16081 | 서울시 용기·포장지제조업 인허가 정보 | daily | `LOCALDATA_072215` |
| `food_cold_storage` | OA-16074 | 서울시 식품냉동냉장업 인허가 정보 | daily | `LOCALDATA_072207` |
| `food_sale_etc` | OA-16080 | 서울시 식품판매업(기타) 인허가 정보 | daily | `LOCALDATA_072213` |
| `food_subdivision` | OA-16075 | 서울시 식품소분업 인허가 정보 | daily | `LOCALDATA_072208` |
| `food_transport` | OA-16076 | 서울시 식품운반업 인허가 정보 | daily | `LOCALDATA_072209` |
| `food_vending` | OA-16077 | 서울시 식품자동판매기업 인허가 정보 | daily | `LOCALDATA_072210` |
| `general_restaurant` | OA-16094 | 서울시 일반음식점 인허가 정보 | daily | `LOCALDATA_072404` |
| `group_meal_food_sale` | OA-16068 | 서울시 집단급식소식품판매업 인허가 정보 | daily | `LOCALDATA_072201` |
| `hfood_dist_sale` | OA-16069 | 서울시 건강기능식품유통전문판매업 인허가 정보 | daily | `LOCALDATA_072202` |
| `hfood_general_sale` | OA-16070 | 서울시 건강기능식품일반판매업 인허가 정보 | daily | `LOCALDATA_072203` |
| `instant_sale_mfg` | OA-16085 | 서울시 즉석판매제조가공업 인허가 정보 | daily | `LOCALDATA_072219` |
| `rest_restaurant` | OA-16095 | 서울시 휴게음식점 인허가 정보 | daily | `LOCALDATA_072405` |
| `tour_entertainment_bar` | OA-16092 | 서울시 관광유흥음식점업 인허가 정보 | daily | `LOCALDATA_072402` |
| `tour_restaurant` | OA-16091 | 서울시 관광식당 인허가 정보 | daily | `LOCALDATA_072401` |

#### livestock

| short (slug) | oa_id | 데이터셋 | 주기 | service_name |
|---|---|---|---|---|
| `livestock_processing` | OA-16072 | 서울시 축산가공업 인허가 정보 | daily | `LOCALDATA_072205` |
| `livestock_sale` | OA-16071 | 서울시 축산판매업 인허가 정보 | daily | `LOCALDATA_072204` |
| `livestock_storage` | OA-16087 | 서울시 축산물보관업 인허가 정보 | daily | `LOCALDATA_072224` |
| `livestock_transport` | OA-16088 | 서울시 축산물운반업 인허가 정보 | daily | `LOCALDATA_072225` |
| `meat_packaging` | OA-16073 | 서울시 식육포장처리업 인허가 정보 | daily | `LOCALDATA_072206` |

#### health_medical

| short (slug) | oa_id | 데이터셋 | 주기 | service_name |
|---|---|---|---|---|
| `affiliated_medical` | OA-16481 | 서울시 부속의료기관 인허가 정보 | daily | `LOCALDATA_010103` |
| `clinic` | OA-16480 | 서울시 의원 인허가 정보 | daily | `LOCALDATA_010102` |
| `hospital` | OA-16479 | 서울시 병원 인허가 정보 | daily | `LOCALDATA_010101` |
| `medical_similar` | OA-16486 | 서울시 의료유사업 인허가 정보 | daily | `LOCALDATA_010110` |
| `postpartum_care` | OA-16482 | 서울시 산후조리업 인허가 정보 | daily | `LOCALDATA_010104` |
| `safety_otc_drug_sale` | OA-16483 | 서울시 안전상비의약품 판매업소 인허가 정보 | daily | `LOCALDATA_010105` |

#### pharmacy

| short (slug) | oa_id | 데이터셋 | 주기 | service_name |
|---|---|---|---|---|
| `pharmacy` | OA-16484 | 서울시 약국 인허가 정보 | daily | `LOCALDATA_010106` |

#### animal

| short (slug) | oa_id | 데이터셋 | 주기 | service_name |
|---|---|---|---|---|
| `animal_hospital` | OA-16007 | 서울시 동물병원 인허가 정보 | daily | `LOCALDATA_020301` |
| `animal_medical_device_sale` | OA-16009 | 서울시 동물용의료용구판매업 인허가 정보 | daily | `LOCALDATA_020303` |
| `animal_pharmacy` | OA-16008 | 서울시 동물약국 인허가 정보 | daily | `LOCALDATA_020302` |
| `livestock_breeding` | OA-16012 | 서울시 가축사육업 인허가 정보 | daily | `LOCALDATA_020401` |

#### hygiene_beauty

| short (slug) | oa_id | 데이터셋 | 주기 | service_name |
|---|---|---|---|---|
| `barber_shop` | OA-16064 | 서울시 이용업 인허가 정보 | daily | `LOCALDATA_051901` |
| `bathhouse` | OA-16146 | 서울시 목욕장업 인허가 정보 | daily | `LOCALDATA_114401` |
| `beauty_shop` | OA-16063 | 서울시 미용업 인허가 정보 | daily | `LOCALDATA_051801` |
| `disinfection` | OA-16125 | 서울시 소독업 인허가 정보 | daily | `LOCALDATA_093011` |
| `laundry` | OA-16065 | 서울시 세탁업 인허가 정보 | daily | `LOCALDATA_062001` |

#### optical_dental

| short (slug) | oa_id | 데이터셋 | 주기 | service_name |
|---|---|---|---|---|
| `dental_lab` | OA-16489 | 서울시 치과기공소 인허가 정보 | daily | `LOCALDATA_010204` |
| `optical_shop` | OA-16490 | 서울시 안경업 인허가 정보 | daily | `LOCALDATA_010201` |

#### lodging

| short (slug) | oa_id | 데이터셋 | 주기 | service_name |
|---|---|---|---|---|
| `lodging` | OA-16044 | 서울시 숙박업 인허가 정보 | daily | `LOCALDATA_031103` |

---

## 6. service_name 채우는 법 (새 데이터셋 추가 시 — 현재 39종은 전부 채워짐)

서울 OpenAPI 응답 행에는 서비스명이 없다. 식품 판매/제조군은 `UPTAENM` 이 곧 업종명이라
스캔으로 자동 매칭됐지만, 비식품군(의료/약국/동물/공중위생/축산)은 `UPTAENM` 이 비거나
하위유형이라 **헤드리스 자동 매핑이 불가**하다. 절차:

1. 포털 데이터셋의 **Open API 탭** 샘플 URL에서 `LOCALDATA_NNNNNN` 코드 확인
   (예: `https://data.seoul.go.kr/dataList/OA-16484/A/1/datasetView.do` → 약국).
2. 후보 코드 정체 확인:
   ```bash
   SEOUL_API_KEY_COMM=... python -m bronze.resolve probe 0xxxxx
   ```
3. `dags/domains/commerce/config/dataset_registry.yaml` 의 해당 항목 `service_name:` 에 입력.
4. 실호출 검증:
   ```bash
   SEOUL_API_KEY_COMM=... python -m bronze.resolve verify
   ```
5. 이 문서 §5 표 재생성(같은 소스라 registry 만 고치면 됨).

식품군 추가 탐색 예:

```bash
SEOUL_API_KEY_COMM=... python -m bronze.resolve scan --prefix 07 --mid 22 24 --last 0 30
```
