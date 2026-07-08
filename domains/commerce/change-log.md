# Change Log — commerce 번들

`dags/domains/commerce/` 의 변경 이력. **작성일·순서 기준 내림차순**(최신이 위).
각 항목은 **최종 반영된 형태**를 기준으로 정리한다(중간에 되돌린 단계는 합쳐서 기술).

---

## 2026-07-08

### 49. silver v1/v2 정규화 + record_json 보존(API별 비공통) + raw→bronze→silver 전구간 검증

request:
- v1/v2 는 backfill 로 해결(bronze 문제 아니면 silver 만 재작업). silver 식별키=(OPNSFTEAMCODE, MGTNO).
- API별 비공통 필드도 key 로 매핑 보존 → gold 에서 API별 table화(무리한 병합/완전정규화 X). history 는
  전 필드 변화 SCD 추적. raw~bronze~silver 전구간 정상동작 확인.

response:
- **silver(dbt)**: `lf(v1,v2)` 매크로로 record_json 에서 정본(v1) 우선 coalesce 정규화 —
  mgtno/opnsfteamcode/status/date/address/coord 모두 v1(MGTNO…)·v2(MNG_NO…) 대응. **record_json 을
  current/history 에 보존** → API별 비공통 필드를 (dataset,opnsfteamcode,mgtno)로 매핑, gold 가 table화.
  history 는 content_hash(전 record) 기반이라 **전 필드 변화 SCD** 유지.
  (ASAC-DBT `feat/silver-v1v2-per-api`: `fe4c7ea`, `7c5db67`.)
- **bronze**: warehouse `iter_increment_rows` 가 v2(MNG_NO) row 를 page 봉투로 오인하던 버그 수정(`e7e1fcc`).
  `project_records` 는 `canonical_get` 으로 v2 mgtno/updatedt 채움(`39b4442`).
- **검증(실 dev Trino/Iceberg)**:
  - silver --full-refresh: history **1,344,778** · current **1,342,691**(v1 무회귀), record_json 전행 채움,
    dental_lab 등 비공통 필드 추출 확인, SCD(한 업소 3버전) 확인.
  - **v2 end-to-end**: env_consulting(093018, v2) raw **208행** → bronze Iceberg **208행**(publishable) →
    silver **208행·distinct 208(붕괴 없음)**, mgtno=MNG_NO·opnsfteamcode=OGDP_INST_CD·상태=SALS_STTS,
    record_json 보존, 비공통 `BPLC_SE_NM`=환경컨설팅회사 추출 확인.
- gold(API별 table화)는 record_json 기반 **후속 레이어**(염두).

### 48. 응답 컬럼 표준 v1/v2 대응 — 환경 13종(신형) 편입 + 양식변경 알림

request:
- (환경) 상권 인허가 13종 수집. datasetView 형식과 대조·상태값 처리기준 확인. key 가 달라도(신형 컬럼)
  기존값과 동일하게 맞추고 **silver 에서 처리**. key 틀어지면 양식변경+error 알림. 알림 호출부에 역할별 설명 추가.

response:
- 라이브 확인: 환경 13종은 **신형(v2) 컬럼 표준** — `MNG_NO`(=MGTNO)·`OGDP_INST_CD`(=OPNSFTEAMCODE)·
  `SALS_STTS_CD/NM`(=TRDSTATE)·`DTL_SALS_STTS_CD/NM`(=DTLSTATE)·`DATA_UPDT_YMD/LAST_MDFCN_YMD`·
  `ROAD_NM_ADDR`·`XCRD/YCRD`. 식별/상태/버전 값 **전부 존재(이름만 다름)**, 13종 전수 확인.
- 별칭 계약: `schemas.COLUMN_ALIASES_V2` + `canonical_get()`/`detect_row_format()`. `Dataset.fmt`(v1|v2) +
  registry `format` 필드. 환경 13종 `category=environment`·`format=v2` 등록(139→152).
- 정규화 위치 = **silver**(grain `(dataset, opnsfteamcode, mgtno)` 붕괴 지점 = 데이터 손실처). bronze 는
  raw 그대로 저장(손실 없음, 증분만 비효율). warehouse 는 `canonical_get` 으로 v2 mgtno/updatedt 채움.
- 알림: `notify_schema_drift`(양식변경→coped=warning / 미인식=error, §19.1 `[commerce][task>level]` 형식) +
  `NOTIFY_ROLES`(역할별 설명 양식) + `role_of()`. bronze 수집 루프에서 등록 fmt≠관측 fmt 시 1회 알림.
- 상태값 처리: 영업상태 `01 영업/03 폐업/04 폐쇄/05 제외사항`(v1 TRDSTATEGBN = v2 SALS_STTS_CD). 상세영업상태
  코드는 **API별 상이**(`11/2/4/BBBB/N…`, datasetView 정의 정본) — NM 동반이라 code→name empirical 확보 가능.
- 테스트: canonical_get/detect_row_format·format·schema_drift·role_of. 전체 pytest 300 통과.
- ⚠️ **silver dbt coalesce(v1↔v2)는 후속**(dbt submodule) — 이게 완료돼야 환경 13종이 silver 에서 정합.

### 47. 분류 2단계화 — 대분류(category) vs 명칭분류(sub_category) 분리

request:
- 명칭에 따른 분류와 대분류(industry)를 구분할 것.

response:
- `Dataset` 에 `sub_category` 필드 추가([schemas.py](include/commerce_core/schemas.py)) + [registry.py](include/commerce_core/registry.py)
  로더 반영. `category`=대분류(유지), `sub_category`=명칭 기반 세분류(대분류 하위, 미지정 허용).
- 산업 32종에 sub_category 부여: meter4·gas3·petroleum2·groundwater3·timber2·sales7·tobacco3·
  job_agency2·education2·funeral1·emission1·animal2.
- **전 139종 sub_category 부여 완료**(산업 32 + culture 56 + 기존 51). food(restaurant/bar/manufacturing/
  sale/meal_service/logistics/vending/bakery)·culture(sports/film_video/game/tourism/amusement/travel/music/
  publishing/performance/culture_arts/camping)·health_medical(institution/care/drug_sale/medical_device) 등.
- `test_registry.py::test_all_have_sub_category`(전 종 sub_category 필수)로 가드. 전체 pytest 296 통과.

### 46. 산업 상권 인허가 32종 추가 (107 → 139) + 라이브 검증

request:
- 산업 분류 상권 인허가(계량기/가스·석유/지하수/목재/각종 판매업/담배/직업소개/장례·요양 교육 등)를
  commerce 로 편입. raw~bronze~silver 이전과 같은 방향으로 적용.

response:
- 소스 34행 중 **중복 2쌍 제거** → 32종 등록(107 → 139), `category: industry`. 중복:
  `LOCALDATA_092701`(목재수입유통업=제재업)·`LOCALDATA_092809`(용기냉동기특정설비=액화석유가스용품제조업체)
  — 동일 코드+OA-id 라 각 1건만 등록(둘째 이름은 코드 재확인 필요, 플래그).
- **라이브 32/32 OK**(fail=0), 식별값 `OPNSFTEAMCODE/MGTNO/UPDATEDT/LASTMODTS` 전 종 존재 →
  bronze/silver 라인 그대로 유효(§45 분석 연장). 신규 최대 통신판매업 total≈932,949·담배소매 95,263.
- 계량기수리업 service_name 은 **비-LOCALDATA `repair092801`**(라이브 total≈113) — 테스트 allowlist 등록.
- `tests/test_registry.py` `EXPECTED_COUNT` 107 → 139 + 비-LOCALDATA allowlist. 전체 pytest 295 통과.
- 레지스트리 헤더 107 → 139 + category 에 `industry` 추가. DAG 는 registry 기반 → 139 job 자동 반영.

### 45. raw 107종 실측 검증 + 응답 필드 커버리지 분석 + bronze 라인 검증

request:
- 현재 기준 모든 raw 호출로 수집 정상 여부 확인. 각 API 응답을 공통/비공통 필드로 분리.
- 기존 bronze/silver 식별값이 107종에 그대로 유효한지 분석. raw·문서 정리 후 bronze/silver 라인 검증.

response:
- 107종 전량 라이브 샘플(1/5) → **수집 107/107 OK, 실패 0**. 응답 row 키 집계: 전 종 공통 14 +
  준공통 5(`DCBYMD`/`TRDSTATENM`/`SITETEL`/`SITEWHLADDR`/`SITEPOSTNO`, 1~2종 결측) + API별 비공통(45 스키마 변형).
- **식별값 `OPNSFTEAMCODE`·`MGTNO`·`UPDATEDT`·`LASTMODTS` = 107/107** → bronze 정렬/식별키·silver 그레인·
  `content_hash`·`TRDSTATEGBN`(상태) 그대로 유효. silver 는 `record_json` schema-on-read(`nullif`)라 비공통/누락 무손실.
- 문서: [docs/pipeline/bronze/api-field-coverage.md](docs/pipeline/bronze/api-field-coverage.md) 신설 + bronze README 인덱스 추가.
- 검증: 전체 pytest 295 통과 + 신규 culture 1종(`traditional_temple`) bronze raw end-to-end 스모크
  (격리 프리픽스 `_verify207` → status=ok, NDJSON+마커+diff-target 생성, 식별값 4개 보존, 검증 후 삭제).
- 주의: bronze 정렬키의 `OPNSFTEAMCODE`/`LASTMODTS` 포함(#198/#193)은 별도 브랜치 → 107종 완전 적용은 #193·#198
  머지 후. silver(dbt) 그레인은 이미 `(dataset, opnsfteamcode, mgtno)`.

### 44. 문화 상권 인허가 56종 추가 (51 → 107)

request:
- 문화 상권 인허가 56종(골프장·체육시설/영화·비디오/게임/관광·여행/공연/음악·음반/출판·인쇄·광고 등)을
  추가. "문화 카테고리지만 결국 상권"이라 commerce 로 편입. 기존과 같이 수집(중복·명칭 충돌 점검 포함).

response:
- `config/dataset_registry.yaml` 에 56종 추가(51 → 107), `category: culture`. LOCALDATA 코드·oa_id·short
  56종 전부 기존 51종/격리분과 중복 없음(기존 `031103`=숙박업 ≠ 신규 `031105/031107`=야영장). 코드 오름차순 배치.
- `tests/test_registry.py` `EXPECTED_COUNT` 51 → 107. 무결성 6종 통과(개수·short/service_name/oa_id 유니크·
  필수필드·`LOCALDATA_` 접두·daily 전량). 전체 pytest 295 통과.
- 레지스트리 헤더 51 → 107 + category 목록에 `culture` 추가. DAG 는 registry 기반이라 107 job 자동 반영.
- docs 상세 호출량 표(api-call-volume 등)는 107종 실측 재산정 후속.

### 43. raw 수집 대상 12종 추가 (39 → 51) + 레지스트리 무결성 테스트

request:
- 위탁급식영업/집단급식소/식품제조가공업/식품첨가물제조업/식용얼음판매업/단란주점영업/유흥주점영업/
  외국인전용유흥음식점업/의료법인/의료기기수리업/의료기기판매(임대)업/동물용의약품도매상 12종의
  raw 수집 라인이 누락 → 추가.
- 중복 없는지·총 51종 맞는지 확인. 기존 bronze 수집 결과와 명칭(short) 겹침 점검.
- 개발 후 테스트 및 로컬 commit(push 없음). branch: `207-raw-collect-etc`.
- (보건 외 문화 상권 50+종 추가 예정 — 후속 배치.)

response:
- `config/dataset_registry.yaml` 에 12종 추가(39 → 51). LOCALDATA 코드 12개 모두 기존 39종과 중복
  없음, `short`·`oa_id`·`service_name` 전부 유니크. 신규 short 는 기존/격리분과 미충돌 확인
  (group_meal_facility≠group_meal_food_sale, food_mfg≠instant_sale_mfg,
  entertainment_bar≠tour_entertainment_bar, medical_device_sale≠animal_medical_device_sale).
- `tests/test_registry.py` 신설 — 개수 51·short/service_name/oa_id 중복 없음·필수필드·`LOCALDATA_`
  접두·daily 전량 수집대상. 전체 pytest 295 통과.
- 레지스트리 헤더 주석 39 → 51 갱신. DAG 매핑은 registry 기반이라 51 job 으로 자동 반영.
- docs 의 상세 호출량("39종" 산정 표: api-call-volume 등)은 문화 배치까지 합쳐 일괄 재산정 예정.

## 2026-07-07

### 42. silver 마스킹 주소 동단위 매핑 스킵 + 품질 warning 알림 규칙

request:
- 주소 값이 `*` 로 마스킹된 행은 silver 단계에서 동단위 법정동/행정동 매핑을 스킵할 것.
- 해당 데이터가 발생하면 warning 레벨로 로그를 남기고, 전체 카운트와 정산건수, 전체 대비 비율을
  알림 인터페이스로 전달할 것.
- 사전에 인지한 품질 이슈 알림은 `[작업>에러레벨]` 단위로 묶고, 작업이 무엇을 하는지 쉽게
  설명하는 규칙을 `CLAUDE.md` 에 남길 것.

response:
- `silver_license_history` 의 `dong_token` 산출에서 `road_address` 또는 `jibun_address` 에 `*` 가 있으면
  `dong_raw` 를 null 로 만들어 신규/재처리 행의 `legal_dong/legal_code/admin_dong/admin_dong_code`
  매핑을 수행하지 않게 했다.
- `silver_license_current` 에서는 기존 history 행이라도 마스킹 주소이면 법정동/행정동 산출 컬럼을 null 로
  내보내도록 막았다. 기존 history 자체를 소급 정리하려면 `dbt run --full-refresh --select silver_license_history+`
  로 전체 재해석해야 한다.
- `silver.quality_tasks.notify_masked_address_dong_skip_summary()` 를 추가하고 `commerce_load_silver` DAG 에
  `notify_masked_address_summary` 태스크를 `dbt_run_silver -> dbt_test_silver` 사이에 연결했다. 집계는 Trino
  단일 aggregate 쿼리로 수행해 Airflow 메모리에 행 데이터를 싣지 않는다.
- `commerce_core.notify.notify_quality_event()` 를 추가해 `[commerce][<task>><level>]` 제목과
  `affected_rows/settled_rows/affected_ratio_pct` 중심의 품질 알림을 보낼 수 있게 했다.
- `CLAUDE.md` 에 사전 인지 품질 이슈 알림 규칙을 추가하고, 주소/행정구역 문서에 마스킹 주소 예외를
  반영했다.

### 41. silver 행정동 코드 컬럼명 확정 + 행정/법정 코드 시군구 prefix 불일치 error 알림

request:
- silver 산출 컬럼의 행정동 코드는 `admin_dong_code` 로 사용할 것.
- 시군구 코드가 행정동/법정동 코드 앞 5자리를 공유하는 것으로 보이므로, 연산 과정에서
  다른 값이 보이면 error 로그로 남기고 알림 인터페이스로 연결할 것.

response:
- dbt silver history/current 산출 컬럼을 `admin_dong_code` 로 정리했다. 기존 오타성
  `admin_dong_cod` 변경은 즉시 되돌렸고, `admin_code` 산출 참조도 `admin_dong_code` 로 맞췄다.
- `enrich_admin_dong_ref` 단계에 `_sgg_prefix_mismatches()` 검증을 추가했다. `legal_dong_code[:5]`,
  `admin_dong_code[:5]`, 산출 `sgg_code` 가 불일치하면 `admin_dong_sgg_prefix_mismatch` 이벤트를
  **error** 레벨로 기록하고, `notify_completion(..., level="error")` 로 알림 인터페이스에 연결한다.
  불일치가 있어도 적재는 계속 진행해 원천 이상을 관측 가능하게 남긴다.
- 문서(`dbt/docs/address-and-geo.md`)에 `admin_dong_code` 컬럼명과 시군구 prefix 불일치 error/알림
  정책을 반영했다. 회귀 테스트 1개를 추가했다.
- 검증: `compileall` 통과, `pytest test_silver_tasks.py` 5개 통과, `python -m security` 차단 이슈 0.

### 40. silver marker 증분 전환 + DAG 명칭 `commerce_load_silver` 정리 + gold 포장 계획

request:
- `dbt/domains/commerce` 와 commerce docs/changelog 맥락을 확인해 silver 레이어가 매번 전체 데이터를
  재적재하는 문제를 marker 기반 증분으로 바꿀 것. marker 가 없으면 해당 경로 전체 백필을 수행하되,
  대용량 백필에서 RAM/CPU 문제가 없도록 stream I/O 와 최소 병렬 처리를 적용할 것.
- Airflow UI 에 보이는 `commerce_localdata_transform`/`common_admin_dong_bronze` 를
  `commerce_load_silver` 단위로 취합해야 하는지 검토하고, bronze 유지가 맞으면 silver/gold 도달 계획을
  문서로 남길 것.

response:
- dbt silver history 를 `incremental` + `append` 로 전환하고 `silver_load_run_marker` 의
  `(dataset, bronze_run_id, status='DONE')` 를 silver 완료 marker 로 사용. 첫 실행/`--full-refresh`/
  target table 부재 시 `is_incremental()` 이 false 라 publishable bronze 전체를 백필하고, 이후에는
  DONE marker 가 없는 `bronze_run_id` 만 읽는다. dbt test 통과 후 Airflow `mark_silver_done` 태스크가
  DONE 을 기록한다. 증분 첫 행의 인접 중복 방지를 위해
  영향 key `(dataset, opnsfteamcode, mgtno)` 의 기존 최신 1행만 조인해 `content_hash` 를 비교한다.
- DONE marker 가 없는 후보 run 은 dbt pre-hook 으로 history 에서 선삭제 후 재삽입되도록 해
  dbt run/test 실패 후 재시도 중복 위험을 줄였다. 기존 history 가 있는 배포 환경은
  `ensure_silver_marker` 태스크가 marker 를 부트스트랩한다.
- dbt profile `threads: 1` 로 낮춰 동시 warehouse 쿼리를 제한했다. full backfill 도 Trino/Iceberg 쿼리
  안에서 수행하고 Airflow/Python 메모리에 전체 데이터를 올리지 않는 구조로 유지했다.
- DAG 파일/ID 를 `commerce_load_silver.py` / `commerce_load_silver` 로 정리했다. 내부 흐름은
  `[enrich_admin_dong_ref, enrich_fill_jibun] -> dbt_run_silver -> dbt_test_silver`.
- `common_admin_dong_bronze` 는 루트 `dags/` 의 공용 마스터 수집 DAG 이므로 commerce DAG 로 병합하지 않는
  것으로 결정했다. commerce 는 공용 raw `raw/common/admin_dong` 최신본을 읽어 자기 스키마의
  `bronze_ref_admin_dong` 만 갱신한다.
- 운영 문서(`dbt/docs/rebuild-and-ops.md`)와 초보자/주소/README 문서를 marker 증분·full-refresh 백필
  계약으로 갱신하고, [docs/pipeline/silver-gold-load-plan.md](docs/pipeline/silver-gold-load-plan.md) 를
  추가해 silver DAG 경계와 gold current 집계 포장 계획을 남겼다. 공식 근거는 dbt incremental,
  dbt-trino incremental strategy, dbt threads 공식 문서 링크로 표기했다.
- 검증: `compileall` 통과, `pytest test_silver_tasks.py test_load_plan.py` 12개 통과(승인 실행),
  `python -m security` 차단 이슈 0, `pytest test_security.py` 162개 통과. 로컬 `dbt parse` 는
  설치된 dbt 환경에 `dbt-trino` 어댑터가 없어 실행 불가(Airflow dbt venv 계약상 배포 환경에서 확인 필요).

### 39. 버전 정렬 1순위에 LASTMODTS 폴백 — UPDATEDT 결측 시 최종수정시점으로 정렬

request:
- UPDATEDT 가 없으면 "MODDT"(최종수정시점)로도 정렬되게 할 것 + MODDT 전건 존재 여부 확인.

response:
- 컬럼 확인: 원천 날짜/수정 컬럼은 UPDATEDT·LASTMODTS·APVPERMYMD·DCBYMD·APVCANCELYMD·
  CLGSTDT/CLGENDDT·ROPNYMD. "MODDT"에 해당하는 것은 **LASTMODTS**(별도 MODDT 컬럼 없음).
- 커버리지 실측: history 1,344,765행 전부 updatedt_ts·lastmodts_ts **100% 존재**(결측 0) →
  현재 UPDATEDT 결측 0건이라 폴백 필요 행 없음(순수 방어적 개선).
- dbt silver(feat/45): `updatedt_sort` 를 `coalesce(updatedt_ts, lastmodts_ts, epoch)` 로 변경
  (기존 `coalesce(updatedt_ts, epoch)`). UPDATEDT 없는 행이 epoch(최하위)로 밀리지 않고 LASTMODTS 로
  정렬됨. lastmodts_sort(2순위)·grain·dedup 불변. 재빌드 행수·테스트 15/15 **동일**(무영향 확인).
  schema.yml·timestamps-and-nulls.md 갱신.

### 38. silver gu/동 파싱 — 시도 접두 변형(서울시·무공백 결합형) 대응

request:
- 원천 주소 검증 결과 지번 동은 **법정동**(98.3% 매치)으로 확인 — silver 법정동 우선 분류가 정합.
  미매치 원인 중 `서울`(단축 접두)도 함께 대응해 달라는 요청.

response:
- 실측 접두 변형: `서울특별시`(표준·공백) 외 `서울시`(82) + **무공백 결합형**
  `서울특별시마포구`·`서울시노원구공릉1동`(구/동이 시도에 붙음)이 존재.
- dbt silver(feat/45) gu/동 regex 를 관대화: 접두 `서울(?:특별시|시)?` + `\s*`(공백 0개 이상),
  구는 lazy `[가-힣]+?구`(구로구구로동 결합형에서 첫 '구'까지만), 동은 `[가-힣]+\d*(?:동|가)` 로
  **번지 앞 정지**(공덕2동461 → 공덕2동). 비서울·마스킹(`당산동*가`) 주소는 여전히 null(정상).
- 효과(재빌드 실측): gu 98.6→**99.72%**, legal_dong 98.68→**99.06%**(+5,127행). dbt 15/15 통과.
- 남은 null(~0.9%)은 원천 마스킹(56%)·비서울 주소(28%) 등 **원천 한계**(코드 결함 아님).
- 규약 문서 갱신: dbt `docs/address-and-geo.md` §2(gu/동 regex·접두 변형 대응).

request:
- ① Juso 로도 못 찾는 비정형 주소(미해결)의 수집 시 로그 + 처리 결과 값을 **성공/완료 알람
  인터페이스로 연결**할 것. ② MGTNO 충돌 378건 재검토 — 승계·개명이면 같은 MGTNO 에 업장명이
  바뀌는 게 정상 아닌가(MGTNO 가 키, 업장명은 키가 아님)라는 문제 제기.

response:
- ① **알림 연결**: `commerce_core/notify.py` 에 `WebhookNotifier`(discord|generic,
  `COMMERCE_NOTIFY_WEBHOOK_URL/KIND` env 팩토리 — URL 은 register_secret 마스킹 등록,
  전송은 netio.http_post) + `notify_completion()`(성공=info/미해결 있으면 warning,
  처리 결과 값 + 미해결 상위 10건) 추가. `enrich_fill_jibun` 종료 시
  `jibun_fill_unresolved` log_event(미해결 주소·행수·샘플 20건) 기록 후 notify_completion
  호출로 배선. URL 미설정이면 기존과 동일한 no-op(로그만). 테스트 8종 추가(총 286 통과).
- ② **재판정(사용자 지적 타당)**: 같은 MGTNO 의 업장명·상태 변경은 U 증분의 정상 버전
  이력이 맞음(키 충돌 아님). 378건 분해 실측 —
  - 323건(rest_restaurant): 같은 스냅샷 안 **공백 변형 중복**(주소 끝 공백 차이, 이름·시각
    동일) → 같은 업소, current 가 1행 선택으로 이미 무해.
  - **55건(tour_restaurant 54·optical_shop 1): 서로 다른 구청(OPNSFTEAMCODE)이 같은 MGTNO
    발급** — MGTNO 는 자치단체 안에서만 유니크. 별개 업소가 병합되던 실 손실 케이스.
- ② 수정(dbt feat/45): silver 에 `opnsfteamcode` 추출 + 업소 식별키를
  **(dataset, opnsfteamcode, mgtno)** 로 확장(history dedup 파티션·current row_number·
  grain 테스트 3종·schema/README/가이드 갱신). 재빌드 후 current +119행(교차 구청 업소 보존,
  예: CDFI3261042006000001 → 7개 구 7개 업소 각 1행), dbt 테스트 15/15 통과.

request:
- bronze 확인 결과 주소는 전체 문자열(시군구·동 분리 필드 없음), 행안부 법정동코드 미제공 →
  ① lodging 처럼 지번 필드가 비는 경우 도로명으로 지번을 채울 것(Juso API, 결과 안 나오면
  정규식 케이스를 늘려 결과가 나올 때까지 복합 처리). null 이었던 규모와 API 호출 횟수를
  로그로 남길 것. ② district→gu rename + gu_code 추가, 동은 법정동/행정동 판별해
  legal_dong/admin_dong 에 넣고 반대쪽은 R2 `raw/common/admin_dong` 최신본 매핑으로 채울 것
  (legal_code/admin_code 포함). ③ 중부원점 X/Y → 위경도 계산 컬럼 추가(순수 계산이면 그걸로 마무리).

response:
- **enrich 태스크 2종 신설**(`include/silver/enrich_tasks.py`, transform DAG 이
  `[enrich_admin_dong_ref, enrich_fill_jibun] → dbt run → dbt test` 로 배선):
  - `bronze_ref_admin_dong` — R2 raw/common/admin_dong 최신(load_date→ingest_ts 최대) 페이지를
    파싱해 **서울만**(전국 2.1만 행은 Iceberg INSERT 커밋 비용 과다 — `ADMIN_DONG_SIDO_FILTER`)
    전량 교체 적재(769행). 코드값 문자열 고정, `sgg_code`=법정동코드 앞 5자리.
  - `bronze_address_enrichment` — 지번(SITEWHLADDR·LOTNO_ADDR 모두) 결측 도로명(유니크)만
    Juso 조회 후 캐시(키 delete-then-insert, filled 영구/not_found 래더판 스킵/error 재시도,
    500건 주기 플러시). 감사: 행 단위 pattern_id/attempts/api_calls/status +
    `jibun_fill_run` log_event(null_jibun_rows/distinct_addresses/api_calls/filled/...).
- **Juso 클라이언트**(`include/silver/juso.py`) — 정규화 래더 p1 괄호절단 → p2 콤마절단 →
  p3 도로명+번호 접두 → p4 '<구> <로> <번호>' 재조립 → p5 시도 생략. 첫 totalCount≥1 에서
  중단. `LADDER_VERSION` 갱신 시 not_found 재시도. 보안: netio.http_get(타임아웃·응답 캡),
  키는 env `JUSO_CONFM_KEY`(.env.commerce, 자동 마스킹). 단위테스트 13종(`tests/test_juso.py`).
- **dbt silver**(ASAC-DBT feat/45): 지번 채움 coalesce(원천→LOTNO_ADDR→Juso) +
  `jibun_address_source` 계보, `district`→`gu` rename + `gu_code`,
  동 토큰 법정동 우선 판별 → `legal_dong/legal_code/admin_dong/admin_code`(다대다는
  결정적 근사 — 숫자 제거 동명 우선·코드 오름차순), **EPSG:5174→WGS84 순수 계산**
  (`latitude/longitude`, 한반도 bbox 밖 null). 좌표계는 시청·GFC 랜드마크 실측으로
  5174 판별(42~90m vs 2097 230~251m). 규약 문서: dbt `docs/address-and-geo.md`.
- **transform DAG `_dbt_command` 수정**: 호스트 전역 `DBT_PROJECT_DIR`(smoke 프로젝트)이
  cwd 보다 우선해 프로필 오류를 내던 잠재 버그 — 커맨드에 `DBT_PROJECT_DIR` 명시 고정.
- 검증: pytest 281 통과 · security 게이트 PASS · DAG import OK · dbt run(134만 행)/test 13종
  통과 · 랜드마크 좌표 소수 7자리 일치(37.5665851, 126.9782039) · 동 매핑률 98.6%.

### 35. silver 타임존 정책 재확정 — timestamp 전부 KST 일원화(dbt) — #34 뒤집음

request:
- silver 레이어에 저장되는 시각을 **모두 KST 기준**으로 표기(기존 UTC → KST 변환).
  특히 collect time(`collected_at`)은 UTC 로 기록돼 다른 시각과 다르게 보였는데, silver 로 갈 때
  **KST 로 완전히 변환**되어 파일로 저장되어야 함.
- dags 브랜치 `feat/113` → `feat/133-commerce-silver-ingest` 로 rename 후 작업·push.

response:
- 직전 #34(UTC 일원화)를 **뒤집어** silver timestamp 전부 **KST** 로 재확정.
- **dbt(feat/45-silver-dbt-ingest)**: `updatedt_ts`/`lastmodts_ts` 의 `- interval '9' hour` 제거
  (원문이 이미 KST 라 파싱만·무변환), `collected_at` 은 bronze UTC 값을 `+ interval '9' hour` 하여
  KST 로 변환. `schema.yml`·`timestamps-and-nulls.md` §1·`beginner-guide.md` 를 KST 정책으로 개정.
- **bronze 는 UTC 원본 유지**(소스 진실) — 변환은 silver 표기 계층에서만. `warehouse._to_naive_utc()`
  및 source freshness(bronze `collected_at` 기준)는 변경 없음.
- 전 시각 컬럼이 동일 +9h 시프트라 정렬키·인접 dedup·grain 불변(dbt 테스트 4종 영향 없음).
- `medallion-implementation-plan.md` §2.2 타임존 항목을 KST 정책으로 갱신.

### 34. silver 타임존 정책 확정 — timestamp 전부 UTC 일원화(dbt)

request:
- 번거롭더라도 국제표준에 맞게 silver 적재 시 KST 시각을 **UTC 로 변환**해 일원화.
  collected_at 은 9시간 차이가 나는 형태이므로 silver 에서 바로 정합하게 넣을 것.

response:
- (사실관계 정정 후 반영) `collected_at` 은 수집 마커가 처음부터 UTC 로 기록한 값 —
  UTC 표준에서는 **무보정 통과**가 정답(+9h 는 이중 보정). 보정 대상은 KST 원문인
  UPDATEDT/LASTMODTS 쪽으로, 파싱 timestamp 에 `- interval '9' hour` 적용.
- **dbt(feat/45-silver-dbt-ingest)**: `updatedt_ts`/`lastmodts_ts` UTC 변환(-9h, 원문 문자열
  보존), collected_at 무보정 주석 명시, schema.yml 설명 갱신,
  timestamps-and-nulls.md §1 을 "silver timestamp 전부 UTC" 정책으로 개정
  (일별 집계는 KST 날짜 컬럼 기준 — UTC date 절단 금지 등 사용 주의 포함).
- 날짜 컬럼(observed_date/load_date/APVPERMYMD/DCBYMD)은 시간 정보가 없는 KST 달력
  날짜라 변환 비대상(정책 문서에 명시). 정렬·인접 dedup·grain 은 고정 오프셋이라 불변.
- medallion-implementation-plan.md §2.2 타임존 항목을 확정 정책으로 갱신.

## 2026-07-05

### 33. silver 암묵 버저닝 확정(dbt) 반영 + transform DAG 신설 + #109 잔재 import 수정

request:
- silver 는 명시 버전 컬럼(version_seq/valid_from/valid_to/is_current) 없이, 키(MGTNO) 안에서
  **UPDATEDT·LASTMODTS 내림차순 정렬이 곧 버전 순서**(암묵 버저닝)가 되도록 확정.
  current 는 그 정렬의 최신 1행. gold 가 나중에 이 정렬로 현재 상태 갱신만 수행.
- dags 쪽 문서를 변경 내용에 맞게 모두 수정하고, **오케스트레이션(transform DAG)도 설정**.
- 재빌드 시 특정 일자·특정 인허가 API(dataset) 단위 재적재/삭제가 **설정 파일 소폭 수정만으로**
  가능해야 함(dbt 쪽 적재형태·재빌드 정책 + 관리 문서 포함).
- dags 는 dev 기반 feat/113-commerce-silver-ingest, dbt 는 feat/45-silver-dbt-ingest 로 푸시.

response:
- **dbt(ASAC-DBT feat/45-silver-dbt-ingest)**: silver 2종 재작성(SCD2 제거, 정렬키
  UPDATEDT→LASTMODTS→observed_date→collected_at→content_hash, LASTMODTS 파싱 추가,
  '' → null 결측 규약) + 테스트 개정(행 유니크 grain (dataset,mgtno,collected_at,content_hash),
  인접 중복 0) + 단위 제외 vars 4종(exclude_datasets/observed_dates/load_dates/bronze_run_ids,
  macros/exclusions.sql) + 문서 4종(rebuild-and-ops·timestamps-and-nulls·dataset-columns·README).
- **transform DAG 신설**: `commerce_localdata_transform.py` — 05:00 KST(적재 04:00 이후),
  BashOperator 2단(dbt run silver → test silver), common_dbt_smoke 와 동일 dbt venv/env 계약,
  무상태(전량 재빌드 오케스트레이션만). `.env.commerce.example` 에
  COMMERCE_DBT_PROJECT_DIR/COMMERCE_DBT_TARGET 항목 추가.
- **#109 잔재 import 수정(파싱 불능 해소)**: `commerce_load_bronze.py`(common.env/registry/
  settings/storage → commerce_core.*, dags-root sys.path 부트스트랩 추가) ·
  `bronze/load_plan.py`(common→commerce_core paths) · `bronze/warehouse.py`(hashing/settings) ·
  `tests/test_load_plan.py`. 적재 라인 단위테스트 27 통과 재확인.
- **문서 갱신**: medallion-implementation-plan.md(§2 표, §2.2 계약 개정, Step 6·10 상태,
  §6 요약, §7-9 결정 반영), 번들 README(데이터 흐름 3단 DAG 라인·구조도).
- 단위 재적재/삭제 운영 계약: 재적재 = bronze 워터마크 파일(`_watermark.json`) 수정 →
  `commerce_load_bronze` → 자동 반영(전량 재빌드) / 삭제·복원 = dbt vars 수정 → `dbt run`.
  절차 문서: `dbt/domains/commerce/docs/rebuild-and-ops.md`.

## 2026-07-04

### 32. 적재 엔진 기준 = 첫 파일(순서), 크기/legacy 아님 — 07-01 증분 오분류 수정

request:
- (지적) 엔진을 파일 크기로 판단하지 말 것. **첫 파일이라 큰 것**이지 크기가 기준이 아니다.
  첫 파일이니 전체 재적재이고, 이후 파일을 PyIceberg 로 돌리면 안 된다.

response:
- **근거 확인**: 07-01 general_restaurant 파일은 row-NDJSON **280건(293KB)** 짜리 증분인데, 이전
  코드가 마커 `rows_total`(=534,748, API 전체수)을 파일 건수로 오독하고 `increment_mode` 없음을
  legacy 전량으로 취급해 PyIceberg 로 잘못 분류했다(06-30 은 page-NDJSON 534,680건 진짜 full).
- **수정(순서 기준)**: `resolve_load_plan` — 데이터셋 bronze 최초 적재(워터마크 없음)의 **첫 파일 =
  전체 재적재(PyIceberg)**, 그 이후 모든 파일 = **증분(Trino)**. 파일 크기/legacy 로 판단하지 않는다.
  기대 건수는 마커 `increment_count`(신뢰 가능)만 사용, `rows_total` 절대 미사용(없으면 rows>0 판정).
  (앞서 잠깐 넣었던 파일-크기 기준 및 `Storage.size` 추가는 되돌림 — abstract 메서드가 테스트 파손도 유발.)
- **검증(실데이터)**: 워터마크 06-30 상태에서 계획 = 27유닛 전부 Trino 증분·PyIceberg 0.
  general_restaurant 07-01 을 Trino 로 실적재 → **280건**(534K 아님), 534,680 → 534,960(+280).
  단위테스트 **266 통과**, security 차단 0.

### 31. commerce_load_bronze 통합 검증 — 이미지 리빌드 + 동시성/receipt 수정 (전 39종 적재 성공)

request:
- 리빌드까지 진행하고 검증. 실패하면 조치하고 성공할 때까지 반복.

response:
- **이미지**: `feat/7-dockerfile-pyiceburg` 의 pyiceberg[s3fs] 0.11.1 로 이미지 리빌드·재기동 →
  pyiceberg/pyarrow/trino 임포트 확인.
- **통합 검증 중 발견·수정 2건**:
  1. `load_state.write_receipt` 가 `bronze_run_id` 키를 찾다 KeyError(load_unit 결과는 `run_id`) →
     `run_id` 폴백 추가.
  2. **Iceberg 동시 커밋 충돌**: 39 load_one 이 동일 테이블에 병렬 append → `CommitFailedException`
     (낙관적 동시성)으로 28/39 실패. → `load_one` **직렬화**(`max_active_tis_per_dagrun=1`) +
     `load_unit_pyiceberg` 를 **트랜잭션 1커밋**(delete+청크 append 묶음, 스냅샷 폭증/충돌창 제거).
- **재적재 가능 검증**: bronze 테이블 drop + 상태파일 삭제 → 워터마크 없음(전체 재적재 조건) 확인 후
  재실행. (사용자 "iceberg/parquet + 상태 삭제 후 재적재 가능" 설계 실증.)
- **결과(dev, max_dates=1, 06-30 legacy 전량)**: **load_one 39/39 성공·0 실패**, bronze
  **1,342,222행 / 39종 / 1,341,784 유니크 mgtno**, manifest 39 전부 publishable, 워터마크 39·pending 0·
  receipt 39. record_json `json_extract_scalar` 파싱 정상(BPLCNM·TRDSTATENM) → silver 준비 완료.
  단위테스트 **264 통과**, security 차단 0. (직렬 백필 ~17분 — 대부분 general_restaurant 53만건.
  일일 증분은 소량이라 빠름. 07-01 이후는 재실행이 catch-up.)

### 30. commerce_load_bronze 크래시 수정 — page-NDJSON(과거 데이터) 적재 지원

request:
- commerce_load_bronze 첫 실행이 죽음. 원인 분석 요청. (판단) 과거 데이터를 스킵하지 말고
  **현재 코드로 읽어 적재**하도록 변경. pyiceberg 는 최신 stable(0.11.1)로 이미지 반영(별도 host 브랜치).

response:
- **원인**: 워터마크 없는 첫 실행이 raw 를 처음부터 스캔 → `2026-06-30`·`2026-07-01` run 이
  **feat/58 이전 page-NDJSON**(줄=API 페이지 응답, 마커에 increment_mode 없음). 로더가 이를
  스킵하지 않고 **ValueError 로 태스크를 죽임**(계획은 스킵인데 구현이 raise). 게다가 35/39
  데이터셋은 row-NDJSON 증분 파일이 없어(07-02/03 identical) 스킵만으론 bronze 가 빈다.
- **수정(과거 데이터 적재)**: `iter_increment_rows` 를 **두 포맷 모두 지원**으로 변경 —
  row-NDJSON(줄=레코드) + page-NDJSON(줄=페이지 응답 → `parse_page(...).rows`, service_name 필요).
  `resolve_load_plan` 은 워터마크 이후 **완료 run 을 시간순 전부 적재**(legacy 전량=PyIceberg,
  changed=Trino, identical=적재없이 전진). diff_target 우회/legacy 스킵 제거.
- **검증**: 실제 데이터 드라이런 — 66 유닛(legacy 62 PyIceberg + changed 4 Trino), general_restaurant
  06-30 legacy 파일에서 실제 레코드 파싱 확인(534,680건, 전체 인허가 컬럼). 단위테스트 **263 통과**,
  `python -m security` 차단 0.
- **pyiceberg**: 이미지에 미설치 확인(trino·pyarrow 는 있음) → host repo `feat/7-dockerfile-pyiceburg`
  브랜치에서 `Dockerfile.airflow` 에 `pyiceberg[s3fs]>=0.11.1,<0.12` 추가(별도 작업). requirements.txt 동기화.

### 29. bronze 적재 — 수집과 분리된 commerce_load_bronze DAG(PyIceberg/Trino) + 파일 상태 (Step 2~3)

request:
- medallion(#28) Step 2~6 구현. dags=ASAC-DAG `feat/113-commerce-bronze`, dbt=ASAC-DBT
  `feat/38-commerce-Bronze-Layer-ingest`.
- (재설계 지시) 수집 DAG 에 적재를 직결하지 말 것 — 대용량에서 Trino OOM. raw 와 bronze 를 분리,
  적재는 어느 raw 를 읽었는지 이력관리(재적재 가능·raw 와 격리된 공간). complete 없어 못 읽은
  일자·대상은 기록 후 다음 실행 재시도(현재-2일 재감시, 3일 폐기). 어느 날짜까지 적재했는지 파일로
  관리(없으면 처음부터 전체 재적재). **전체 재적재는 PyIceberg, 그 외는 Trino 증분**(한 번에 모든
  날짜 X). **RDB 사용 금지.** 로더 content_hash dedup 은 증분 경로에서 무의미 → 미적용.

response:
- **수집·적재 분리**: `commerce_raw.py` 는 raw-only 로 복원(이전 결합 되돌림 — Trino 무의존).
  신규 **[commerce_load_bronze.py](commerce_load_bronze.py)** DAG 가 적재 전담
  (`resolve_plan → plan_units → ensure_warehouse → load_one.expand → finalize`).
- **적재 엔진 [include/bronze/warehouse.py](include/bronze/warehouse.py)**: mode=first(전체 스냅샷)
  → **PyIceberg**(R2 Data Catalog REST + R2 S3 FileIO, `delete`+Arrow `append`, 커밋 1회 — Trino
  코디네이터 우회로 OOM 회피), mode=changed(소량) → **Trino** 증분(`?` 파라미터 바인딩). 단일 테이블
  `commerce.bronze_localdata_license`(39종 dataset 컬럼 구분) + `bronze_collection_run_manifest`
  (데이터셋별 발행 게이트). row-NDJSON 라인 스트리밍, canonical sha256 content_hash(컬럼 보존, 로더
  dedup 없음). 식별자만 assert_identifier 보간(allow-sql).
- **파일 상태(RDB 없음) [include/bronze/load_state.py](include/bronze/load_state.py)**: raw 와 격리된
  `{prefix}/commerce_bronze_state/`(commerce_ prefix) 에 워터마크(데이터셋별 마지막 적재 run) +
  pending(complete 없는 (date, short), 현재-2일 재감시·3일 폐기) + receipt(적재 감사 로그).
  **[load_plan.py](include/bronze/load_plan.py)**: `resolve_load_plan`(무손실 skip — diff-target 은
  완료 run 에서만 전진하므로 incomplete 건너뛰어도 무손실), `commit_watermark`(적재 성공분까지만
  전진, 실패 run 직전 정지 → 다음 실행 재시도). 실행당 `COMMERCE_LOAD_MAX_DATES`(기본 3) 바운드.
- **격리·명명**: Iceberg 물리 저장은 R2 Data Catalog 관리(폴더=UUID, 클라이언트 지정 불가) —
  구분 핸들은 논리 스키마 `commerce`. 상태/이력 파일만 `commerce_` prefix 로 직접 관리.
- **env/deps**: `.env.commerce.example`(COMMERCE_SCHEMA·COMMERCE_BRONZE_STATE_LAYER·
  COMMERCE_LOAD_MAX_DATES·TRINO_*·R2 Data Catalog), `requirements.txt` 에 `trino`·`pyiceberg[s3fs]`
  추가(**이미지 추가 필요**).
- **테스트**: `test_warehouse.py`·`test_load_state.py`·`test_load_plan.py` 신규 — 전체 **260 통과**
  (239→+21), `python -m security` 차단 0. 두 DAG(collect raw-only / load) 파싱·배선 검증.
  PyIceberg/Trino 실제 왕복은 이미지 통합(Step 4).
- **silver/gold(dbt)** 는 ASAC-DBT `feat/38` 에 별도 구현(Step 5~6): `dbt/domains/commerce/`
  (sources·silver_license_history SCD2·silver_license_current·테스트). Step 4/7 백필,
  Step 8 좌표 보정, transform DAG(Step 10)는 후속.

### 28. medallion 구현 계획 문서 — raw→bronze(Iceberg)→silver→gold + 좌표 보정 설계 (제안)

request:
- python/airflow/iceberg/dbt/trino/R2 환경에서 bronze·silver·gold 역할 분담과 "raw 적재를
  bronze 레이어로 통합할지"를 검토하고, 단계별 to-do 의 **구현 방법까지 정의한 docs** 요청.
- 추가 요구: 업장 좌표(중부원점 TM 추정)와 주소 기반 **재보정 위경도**를 silver 에서 조인하도록
  설계 포함(어떤 로직/API 로 보정할지 확인 필요). 제약: `dbt/` 하위 타 도메인 폴더는 타인
  작업물이라 의존·병합하지 않음, dbt commerce 프로젝트는 미구현 상태에서 신규 설계.

response:
- **[docs/pipeline/medallion-implementation-plan.md](docs/pipeline/medallion-implementation-plan.md) 신설**(제안·승인 대기 항목 포함):
  raw=R2 랜딩(현행 유지, #75/#21 결정 준수) / bronze=Iceberg `commerce.bronze_localdata_license`
  단일 변경로그 테이블(record_json 통짜 + 계보 컬럼, `load_date` 파티션, delete-then-insert 멱등,
  dataset 단위 manifest 발행 게이트) / silver=dbt SCD2(`silver_license_history`·
  `silver_license_current`, 전순서 버전 정렬 + 연속 중복 제거로 A→B→A 원복 보존) /
  gold=`gold_commerce_license_status_current`(현황 스냅샷, 일별 추이는 phase-2). **raw→bronze 통합 안 함**
  (Trino 는 iceberg 커넥터뿐이라 JSONL 직질의 불가 — 부족한 것은 적재 단계).
- 백필·분기 정합성 복구는 `_diff_target` 전체본 기반 `mode=full_reconcile` 단일 코드 경로.
- **좌표 보정 설계**: 원천엔 X/Y(좌표계 미표기)뿐, 위경도 없음 확인 → 2트랙(좌표 변환 pyproj
  ·주소 지오코딩 API)+EPSG(2097 vs 5174) 실측 판별, `commerce_geocode_collect` 신규 수집 라인
  (raw 보존 → `bronze_geocode_address`, address_key 멱등·증분), silver 에서 LEFT JOIN 으로
  `lon/lat_corrected`·`location_source`·`location_quality` 산출. API 후보(VWorld/Kakao/Naver/juso)
  비교·약관/쿼터 체크리스트는 확정 전 조사 항목으로 명시.
- Step 1~10 순차 to-do(각 구현 방법·완료 기준) + 결정 대기 7건(§7) 정리.
  인덱스 갱신: docs/pipeline/README.md · docs/README.md 문서 맵.

### 30. HTTP 호출 경계를 공통 클라이언트로 전환 (#78)

request:
- 소스 API 호출을 `dags/common/http`(#78 HttpCore)로 통합. 단, commerce 는 §20 보안
  게이트가 `netio.http_request` 사용을 명시하므로 그 커버리지(SSRF 가드·응답 상한·TLS
  강제·예외 마스킹)를 잃지 않을 것.

response:
- `include/bronze/clients.py` 의 `SeoulOpenApiClient` 수동 재시도 루프를 HttpCore 로 대체.
  **netio 를 HttpCore 의 Transport(`_NetioTransport`)로 감싸** §20 커버리지를 그대로 유지하고,
  그 위에 통합 재시도(429/5xx+연결오류)·redaction 로깅·rate limit·typed 예외(HttpProblemError)를
  얹음(합성 — HttpCore Transport 계약의 의도된 확장점).
- **업무 오류 분류(INFO-000/100/200 등, `parse_page`)는 도메인에 그대로 유지** — HTTP 200
  응답 본문에서 판정, HttpCore 는 전송/HTTP 상태만 담당. 재시도 소진/HTTP 오류는
  `SeoulApiError("ERROR-NETWORK", redact(...))` 로 변환해 bronze 마커 계약 보존.
- `rate_limit=None` — 기존 `SEOUL_REQUEST_DELAY_SECONDS` 간격 유지(이중 지연 방지).
- (#78 코드리뷰 반영) `security/redaction.py` structural 패턴에 `serviceKey=` 쿼리 키 추가
  (공공데이터포털/KMA 형식 — dags/common/security 로 재복사), `parse_page` 의 비정수
  `list_total_count` 를 `SeoulApiError("ERROR-PARSE")` 로 래핑(원시 ValueError 누출 차단),
  `bronze/resolve.py` CLI 에 dags 루트 부트스트랩 추가(`python -m bronze.resolve` 복구).
- 검증: commerce 241 테스트 통과, 오프라인 스모크(성공·재시도·소진·업무오류 4경로) 통과,
  URL 경로 키가 HttpCore 로그에서 마스킹 확인.

### 29. 서울 base URL env 이름 통일 — SEOUL_OPEN_API_BASE_URL (#78)

request:
- 루트 `.env` 통합 원칙(#72)에 따라 base URL env 이름도 루트 이름
  `SEOUL_OPEN_API_BASE_URL`로 통일할 것 — 사용자 결정.

response:
- `settings.py` 읽기 훅 `SEOUL_OPENAPI_BASE_URL` → `SEOUL_OPEN_API_BASE_URL` 개명,
  configuration.md·test_security.py 예시 동반 개명. 실환경 값 이관 불필요 — #72 때
  `.env.commerce` 항목은 이미 삭제(코드 기본값과 동일)돼 코드 훅만 남아 있었음.
- 공용 서울 어댑터(`dags/common/http/seoul.py`)도 같은 이름 하나만 읽는다.

### 28. include/common → include/commerce_core 개명 + storage 승격 (#109)

request:
- dags/common(공통 상위 패키지, #77 에러 모듈)과 commerce top-level `common` 이 단일 프로세스
  DagBag 로드(`airflow dags test` 등)에서 충돌(sys.modules 캐시가 `import common.errors` 를 가림).
- 방침(Q&A 합의): **dags/common 을 상위 개념으로** 보고 **겹치는 기능만 먼저 합침**,
  나머지 모듈은 **무삭제 보존**(notify 포함), 잔류 패키지 이름은 `commerce_core`.

response:
- **storage 승격**: `storage.py` 의 범용 부분(Storage/LocalStorage/R2Storage)을 `dags/common/storage.py`
  로 이동(+`build_storage` 팩토리, Settings 결합 제거). commerce 쪽은 `commerce_core/storage.py`
  얇은 어댑터로 대체 — 클래스 재수출 + `get_storage()` 가 기존 Settings/env 계약 그대로 유지(동작 불변).
- **개명**: `include/common/` → `include/commerce_core/` (env/hashing/notify/paths/registry/schemas/
  settings 전 모듈 보존). 소비처 17파일 import 전환(`from common.*` → `from commerce_core.*`),
  DAG·scripts·conftest 부트스트랩에 dags 루트 추가(`common.storage` 해석용).
- **금지 규약 추가**: 번들 안에 top-level `common` 패키지 재도입 금지(CLAUDE.md §19 명시).
- 검증: commerce 전 테스트 239 통과, `python -m security` 차단 0, 단일 프로세스 전체 DagBag
  파싱 재현 테스트 통과(충돌 해소 확인).

### 27. 취약점 코퍼스 — 정적 detector 20종 발화 증명 + 격리 (feat/96)

request:
- 보안 플러그인 검증/증명 목적으로 **취약점 코드(코퍼스)** 작성. 자기 프로젝트라 방어적 용도의
  취약 샘플 생성은 문제 없음. (Q&A) 산출물 형태 = **취약점 코퍼스**(detector 별 취약 샘플 +
  self-audit 격리 + "스캔하면 N건 검출" e2e 테스트).

response:
- **격리 설계(무침습)**: 샘플을 `tests/vuln_corpus/*.pysample` 로 저장. self-audit(`_iter_files`)는
  `.py/.md/...` 확장자만 스캔하므로 `.pysample` 은 **불가시** → 취약 코드가 트리에 있어도 게이트
  청결 유지(audit.py 수정 0). e2e 테스트가 각 샘플을 tmp 에 실제 파일명(`.py`/`requirements.txt`/
  `.env.commerce.example`/`.gitignore`)으로 복사해 매핑 detector 발화를 잠근다.
- **다중 에이전트 워크플로**(map→author→적대적 검증): audit.py 의 `STATIC_CHECKS` 20종을 매핑,
  detector 별 **읽기 쉬운 취약 샘플**을 병렬 저작하고 각 샘플을 실제 정규식과 대조해 발화 예측 검증.
  22개 샘플 산출(credential_material 은 PEM·벤더토큰·URL userinfo 3종) → **정적 detector 20종 전수 커버**.
- **push-safety**: 벤더 토큰(`ghp_…`)·PEM 개인키·bidi 제어문자는 커밋 시 GitHub push protection 을
  건드리므로 커밋 금지 — 샘플엔 `__ASSEMBLE__` placeholder, 테스트가 조각 결합으로 런타임 조립
  (pem/ghp_token/bidi). 나머지는 provider 포맷 아닌 **합성 값**이라 커밋 안전(전 파일 push-safe 스캔 통과).
- **Ground-truth**: 22 샘플을 조립·materialize 해 매핑 detector 실행 → **전수 발화 확인**(fires + detail 일치).
- **검증**: 전 테스트 **239 통과**(213→+26: 파라미터화 발화 22 + 완전성/격리/게이트청결/통합스캔 4),
  `python -m security` 차단 0(코퍼스 무누출 확인). `test_every_static_detector_has_a_corpus_sample` 로
  신규 detector 추가 시 대응 샘플 누락을 자동 강제. 문서: [tests/vuln_corpus/README.md](tests/vuln_corpus/README.md).

### 26. 보안 플러그인 정밀 리뷰 — opus 구현부 6개 결함 수정 + 회귀 잠금 (feat/96)

request:
- 어제(fable) 진행 중 **opus 로 구현된 영역**에 미흡한 부분이 있으니, 해당 영역을 확인해
  **전체 조치 후 테스트**까지 진행할 것.

response:
- **적대적 정밀 리뷰**로 opus-era 모듈에서 실동작 결함 6종 확인·수정(전부 commerce 번들 안):
  - **P1 (HIGH) TLS 가드 우회** — `netio.http_request` 가 `verify is False` 만 차단해
    `verify=0`/`""` 로 인증서 검증 비활성이 통과. → falsy 전부 차단(None=기본 위임·truthy=CA 경로만 허용).
  - **P2 (HIGH) IPv6 SSRF 우회** — `::7f00:1`(IPv4-compatible `::/96`)·6to4/teredo 내장 IPv4 가
    미검사로 루프백/IMDS 도달 가능. → `::/96` 차단 + `sixtofour`/`teredo` 내장 IPv4 를 v4 정책으로 재검사.
  - **P3 (MED) dict '키' 미마스킹** — `Redactor.redact`·`events._json_safe` 가 값만 가리고 키는
    통과 → 시크릿이 키로 오면 누출. → 두 경로 모두 문자열 키 마스킹(등록 시크릿 포함 키만 치환, 일반 필드명 보존).
  - **P4 (MED) 비밀번호 검증 크래시** — `verify_password(None)`/`needs_rehash(None)` 가
    AttributeError(DB NULL → 로그인 DoS). 문서화된 "깨끗한 거부"와 모순. → 비문자열 저장값을 크래시 없이 거부(False)/재해시(True).
  - **P5 (MED) 이식성 회귀** — `audit.py` 의 `(?i:...)` 스코프 인라인 플래그가 **Python 3.11+ 전용**
    → <3.11 프로젝트에서 audit import 시 `re.error`(플러그인 이식성 목표와 모순). → 문자클래스로 재작성(3.9+ 동작).
  - **P6 (MED) 게이트 CLI 크래시** — 문서화된 `python -m security` 가 Windows **cp949** 콘솔에서
    `—`(em-dash) 로 `UnicodeEncodeError`. → 비UTF-8 콘솔에서 UTF-8 buffer 폴백(출력 보장).
- **회귀 테스트 6종(20 파라미터 케이스) 추가** — 각 수정을 잠금(자기감사 회피 위해 위반 샘플은 조각 결합/tmp).
- **검증**: 전 테스트 **213 통과**(190→213), `python -m security` 차단 0(경고 3건은 CLI 단독 실행 런타임
  점검 = 정상). cp949 콘솔에서 게이트 CLI 정상 출력 확인.

### 25. 보안 플러그인 커버리지 확장 — SSRF·아카이브·암호·정적점검 11종 + 적대적 검증 (feat/96)

request:
- 추가 개발된 보안 모듈의 커버 영역 **외의 다른 취약점**도 커버할 수 있게, **최신 보안
  가이드라인**을 이용해 추가 대응 모듈 구성.
- 사용법 문서와 **적용 기술 목록·해설 문서**를 security 문서로 남길 것. 커밋·push.

response:
- **가이드라인 리서치**(다중 에이전트 웹 조사): OWASP Top 10:2025(A03 공급망·A10 예외처리
  신설, SSRF→A01), CWE Top 25 2025(SQLi #2·경로탐색 #6·자원무제한 #25 신규), ASVS 5.0.0,
  PEP 706·Trojan Source(CVE-2021-42574)·SSRF/비밀번호 저장 치트시트 확보 → SEC-01~20 계획.
- **신규 모듈 2종**: `crypto.py`(CSPRNG 토큰·상수시간 비교·PBKDF2-HMAC-SHA256 600k 비밀번호
  해시, NFKC 정규화·자기서술 인코딩·needs_rehash), `archive.py`(zip-slip·압축폭탄·심링크 차단
  안전 추출, PEP 706 filter, 스트리밍 바이트 재검증).
- **기존 모듈 확장**: `netio`(SSRF 가드 `assert_url_allowed` — 명시 CIDR 차단 IPv4/6·IMDS,
  호스트명 DNS 해석; 응답 크기 상한 `max_response_bytes`), `redaction`(URL userinfo 마스킹·
  `sanitize_log_value` 로그 인젝션 무력화), `log_filter`(`neutralize_controls` opt-in).
- **정적 점검 7→18종**: credential_material(PEM/벤더 토큰/URL 비번, CRITICAL)·trojan_source·
  sql_injection·unsafe_extract·insecure_file_ops·weak_hash·insecure_random·web_misconfig·
  xml_parsing·cleartext_http·requirements_hygiene 신설 + tls/yaml/dangerous 강화. 런타임 점검 4종.
- **적대적 검증**(다중 에이전트, 차원별 리뷰→독립 검증): 확인된 **16개 결함 전부 수정** —
  SSRF 호스트명 우회(resolve_dns 기본 True), 로그 포맷문자열 %-지정자 훼손(렌더 후 마스킹),
  tar 압축폭탄(next() 스트리밍+압축입력 상한), 정적 점검 우회(shell=True 멀티라인·SQL 내부
  따옴표·yaml 위치 로더·자격증명 라인공유·filter 부분일치·requirements 환경마커·weak_hash
  대문자·userinfo 토큰단독·verify_password 크래시·NFKC 누락 등).
- **검증**: 회귀 테스트 20건 추가 → **전 테스트 174 통과**, `python -m security` 차단 0
  (자기매칭 방지: 패턴은 조각 결합/chr()/이스케이프로 작성).
- **문서**: [docs/security/usage.md](docs/security/usage.md)(사용법) ·
  [docs/security/techniques.md](docs/security/techniques.md)(적용 기술 목록+해설·가이드라인 매핑)
  신규, security.md 위협 모델 확장, README·CLAUDE §20·Share.md 갱신.
- **커버리지 3차 확장(commerce 밖 조사 반영)**: sample 의 auth 백엔드(FastAPI)·notifications·
  dbt·DB IO 를 조사(auth 는 이미 파라미터화 ORM·CSRF·세션·레이트리밋으로 견고) → 플러그인이
  아직 못 잡던 **백엔드/DB IO 취약점 클래스**를 이식 대비로 추가: 신규 `dbio.py`
  (`assert_identifier` 동적 식별자 검증 · `mask_dsn` URL+libpq DSN 비밀번호 마스킹), 정적 점검
  2종(`no_sql_text_injection` SQLAlchemy `text()` 주입 HIGH · `open_redirect_advisory` CWE-601
  MEDIUM). 정적 점검 18→20종. 조치는 전부 **commerce 번들 안**에서 수행(플러그인이 이식원). 검증:
  전 테스트 **190 통과**, `python -m security` 차단 0.

### 24. 통합 보안 플러그인化 — install_security() 원샷 + net/file/API/이벤트 가드 (feat/96)

request:
- 1차: `dags/domains/commerce` 안에서 대응 가능한 **모든 보안이 적용**되게 처리.
- 2차: 전체 프로그램(모든 IT 프로젝트)에서 쓸 수 있는 **통합 보안처리 플러그인**으로 —
  현 폴더 구조를 유지한 채, network IO·file IO·log·stdout·예외처리·API receipt/response 를
  security 코드 **하나의 적용**으로 커버하도록 사전 대응(ready). 추후 common 폴더로 제공 시
  다른 프로젝트는 **받아쓰기만 하면 되는 수준**으로.
- 단, 로그 분석은 가능해야 함 — 취약점을 만들지 않는 경계에서 처리/에러 로그의
  기록·해석·전송이 되게 구조화. 이식 방법과 제공 기능을 정리한 보안 문서 작성.
- 브랜치: `feat/96-security-plugin`.

response:
- **플러그인 신규 모듈 6종** ([include/security/](include/security/), 전부 stdlib only·번들 비종속):
  `bootstrap.py`(**`install_security()` 원샷** — env 시크릿 적재 + 로그/stdout·stderr/
  sys·threading 예외훅 마스킹, idempotent·기동 비차단), `stdio_guard.py`(print·미처리
  트레이스백 마스킹), `netio.py`(`http_request/get/post` — timeout 자동 주입·TLS 검증 비활성
  차단·예외 args 스크럽 후 같은 타입 재전파), `fileio.py`(`safe_key`/`safe_join` 경로 주입
  차단 + `write_json_redacted` at-rest 마스킹 저장), `api_guard.py`(`api_receipt`/
  `response_summary`/`scrub_url·headers·params` — 저장 가능한 무시크릿 요청 영수증·응답 요약),
  `events.py`(`log_event`/`log_exception` — **마스킹된 단일 라인 JSON** 처리/에러 로그, 반환
  dict 는 알림 전송에도 안전 → 분석 가능성 유지). `redaction.py` 에 `scrub_exception`(예외
  체인 args 마스킹) 추가, audit 에 런타임 점검 3종(log/stdout/excepthook 설치 여부) 등록.
- **1차 wiring**: DAG(`commerce_raw.py`)·scripts 2종 → `install_security()`;
  bronze `clients.py` HTTP 호출 → `netio.http_request`(정책 단일점);
  `silver_tasks.build_silver` 경계에 `assert_safe_segment(short)`/`assert_iso_date` 추가.
  기존 redact 지점(마커 error·notify·재시도 로그)은 유지(이중 방어).
- **검증**: 신규 테스트 24케이스 포함 전 테스트 **106 통과**, `python -m security` 차단 0
  (런타임 설치 3종은 CLI 단독 실행에서 warn = 정상, 문서화).
- **문서**: [docs/security/security.md](docs/security/security.md)(위협 모델 11경로·모듈 표·
  로그 분석 경계·검증), [docs/security/adoption.md](docs/security/adoption.md)(받아쓰기 이식
  가이드 + common 승격 계약 + 에이전트 프롬프트), docs/security/README·CLAUDE.md §20·Share.md 갱신.

## 2026-07-02

### 23. DAG 명칭 변경 — commerce_localdata_{elt,recollect} → commerce_{collect,recollect}_raw

request:
- `commerce_localdata_elt` → `commerce_collect_raw`, `commerce_localdata_recollect` →
  `commerce_recollect_raw` 로 DAG 명칭 변경.

response:
- **DAG 파일**: dag_id·TaskFlow 함수명·하단 호출·docstring 의 이름 갱신. 파일명도 정합화 —
  `commerce_localdata.py` → **`commerce_raw.py`**(git mv; 두 raw 수집 DAG 공용, feat/73 선례
  "파일=dag 네이밍" 준수).
- **문서/코드**: 번들 문서 전반의 dag_id 참조 일괄 치환 + 기존 stale 파일 참조
  (`seoul_commerce_dag.py`)를 `commerce_raw.py` 로 정정, `markers.py` docstring 반영.
  change-log 과거 이력은 불변 유지.
- **검증**: 전 테스트 **82 통과**, 보안 게이트 PASS. 컨테이너 `dags reserialize` 후 신규 dag_id 2개가
  `commerce_raw.py` 에서 **import 오류 없이 등록** 확인. 옛 `commerce_localdata_*` 는 파일 삭제로
  메타DB 고아 등록으로 남음(실행 이력 보존, UI 에서 removed 표시 — `airflow dags delete` 는 선택).

### 22. 증분 저장 흐름 확정 — 랜딩→비교(조기중단)→증분→diff 이동(수집일 태깅)

request:
- R2 배치 점검 결과 `run_id=07-01` 이 full 을 그대로 들고 있는 어긋남 지적("위치가 정 반대").
  확정 흐름: 최초 run(06-30)은 비교 대상이 없으니 full 적재 + 그 내용이 diff 폴더에 복사(정렬본+해시).
  이후 run 은 ① API 결과를 run 경로에 **먼저 저장** → ② 정렬 → ③ diff 폴더 파일과 비교하며
  **다른 내용만 별도 위치에 저장**(정렬돼 있으므로 **같은 정보가 위치하면 비교 중단**) →
  ④ 구 diff 삭제 후 **오늘본을 run 경로에서 diff 로 이동**, **완료(이동됨)/중단(안 옮겨짐)을
  파일명의 수집일(연/월/일)로 구분**.
- (합의 Q&A) 증분 위치 = run 폴더 안(`run_id=X/<short>.jsonl`), full 랜딩 = `run_id=X/_full/`.
  이력 재정리 = run_id=07-01 full 을 06-30 대비 증분으로 교체(권장안 채택).

response:
- **paths.py**: `bronze_full_landing_key`(`_full/` 랜딩), diff-target 키를 **수집일 태깅**
  (`_diff_target/<short>.<YYYY-MM-DD>.jsonl` + 같은 이름 `.key`)으로 변경, `run_collect_date`·
  `diff_target_prefix`(발견용 접두) 추가.
- **incremental.py**: `diff_new_rows(stop_on_aligned_match=)` — 정렬 프런티어에서 같은 정보(키+내용)
  첫 일치 시 **비교 중단**(전제: 내용 변경 시 UPDATEDT 갱신). `find_diff_target` — `<short>.` 접두
  나열로 최신 날짜 diff 발견(구형 무날짜 인식). `incremental_store` 재작성: ①landing 업로드(수집분
  보존 우선) → ②비교 → ③증분만 run 폴더 저장(첫 수집=full) → ④새 날짜 diff copy→구 날짜 삭제→
  landing 삭제(**이동**; 새 파일 먼저 만들어 중단 시 자가 복구, 동일자 재실행 자기삭제 가드).
  identical 이어도 ④ 수행(diff 파일명 날짜=최신 완료 수집일).
- **bronze_tasks.py**: 수집일(run_id 파생, 비형식이면 observed_date 폴백)·prev 발견 배선, 마커에
  `diff_target_key` 추가. **storage.py**: `copy()` 재추가(ABC read+write, R2 `copy_object` 서버사이드).
- **이력 재정리**: `scripts/retrofit_run_increment.py` — run_id=07-01 full 을 06-30 대비 증분으로
  교체 + 구형 무날짜 diff 파일을 `<short>.2026-07-01.*` 로 리네임(dry-run 기본·`--apply`).
  `scripts/seed_diff_target.py` 도 수집일 태깅으로 갱신.
- **검증**: 단위테스트 확장(조기중단 소비량·발견 최신날짜/구형·랜딩 이동·동일자 재실행 가드·시드→
  identical) 포함 **전 82 통과**. 설계 문서 §1/3/4 갱신. 보안 게이트 PASS.
- **R2 재정리 실행 완료(실측)**: run_id=07-01 데이터 **1,248MB(39 full) → 0.48MB(증분 23파일)**
  — 무변경 16종은 파일 삭제(identical=마커만), 변경 23종만 증분(예: general_restaurant 534,748행→
  280행/293KB, bakery 12행 — 사전 실증치와 일치). `_diff_target` **78/78 전부 날짜 태깅**
  (`<short>.2026-07-01.*`, 무날짜 잔존 0). run_id=06-30 full(최초)·`_backup` 무손상.
  총 용량 5,195MB → **3,947MB**(약 1.25GB 회수).

### 21. 원천 레이어 리네임 bronze/commerce → raw/commerce + 레이어 접두 .env 관리

request:
- R2 데이터가 이미 `bronze/commerce/` → `raw/commerce/` 로 이동된 상태(`move_bronze_to_raw` 실행 —
  실측: bronze/commerce=0객체, raw/commerce=314객체[run 스냅샷·`_diff_target`·`_backup`])인데 **코드는
  아직 `bronze/commerce`** 를 써서 DAG 이 이동 데이터·시드 diff-target 을 못 읽는 어긋남 발생.
- (결정) **raw/commerce 로 통일**하고, 레이어 접두 값을 **.env 로 관리**하도록 변경.
- (팀 결정 #75) R2 오브젝트 원본 경로는 `raw/` 채택, Iceberg 테이블명·dag_id 의 `bronze` 명칭은
  유지 (용어: **raw = R2 랜딩 원본, bronze = Iceberg 웨어하우스 원본층**).

response:
- **paths.py**: `BRONZE_LAYER="bronze/commerce"` → `RAW_LAYER=os.getenv("COMMERCE_RAW_LAYER","raw/commerce")`,
  `SILVER_LAYER=os.getenv("COMMERCE_SILVER_LAYER","silver/commerce")`. 모든 경로 함수가 `RAW_LAYER` 사용.
  기본값 = 목표 경로라 env 미설정이어도 raw/commerce. (함수명 `bronze_*` 는 대규모 리팩터 회피 위해
  유지 — 경로 문자열만 raw 로 전환.)
- **.env.commerce(.example)**: `COMMERCE_RAW_LAYER=raw/commerce` · `COMMERCE_SILVER_LAYER=silver/commerce`
  추가(관련 값 .env 관리), 경로 주석 raw/commerce 로 갱신.
- **문서 7종**: `bronze/commerce` 경로 리터럴 → `raw/commerce`(README·storage·common_info·configuration·
  recollect·incremental-sort-diff·deploy-prod). change-log 과거 이력은 불변 유지.
- **scripts 정리**: 1회성 다 쓴 것 삭제(`prune_duplicate_runs`·`backup_diff_target`·`move_bronze_to_raw`
  — 뒤 둘은 없어진 `storage.copy` 의존), 재사용 가치 있는 `seed_diff_target.py`(step0)만 존치.
  `.airflowignore` 에 `scripts/**` 추가(파서 제외).
- **검증**: 전 테스트 **79 통과**(경로 단정 raw/commerce 로 갱신), 보안 게이트 PASS. 기능 확인 —
  `paths.bronze_diff_target_key`=`raw/commerce/_diff_target/…` 가 R2 이동본과 정합(실존 확인),
  `COMMERCE_RAW_LAYER` override 반영 확인 → **DAG 이 이동 데이터·시드 diff-target 을 그대로 사용.**
- **⚠ 배포 순서(#75 계약)**: 마커 조회(`markers.py`)·diff-target 경로가 `RAW_LAYER` 에서 파생 —
  **데이터 이관 완료 후 배포 필수**. 이관 전 배포 시 과거 run 미인식 → 전량 재수집·동일자 제외 계약 공백.
- **후속(선택)**: 함수/변수명·docstring·문서의 conceptual "bronze" 용어를 raw 로 통일(대규모 리네임은 별도).

### 20. DAG 네이밍 통합 — seoul_commerce_daily/recollect → commerce_localdata_elt/recollect (feat/73-dag-naming)
request:
- 팀 공통 DAG 네이밍 규칙 `<domain>_<dataset>_<stage>` 확정(#73): stage 역할형(elt/recollect 등),
  commerce dataset=localdata, 파일명=dag_id(밀접한 DAG 쌍은 공통 접두 파일명).
response:
- dag_id: `seoul_commerce_daily` → `commerce_localdata_elt`, `seoul_commerce_recollect` →
  `commerce_localdata_recollect`. 파일 `seoul_commerce_dag.py` → `commerce_localdata.py`(두 DAG 공존).
- 번들 docs/README의 dag_id 참조 일괄 갱신. 스토리지 경로·마커 계약은 dag_id와 무관하므로 변경 없음.
- 옛 dag_id 실행 이력은 Airflow 메타DB에 보존(삭제 안 함), 신규 id로 새로 시작.

### 19. 인증키 env-var 계약 변경 — SEOUL_OPENAPI_KEY → SEOUL_API_KEY_COMM, 루트 .env 로 이관 (feat/70-env-key-unification)
request:
- 도메인별 서울 API 키 환경변수를 `SEOUL_API_KEY_<도메인약어>` 규칙으로 통합(#70). commerce 는
  `SEOUL_OPENAPI_KEY` → `SEOUL_API_KEY_COMM`.
- commerce 인증키는 `.env.commerce` 가 아니라 **호스트 루트 `.env` 로 이관**한다(번들 자립 의도의
  부분 폐기 — 사용자 승인). `SEOUL_OPENAPI_BASE_URL` 은 `settings.py` 기본값과 동일하므로
  `.env.commerce` 에서 삭제.
- 배경: 루트 `.env` 의 culture 키가 같은 이름(`SEOUL_OPENAPI_KEY`)이라 setdefault 로더 특성상
  commerce 가 culture 키로 호출하던 충돌 해소.
response:
- `settings.py` 가 `SEOUL_API_KEY_COMM` 을 읽도록 변경(내부 필드명 `seoul_openapi_key` 유지).
  `clients.py`/`resolve.py` 오류 메시지, `test_security.py`, 번들 docs/README/deploy 문서 일괄 반영.
- `.env.commerce`/`.env.commerce.example` 에서 인증키·BASE_URL 제거 + 이관 안내 주석.
  키 입력 위치 안내를 "루트 `.env`" 로 수정(configuration.md gap 표 포함).
- 신규 이름은 `KEY` 포함 → security 자동 마스킹(`_SECRET_NAME_RE`) 유지 확인.

### 18. 재수집 규칙 변경 — 동일자 성공분 제외·KST 일자 가드·한 파일 관리 (feat/59-recollect-rule-change)
request:
- **동일자 수동 재실행**: 같은 날짜에 수동 실행 이력이 있으면, 실행 전에 **이미 성공한 API 는 제외**하고
  수집한다.
- **recollect run_id 관리**: 실패분을 재수집할 때 (1) run_id 를 동일하게 맞춰 재수집하거나, (2) 기존 실패
  파일을 삭제하고 별도 run_id 로 재수집하여 **하나의 파일로 관리**되게 한다.
- **KST 일자변경 가드**: recollect 라도 **한국시간(UTC면 보정, KST면 그대로) 기준 일자가 바뀌면** 사실상
  다른 일자 정보라서 그 정보는 재수집하지 않는다.
- 브랜치 feat/59-recollect-rule-change (feat/58 기반).
response:
- **markers.py**: `run_date`(run_id→KST 날짜), `completed_shorts_on_date`,
  `plan_excluding_same_day_completed`(동일 KST 일자 completed 제외),
  `recollect_targets_same_day`(최근 run 의 incomplete 중 **KST 오늘과 같은 날짜만**, 날짜 바뀌면 빈 리스트),
  `cleanup_incomplete`(성공 run 제외 **같은 KST 일자** 실패 파편 삭제 — option2 한 파일). `common/storage.py`
  에 `delete` 추가(ABC/Local/R2).
- **DAG 배선**: daily `plan_all_targets`→동일자 성공분 제외, recollect `find_incomplete_targets`→KST 가드,
  `ingest_one`→성공 시 같은 일자 실패 파편 `cleanup_incomplete`(한 파일).
- **검증**: 단위테스트 **6 통과**(run_date·동일자 제외·전날 미제외·KST 가드·cleanup 유지/타일자 보존). DAG 구문 OK.
- **미검증(정직)**: end-to-end 는 실제 DAG 실행(Airflow) 필요 — 오프라인 단위테스트까지.

### 17. bronze 증분화 코어 — 정렬·검증키·diff (feat/58-commerce-bronze-sort-diff)
request:
- bronze 수집이 매 실행 전체를 다시 받는 문제를 해소한다. 각 API 파일에서 UPDATEDT 존재를 먼저
  확인하고(모두 있으면) **UPDATEDT 내림차순 row 정렬**(날짜가 숫자가 아니면 숫자키로 치환). 정렬은
  **전량 RAM 금지 → 파일 단위 스트리밍**, 퀵정렬보다 낮은 Big-O 가 있으면 그 방식. 저장 포맷은
  **row-NDJSON(UPDATEDT desc)로 전환**. **API 단위 해시 검증키**를 만들고, 수집 파일 삭제 로직은
  재검증 통과 뒤 **맨 마지막(step4)**에만 적용. 브랜치 feat/58-commerce-bronze-sort-diff.
- (합의 Q&A) **정렬 알고리즘** = 외부 병합 정렬(스트리밍·바운디드 RAM). **저장 포맷** = row-NDJSON(UPDATEDT desc).
- (합의 Q&A — 증분 저장/비교기준 모델, 크리티컬) 구 데이터 소실·버전이력 유실 방지를 위해 **2계열**:
  ① **save(증분 영구 저장)** — 첫 수집=full, 이후=신규/변경분만 누적(이력 보존, 삭제 안 함).
  ② **diff-target(롤링 최신본)** — 다음날 비교 기준. 첫 수집 시 save 와 **같은 내용으로 따로 생성**.
  매일: 오늘본 vs diff-target diff → 신규분을 save 로 증분 저장 → **diff-target 을 오늘본으로 교체(구 diff-target 삭제)**.
response:
- **-1단계 확인(실데이터)**: 39종 전부 UPDATEDT 100% `datetime` → 14자리 정수키로 정렬 가능 확인.
- **include/bronze/incremental.py**: `external_merge_sort`(청크→임시파일→heapq 병합, 스트리밍·바운디드
  RAM, O(n log n)), `verification_key`(정렬본 순서민감 sha256), `diff_new_rows`(정렬 병합 스트리밍 diff
  — 같은 키는 정규화 문자열 직접비교로 hot loop 경량). **파일 브리지**: `sort_rows_to_file`(정렬→row-NDJSON+키),
  `read_rows`, `build_increment`(첫수집=full / 동일=증분없음 / 상이=diff 신규분 — 모델 그대로 구현).
- 단위테스트 **13 통과**(정렬·순서민감키·diff 4종 + 파일브리지 first/identical/changed + orchestration
  first→identical→changed).
- **DAG 통합**: `common/paths.py`에 diff-target 경로(`_diff_target/<short>.jsonl` + `.key` 사이드카).
  `incremental_store`(스토리지 브리지: 전날 target 다운로드→비교→증분 업로드→target 롤링 교체).
  `bronze_tasks._write_bronze`가 **status==ok 일 때만** 페이지→row 파싱→증분 저장(중간 중단은 미저장),
  마커에 `verification_key/increment_mode/increment_count/sorted_row_count` 기록. page-NDJSON → row-NDJSON.
- **step0**: `seed_diff_target`(1회성 diff-target/검증키 시드). 미실행이어도 첫 수집이 self-seed 하므로 선택.
- **step4**: 본 모델은 raw 페이지가 휘발(메모리)이라 "수집 파일 삭제" 별도 대상 없음 → "미저장(status!=ok) +
  재검증"으로 갈음(단위테스트로 first/identical/changed 재검증).
- **docs**: [docs/pipeline/bronze/incremental-sort-diff.md](docs/pipeline/bronze/incremental-sort-diff.md)
  (모델·정렬·검증키·diff·수집흐름·step0·검증). 단위테스트 **14 통과**.
- **라이브 end-to-end 검증 완료(실 Seoul API, 격리 프리픽스 `_verify58`)**: run1=first(row-NDJSON 확인:
  MGTNO 있음/LOCALDATA 봉투 아님) + diff-target 생성 → run2 동일 데이터=identical(증분 파일 미생성) →
  변경분=changed(변경 업장1 + 신규행만 증분, diff-target 3행으로 롤링). **이력 보존 확인**: 이전 run 증분
  유지 + 같은 업장(mgtno)의 **원본('태평')·변경('태평_CHG') 두 버전 공존 → 이력 추적 가능(True)**.
  검증 후 `_verify58` 6객체 전량 삭제(실 bronze 무오염). **조치 필요 없음**(정상 동작).
- **사이드 이펙트 분석/대응**: 기존 bronze=page-NDJSON, 신규=row-NDJSON → **형식 혼재**(이력 손실 아님 —
  구 run 보존 + 신규 run 은 증분). 실운영 첫 수집은 `_diff_target` 미존재라 mode=first 로 전체 저장
  (자가 시드; step0 로 사전 시드하면 첫 수집부터 diff). **다운스트림(dbt 로더) row-NDJSON 대응은 feat/58 밖**.
- 커밋·푸시(feat/58). (부수) CLAUDE.md 영어 통일 + Change Log Rule 에 request:/response: 규격 명시(별도 커밋).

## 2026-06-30

### 16. 보안 대응 전용 패키지 + 단일 포인트 종합검증 도입 (`include/security/`)
- **배경**: 로그/예외(특히 `requests` 네트워크 실패 메시지)에 서울 OpenAPI 인증키가 박힌 URL 이
  들어가, 로그뿐 아니라 **bronze 마커 JSON(error 필드)으로 키가 영구 저장(at-rest 누출)**될
  위험이 있었다. 그 외 흔한 공격/누출 경로(알림 전송, 경로 주입, 하드코딩 키, `.env` 추적,
  `yaml.load`/`eval`/`verify=False`/timeout 누락)도 함께 상정해 종합 대응.
- **추가**: 이식 가능한 **stdlib-only 독립 패키지** `include/security/`:
  - `redaction.py` — literal(env 시크릿 실제값) + structural(서울 URL 경로키·`Bearer`·`AKIA`·
    `secret=`/`token=` 등) **2중 마스킹**. `redact()` 는 str/dict/list/예외 재귀.
  - `log_filter.py` — `install_log_redaction()` 가 루트/airflow 로거·핸들러에 마스킹 필터 부착
    (msg/args/traceback 마스킹, idempotent).
  - `inputs.py` — `assert_iso_date`/`assert_safe_segment`(경로 주입 차단).
  - `audit.py` — 정적 점검 7종 + 런타임 자기검증(redactor/log).
  - `verify.py`(+`__main__.py`) — **단일 포인트** `run_security_verification()`/`assert_secure()`
    및 CLI `python -m security`(exit code=차단 이슈 유무).
- **적용**: DAG 는 env 적재 직후 `install_log_redaction()` 호출 + `resolve_observed_date` 에
  `assert_iso_date()`. bronze `clients.py`(예외/경고 로그)·`bronze_tasks.py`(마커 error·실패 로그)·
  `common/notify.py`(알림 message/context)에 `redact()` 적용(이중 방어).
- **검증**: 전체 단위테스트 58 통과(보안 30 신규 — 마스킹/로그필터/입력검증/정적감사 +
  **bronze 마커 at-rest 키 비노출 end-to-end**), `python -m security` 차단 이슈 0.
- **이식성**: `include/security/` 디렉터리 복사 + DAG 한 줄(`install_log_redaction()`) + 누출
  지점 `redact()` 로 타 번들/프로젝트에 일괄 적용. 시크릿은 env 이름 규칙으로 자동 식별.
- **점검/연결 구조(거버넌스)**: 에이전트(Claude/Codex)가 수시로 불러오고 적용·점검하도록 연결.
  CLAUDE.md **§20 Security Gate**(Recall/Apply 트리거/Check) + §18 Final Quality Gate 에 보안 항목 +
  §19 CLAUDE-chain 에 `security` 포함(세션 이동에도 따라옴). Share.md **§5 보안** 섹션.
  타 프로젝트 이식 가이드 `docs/security/adoption.md`(복사-붙여넣기 프롬프트 포함) 신설.
- 파일: `include/security/*`(신규), `seoul_commerce_dag.py`, `include/bronze/clients.py`,
  `include/bronze/bronze_tasks.py`, `include/common/notify.py`, `tests/test_security.py`(신규),
  `docs/security/{README,security,adoption}.md`(신규), `docs/README.md`·`Share.md`·`README.md`·`CLAUDE.md`(인덱스/규약).

### 15. silver 가공을 bronze DAG에서 분리 — DAG 라인은 원본 수집(bronze) 전용
- **배경**: `seoul_commerce_daily`/`seoul_commerce_recollect` 의 공통 흐름(`_wire`)이 bronze 수집과
  silver 적재를 한 DAG 안에 묶고 있었다. bronze 는 "원본 수집"만 담당해야 한다는 역할 경계에 맞춰
  silver 를 DAG 오케스트레이션에서 **완전히 분리**.
- **변경**: DAG 파일에서 `from silver import silver_tasks` 임포트, `build_silver_one` 태스크,
  `_wire` 의 `build_silver_one.expand(...)` 결선을 제거. 흐름은
  `… → ingest_one.expand → finalize_run` 로 단순화. `finalize_run` 은 ingest 요약만 집계(불변).
- **보존**: silver **로직은 그대로 유지**(`include/silver/silver_tasks.py`·`validators.py` 무수정).
  사용자 결정에 따라 **별도 silver DAG 는 생성하지 않음** — 로직만 보존하고 오케스트레이션은 비움.
  `observed_date` 파라미터/파생값은 여전히 silver 파티션 키 의미로 남는다.
- 검증: `seoul_commerce_dag.py` 구문 검사 통과 + 잔여 silver 참조는 docstring 설명뿐(임포트/결선 없음).
- 파일: `seoul_commerce_dag.py`(docstring 다이어그램·임포트·태스크·`_wire`).

### 14. bronze 경로에 연/월/일 파티션 추가 (`/<YYYY>/<MM>/<DD>/run_id=…`)
- bronze 저장 구조를 `…/bronze/commerce/run_id=<ts>/…` → **`…/bronze/commerce/<YYYY>/<MM>/<DD>/run_id=<ts>/…`**
  로 변경. 연/월/일은 **run_id 날짜에서 파생**(별도 인자 없음) → 같은 날 실행이 같은 날짜 폴더에 모인다.
- 구현은 `paths.bronze_run_dir` 한 곳(+ `_run_date_dir` 헬퍼, 날짜 형식 아니면 방어적으로 파티션
  생략). object/marker/`_RUN` 키가 전부 따라옴. `markers.list_run_ids` 는 `run_id=` 부분문자열로
  추출하므로 **무수정 동작**(신·구 레이아웃 모두 인식). silver 경로(observed_date 파티션)는 불변.
- 검증: 단위테스트 28 통과 + R2 실적재로 `bronze/commerce/2026/06/30/run_id=…/food_cold_storage.jsonl`
  + 마커 확인, `latest_run_id` 정상 인식.
- 파일: `include/common/paths.py`, `include/bronze/bronze_tasks.py`(docstring),
  `tests/test_markers.py`·`tests/test_bronze_tasks.py`, `README.md`,
  `docs/architecture/storage.md`, `docs/pipeline/common_info.md`.
- ⚠️ 기존 구레이아웃 run(`…/run_id=2026-06-30_160452_591/`, 전환 전 적재)은 그대로 남는다 —
  다음 수집부터 신규 레이아웃. 필요 시 구 run 정리.

### 13. 인허가 39종 전 종 수집 — 잔여 14종 service_name 채움(25→39)
- **배경**: 레지스트리 39종 중 14종이 `service_name: null`(코드 미입력) → `enabled_for_schedule()`
  가 코드 채워진 것만 job 으로 만들어 **25 job 만 수집**되고 있었다. (job 단위 = API 단위 =
  `ingest_one[<short>]` 1 인스턴스. category 는 그룹 라벨일 뿐 job 수와 무관.)
- **해소**: 사용자 전달 **포털(data.seoul.go.kr) 정본 LOCALDATA 코드 14종**을 레지스트리에 입력하고
  14종 전부 API 호출로 검증(`INFO-000` + 업태 일치). → `enabled('daily')=39, pending=0`.
  - 공중위생 5: 미용 `051801`·이용 `051901`·세탁 `062001`·소독 `093011`·목욕 `114401`
  - 축산 5: 판매 `072204`·가공 `072205`·포장 `072206`·보관 `072224`·운반 `072225`
  - 관광 2·건기식일반·숙박: `072401`·`072402`·`072203`·`031103`
- **정정**: 이전 분석의 *"공중위생은 비-LOCALDATA 코드"* 주장은 **오류**였다 — 자동 스캔이 prefix
  01/02/03/07 만 봐서 못 찾았을 뿐, 공중위생도 정상 LOCALDATA(05/06/09/11)를 쓴다.
- **호출량 재산정**: 39종 = **데이터 1,360 + 게이트 1 = 1,361회/수집**, 약 134만 건(실측 2026-06-30).
  (이전 25종 1,049회에서 증가.)
- 파일: `config/dataset_registry.yaml`, `docs/pipeline/common_info.md`(카탈로그),
  `docs/pipeline/bronze/{api-call-volume,uncollectable-datasets,resolve-worklist,README}.md`,
  `docs/pipeline/{README,non-license-datasets}.md`, `Share.md`.

### 12. R2 적재 복구 — `R2Storage` boto3 전환 · `STORAGE_BACKEND=r2`
- **증상**: Airflow 실행은 됐으나 R2 에 적재 이력 없음. **원인 2가지** — (1) `.env.commerce` 의
  `STORAGE_BACKEND=local` + `R2_BUCKET` 공백 → 컨테이너 휘발성 볼륨(`/opt/airflow/data`)에만 적재,
  (2) `R2Storage` 가 `s3fs` 기반인데 호스트 이미지에 **s3fs 미설치**(boto3/pandas/pyarrow 는 있음).
- **해결**: `R2Storage` 를 **boto3** S3 클라이언트로 재구현(path-style·SigV4·region `auto`) — 이미지에
  이미 있는 boto3 만 사용해 **번들 안에서 자립 해결**(호스트 이미지 변경 불필요). `.env.commerce` 를
  `STORAGE_BACKEND=r2` + R2 블록(`R2_BUCKET=${R2_DEV_BUCKET_NAME}` 등 루트 `.env` dev 키 참조)으로 복구.
  → **로컬(도커)에서 실행해도 R2(`seoul-dev`)에 적재**된다.
- **검증**: 컨테이너에서 boto3 R2 write/read/list 확인 + `food_cold_storage`(50행, 실키) bronze 1건을
  `bronze/commerce/run_id=…/food_cold_storage.jsonl` + `_markers/...completed` 로 R2 적재 후 정리.
- 의존성 문서 정정: R2=boto3·silver=pandas/pyarrow 는 **이미지에 이미 포함**(추가 설치 불필요),
  s3fs 는 미사용. (이전 "패키지 미포함/설치 필요" 서술 수정.)
- 파일: `include/common/storage.py`, `.env.commerce(.example)`, `requirements.txt`, `CLAUDE.md`,
  `README.md`, `docs/architecture/storage.md`, `docs/configuration/{configuration,environments}.md`,
  `docs/operations/{deploy-dev,deploy-prod}.md`.

### 11. 재수집 DAG · 알림 인터페이스 · API별 진행 가시성 · change-log 규칙
- **재수집 파이프라인**: `seoul_commerce_recollect` DAG(6h) 추가 — 최근 run 의 마커를 읽어
  **미완료(incomplete/미시도) API만 재수집**. 대상이 없으면 수집 진행 안 함(빈 매핑 → run 폴더
  미생성). 마커 조회 헬퍼 `bronze/markers.py`, `paths.bronze_root()`. `finalize_run` 은 빈 실행 시
  `_RUN` 마커 생략. DAG 정의를 공통 태스크(모듈 레벨) 공유 + daily/recollect 2개로 정리.
- **API별 진행 가시성**: `ingest_one`·`build_silver_one` 에 `map_index_template="{{ short }}"` →
  Airflow Grid/Graph 에서 매핑 인스턴스가 **API 이름**으로 표시(성공/실패/대기 가시화). 실측 확인.
- **알림 인터페이스(비활성)**: `common/notify.py` — `Notifier`/`NoopNotifier`/`notify_exception`.
  예외 로그를 알림으로 보낼 수 있는 인터페이스만 제공(**기본 no-op, 미와이어링**).
- **change-log 규칙**: 대단위 변경은 `change-log.md` 에 작성일·순서 내림차순으로 기록하도록
  CLAUDE.md(§19 Change Log Rule)에 명시. 경로는 Share.md §4·docs/README.md 로 인덱싱.
- 파일: `seoul_commerce_dag.py`, `include/bronze/markers.py`(신규)·`include/common/notify.py`(신규),
  `include/common/paths.py`, `tests/test_markers.py`·`tests/test_notify.py`(신규),
  `docs/operations/recollect-and-alerts.md`(신규), `CLAUDE.md`, docs 인덱스/architecture/operations/README.

### 10. 인허가 외 2종 격리 · monthly/irregular DAG 비활성 (41 → 39종)
- `medical_location`(병의원 위치정보)·`food_hygiene_status`(식품위생업소 현황)은 LOCALDATA
  인허가 표준이 아니어서 **수집 대상에서 제외(격리)**.
- 레지스트리에서 제거 → `config/non_license_datasets.yaml` 로 파킹. 사유/재활성 절차는
  `docs/pipeline/non-license-datasets.md`.
- 두 주기에 인허가 대상이 0종이라 **`SCHEDULES = {"daily"}`** 로 축소 → `seoul_commerce_daily`
  1개만 생성(monthly/irregular DAG 비활성).
- 결과: 인허가 레지스트리 **39종 = 해석 25 / 미해석 14**.
- 파일: `config/dataset_registry.yaml`, `config/non_license_datasets.yaml`(신규),
  `seoul_commerce_dag.py`, `docs/pipeline/non-license-datasets.md`(신규), 카탈로그/uncollectable/
  worklist/caveats/api-call-volume 갱신.

### 9. DAG 명칭 통일: `seoul_license_*` → `seoul_commerce_*`
- 파일 `seoul_license_dag.py` → **`seoul_commerce_dag.py`**, DAG id `seoul_license_{daily,monthly,
  irregular}` → `seoul_commerce_*`, 태그에서 중복 `license` 제거.
- 모든 문서의 DAG id/경로 참조 일괄 변경.
- 파일: `seoul_commerce_dag.py`(이름변경), 전체 docs/README/Share.

### 8. `.airflowignore` glob 전환 (Airflow 3.x 호환)
- Airflow 3.x 기본 `dag_ignore_file_syntax=glob` 인데 regexp(`^include/`)라 무효 → 번들 내부
  (`include/`·`config/`·`tests/`·`docs/`)가 DAG 파일로 오스캔되던 문제 수정.
- glob 패턴(`include/**` 등)으로 변경. 컨테이너에서 dag-processor가 DAG 파일만 파싱 확인.
- 파일: `.airflowignore`.

### 7. bronze 저장 구조 재설계 — run_id 스냅샷 · API당 1파일 · 마커
- **DAG 실행 1회 = `run_id=<YYYY-MM-DD_HHMMSS_mmm>` 폴더 1개**. API당 **1파일**
  (`<short>.jsonl`, 줄=원본 페이지 NDJSON).
- 수집 상태는 **API당 마커 1개**(`_markers/<short>.completed | .incomplete`) + 실행 마커
  (`_RUN.*`). 리니지는 마커 JSON 에 포함.
- **외부 매니페스트 제거**(`commerce/_manifest/manifest.json`) — bronze 는 run_id 폴더 안에서만
  파일 생성. 중복 제거는 silver 가 `MGTNO` 로. force 파라미터/스킵 제거(매 실행 전체 수집).
- `COMMERCE_STORAGE_PREFIX` 추가(`{prefix}/bronze/commerce/…`). silver 는 단일 NDJSON 키를 읽음.
- 파일: `include/common/paths.py`, `include/bronze/bronze_tasks.py`, `include/silver/silver_tasks.py`,
  `include/common/settings.py`, `seoul_commerce_dag.py`, `include/bronze/manifest.py`(삭제),
  tests, storage/architecture/operations/common_info 등 갱신.

### 6. 미해석 데이터셋 코드 해석 13종 (의료·동물)
- `sample` 키 실호출 + BPLCNM 식별로 의료/약무 `0101xx`·의료기사 `0102xx`·동물 `0203xx`
  계열 **13종**의 `service_name` 확정(병원/의원/부속/산후조리/안전상비/약국/안마/안경/치과기공/
  동물병원/동물약국/동물용의료용구/가축). `resolve.verify` 통과.
- 모호한 14종은 **후보 코드 + 워크리스트**로 정리(오수집 방지 위해 미입력).
- 파일: `config/dataset_registry.yaml`, `docs/pipeline/bronze/uncollectable-datasets.md`,
  `docs/pipeline/bronze/resolve-worklist.md`(신규).

### 5. bronze 수집 주의사항 문서(caveats)
- 실호출에서 발견한 특이사항을 **API별 + `[bronze]`/`[silver]` 단계 태그**로 정리(정렬 키
  없음·날짜 공백 패딩·상태 in-place·UPTAENM 공란·비-LOCALDATA 스키마·대용량 등).
- 파일: `docs/pipeline/bronze/caveats.md`(신규).

### 4. docs 주제별 폴더 재분류 + 인덱싱
- 평면 문서를 `architecture/` · `configuration/` · `operations/` · `pipeline/`(+`bronze/`)로
  분류. 마스터/폴더별 README 인덱스 작성, 모든 상대 링크 갱신.
- 파일: `docs/**`.

### 3. bronze 실호출 분석 문서
- 페이지네이션 정렬(위치 기반·안정이나 정렬 기준 컬럼 없음 → `MGTNO` dedupe), API 호출량
  (수집 1회 호출 수 산정), 영업상태 추적 모델(업장당 1행 in-place), 수집 불가 원인 분석.
- 파일: `docs/pipeline/bronze/pagination-ordering.md`·`api-call-volume.md`·
  `status-tracking-model.md`·`uncollectable-datasets.md`(신규).

### 2. `SEOUL_MAX_PAGES` 무제한 기본값
- 일반 API 는 호출 횟수 제한이 없으므로, **값이 없으면(미설정/빈값/0/음수) 무제한**(`None`).
  양수만 부분 수집 캡. `settings._env_limit` 추가.
- 파일: `include/common/settings.py`, `include/bronze/bronze_tasks.py`, `.env.commerce(.example)`, docs.

### 1. 환경변수 자립화 + 현행 환경 반영
- 번들 자체 환경파일 **`.env.commerce`** + 로더(`include/common/env.py`, `load_commerce_env()`)
  도입 — DAG 임포트 시 `os.environ` 에 setdefault. 루트 `.env` 와 겹치는 R2 값은 **`${VAR}`
  참조**로 불러옴(중복 저장 X). 시크릿은 gitignore, 템플릿만 추적.
- `requirements.txt`(번들 의존성 명세) 추가. CLAUDE.md 에 **작업 경계**(번들 안에서만) 명시.
- 문서를 **현행 환경**(LocalExecutor·단일 루트 `.env`·`elt-infra` compose·UI :30585)에
  맞춰 전면 갱신(기존 CeleryExecutor/serving DB/`.env.local·dev·prod` 서술 정리).
- 파일: `.env.commerce`(신규)·`.env.commerce.example`(신규)·`.gitignore`(신규)·
  `include/common/env.py`(신규)·`requirements.txt`(신규)·`seoul_commerce_dag.py`·
  `include/bronze/resolve.py`·`CLAUDE.md`·`Share.md`·`README.md`·`docs/**`.
