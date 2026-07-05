# Medallion 구현 계획 — raw → bronze(Iceberg) → silver → gold + 좌표 보정

작성일 2026-07-03 · 상태 **제안(초안, 멘토 승인 대기 항목 포함 — §7)** ·
갱신 2026-07-05 — silver **암묵 버저닝** 확정(SCD2 컬럼 제거, §2.2) + transform DAG 구현(Step 10)
선행 결정: raw/bronze 용어 확정(`dags/docs/plans/2026-07-02-feat-r2-raw-prefix.md`, #75) ·
원천 레이어 리네임([change-log #21](../../change-log.md)) · 증분 저장 흐름([change-log #22](../../change-log.md)).

이 문서는 commerce 도메인의 **레이어별 역할 결정과 단계별 구현 방법**, 그리고
**주소 기반 좌표 보정(geocode enrichment)** 설계를 정의한다. dbt 쪽 산출물은
`dbt/domains/commerce/`(신규, 자립형)이며 **`dbt/` 하위 타 도메인 폴더는 타인 작업물이므로
의존·수정하지 않는다**(접속 계약인 Trino 카탈로그/env 만 공유).

---

## 1. 배경 — 확정된 사실

| 사실 | 근거 |
|---|---|
| `commerce_collect_raw`(@daily)가 39종 인허가 전량을 수집, run당 API 호출 ~1,361회 / ~134만 행 | [commerce_raw.py](../../commerce_raw.py) · [bronze/api-call-volume.md](bronze/api-call-volume.md) |
| **저장은 증분**: 전량 fetch → `_diff_target/` 롤링 전체본과 정렬-diff → 변경분만 run 폴더에 저장(최초 run만 full). 실측 일 1,248MB → 0.48MB | [incremental.py](../../include/bronze/incremental.py) · [incremental-sort-diff.md](bronze/incremental-sort-diff.md) · change-log #22 |
| run_id = KST 실행시각 `YYYY-MM-DD_HHMMSS_mmm`(6자리=HHMMSS, 뒤 3자리=밀리초) | [commerce_raw.py](../../commerce_raw.py) `make_bronze_run_id` |
| raw = R2 오브젝트 랜딩(`raw/commerce/...` + 마커 + `_diff_target/`), bronze = **Iceberg 웨어하우스 원본층** — 용어는 팀 결정 #75로 확정, commerce 는 접두 리네임까지 완료 | change-log #21 |
| Trino 에는 iceberg(REST, R2 Data Catalog) 커넥터만 있음 → R2 의 JSONL 은 dbt/Trino 에서 직접 질의 불가 | `sample/trino/catalog/iceberg*.properties` |
| 공통 19컬럼에 좌표는 `X`,`Y` 뿐(**좌표계 미표기**), 주소는 `SITEWHLADDR`(지번)·`RDNWHLADDR`(도로명). **위경도 컬럼은 없음** → WGS84 위경도는 파생·보정 산출물 | [schemas.py](../../include/common/schemas.py) · [common_info.md](common_info.md) |
| silver 는 라이브러리 코드만 존재(미배선, pandas→parquet). row-NDJSON 전환 후 파서 미조정 이슈 열림 | [silver_tasks.py](../../include/silver/silver_tasks.py) · [incremental-sort-diff.md](bronze/incremental-sort-diff.md) §6 |
| dbt commerce 프로젝트 없음. Airflow 이미지 메인 venv 에 `trino` 패키지 있음(dbt 는 별도 venv, `common_dbt_smoke.py` 계약) | `sample/Dockerfile.airflow` · `sample/dags/common_dbt_smoke.py` |

**결정: raw 를 bronze 로 개명·통합하지 않는다.** 부족한 것은 명칭이 아니라
**raw 증분 → Iceberg bronze 적재 단계**다. 현행 raw 는 CLAUDE.md §2.2(원본 보존)를 이미 충족한다.

## 2. 레이어 설계

| 레이어 | 위치 | 역할 | 담당 |
|---|---|---|---|
| raw | R2 `{prefix}/raw/commerce/YYYY/MM/DD/run_id=.../`(내부에 `_markers/`) + 레이어 루트 `{prefix}/raw/commerce/_diff_target/`(run 무관 롤링 전체본) | 불변 원본·수집 상태·롤링 전체본. 재처리의 유일한 원천. **일절 변경 금지, Trino/dbt 직접 읽기 금지** | 기존 DAG (변경 없음) |
| bronze | Iceberg `<catalog>.commerce.bronze_localdata_license` | raw 증분의 append-only **변경로그**. `record_json` 통짜 보존(schema-on-read) + 계보 컬럼. 파싱·정제 금지 | **`commerce_load_bronze` DAG**(수집과 분리, PyIceberg/Trino) |
| silver | dbt `silver_license_history` · `silver_license_current` · `silver_geocode_address` | 파싱(19컬럼+파생)·형변환·중복제거·**암묵 버저닝(정렬키 기반)**·**좌표 보정 병합** | dbt/domains/commerce |
| gold | dbt `gold_commerce_license_status_current` | silver 만 참조하는 얇은 집계(자치구×업종×영업상태 현황) | dbt/domains/commerce |

"같은 key(MGTNO), 달라지는 value" 버저닝: 일별 스냅샷 전체를 쌓지 않고(1GB×365 낭비),
bronze 변경로그 + silver `content_hash` 변경 감지 → **암묵 버저닝**으로 처리한다
(명시 버전 컬럼 없음 — 정렬키 내림차순이 곧 버전 순서. 2026-07-05 사용자 확정, §2.2).

### 2.1 bronze 테이블 계약

39종이 동일 스키마이므로 **단일 테이블 + `dataset` 컬럼**(테이블 39벌 금지 — 모델 중복 방지).

```sql
CREATE TABLE IF NOT EXISTS <catalog>.commerce.bronze_localdata_license (
  dataset         varchar,        -- short (예: general_restaurant)
  mgtno           varchar,        -- 소스 네이티브 키(내부 ID 대체 금지)
  updatedt        varchar,        -- 원본 갱신시각(14자리 기대, 비정형 가능)
  record_json     varchar,        -- 원본 레코드 1행 통짜(가공 금지)
  content_hash    varchar,        -- 키 정렬 canonical JSON sha256
  observed_date   varchar,        -- 논리 수집일(KST)
  load_date       varchar,        -- 적재일(KST) — 파티션
  bronze_run_id   varchar,        -- run 폴더 run_id (어느 raw run 에서 왔는지)
  dag_run_id      varchar,        -- 수집 Airflow run_id(계보)
  raw_object_key  varchar,        -- R2 원본 객체 키(계보)
  increment_mode  varchar,        -- 마커 increment_mode 승계(first|changed) → 엔진 분기 기준
  record_seq      integer,        -- 파일 내 행 순번
  schema_version  varchar,
  collected_at    timestamp(6)
) WITH (format = 'PARQUET', partitioning = ARRAY['load_date'])
```

- 파티션은 `load_date`(적재일) 만. 물리 저장은 **R2 Data Catalog(관리형 Iceberg)** 이 테이블별로
  배치하며 폴더명은 카탈로그가 UUID 로 부여한다(`__r2_data_catalog/<uuid>/`, 클라이언트가 지정
  불가). 구분 핸들은 **논리 식별자 `<catalog>.commerce.bronze_localdata_license`**(스키마=commerce).
- 멱등: `(dataset, bronze_run_id)` 단위 delete-then-(insert|append).
- **로더 dedup 없음**: raw 증분은 이미 diff-target 대비 변경분만 담긴 결과라 로더의 content_hash
  read-back 대조는 무의미(사용자 확정). `content_hash` 는 컬럼으로만 보존 → silver 연속 dedup 에서 사용.
- **적재 엔진 분기**: `increment_mode`=first(=전체 스냅샷) → **PyIceberg**(Trino 우회, Arrow 배치
  append, 적재당 커밋 1회 — 대용량 503MB·백필의 Trino OOM 회피), =changed(소량 변경분) → **Trino** 증분.
- 발행 게이트: `bronze_collection_run_manifest` — **그레인 (source_id=dataset, bronze_run_id)**,
  적재 단위별 `SUCCESS/is_publishable`(rows_loaded==increment_count) delete-then-insert 멱등.
  silver 조인: `bronze b INNER JOIN manifest m ON b.dataset = m.dataset AND
  b.bronze_run_id = m.bronze_run_id AND m.status='SUCCESS' AND m.is_publishable`.
- 카탈로그: dev `iceberg_dev`(버킷 seoul-dev) / prod `iceberg`. 스키마 `commerce`(신설, §7 승인 항목).

#### 2.1.1 적재 상태(파일 기반, RDB 없음) — raw 와 격리

수집 DAG 와 **분리**된 `commerce_load_bronze` DAG 가 적재 전담. 상태는 **RDB 없이 파일**로,
raw 와 **완전히 격리된 공간**(`{prefix}/commerce_bronze_state/` — `raw/` 밖, `commerce_` prefix)에 둔다.
Iceberg 테이블 + 이 상태파일을 삭제해도 raw 는 불변이라 **전체 재적재가 항상 가능**하다.

- **워터마크** `_watermark.json` = `{short: 마지막 적재 run_id}`. 없으면 처음부터 전체 재적재.
- **pending** `_pending.json` = complete 없어 미적재인 `(date, short)`. 현재-2일까지 재감시, **3일 경과 폐기**.
- **receipt** `receipts/<date>/<run>__<short>.json` = 적재 단위 감사 로그(엔진·행수·계보).
- **무손실 skip**: diff-target 은 `status=ok` run 에서만 전진하므로 incomplete run 을 건너뛰어도
  다음 완료 run 의 증분이 그 변경분을 포함 — 유실 없음. 그래서 워터마크는 **완료 run 만** 반영하고,
  **적재 실패 run 은 전진시키지 않아** 다음 실행이 재시도한다.
- **바운드**: 한 번에 모든 날짜를 적재하지 않고 실행당 최대 날짜 수(`COMMERCE_LOAD_MAX_DATES`)로 제한,
  backfill catch-up 은 재실행이 이어감.

### 2.2 silver / gold 계약 (2026-07-05 개정 — 암묵 버저닝, 사용자 확정)

- **버전 정렬키(전순서, 상시 적용)**: `coalesce(updatedt_ts, epoch), coalesce(lastmodts_ts,
  epoch), observed_date, collected_at, content_hash` — **UPDATEDT 1순위 + LASTMODTS 2순위**,
  타이브레이커를 폴백이 아니라 **항상** 포함해 null·비정형·동률·역행 모두에서 결정적 순서 보장.
  (§7-9 결정: 역행도 소스 시각 순서를 따른다 — 관측 순서 우선 아님.)
- **명시적 버전 컬럼 없음**: `version_seq`/`valid_from`/`valid_to`/`is_current` 를 두지 않는다.
  history 는 정제된 변경로그이며 (dataset, mgtno) 안에서 위 정렬키 **내림차순이 곧 버전 순서**
  (**암묵 버저닝**). 버전 순서가 필요한 소비처(gold 의 현재 상태 갱신 포함)는 이 정렬을 재현한다.
- `silver_license_history` — 행 식별 grain `(dataset, mgtno, collected_at, content_hash)`.
  publishable 필터(데이터셋별 게이트, §2.1) → 파싱(공통 19 중 사용 필드 + LASTMODTS,
  빈 문자열 → null) → **연속 중복 제거**:
  `lag(content_hash) over (partition by dataset, mgtno order by <버전 정렬키>) = content_hash`
  인 행만 제거. diff 재유입·reconcile 재방출(직전 버전과 동일 → 인접 중복)은 걸러내되
  **정당한 원복(A→B→A)은 보존**한다 — 전역 `(dataset, mgtno, content_hash)` dedup 금지.
  파생 컬럼: 자치구 `district`(주소 파싱 — 이력 집계를 위해 history 에 둠), 주소 키 2종(§4.5),
  `updatedt_ts`/`lastmodts_ts`(+정렬 전용 `*_sort`, 결측=epoch).
- `silver_license_current` — grain `(dataset, mgtno)`. history 정렬 **내림차순 최상위 1행**
  (`row_number()=1`) + 좌표 보정 컬럼(§4.5, geocode 조인).
- `gold_commerce_license_status_current` — **current 기준 스냅샷 집계**(grain
  `(district, dataset, trdstategbn)`). silver `ref()` 만, 신규 파싱·중복제거 금지.
  일별 추이/상태 전이 gold 는 date-spine 설계가 필요해 phase-2 로 이연.
- materialization 은 전부 `table` 전량 재빌드 — **silver 는 bronze 의 순수 함수**(멱등·재처리
  자동). incremental 은 R2 Data Catalog 이슈로 당분간 보류(후속 이슈). **단위 재적재/삭제**는
  bronze 워터마크 파일(§2.1.1)과 dbt vars(`exclude_datasets`/`exclude_observed_dates`/
  `exclude_load_dates`/`exclude_bronze_run_ids`)로 제어 — 운영 가이드:
  `dbt/domains/commerce/docs/rebuild-and-ops.md`.
- **타임존 주의**: `collected_at` 은 UTC, `UPDATEDT`/`LASTMODTS`/`observed_date`/`load_date` 는
  KST — 직접 비교 금지. 정리: `dbt/domains/commerce/docs/timestamps-and-nulls.md`.

## 3. 단계별 구현 방법 (to-do)

각 단계는 이전 단계 검증 후 착수한다(walking skeleton 우선).

### Step 1. 계약·결정 문서화 *(이 문서가 초안)*

- **방법**: 이 문서 리뷰·확정. 단일 테이블 채택 사유, `commerce` 스키마 신설, `_diff_target`
  기반 백필, 삭제 감지 정책(§5), 좌표 보정 방침(§4)을 승인 항목(§7)과 함께 확정.
  확정 시 [change-log.md](../../change-log.md) 기록.
- **완료 기준**: #75 결정과 모순 없음, §7 항목 결론 기록.

### Step 2. bronze 적재 엔진 + 상태 모듈 *(구현 완료)*

- **`include/bronze/warehouse.py`** — 적재 엔진(2경로):
  - `ensure_schema_and_tables()` — Trino DDL 로 스키마 + bronze/manifest 테이블 IF NOT EXISTS(단일화).
  - `iter_increment_rows(storage, key)` — 증분 파일(row-NDJSON) **라인 스트리밍**(503MB 대비 메모리
    바운드). page-NDJSON 은 적재 범위 아님 → 거부.
  - `project_records(...)` — 레코드 → 컬럼 dict(record_json 통짜 + canonical sha256 content_hash).
  - `load_unit_trino(...)` — `DELETE WHERE dataset=? AND bronze_run_id=?` 후 INSERT.
    **값 전부 `?` 파라미터 바인딩**(record_json 등 외부 데이터 SQL 조립 금지, §20). 배치 200행.
  - `load_unit_pyiceberg(...)` — R2 Data Catalog REST + R2 S3 FileIO 로 테이블 load →
    `delete(dataset&bronze_run_id)` + Arrow 배치 `append`. **Trino 우회**(코디네이터 OOM 회피).
  - `load_unit(...)` — 엔진 dispatch + receipt 기록. `write_manifest(...)` — 데이터셋별 발행 게이트.
  - 식별자(catalog/schema)는 `assert_identifier` 통과분만 보간(`security: allow-sql` 표식).
- **`include/bronze/load_state.py`** — 워터마크·pending·receipt(파일 기반, raw 격리 §2.1.1).
- **`include/bronze/load_plan.py`** — `resolve_load_plan()`(워터마크+raw run → 적재 단위·엔진·pending),
  `commit_watermark()`(적재 성공분까지만 전진 — 실패 run 직전에서 멈춤).
- **완료 기준(달성)**: 단위테스트 통과(`test_warehouse.py`·`test_load_state.py`·`test_load_plan.py` —
  멱등 delete-then-insert, 파라미터 바인딩, row/page-NDJSON, 엔진 dispatch, 워터마크/pending/만료),
  `python -m security` PASS. (PyIceberg/Trino 실제 왕복은 이미지 통합 검증 — Step 4.)

### Step 3. 적재 DAG — `commerce_load_bronze.py` (수집과 분리) *(구현 완료)*

- **방법**: 수집(`commerce_collect_raw`/`recollect`)과 **완전 분리**된 적재 전용 DAG(무거운 Trino/
  PyIceberg 작업 격리 — 사용자 확정). raw 는 읽기 전용, 계약 무변경.
  `resolve_plan → plan_units → ensure_warehouse → load_one.expand → finalize`.
  - `resolve_plan`: 워터마크/pending + raw run 목록 → 적재 단위(엔진 포함)·pending 갱신 산출.
  - `load_one`(short별 expand, 독립 `retries`): 엔진(pyiceberg/trino)으로 멱등 적재 + receipt.
  - `finalize`(ALL_DONE): **적재 성공분까지만** 워터마크 전진(실패 run 은 다음 실행 재시도),
    pending 커밋(만료분 폐기), manifest 발행.
- **엔진/백필**: 워터마크 없음 → 각 데이터셋 첫 run(mode=first)이 PyIceberg 로 전체 적재, 이후
  changed run 은 Trino 증분. 실행당 `COMMERCE_LOAD_MAX_DATES`(기본 3일)로 바운드, catch-up 은 재실행.
- **주의**: 적재 실패가 raw 마커/데이터를 오염시키지 않음(완전 분리). manifest 미발행 → silver 자동 보호.
- **완료 기준(코드)**: 두 DAG(collect raw-only / load) 파싱·배선 검증 완료. dev 실제 적재는 Step 4.

### Step 4. 워킹 스켈레톤 백필 (소형) *(이미지 통합 검증 — 후속)*

- **방법**: 소형 데이터셋 1~2종(예: `affiliated_medical` ~127KB)만 `commerce_load_bronze` 수동
  트리거(`max_dates` 작게)로 적재해 종단 검증. 첫 run 은 PyIceberg 경로.
- **완료 기준**: bronze 행수 = 증분 파일 라인 수, Trino 로 `record_json` 샘플 질의 성공,
  워터마크/manifest/receipt 기록 확인. (이미지에 `trino`·`pyiceberg` 설치 필요 — §7.)

### Step 5. dbt 프로젝트 골격 — `dbt/domains/commerce/` (신규·자립형)

- **방법**: `dbt_project.yml`(name=commerce, `+materialized: table`),
  `profiles.yml`(type trino / method none / `database={{ env_var('TRINO_ICEBERG_CATALOG'|'TRINO_DEV_ICEBERG_CATALOG') }}`
  / `schema={{ env_var('COMMERCE_SCHEMA','commerce') }}` / `DBT_TARGET` dev|prod),
  `models/sources.yml`(source `commerce_bronze` → `bronze_localdata_license`
  loaded_at_field `collected_at`, freshness 는 dev warn-only·error 는 prod 전환 시 활성 +
  `bronze_collection_run_manifest`).
  **타 도메인 dbt 폴더 무접촉** — 접속 env 계약만 공유. `dbt/` 는 번들 밖 **별도 git 저장소**
  이므로 커밋은 dbt 저장소의 브랜치/PR 절차를 따른다(§7 승인 항목).
- **완료 기준**: `dbt parse` 성공, Step 4 데이터로 source 조회·freshness **실행** 확인
  (freshness pass 는 일일 적재 가동 상태에 의존하므로 완료 조건으로 삼지 않음).

### Step 6. silver 모델 + 테스트 *(구현 완료 — 2026-07-05 암묵 버저닝으로 개정)*

- **방법**: §2.2 계약대로 `models/silver/silver_license_history.sql`,
  `silver_license_current.sql`. 파싱은 `json_extract_scalar(record_json, '$.FIELD')` →
  공통 컬럼(+LASTMODTS, '' → null) + `district`(정규식 `서울특별시\s+(\S+구)`, 도로명
  우선·지번 폴백)·주소 키 2종 파생(모두 history 에서). 테스트: 행 유니크
  (dataset, mgtno, collected_at, content_hash) · current grain unique ·
  `assert_uses_publishable_runs`(dataset 단위 게이트) ·
  인접 중복 0건(A→B→A 원복 보존 확인 포함).
- **완료 기준**: `dbt run+test` 전체 PASS, current 행수 = 활성 MGTNO 유니크 수.
  (구 SCD2 형태로 dev 실측 완료 이력 있음 — 개정 후 재검증은 transform DAG 첫 가동으로.)

### Step 7. 전체 39종 백필 + 전수 검증

- **방법**: `mode=full_reconcile` 로 전체(~1GB, `general_restaurant` 503MB 포함) 적재.
  INSERT VALUES 배치라 수 시간 예상 — dataset 단위 재개 가능(멱등키 dataset별). 소요·배치 수 실측 기록.
- **완료 기준**: 39종 전부 적재, dataset별 행수 = diff-target 라인 수, silver 재실행 후 테스트 PASS.

### Step 8. 좌표 보정 파이프라인 (상세 §4)

- 8a. 지오코딩 API 확정(§4.3 체크리스트 실측) + 좌표계(EPSG) 판별(§4.2).
- 8b. 수집 라인 구현(`commerce_geocode_collect` DAG + `include/bronze/geocode_tasks.py`,
  raw 랜딩 → `bronze_geocode_address` 적재).
- 8c. dbt `silver_geocode_address` + `silver_license_current` 조인·보정 컬럼(§4.5) + parity 테스트.
- 8d. 유니크 주소 백필(쿼터 분할, 영업중 우선 — §4.6).
- **완료 기준**: current 의 `location_source` 분포 리포트(geocode/source_xy/none),
  지오코딩 **시도 주소 중** `status=ok`∧bbox 통과 비율 ≥ 99%
  (`lon/lat_corrected` 자체는 bbox 통과값만 채택하므로 그 통과율은 지표가 아님),
  미보정분은 quality 플래그로 식별 가능.

### Step 9. gold 모델 + 테스트

- **방법**: `models/gold/gold_commerce_license_status_current.sql` —
  grain `(district, dataset, trdstategbn)`, current 기준 스냅샷 집계(§2.2). 테스트:
  grain unique · counts-match-silver · row-counts-positive.
  (일별 추이·상태 전이 gold 는 date-spine 설계와 함께 phase-2.)
- **완료 기준**: `dbt run+test` PASS, gold 는 `ref(silver)` 만 참조.

### Step 10. transform DAG + 마무리 *(DAG 구현 완료 2026-07-05 — 잔여 문서 정리 후속)*

- **구현됨**: `commerce_localdata_transform` DAG — schedule 05:00 KST(적재 04:00 이후),
  BashOperator 2단: dbt run silver → test silver (gold 단계는 Step 9 구현 시 2단 추가).
  `common_dbt_smoke.py` 와 동일한 dbt venv/env 계약(`DBT_BIN`=이미지 dbt venv,
  target 기본 dev — `.env.commerce` 의 `COMMERCE_DBT_TARGET`/`COMMERCE_DBT_PROJECT_DIR`).
  DAG 는 무상태(전량 재빌드 오케스트레이션만) — 재실행 항상 안전.
- **잔여(후속)**: 기존 pandas silver([silver_tasks.py](../../include/silver/silver_tasks.py))
  deprecated 표기, [storage.md](../architecture/storage.md) 의 구식 기술("전체 페이지 NDJSON")
  갱신, [incremental-sort-diff.md](bronze/incremental-sort-diff.md) §6 오픈 이슈 종결,
  분기별 full_reconcile 운영 캘린더 문서화.
- **완료 기준**: dev 에서 수집 → bronze 적재 → geocode → transform 이 하루 사이클로
  end-to-end 성공. 기존 R2-parquet silver 경로에 신규 기록 없음.

## 4. 좌표 보정(geocode enrichment) 설계

### 4.1 실태와 문제

- 원천 좌표는 `X`,`Y` 두 컬럼뿐이고 **좌표계가 응답에 명시되지 않는다**. 지방행정 인허가
  표준(LOCALDATA)은 통상 **중부원점 TM(Bessel)** 계열인데, 한국 TM 은
  **EPSG:2097 vs EPSG:5174**(10.405″ 경도 보정 유무) 함정이 있어 잘못 고르면 수백 m 오차가 난다.
  → 문서 가정이 아니라 **실측으로 판별**한다(§4.2).
- 결측·0값·자릿수 오류 좌표가 존재할 수 있다(공공 인허가 데이터 통상 품질).
- **위경도(WGS84)는 원천에 없으므로** 지도/조인용 위경도는 전부 파생 산출물이다 — 따라서
  "보정"은 (a) X/Y 좌표계 변환과 (b) **주소 기반 지오코딩** 두 트랙의 병합으로 정의한다.

### 4.2 보정 전략 — 2트랙 + 실측 판별

1. **트랙 A — 좌표 변환**: `X/Y` 를 TM → WGS84 변환. `pyproj` 필요 —
   **호스트 이미지 패키지 추가는 번들 밖 변경이므로 사전 합의 필수**(CLAUDE.md Working Scope).
   합의 전에는 트랙 B 만으로 보정 좌표를 산출하고 X/Y 는 보존만 한다.
2. **트랙 B — 주소 지오코딩**: `RDNWHLADDR`(도로명, 우선) / `SITEWHLADDR`(지번, 폴백)를
   외부 API 로 지오코딩해 WGS84 위경도 획득. **외부 API 호출은 '수집'이므로 Airflow 담당** —
   dbt/Trino 는 API 를 호출할 수 없고, 응답 원본은 raw 에 보존해야 한다(§2.2 원칙 동일 적용).
3. **EPSG 실측 판별**(Step 8a): 지오코딩 성공 표본 ~1,000건에 대해 X/Y 를
   EPSG:2097 / 5174 / (대조군 5181·5186) 로 각각 변환 → 지오코딩 좌표와의 중앙값 거리가
   최소인 CRS 채택, 결과를 이 문서에 기록.

### 4.3 지오코딩 API 후보 — 비교와 확인 체크리스트

| 후보 | 성격 | 반환 좌표계 | 강점 | 반드시 확인할 것 |
|---|---|---|---|---|
| **VWorld 지오코더**(국토부) | 공공 무료 | WGS84 옵션 | 대량·저장에 상대적으로 관대, 도로명/지번 모두 | 일 쿼터(키당), 키 발급 조건, 결과 저장 약관 |
| **Kakao Local 주소 검색** | 상용(무료 쿼터) | WGS84 | 한글 주소 매칭 정확도 우수 | **결과 저장·캐싱 약관 제약(유명)**, 일 쿼터 |
| Naver Cloud Geocoding | 상용(크레딧) | WGS84 | SLA/기업 환경 | 월 무료 구간, 과금, 저장 약관 |
| juso.go.kr(도로명주소) | 공공 무료 | 별도(변환 필요 가능) | 주소 **정제·표준화** 최강 | 좌표제공 API 별도 승인, 반환 좌표계 |

**권장(잠정)**: 1차 **VWorld**(공공·무료·저장 관대) → 실패/저신뢰분만 2차 Kakao(약관 통과 시).
쿼터·약관은 시점에 따라 바뀌므로 **Step 8a 에서 실측·기록 후 확정**한다(사용자 지시와 일치:
"어떤 로직/API 로 재보정하는지 확인"이 선행 액션이다). 어떤 후보든:
- 응답 원본을 raw 에 보존해도 되는지(약관) 확인 — 불가하면 **파생 좌표만 저장**하고
  `response_json` 컬럼 생략(§2.2 원본 보존 원칙의 예외로 문서화).
- API 키는 env 로만(`VWORLD_API_KEY` 등 — KEY 네이밍 규약으로 자동 마스킹),
  URL 에 키가 들어가는 방식이면 마커/로그 저장 전 `scrub_url()`.

### 4.4 수집 파이프라인 — `commerce_geocode_collect` (신규 DAG)

별도 DAG 로 분리하는 근거(§11 예외 조건 충족): 외부 API 쿼터·장애 격리 +
백필 모드의 장시간 실행이 본 수집 라인을 막으면 안 됨. *(DAG 명칭은 #73 stage 어휘에
`geocode` 가 없으므로 §7 논의 항목.)*

```text
resolve_targets      : bronze_localdata_license 유니크 주소 전체 − 보정 완료 집합(재시도 정책
                       아래 참조), 우선순위(영업중 우선) 정렬 + 일 쿼터 상한 LIMIT.
                       일상/백필이 **같은 단일 안티조인**이라 누락일 자동 캐치업 —
                       별도 backfill 모드 불필요(params 는 상한값 조정만)
geocode_batch        : netio.http_get(timeout, max_response_bytes) + rate-limit 준수(딜레이) 호출
                       → raw/commerce/geocode/YYYY/MM/DD/run_id=<...>/geocode_<provider>.jsonl
                       + _markers (기존 경로·마커 규칙 동일 적용)
load_geocode_bronze  : bronze_geocode_address 에 delete-then-insert (bronze_run_id 단위)
finalize             : _RUN 마커 + metrics (성공/실패/not_found 카운트)
```

```sql
CREATE TABLE IF NOT EXISTS <catalog>.commerce.bronze_geocode_address (
  address_key    varchar,       -- sha256(address_norm) — 조인 키
  address_norm   varchar,       -- 정규화 주소(규칙 v1: trim → 연속 공백 1개 → '(' 이후 절단)
  address_input  varchar,       -- 실제 요청에 쓴 주소 원문
  addr_type      varchar,       -- road | jibun
  provider       varchar,       -- vworld | kakao | ...
  lon            double,
  lat            double,
  match_level    varchar,       -- 제공자 매칭 정확도 코드
  status         varchar,       -- ok | not_found | error
  response_json  varchar,       -- 원본 응답(redact 후; 약관 불가 시 생략)
  raw_object_key varchar,
  requested_at   timestamp(6),
  load_date      varchar
) WITH (format = 'PARQUET', partitioning = ARRAY['load_date'])
```

- **멱등/증분의 핵심은 `address_key`**: 같은 주소는 한 번만 지오코딩. 주소가 바뀐 업소는
  새 address_key 가 되어 자동으로 재대상화된다.
- **재시도 정책(제외 집합 정의)**: 제외 = `status='ok'` 인 address_key ∪ (`not_found` 이고
  시도 ≥ 3 또는 최근 시도 후 TTL 90일 미경과). `error`(일시 장애·쿼터 소진)는 시도 상한(5)까지
  계속 재대상화. 시도 횟수는 address_key 별 bronze 행 수 집계로 산출(별도 상태 저장소 불필요).
- **2단계 대상화**: 1차는 도로명 키. 도로명 `not_found` 확정 주소만 2차로 지번 키를 발행.
- 주소 정규화 규칙 v1 은 **Python(수집)과 dbt SQL(조인) 양쪽에 동일 구현** —
  대표 주소 픽스처 ~20종으로 Python(pytest)·dbt(test) **parity 테스트** 필수.
- 신규 DAG 이므로 `install_security()` 원샷 적용, 예외·마커는 `log_exception()`/`api_receipt()`.

### 4.5 silver 병합 — 보정 좌표 컬럼

`silver_geocode_address`(dbt): bronze_geocode 에서 address_key 당 최신 1행(성공 우선,
`requested_at desc`). license silver 는 **주소 키 2종**(`address_key_road`,
`address_key_jibun`)을 파생하고, `silver_license_current` 가 두 키로 각각 LEFT JOIN 후
`coalesce(도로명 성공, 지번 성공)` 으로 채택하여:

| 컬럼 | 정의 |
|---|---|
| `lon_source_xy`, `lat_source_xy` | 트랙 A 변환값(pyproj 합의 전 null) |
| `lon_geocoded`, `lat_geocoded` | 트랙 B 지오코딩 값 |
| `lon_corrected`, `lat_corrected` | `coalesce(지오코딩(신뢰 match_level + 서울 bbox 통과), 변환값(bbox 통과), null)` |
| `location_source` | `geocode` \| `source_xy` \| `none` |
| `location_quality` | bbox 검증·매칭 수준·양 트랙 거리 차(가능 시) 요약 플래그 |

- 서울 bbox(마진 포함 lon 126.72~127.30, lat 37.40~37.72 — 대략 검증용, 북단 도봉동 ~37.70)
  밖이면 채택하지 않고 플래그만 남긴다.
- 원본 `X`,`Y` 는 그대로 통과(보존) — 보정값이 원본을 대체하지 않는다(§2.4 재처리 원칙).
- history 는 주소 키 2종만 보유(조인은 current 에서만 — 비용/사용처 기준. 필요 시 후속 확장).

### 4.6 백필·쿼터 전략

- 대상은 행(~134만)이 아니라 **유니크 주소**(실측 필요 — Step 8a 에서 계수).
- 우선순위: ① 영업중(`TRDSTATEGBN` 영업) ② 최근 폐업 ③ 나머지. 일 쿼터로 분할하고
  `address_key` 멱등이라 중단/재개 안전. 진행률은 geocode DAG metrics 로 관측.
- 일상 운영에서 안티조인의 신규 대상은 사실상 당일 증분에 등장한 주소뿐이라 쿼터 부담 미미.

### 4.7 좌표 보정 리스크

- **약관**: 지오코딩 결과의 저장·재배포 제약(특히 상용 API). 확정 전 저장 설계를 잠그지 말 것.
- **매칭 오류**: 지번/도로명 불일치·신축 등으로 오매칭 가능 → `match_level` 필터 + bbox 검증 +
  양 트랙 거리 차 플래그로 다층 방어. 무리한 자동 확정보다 `location_quality` 로 노출.
- **주소 표기 변경**: 도로명 개편 시 대량 재지오코딩 발생 가능(address_key 변경) — 쿼터 계획에 반영.
- **pyproj**: 호스트 이미지 변경 합의 실패 시 트랙 A 는 보류 — 트랙 B 단독으로도 설계는 성립.

## 5. 운영·리스크 (공통)

- **삭제 미감지**: diff 기반이라 소스 물리 삭제 행이 current 에 유령으로 남을 수 있음(인허가는
  폐업이 상태값이라 드묾). → **분기별 `full_reconcile`** + current 키셋 vs `_diff_target` 키셋
  차집합 점검을 운영 캘린더에 등록.
- **`_diff_target/` = 유일 고위험 지점**: 이동·삭제 시 전량이 증분으로 재유입. 로더·geocode 모두
  읽기 전용. (silver 의 연속 중복 제거가 이력 오염의 최후 방어선.)
- **recollect 와 bronze**: 같은 KST 날짜에 recollect 성공으로 bronze_run_id 가 복수 생기는 것은
  정상(양쪽 DAG 모두 적재 — Step 3). `cleanup_incomplete` 가 raw 실패 파편을 지워도 incomplete
  run 엔 증분 파일이 없어 bronze 적재와 무관.
- **dbt 전량 재빌드의 장기 성장**: 수년 뒤 재빌드 시간 증가 시 incremental 전환 검토(현재는
  R2 Data Catalog 이슈로 보류).
- **초기 백필 처리량(해결)**: Trino INSERT VALUES 로 대용량을 넣으면 Iceberg 커밋 폭증 + 코디네이터
  OOM(실측 확인). → 전체 스냅샷(mode=first)은 **PyIceberg**(커밋 1회, 메모리 바운드)로 적재하고
  일일 소량 변경분만 Trino. 실행당 날짜 수 바운드로 catch-up.
- 모든 단계에서 `python -m security` 게이트 PASS + change-log 기록 유지.

## 6. 실행 순서 요약

```text
[Step 1 문서] → [2 적재 엔진·상태 ✓] → [3 적재 DAG ✓] → [4 소형 백필(이미지 통합)] → [5 dbt 골격 ✓]
→ [6 silver+테스트 ✓(07-05 암묵 버저닝 개정)] → [7 전체 백필] → [8a API·EPSG 확정 → 8b geocode 수집
→ 8c 조인 → 8d 주소 백필] → [9 gold] → [10 transform DAG ✓ + 잔여 문서 정리]
```
(✓ = 코드 구현 완료. 4·7 백필과 dbt run/test 는 Trino·PyIceberg·dbt-trino 설치된 이미지에서 검증.)

Step 8a(API 조사·좌표계 판별)는 Step 2~7 과 독립이므로 **병행 선행 가능**.

## 7. 결정 대기 / 확인 필요 항목

| # | 항목 | 성격 |
|---|---|---|
| 1 | Iceberg `commerce` 스키마 신설 + 단일 bronze 테이블(관례 `bronze_<dataset>` 대비 이탈) | 멘토 승인 |
| 2 | 지오코딩 API 확정(쿼터·약관·저장 가능 여부 실측) — §4.3 체크리스트 | 조사 후 결정 |
| 3 | X/Y 좌표계(EPSG 2097 vs 5174) 실측 판별 결과 기록 | 조사 |
| 4 | 호스트 이미지에 `trino`·`pyiceberg[s3fs]` 추가(bronze 적재) + `pyproj`(좌표 트랙 A) | 번들 밖 — 사전 합의(적재용은 사용자 승인) |
| 5 | DAG 명칭 정합 — geocode(#73 stage 어휘에 없음) · `commerce_load_bronze`/`commerce_transform`(수집 DAG 는 #23 에서 `commerce_collect_raw` 계열) | 네이밍 논의 |
| 6 | (해결) 수집·적재 **분리** — `commerce_load_bronze` 별도 DAG. 수집 DAG 는 Trino 무의존(raw-only 복원) | 반영 완료 |
| 7 | prod 버킷 명칭 정리(`seoul-prod` 문서 vs 호스트 실제 `seoul`) — prod 전환 전 | 호스트 측 정리 |
| 8 | `dbt/domains/commerce/` 신설 — 번들 밖·별도 git 저장소(dbt) 작업, 브랜치/PR 절차 준수 | 사전 합의 |
| 9 | (결정 2026-07-05) UPDATEDT 역행 시 버전 순서 — **소스 시각 우선**(UPDATEDT→LASTMODTS 정렬, 사용자 확정. §2.2) | 반영 완료 |
