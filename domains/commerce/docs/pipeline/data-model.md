# commerce 데이터 모델 — 레이어 계보 · 테이블 정의 · 139→152 공통화

> 이 문서는 commerce(서울 LOCALDATA 인허가) 파이프라인에서 **어떤 값이 어떻게 연결·추가되고,
> 각 레이어의 테이블 정의가 무엇인지**를 한 장으로 정리한다. 레이어별 상세는
> [raw/](raw/README.md) · [bronze/](bronze/README.md) · [silver/](silver/README.md) · [gold/](gold/README.md),
> API 응답 컬럼 계약은 [common_info.md](common_info.md), 분류 체계는
> [PROJECT.md §1](../PROJECT.md) 을 본다. 실측일 기준 **152종**(v1 인허가 139 + v2 환경 13).

---

## 1. 한눈에 — 메달리온 레이어

```text
[수집원] 서울 열린데이터광장 OpenAPI (LOCALDATA 표준)   152종 = v1 139 + v2(환경) 13
   │  commerce_collect_raw(@daily) / commerce_recollect_raw(6h) / commerce_collect_watchdog
   ▼
[raw]    R2 오브젝트(NDJSON) — {prefix}/raw/commerce/load_date=YYYY-MM-DD/run_id=.../<short>.jsonl (마커는 ops 마커 존)
   │       불변 원본 · 수집 상태 마커 · 롤링 diff 전체본(_diff_target). 테이블 아님(파일).
   │  commerce_load_bronze(04:00 KST) — 최근 N일 창 증분, 엔진 분기(first=PyIceberg / 이후=Trino)
   ▼
[bronze] Iceberg  bronze_localdata_license (record_json 통짜, schema-on-read, 152종 단일 테이블)
   │                + bronze_collection_run_manifest (발행 게이트)
   │  commerce_load_silver(05:00 KST) — dbt (Trino) · 보강 참조 전량교체 · DONE 마커 증분
   ▼
[silver] dbt  silver_license_history (증분 append, 전 버전 보존)
   │           silver_license_current  (grain 최신 1행)
   │           silver_license_detail_health (보건 대분류 업종별 상세)
   │              ← 보강: bronze_ref_admin_dong · bronze_address_enrichment(Juso) · silver_load_run_marker
   ▼
[gold]   서빙 Postgres 마트 — 카탈로그 구동(Python) 증분 적재. commerce_load_gold(06:00 KST). 자세히: gold/README.md
```

용어 주의: 이 프로젝트에서 **raw = R2 의 NDJSON 랜딩**(수집), **bronze = Iceberg 웨어하우스 원본층**(적재)
이다. (초기 문서가 수집을 "bronze"로 부르던 흔적이 있어 2026-07 재편으로 폴더를 레이어별로 분리했다.)

---

## 2. 핵심 — "139 공통"과 "152종"의 관계 (오해 해소)

**bronze 를 "공통 139종"이라 읽으면 오해다.** 실제 관계는 다음과 같다.

1. **bronze 는 스키마를 강제하지 않는다(schema-on-read).** `bronze_localdata_license` 는 응답 레코드
   1행을 `record_json` 문자열로 **통짜 보존**하고, 고정 컬럼은 계보/식별용 14개뿐이다(§4.1). 그래서
   v1 이든 v2 든 **필드명이 달라도 테이블 모양은 동일** — 152종이 한 테이블에 그대로 들어간다.
2. **"공통 14"는 v1 139종의 응답 필드 교집합일 뿐이다.** v2(환경) 13종은 필드명 체계가 완전히
   다르다(`MGTNO`→`MNG_NO`, `TRDSTATEGBN`→`SALS_STTS_CD`, `X/Y`→`XCRD/YCRD` …). 즉 bronze 단계에서는
   **139 과 13 이 아직 안 합쳐져 있다.** (측정 근거: [raw/api-field-coverage.md](raw/api-field-coverage.md).)
3. **합쳐지는 지점은 silver 다.** `silver_license_history` 의 `parsed` CTE 가 **`lf(v1, v2)` 매크로**로
   v1 필드를 우선, 없으면 v2 필드를 꺼내 **하나의 canonical 컬럼셋**으로 정규화한다. 이 CTE 를 지나는
   순간부터 **152종 전부가 공통 스키마**가 된다.

```text
bronze  ┌ v1 139종: MGTNO, TRDSTATEGBN, RDNWHLADDR, X, Y, UPDATEDT ...   ┐  (record_json 안, 미통합)
        └ v2  13종: MNG_NO, SALS_STTS_CD, ROAD_NM_ADDR, XCRD, YCRD ...   ┘
                              │  silver: lf('MGTNO','MNG_NO') = coalesce(v1, v2)
                              ▼
silver         mgtno, trdstategbn, road_address, source_coord_x/y, updatedt ...   → 152종 공통
```

`lf` 매크로(실체) — [`dbt/domains/commerce/macros/localdata_field.sql`](../../../../../dbt/domains/commerce/macros/localdata_field.sql):

```sql
{% macro lf(v1, v2=none) -%}
  {%- if v2 -%}
    coalesce(nullif(trim(json_extract_scalar(record_json,'$.{{ v1 }}')),''),
             nullif(trim(json_extract_scalar(record_json,'$.{{ v2 }}')),''))
  {%- else -%}
    nullif(trim(json_extract_scalar(record_json,'$.{{ v1 }}')),'')
  {%- endif -%}
{%- endmacro %}
```

적용부 — [`silver_license_history.sql`](../../../../../dbt/domains/commerce/models/silver/silver_license_history.sql) `parsed` CTE:

```sql
{{ lf('MGTNO','MNG_NO') }}          as mgtno,
{{ lf('OPNSFTEAMCODE','OGDP_INST_CD') }} as opnsfteamcode,
{{ lf('TRDSTATEGBN','SALS_STTS_CD') }}   as trdstategbn,
{{ lf('RDNWHLADDR','ROAD_NM_ADDR') }}    as road_address,
{{ lf('X','XCRD') }}                as source_coord_x,
{{ lf('Y','YCRD') }}                as source_coord_y,
{{ lf('UPDATEDT','DATA_UPDT_YMD') }}     as updatedt,
...   -- v1↔v2 별칭 전체 표: schemas.py COLUMN_ALIASES_V2
```

> 참고: Python `COLUMN_ALIASES_V2`([schemas.py](../../include/commerce_core/schemas.py))는 **bronze** 가
> 승격 컬럼(`mgtno`,`updatedt`)을 v1/v2 무관하게 채울 때 쓰는 `canonical_get` 용 매핑이고,
> **silver 의 실제 v1/v2 병합은 위 `lf()` 매크로**가 담당한다(둘은 같은 별칭을 SQL/Python 두 곳에서 구현).
> record_json 은 silver 이후에도 그대로 실려 다녀(§4.4 current/detail) API별 비공통 필드는 유지된다.

---

## 3. 값이 "연결"되는 축 — 조인키·계보 컬럼

레이어를 가로지르며 같은 레코드를 잇는 키는 다음과 같다.

| 키 | 의미 | raw | bronze | silver | 비고 |
|---|---|:--:|:--:|:--:|---|
| `dataset` (=short) | API(데이터셋) 식별 | 파일명 | 컬럼 | 컬럼 | 152종 slug, registry 단일 출처 |
| `MGTNO` / v2 `MNG_NO` → `mgtno` | 인허가 관리번호 | record | 승격 컬럼 | 식별 grain | 발급 자치단체 안에서만 유니크 |
| `OPNSFTEAMCODE` / v2 `OGDP_INST_CD` → `opnsfteamcode` | 개방자치단체코드 | record | record_json | 식별 grain | **MGTNO 와 함께**여야 전역 유니크(#198) |
| `bronze_run_id` | 어느 raw run 에서 왔나 | run_id 폴더 | 컬럼 | marker | `YYYY-MM-DD_HHMMSS_mmm`(KST) |
| `content_hash` | 레코드 내용 지문 | 마커(page) | 컬럼 | 인접중복 판정 | canonical JSON sha256 |
| `collected_at` | 수집 시각 | — | UTC(원본) | +9h KST | bronze=UTC 진실, silver=KST 일원화 |
| `address_key_road/jibun` | 주소 지오코딩 조인 | — | — | 파생(sha256) | Juso 캐시(`bronze_address_enrichment`) 조인 |
| `sgg_name`/행정동 코드 | 자치구·법정/행정동 | — | — | 파생 | `bronze_ref_admin_dong`(서울만) 조인 |

silver 식별 grain 은 **`(dataset, opnsfteamcode, mgtno)`** 다(MGTNO 가 자치단체 내에서만 유니크하므로
opnsfteamcode 를 반드시 포함). history 의 행 grain 은 여기에 `(collected_at, content_hash)` 를 더한다.

---

## 4. 테이블 정의 (레이어별)

### 4.0 raw — 테이블 아님(R2 오브젝트)

RDB/외부 매니페스트 없이 **R2 오브젝트 + 마커 파일**이 상태의 전부다. 경로/마커 계약은
[raw/README.md](raw/README.md) · [common_info.md](common_info.md). 코드: `include/commerce_core/paths.py`.

```text
{prefix}/raw/commerce/load_date=YYYY-MM-DD/run_id=<...>/<short>.jsonl        # API당 1파일(NDJSON)
{prefix}/ops/control/state/commerce/markers/load_date=YYYY-MM-DD/run_id=<...>/<short>.completed|.incomplete
{prefix}/ops/control/state/commerce/diff_target/<short>.<YYYY-MM-DD>.jsonl      # run 무관 롤링 전체본(증분 기준)
```

### 4.1 bronze — `bronze_localdata_license` (Iceberg)

레코드 1행 = 1행. **`record_json` 통짜 + 계보 컬럼**(파싱·정제 금지, append-only 변경로그).
정의: [`include/bronze/warehouse.py`](../../include/bronze/warehouse.py). 멱등 단위 `(dataset, bronze_run_id)`.

| 컬럼 | 타입 | 의미 |
|---|---|---|
| `dataset` | varchar | short(API) |
| `mgtno` | varchar | 승격 키(`canonical_get`: v1 MGTNO / v2 MNG_NO) |
| `updatedt` | varchar | 승격 갱신일(v1 UPDATEDT / v2 DATA_UPDT_YMD) |
| `record_json` | varchar | **원본 레코드 통짜**(가공 금지) |
| `content_hash` | varchar | 키정렬 canonical JSON sha256 |
| `observed_date` / `load_date` | varchar | 논리 수집일 / 적재일(KST, **파티션=load_date**) |
| `bronze_run_id` / `dag_run_id` / `raw_object_key` | varchar | 계보(어느 raw run·DAG run·R2 객체) |
| `increment_mode` | varchar | first(전체) / changed(증분) — 엔진 분기 근거 |
| `record_seq` | integer | 파일 내 행 순번 |
| `schema_version` | varchar | 스키마 버전 |
| `collected_at` | timestamp(6) | 수집 시각(**UTC 원본**) |

### 4.2 bronze — `bronze_collection_run_manifest` (발행 게이트)

silver 가 "발행 가능한 run"만 읽도록 하는 게이트. grain **`(source_id, bronze_run_id)`**,
`source_id = commerce_localdata_<short>`. 정의: `warehouse.py` `write_manifest`.

| 컬럼 | 의미 |
|---|---|
| `source_id`, `dataset`, `bronze_run_id`, `dag_run_id` | 식별/계보 |
| `status`(SUCCESS/…), `is_publishable`(bool) | 발행 판정 — silver 조인 조건 |
| `rows_loaded`, `rows_expected` | 적재/기대 행수 |
| `observed_date`, `load_date`(파티션), `engine`, `event_at` | 메타 |

### 4.3 silver 보강 참조·캐시 (Iceberg, Airflow 가 적재)

| 테이블 | 정의 | grain / 적재 | 용도 |
|---|---|---|---|
| `bronze_ref_admin_dong` | `include/silver/enrich_tasks.py` | 전량 교체(DELETE→INSERT) | 행정동↔법정동 매핑(**서울만** `sido_name='서울특별시'`) → gu_code/법정·행정동 파생. 13열(sido/sgg/행정·법정동 코드·명 등) |
| `bronze_address_enrichment` | `enrich_tasks.py` | `road_address_norm` 키 단위 upsert | 도로명→지번 **Juso 보강 캐시**(`status='filled'` 만 silver 소비) |
| `silver_load_run_marker` | `include/silver/silver_markers.py` | `(dataset, bronze_run_id, status='DONE')` | silver 증분 DONE 마커(dbt test 통과 후 기록) |

### 4.4 silver 모델 (dbt, Trino)

정의: [`dbt/domains/commerce/models/silver/`](../../../../../dbt/domains/commerce/models/silver/). **gold 는 서빙 Postgres 마트로 구현·가동 중**(카탈로그 구동 Python, `commerce_load_gold` — dbt 모델 아님. 명세: [DB/gold/](../../../../../dbt/domains/commerce/docs/DB/gold/)).

| 모델 | materialization | grain | 설명 |
|---|---|---|---|
| `silver_license_history` | incremental **append** | 행 `(dataset, opnsfteamcode, mgtno, collected_at, content_hash)` | 파싱(lf, 152 공통)+보강+**인접중복 제거**. **전 버전 보존**(값이 바뀌어도 과거행 유지, A→B→A 원복 보존) |
| `silver_license_current` | incremental **delete+insert**(unique_key=grain) | `(dataset, opnsfteamcode, mgtno)` | history 버전정렬 **최신 1행**(row_number=1) + 마스킹 주소 동단위 null 처리 |
| `silver_license_detail_health` | incremental **delete+insert** | `(dataset, opnsfteamcode, mgtno)` | **보건 대분류** 업종별 상세 컬럼(record_json 에서 업종 고유 필드 추출, ~130열) |

> 주의: 스케줄 실행(`commerce_load_silver`)은 `silver_license_history silver_license_current` **2개만**
> 빌드한다. `silver_license_detail_health` 는 전체 `dbt run`/`--full-refresh` 로만 갱신된다.

### 4.5 gold — 서빙 Postgres 마트 (구현·가동 중)

gold 는 dbt 모델(`models/gold/`)이 아니라 **카탈로그 구동 Python**(`commerce_load_gold` DAG)로 silver 를
읽어 서빙 Postgres 에 증분 적재한다(entity/history·detail 78·dim 3·view 320). 서빙 DB 의 인덱스/bigserial/뷰
계약이 dbt-trino 로 표현 불가해 Python 이다. 규칙: [include/gold/](../../include/gold/) · 명세:
[DB/gold/](../../../../../dbt/domains/commerce/docs/DB/gold/) · 개요: [gold/README.md](gold/README.md).

---

## 5. 값이 "추가"되는 지점 — 레이어별 파생·보강

| 레이어 | 새로 생기는 값 | 근거 |
|---|---|---|
| raw | 계보 메타(run_id·마커·content_hash·pages), 롤링 diff 로 **변경분만** | `bronze_tasks`, `_diff_target` |
| bronze | 승격 컬럼(mgtno/updatedt), content_hash, 적재 계보, manifest 발행판정 | `warehouse.py` |
| silver(파싱) | **v1/v2 병합 canonical 컬럼**(lf), 빈문자→null, timestamp 파싱, KST 변환 | `parsed`/`keyed` CTE |
| silver(중복제거) | 인접중복 제거(전 버전 보존), 버전정렬키 `updatedt_sort`/`lastmodts_sort` | `ordered`/`deduped` CTE |
| silver(보강) | 자치구 `gu`/`gu_code`, 법정·행정동 명/코드, 지번 Juso 보강, **좌표 EPSG:5174→WGS84 변환** | `enrich_tasks` + history 보강 CTE |
| gold | 카탈로그 구동 서빙 Postgres 마트(entity/history·detail·dim·view) | 구현·가동 중 |

**불변식**: `silver_license_history` 는 append-only(전 버전 보존) — 값이 바뀌어도 이전 값은 남는다.
`record_json` 은 silver current/detail 까지 실려 다녀 **비공통(업종별) 필드는 유실 없이 보존**된다.

---

## 6. 참조

- 분류 체계(대·중·소): [../PROJECT.md](../PROJECT.md) §1
- API 응답 컬럼 계약(152종, v1 공통14+준공통5+v2 별칭): [common_info.md](common_info.md)
- 레이어 상세: [raw/](raw/README.md) · [bronze/](bronze/README.md) · [silver/](silver/README.md) · [gold/](gold/README.md)
- 필드 커버리지 실측: [raw/api-field-coverage.md](raw/api-field-coverage.md)
- 코드: registry `config/dataset_registry.yaml` · bronze `include/bronze/warehouse.py` ·
  silver `dbt/domains/commerce/models/silver/` · 보강 `include/silver/enrich_tasks.py`
