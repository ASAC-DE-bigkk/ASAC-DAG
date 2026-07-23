# PROJECT.md — commerce 도메인 프로젝트 정책 (특수 요소)

이 문서는 commerce(서울 **LOCALDATA 인허가**) 도메인의 **프로젝트 고유 정책**을 관리한다.
코드/도구가 아니라 "이 프로젝트에서만 성립하는 결정"을 모은 **단일 소스**다.
정책이 바뀌면 **이 문서를 먼저 갱신**하고, 코드·리포트가 이 문서를 따른다.

관련 지침: 작업 경계·기본 규칙은 [../CLAUDE.md](../CLAUDE.md), 파이프라인 상세는 [README.md](README.md).

---

## 1. 데이터 분류 체계 (대분류 › 중분류 › 소분류)

LOCALDATA 인허가 데이터셋(현재 152종)은 **3단**으로 분류한다. 별도 저장 필드를 추가하지 않고
registry(`config/dataset_registry.yaml`)의 `category` / `sub_category` 에서 **파생**한다.

### 대분류 (major, 4종)

| 대분류 | key | 구성 |
|---|---|---|
| 보건 | `health` | **문화·산업·환경을 제외한 나머지 전부** — food, livestock, health_medical, pharmacy, animal, hygiene_beauty, optical_dental, lodging |
| 문화 | `culture` | culture |
| 산업 | `industry` | industry |
| 환경 | `environment` | environment |

> 규칙: **문화·산업·환경 외 = 전부 보건.**

### 중분류 (mid)

- **보건 하위** = registry `category` → 식품(food) · 축산(livestock) · 의료(health_medical) ·
  약국(pharmacy) · 동물(animal) · 위생·미용(hygiene_beauty) · 안경·치과(optical_dental) · 숙박(lodging)
- **문화·산업·환경 하위** = registry `sub_category` (API별 부여):
  - **문화**: 게임제공업(game) · 영화·비디오(film_video) · 공연(performance) · 관광(tourism) ·
    유원시설(amusement) · 문화예술(culture_arts) · 음악(music) · 야영장(camping) · 여행업(travel) ·
    출판·인쇄(publishing) · 체육시설(sports)
  - **산업**: 판매업(sales) · 목재(timber) · 계량기(meter) · 가스(gas) · 석유(petroleum) ·
    지하수(groundwater) · 대기배출(emission) · 담배(tobacco) · 장사(funeral) · 평생교육(education) ·
    직업소개(job_agency) · 동물(animal)
  - **환경**: 가축분뇨(manure) · 위생용품(sanitation) · 폐기물(waste) · 물재생·정화(sewage) ·
    수질오염(pollution) · 환경관리업(env_service)

### 소분류 (minor)

- **API 단위** = registry `short` (= bronze 파일명, 고유). 표기는 `한글(영문)` = `name_ko(short)`.

### 파생 규칙 (구현이 따라야 하는 계약)

```
major = category            if category in {culture, industry, environment}
      = "health"            otherwise
mid   = category            if major == "health"
      = sub_category        otherwise (문화·산업·환경)
minor = short               (API)
```

구현 참조: [`include/commerce_core/run_report.py`](../include/commerce_core/run_report.py)
— `_major`, `mid_key`, `MAJOR_KO`, `CATEGORY_KO`, `SUB_KO`. 중분류 한글 라벨을 늘리거나 고칠 때는
**이 문서와 `SUB_KO` 를 함께** 갱신한다.

---

## 2. 리포트 표기 정책 (#218 — DAG 완료 알림)

- **건수는 신규(정렬 파일 diff)** 중심: collect=`increment_count`, bronze=`rows_loaded`,
  silver=**이번 실행 신규 처리행**(이 DAG run 중 DONE 마킹된 run 의 history 적재행 — 누적 현황 아님).
  전체수집량은 병기(collect/bronze). → 전체 API 호출량·누적 현황이 아니라 **실제 신규·변경분**이 지표.
- **섹션 순서 = 에러 › 경고 › 성공.** (실패 → 부분경고 → 미수집 → API별 신규(성공) → 변경내역 없음)
  세 그룹은 **빈 줄(`\n\n`)로 간격**을 둬 시각적으로 분리한다.
- **실패는 어떤 DAG task 에서 났는지 `@task` 로 명시**(collect=`@ingest_one`, bronze=`@load_one`,
  silver=`@dbt_run_silver·dbt_test_silver`). 하드 실패(예외)는 on_failure 콜백이 별도 통지.
- 상세는 **대분류 › 중분류 › 소분류** 순, **실수집(신규>0) API 위주**로 표기.
- 신규 0(정상 수집)인 API 는 개별 나열하지 않고 **"{대분류} 하위 N개 변경내역 없음"** 으로 요약.
- 전 API(scope) 대비 결과 없는 것은 **미수집(결과없음)** 으로 reconcile 해 누락 없이 집계.

### 표기 형식 (정렬·단계 마커)
- **정렬이 필요한 표(대분류별 통계)**: Discord monospace 는 CJK 글리프 폭이 ASCII 2배가 아니어서
  (폰트별 상이) 한글을 열 사이에 넣으면 밀린다 → **정렬 격자는 ASCII(숫자)만, 한글 이름은 항상 줄 끝**.
- **분류 단계 표기는 작은 텍스트 마커 + 들여쓰기**(큰 이모지는 과해서 지양): **대분류 = `**볼드**`(마커
  없음) · 중분류 = `•` · 소분류 = `◦`**, 전각 공백(`　`)으로 뎁스 들여쓰기. 소비: `MARK_MID/MARK_API/_INDENT`.
- **API별 신규 상세는 코드블록이 아니라 마커 아웃라인**. 코드블록 안에 이모지/마커를 넣으면 폭이 또
  불규칙해져 정렬이 깨지므로, **상세는 아웃라인, 정렬 표만 코드블록**으로 분리한다.

---

## 3. 재개(resumability) 표준 — 부분 성공/실패/중단 처리

파이프라인 전 단계는 **명확한 단위**로 재개 가능해야 한다: **완료=제외 · 실패=이어받기 · 진행중 중단=
미완성분 drop 후 재실행.** 단위는 적당한 수준(per-dataset / per-run)이며 행 단위처럼 과하게 세분하지 않는다.

| 단계 | 단위 | 완료 제외 | 실패 이어받기 | 중단 → 미완성 drop |
|---|---|---|---|---|
| **raw** | (dataset, run) | 당일 `completed` 마커 제외 | `recollect`(incomplete 재수집) | 재수집이 파일 덮어씀 + `cleanup_incomplete` |
| **bronze** | (dataset, run) | 워터마크 이후만 | `commit_watermark`(실패 run 직전까지만 전진) | PyIceberg **delete+append 원자 트랜잭션**(재적재 시 delete 선행) |
| **silver** | (dataset, bronze_run_id) | **DONE 마커**(dbt test 통과분) | 미마커 run 재처리 | `delete_unmarked_silver_history_runs`(미완성 이력 삭제 후 재append). current/detail 은 grain 단위 incremental(원자) |
| **gold** | (Iceberg·dbt) grain/워터마크 | incremental 워터마크(collected_at) 이후만 | 미반영 창 재처리 | dbt incremental(delete+insert/append) · full-refresh(§4) |
| **유지보수(#226)** | (테이블, op) | 멱등 → 성공 재실행 무해 | 실패 op 만 재실행 | 각 op 원자·멱등 |

- **불변식**: `silver_license_history` 는 **append-only(전 버전 보존)** — 값이 바뀌어도 이전 값은 삭제되지
  않는다. current/detail 은 '최신 포인터'만 grain 단위로 교체(#81).
- **신규 개발 규약**(gold 포함): (1) 처리 단위를 명확히 정하고, (2) 완료 마커/워터마크로 완료분 skip,
  (3) 중단 시 미완성분을 식별해 **drop 후 재실행**(원자 트랜잭션 또는 delete-then-write). 단위는 적당히 큼.

---

## 4. 서빙 레이어 정책 — gold(Iceberg) → D1(SQLite) 선별 서빙

> 2026-07-14 사용자 확정: **서빙 DB 는 Postgres 가 아니다.** 기존 serving Postgres 경로(Python
> gold 적재·`commerce_entity_key` bigserial·전용 컨테이너)는 폐기한다. 서빙 대상은
> **Cloudflare D1(SQLite)** 이며, gold 는 bronze/silver 와 동일한 **Iceberg 카탈로그** 구조로 만든다.

### 4.1 아키텍처 — 레이어 정의(2026-07-15 사용자 확정, #70 재분류)

```
Raw/Bronze  원본 보존
Silver      결측 처리 · 표준화 · 중복 제거 · **테이블 단위 정리 · JOIN 가능한 모델링(원형)**
Gold        업무 목적별 **집계·지표·인사이트만**
Serve       API·화면 조회 최적화 최종 결과 = D1(SQLite) 선별 export(예정)
```

- **원형(entity/entity_history/detail)은 silver 소속** — 명칭 `silver_*`, 파이프라인도
  `commerce_load_silver` 에 편승(dbt 4모델 + 카탈로그 구동 detail 적재).
- **gold 는 집계 전용** — `gold_license_dong_summary` 등. `commerce_load_gold`(06:00)는
  집계만 빌드·누적한다.
- detail 생성용 스펙(실측→클러스터 규칙)은 **파생 과정 메타** — `meta_detail_catalog`
  (별도 meta_ 단위로 관리).
- 전부 Iceberg(dbt-trino/Trino) — 리니지는 dbt/Cosmos 네이티브 + 물리명 stitch.

### 4.2 DB 특성에 따른 설계 원칙 (필수 준수)

서빙 DB 특성이 gold·서빙 레이어 설계를 결정한다. **D1(SQLite) 특성** 과 그에 따른 원칙:

| D1/SQLite 특성 | 설계 원칙 |
|---|---|
| DB 당 용량 상한(D1 10GB, 실용 권장 ≪1GB)·단일 파일 | **소형 테이블만 export** — 전량 이력·`record_json`(통짜 JSON)·수백만 행 원장은 D1 금지, Iceberg gold 에만 둔다 |
| 단일 writer·동시 쓰기 제약 | export 는 **전량 교체 스냅샷**(재생성) 우선 — 증분 upsert 대신 파일 단위 재생성이 단순·안전 |
| 시퀀스/bigserial 없음(rowid 만) | **식별자는 자연키/결정적 키** — DB 발급 서러게이트 금지. `commerce_entity_key`(Postgres bigserial) 계약 종료, 자연키 = (dataset, opnsfteamcode, mgtno) |
| 서버리스 엣지 **읽기 최적화** | export 대상은 조회 형태로 **사전 집계/평탄화**(조인 최소화) — 행정동 집계·현재 상태 등 |
| 동적 타이핑(타입 강제 약함) | export 시 타입 정규화(문자/정수/실수 명시) — 좌표 double·코드 varchar 유지 |

**규약**: 서빙 DB 를 바꾸거나 export 대상을 늘릴 때는 반드시 이 표(§4.2)에 대상 DB 특성을 먼저
정리하고, gold(Iceberg) 모델과 export 계층을 그 특성에 맞게 설계한다 — "DB 가 바뀌면 서빙 설계도
바뀐다"가 원칙이다.

### 4.3 원형(silver)·집계(gold) 구조 — RDB 관계형 모델링 승계(재심의 2026-07-14 · 재분류 #70)

> 집계 테이블만으로 축소하지 않는다 — **API 별로 컬럼이 크게 상이**하므로, 기존 RDB gold 의
> 관계형 모델링(코어 + 카탈로그 구동 detail)을 Iceberg 로 그대로 승계해 **서빙 가능한 단위**
> (테이블·컬럼)로 데이터를 뽑아낼 수 있어야 한다(사용자 확정).

| gold 객체(Iceberg) | 도구 | 내용 | D1 export |
|---|---|---|---|
| `silver_license_entity` | dbt | 업소 현재 상태(공통 컬럼, 자연키 grain) — **원형=silver(#70)** | 선별(필터/컬럼 축소) 후보 |
| `silver_license_entity_history` | dbt | 버전 이력 프로젝션(append, collected_at 증분) | ❌ (대용량 — Iceberg 전용) |
| `meta_detail_catalog` | Python(Trino) | detail 스펙 정본(파생 과정 — 별도 meta_ 단위) | ❌ (내부 메타) |
| `silver_<domain>_detail` ×N | Python(Trino) | **API 별 상이(비공통) 컬럼 평탄화** — 카탈로그 구동(원형=silver) | 대상별 선별 후보 |
| `gold_license_dong_summary` | dbt | 행정동별 업소/영업/폐업 집계(소형) | ✅ 1순위 |
| `gold_license_flow_daily/monthly/yearly` | dbt | 개업/폐업 흐름 × 업종 3단 × 지역 3축 — **완결 기간만+지연보정 창**(이미 적재된 기간 재적재 없음, 중복 불가) | ✅ 기간 키 증분 |
| `gold_license_status_duration` | dbt | 상태 전이 지속기간 요약(업종·상태군·진행중) — 이력 기반 | ✅ (소형) |
| `gold_env_facility_operation` | dbt | 가동 시간·일수 축(환경 2종 — **영업시간/요일 필드는 원천 부재** 실측) | 후보 |

- **서빙 단위 추출** = 코어(entity) ⋈ detail(자연키 조인) — D1 export 는 이 조합에서 대상별
  필터·컬럼 축소로 뽑는다(§4.2 원칙).
- detail 이 dbt 가 아닌 이유: 카탈로그 구동(런타임 실측으로 테이블 셋이 변함) — dbt 정적 모델로
  표현 불가. 단 적재는 Trino `INSERT INTO … SELECT`(문장 원자·행이 Python 을 안 거침)로 수행.
- 재개 표준(§3): dbt incremental(워터마크) + detail 은 **테이블 자체의 멤버별 max(collected_at)**
  이 워터마크(별도 마커 없음) + full-refresh(`commerce_load_gold_refresh`).
- **뷰 320 개만 미승계**(Iceberg REST 카탈로그 뷰 제약) — detail 직접 조회(`WHERE dataset=…`)로
  대체. code_value 집계는 후속 포팅 후보.

---

## 5. 작업 워크플로 (이슈 → 브랜치 → PR)

commerce 도메인의 모든 변경은 **GitHub 이슈 → 이슈 번호 기반 브랜치 → PR** 순서를 따른다.
"먼저 이슈로 무엇을·왜 바꾸는지 기록"하고, 브랜치·PR 이 그 이슈로 추적되게 하는 것이 목적이다.

1. **이슈 먼저**: 작업 단위마다 ASAC-DAG 에 이슈를 새로 만든다. 제목은 `[Feat]/[Fix]/[Docs]/[Chore]`
   접두 + 요약, 본문은 기존 이슈 폼(**목표·배경 / 작업 체크리스트 / 완료 조건 / 카테고리 / 관련 레이어 /
   브랜치 이름(예정) / 관련 자료**)을 따른다. 라벨은 `type: *` + `area: *`(문서면 `documentation`).
2. **브랜치명 = `{type}/{issue#}-{slug}`** — 이슈 번호를 반드시 포함한다.
   - `type` ∈ `feat`(기능) · `fix`/`hotfix`(버그·긴급수정) · `docs`(문서) · `chore`(잡무).
   - 예: 이슈 #223 → `hotfix/223-bronze-load-recent-window`, #241 → `docs/241-api-field-coverage`.
   - `slug` 은 영문 kebab-case 로 짧게. 브랜치는 원칙적으로 `dev` 에서 딴다.
3. **PR**: 브랜치 완성 후 `dev` 를 base 로 PR 을 올리고, 본문에 **`Closes #<issue>`** 로 이슈를 연결해
   머지 시 자동 종료되게 한다. PR 제목도 `type(scope): 요약 (#issue)` 형태를 권장한다.
4. **커밋 상시 · push/PR 은 승인 후**: 커밋은 언제든 하되 **원격 push 와 PR 생성은 사용자 승인 후**에
   한다(근거·상세: [../CLAUDE.md](../CLAUDE.md) Working Scope). 변경은 자기 도메인
   (`dags/domains/commerce/`) 안에서만 한다.

---

## 6. 문서 지도 (뎁스별 인덱스)

commerce 전체 문서를 **레포·폴더 뎁스**로 조망하는 마스터 인덱스. 각 폴더는 **자체 README** 로 다시
진입하고, 그 README 가 폴더 안 파일을 인덱싱한다(폴더→README→파일 3단). 여기서는 최상위에서 **어디에
무엇이 있는지**를 한 줄 요약으로 정리한다. 파이프라인·운영·보안·정책은 **dags 번들**,
silver/gold 변환·DB 명세는 **dbt 번들**(별도 서브모듈 ASAC-DBT — 경로로 표기).

### 6.1 dags/domains/commerce/docs/ — 수집~적재 파이프라인·운영·정책

- [README.md](README.md) — 문서 최상위 인덱스(주제별 폴더 진입)
- **PROJECT.md**(이 문서) — 프로젝트 고유 정책 단일 소스(분류·리포트·재개·워크플로·문서지도)
- [architecture/](architecture/README.md) — 설계·구조
  - [architecture.md](architecture/architecture.md) — 메달리온 배치 아키텍처(DAG·계층 책임·LocalExecutor)
  - [project_setting.md](architecture/project_setting.md) — 카테고리 자립형 폴더 규약(heritage)·이식성
  - [storage.md](architecture/storage.md) — 결정적 저장 경로·마커·리니지·R2
- [configuration/](configuration/README.md) — 실행 인자·환경
  - [configuration.md](configuration/configuration.md) — 환경변수·`.env.commerce` 주입·우선순위
  - [environments.md](configuration/environments.md) — 스토리지 백엔드(local/r2) 환경 분리
- [pipeline/](pipeline/README.md) — 레이어별 파이프라인
  - [common_info.md](pipeline/common_info.md) — API 응답 컬럼 계약(v1 공통14+준공통5 / v2 별칭)·식별값
  - [data-model.md](pipeline/data-model.md) — 레이어 계보·조인키·테이블 정의·**139→152 공통화(lf 매크로)**
  - [non-license-datasets.md](pipeline/non-license-datasets.md) — 인허가 외 격리 사유·재활성 절차
  - [raw/](pipeline/raw/README.md) — 수집 레이어 분석 8종(field-coverage·call-volume·caveats·incremental-sort-diff·pagination-ordering·status-tracking·resolve-worklist·uncollectable)
  - [bronze/](pipeline/bronze/README.md) — Iceberg 적재(테이블·엔진분기·워터마크·유지보수·재개)
  - [silver/](pipeline/silver/README.md) — dbt 정규화(모델·v1/v2 통합·보강·증분/재개 마커)
  - [gold/](pipeline/gold/README.md) — 서빙·집계 gold(설계/계획)
  - (역사적 계획: [medallion-implementation-plan.md](pipeline/medallion-implementation-plan.md) · [silver-gold-load-plan.md](pipeline/silver-gold-load-plan.md))
- [operations/](operations/README.md) — 운영 런북
  - [operations.md](operations/operations.md) · [deploy-local.md](operations/deploy-local.md) · [deploy-dev.md](operations/deploy-dev.md) · [deploy-prod.md](operations/deploy-prod.md) · [recollect-and-alerts.md](operations/recollect-and-alerts.md)
- [security/](security/README.md) — 보안 게이트
  - [security.md](security/security.md)(위협모델·가드·단일점검) · [usage.md](security/usage.md) · [techniques.md](security/techniques.md) · [adoption.md](security/adoption.md)

### 6.2 dbt/domains/commerce/docs/ — silver/gold 변환·DB 명세 (ASAC-DBT 번들)

> 별도 서브모듈이라 GitHub 상 상대링크가 끊기므로 **경로**로 표기한다. 진입점은
> `dbt/domains/commerce/docs/README.md`(용도별 인덱스) → 하위 폴더 README.

- `docs/README.md` — dbt 문서 용도별 인덱스(설계근거·규약·운영·DB명세·기계용)
- 설계 근거: `silver-noncommon-catalogs.md`(**API 152종 실측 → 공통/비공통 분리·이력·cluster/single 경계**) · `dataset-columns.md`
- 규약: `address-and-geo.md`(주소·행정동/법정동·좌표) · `timestamps-and-nulls.md`
- 운영·학습: `rebuild-and-ops.md`(§6 청크 백필) · `beginner-guide.md`(모델 읽기·Trino 조회)
- `DB/` (`DB/README.md` — ERD·키 규약)
  - `DB/silver/` — silver 테이블(history/current/marker)
  - `DB/gold/` — `tables.md`(entity+이력·dim·detail 78) · `views.md`(도메인8×2·API152×2) · `cluster-domain-coherence.md`(**cluster 8 도메인 정합성 검증**)
- 기계용 산출물: `api-field-inventory.csv`(152) · `api-field-clusters.json` · `gold-catalog.csv`(103)

---

## 7. 변경 이력

- 2026-07-23: **서빙 D1 export 구현 + 계약 tier 화**(#493 · ASAC-DBT#334) — §4 의 gold→D1 선별 export 를
  gold **빌드 라인과 분리한 신규 DAG**(`commerce_serving_export`, gold 완료 Asset 트리거)로 구현(사용자
  확정 — spec §1.4 의 gold DAG 내 편입 대신 분리). gold `meta.serving` 을 행수캡 `enabled` →
  **`serving_tier`(d1_direct/d1_rollup/iceberg_api)** 로 재정리해 "지정 품목"을 정본화. 코어 서빙셋
  (direct 15 + rollup 7); dim·current-period 는 후속.
- 2026-07-14: **서빙 레이어 전면 개편(§4 신설)** — 서빙 DB Postgres 폐기, 대상 = **D1(SQLite)**.
  gold 는 bronze/silver 와 동일 **Iceberg 카탈로그**(dbt)로 재구축하고 **선별 소수 테이블만 D1 export**
  (예정). "DB 특성(용량 상한·단일 writer·시퀀스 없음·엣지 읽기 최적화)에 따라 gold·서빙 레이어를
  설계한다" 원칙 명문화(§4.2). `commerce_entity_key`(bigserial) 계약 종료 — 자연키로 전환. 기존
  serving Postgres 리소스(컨테이너·볼륨·Python gold 적재 경로) 삭제(사용자 지시).

- 2026-07-14: **silver 리포트 지표를 현재행수(누적) → 이번 실행 신규 처리행으로 변경**(#66 후속,
  사용자 지시). 근거: 누적 현황은 신규 적재가 없어도 매일 전체를 반복 보고 — collect/bronze 와 같은
  "실제 신규분" 지표로 통일. 구현: 이 DAG run 중 DONE 마킹된 run(marker_source=dbt_test_silver·
  processed_no_rows)의 history 적재행을 dataset 별 집계(seed 청크빌드·Cosmos 증분 모두 포괄).
- 2026-07-10: **문서 지도(§5)** 신설 — 양 번들(dags/dbt) 문서를 폴더 뎁스로 인덱싱(폴더→README→파일).
  dbt docs 에 폴더별 README(root·DB/gold·DB/silver) 추가로 3단 인덱싱 완성.
- 2026-07-09: **정책 변경 시 §변경 이력에 요약 항목 남기기** 규정을 CLAUDE.md(Project policy)에 명문화
  (#241). PROJECT.md §변경 이력=정책 요약, `change-log.md`=구조 변경 운영 이력으로 역할 구분.
- 2026-07-09: **작업 워크플로(이슈 → `{type}/{issue#}-{slug}` 브랜치 → `Closes #` PR)** 를 §4 로
  명문화(#241). 기존 관행(이슈 번호 브랜치)을 정책 단일 소스에 편입.
- 2026-07-09: **3단 분류(대분류 4 / 중분류 / 소분류)** 도입, DAG 완료 리포트에 반영(#218 후속).
  이전에는 registry `category`(11종)를 단일 대분류로 사용했음 → 이제 그 11종은 **중분류**로 내려가고,
  대분류는 보건/문화/산업/환경 4종으로 재정의.
- 2026-07-09: 리포트 표기 보강 — 섹션 순서 **에러›경고›성공**(그룹 간 빈 줄 간격), 실패에 **`@task`
  명시**, 분류 단계는 **작은 텍스트 마커(볼드/•/◦)+들여쓰기** 아웃라인(초기 색 이모지 🟣🔵🟠는 크기 과해 교체).
- 2026-07-09: **재개(resumability) 표준**(§3) 명문화 — 단위별 완료제외·실패이어받기·중단 시 미완성 drop.
  **Iceberg 유지보수 일일 task(#226)**((테이블,op) 멱등 + 자원 리포트), bronze 적재 **최근 N일 창**(#223),
  silver current/detail **증분화**(#81), silver **분야별 detail**(#80) 병행.
