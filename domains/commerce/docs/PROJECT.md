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
  silver=현재행수(누적). 전체수집량은 병기. → 전체 API 호출량이 아니라 **실제 신규·변경분**이 지표.
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
| **gold** | (미구현) | — | — | 구현 시 **동일 패턴**: DONE 마커 + 미마커 drop, 단위=(집계키, 입력 run) |
| **유지보수(#226)** | (테이블, op) | 멱등 → 성공 재실행 무해 | 실패 op 만 재실행 | 각 op 원자·멱등 |

- **불변식**: `silver_license_history` 는 **append-only(전 버전 보존)** — 값이 바뀌어도 이전 값은 삭제되지
  않는다. current/detail 은 '최신 포인터'만 grain 단위로 교체(#81).
- **신규 개발 규약**(gold 포함): (1) 처리 단위를 명확히 정하고, (2) 완료 마커/워터마크로 완료분 skip,
  (3) 중단 시 미완성분을 식별해 **drop 후 재실행**(원자 트랜잭션 또는 delete-then-write). 단위는 적당히 큼.

---

## 4. 작업 워크플로 (이슈 → 브랜치 → PR)

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

## 5. 변경 이력

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
