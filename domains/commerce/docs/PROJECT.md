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
- **실패는 어떤 DAG task 에서 났는지 `@task` 로 명시**(collect=`@ingest_one`, bronze=`@load_one`,
  silver=`@dbt_run_silver·dbt_test_silver`). 하드 실패(예외)는 on_failure 콜백이 별도 통지.
- 상세는 **대분류 › 중분류 › 소분류** 순, **실수집(신규>0) API 위주**로 표기.
- 신규 0(정상 수집)인 API 는 개별 나열하지 않고 **"{대분류} 하위 N개 변경내역 없음"** 으로 요약.
- 전 API(scope) 대비 결과 없는 것은 **미수집(결과없음)** 으로 reconcile 해 누락 없이 집계.

### 표기 형식 (정렬·이모지)
- **정렬이 필요한 표(대분류별 통계)**: Discord monospace 는 CJK 글리프 폭이 ASCII 2배가 아니어서
  (폰트별 상이) 한글을 열 사이에 넣으면 밀린다 → **정렬 격자는 ASCII(숫자)만, 한글 이름은 항상 줄 끝**.
- **분류 단계 식별 이모지**(색으로 단계 식별, 내용별 아님. 상태색 ❌빨강/⚠️노랑/✅초록과 겹치지 않게):
  **🟣 대분류 · 🔵 중분류 · 🟠 소분류.** 소비: `EMJ_MAJOR/EMJ_MID/EMJ_API`.
- **API별 신규 상세는 코드블록이 아니라 이모지 아웃라인**(전각 공백 들여쓰기). 코드블록 안에 이모지를
  넣으면 폭이 또 불규칙해져 정렬이 깨지므로, 상세는 아웃라인, 정렬 표는 코드블록으로 분리한다.

---

## 3. 변경 이력

- 2026-07-09: **3단 분류(대분류 4 / 중분류 / 소분류)** 도입, DAG 완료 리포트에 반영(#218 후속).
  이전에는 registry `category`(11종)를 단일 대분류로 사용했음 → 이제 그 11종은 **중분류**로 내려가고,
  대분류는 보건/문화/산업/환경 4종으로 재정의.
- 2026-07-09: 리포트 표기 보강 — 섹션 순서 **에러›경고›성공**, 실패에 **`@task` 명시**, 분류 단계
  **색 이모지(🟣🔵🟠)**, 상세는 이모지 아웃라인으로.
