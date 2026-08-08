# 도메인 전환 킷 — 커머스 외 전 도메인을 run_pattern 안전 게시로

ASK-Seoul-Serving#217 이 정리한 대로 실수요의 98.8% 가 `run_pattern`(커머스식)을 필요로 한다.
커머스는 완료(패턴 484 + 보안 감사·게시 게이트·린트·규약·카탈로그). **이 킷은 나머지 4개
도메인**(citydata·culture·transit·traffic_weather)**을 같은 수준으로 올리기 위한 사전 작업물**
이다. 현재 상태는 아무것도 안 바꿨다 — 나중에 이 파일들로 **구성만 하면 금방 적용**되도록 준비.

## 핵심 발견 (왜 이 방식인가)

1. **타 도메인은 공유 게시기**(`common/serving/publisher.py`)를 쓴다 — 커머스만 자체 게시기.
   따라서 감사를 **공유 경로 한 곳에 배선**하면 4개 도메인이 동시에 감사를 받는다(4벌 복제 불요).
2. **커머스 감사 코어는 이미 도메인 무관**(`audit_pattern_sql(sql, allowlist)` — allowlist 인자).
   커머스 전용은 `commerce_allowlist()`(SERVING_SPEC 파생) 하나뿐.
3. 타 도메인 D1 테이블 = **dbt 모델명 그대로**(`gold_<domain>_*`), 커머스는 `d1_*`. 도구는 양쪽
   명명을 유도한다.

## 실측 준비 상태 (2026-08-09, read-only)

| 도메인 | 제품 | 패턴 | 검증 | 공유감사 위반 | 린트 오류 | 상태 |
|---|---:|---:|---:|---:|---:|---|
| traffic_weather | 11 | 80 | 80 | 0 | 0 | 인프라만 붙이면 완료 |
| culture | 7 | 22 | 22 | 0 | 0 | 인프라만 붙이면 완료 |
| transit | 5 | 12 | 12 | 0 | 0 | 인프라만 붙이면 완료 |
| **citydata** | 14 | 42 | **0** | 0 | 0 | 인프라 + **검증 42건**(실행은 확인됨) |

- **기존 156패턴 전부 공유 감사 위반 0** → 배선 켜도 무중단.
- **내부표 참조 0·크로스도메인 참조 0** → 도메인 격리 이미 성립.
- citydata 42건은 `verified_at` 부재(게이트웨이 409). 프리체크로 **42/42 실 D1 실행 확인**
  (0행 9건은 예시값/allow_empty 판단 필요). 스탬핑만 남음.

## 킷 구성물

| 파일 | 무엇 | 상태 |
|---|---|---|
| `pattern_audit.py` | 공유 정적 감사기(커머스 코어 일반화) | ✅ 4도메인 위반 0 검증 |
| `lint_usage_patterns.py` | 공유 CI 린트(다중 파일·양쪽 명명) | ✅ 5도메인 오류 0(커머스 회귀 포함) |
| `generate_pattern_catalog.py` | 공유 카탈로그 생성(도메인 파라미터) | ✅ 4도메인 정상 |
| `precheck_patterns.py` | read-only 실행 확인(bare 예시값) | ✅ citydata 42/42 |
| `publisher_wiring_patch.md` | 공유 게시기 배선 diff + 커머스 중복제거 | 초안(적용 대기) |
| `convention-shared.md` | org 공통 규약 초안 | 초안 |
| `ci-gate-template.yml` | 도메인별 CI 게이트 템플릿 | 템플릿 |
| `per-domain/*.md` | 도메인별 allowlist + 체크리스트 | ✅ 4개 |
| `catalogs/*.md` | 도메인별 생성 카탈로그(미리 뽑아둠) | ✅ 4개 |

## 커머스 코드에서 확장·수정한 부분 (일반화 중 발견)

커머스 도구를 타 도메인에 그대로 쓰면 깨지는 곳을 실측으로 찾아 일반화판에 반영:

1. **린트** — `declared_tables` 를 `d1_table` 에서만 유도 → 타 도메인 전 패턴 E5 오탐. **모델명
   에서도 유도**하도록 수정(양쪽 명명).
2. **카탈로그** — 헤더 "commerce" 하드코딩 + 그룹키 `?` + product `commerce_` 고정. **도메인
   파라미터화 + product_id 유도**.
3. **검증(프리체크)** — 예시값 추출이 따옴표/숫자만 → 타 도메인 bare 값(`:area=성수동`) 미해결.
   **bare 값도 처리**.
4. **감사** — 코어는 이미 일반. 단 **커머스 사본과 공유본이 갈라질 위험** → 커머스를 공유본의
   얇은 래퍼로 바꾸는 diff 를 `publisher_wiring_patch.md` §커머스 정합에 포함.

## 적용 순서 (나중에 — 이 킷으로 구성만)

1. `common/serving/` 에 `pattern_audit.py`·`lint_usage_patterns.py`·`generate_pattern_catalog.py`·
   `precheck_patterns.py` 배치. 게시기 배선(`publisher_wiring_patch.md`). 커머스도 공유본 래퍼로.
2. 도메인별 CI 게이트(`ci-gate-template.yml` 치환) + 카탈로그 생성 + 규약 링크.
3. **citydata 검증**: 프리체크 통과분 스탬핑(운영 D1 쓰기 권한 = 사람). 0행 9건 처리.
4. (후속·상시) 신규 패턴 저작 확장 — #217 이 지적한 운영 트레드밀. 도메인별로 진행.

각 단계는 통합 이슈에서 추적한다.
