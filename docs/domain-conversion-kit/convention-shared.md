# usage_patterns 표기·보안 규약 (org 공통 초안) — 전 도메인

커머스가 확립한 `usage-patterns-convention.md`(ASAC-DBT#471)를 **도메인 무관 규약**으로 올린
초안. 게이트웨이 실행 계약은 전 도메인 공통이므로 규약도 공통이어야 한다. 도메인 고유값(테이블
명명·allowlist)만 도메인별로 다르다.

## 0. 게이트웨이 실행 계약 (전 도메인 공통 — 실측)

| # | 계약 | 위반 시 |
|---|---|---|
| 1 | `product_id`·`pattern_id` 는 `^[a-z0-9_]+$` | 400 |
| 2 | 주석 제거 후 `SELECT`/`WITH` 단일문만 | 400 |
| 3 | `:name` 등장 순서대로 `?` prepared bind (값 주입 불가) | — |
| 4 | 모든 파라미터 필수, 문자열/숫자만, 선언 밖 400 | 400 |
| 5 | `n`/`limit`/`top_n` 만 숫자 강제 + 상한 5000 clamp | 상한 미적용 |
| 6 | `verified_at` 없으면 실행 거부 | 409 |
| 7 | **테이블 스코프 미검사** — 공유 D1 전체에 verbatim 실행 | 게시자 감사로 차단 |

## 1. 표기 (커머스 규약과 동일)

- `pattern_id`: `^[a-z0-9_]{1,64}$` (하이픈 금지 — Serving#178).
- 파라미터는 **실바인딩만** — `값 /* :name */`(값 박힘) 금지(Serving#179). 행수는 `LIMIT :n`.
- 예시값 주석: SQL 첫 줄 `-- :y='2025', :n=10`. **커머스는 따옴표, 타 도메인은 bare 값(**
  `-- :area=성수동, :date=2026-07-30`**)도 쓴다** — 검증 도구는 양쪽을 읽는다(precheck 참조).
- 조합 관용구(차원/정렬/지표 스위치·센티널·임계값·기간창·배열 IN(json_each))는 커머스 규약 §4.

## 2. 도메인 고유값 — 테이블 명명

- **커머스**: D1 테이블 `d1_*`(예 `d1_churn_yearly`), `meta.serving.d1_table` 로 선언, product_id
  `commerce_<name>`.
- **타 도메인**: D1 테이블 = **dbt 모델명 그대로**(예 `gold_culture_activity_by_dong`,
  공유 게시기 `contract.model_name`), `d1_table` 미선언, product_id `<domain>_<name>`.
- 감사·린트의 allowlist 는 이 명명 규칙을 **둘 다** 유도한다:
  `pattern.d1_table → serving.d1_table(str) → 모델명`.

## 3. 보안 감사 (공유 — publisher 배선 후 전 도메인 자동)

- 정본 감사기: `common/serving/pattern_audit.py`(토크나이저 기반 — 콤마 조인·서브쿼리·스키마
  한정·pragma TVF 방어. 레드팀 검증 ASAC-DAG#743). allowlist = 그 도메인 게시 테이블 ∪
  선언 크로스도메인 소스 ∪ {json_each}. deny-by-default 라 내부표(`_keys`·`_usage`·`_ops_*`)·
  카탈로그/핸드오프 표·타 도메인 표는 자동 거부.
- 게시 게이트: `common/serving/publisher.py` 가 게시 직전 감사, 위반분 제외 + 경보.
- 사전검사(CI): `lint_usage_patterns.py`(E1~E7). 전체 차단 3단계: verified_at NULL /
  yml 제거 후 게시 / external:false.

## 4. 검증 (verified_*)

- 손 백필 금지 — 실행 실측만. 신규 패턴은 `verified_rows: 0` 자리만 두고 검증 스크립트로 스탬프.
- `verified_at` 없으면 게이트웨이가 실행 거부(계약 6) — **검증 전 패턴은 자동 비활성**(안전망).
- 프리체크(`precheck_patterns.py`)로 배포 전 read-only 실행 확인(상태 변경 없음).

## 5. "무엇을 주는가" — SQL 에서 뽑는다

- **패턴에 임의 필드 추가 금지** — org 공통 Serving Contract(`serving_contract/schema.yml`)의
  `usage_pattern_fields` 화이트리스트 밖 필드는 `serving-contract-gate` 가 막는다(커머스 판이
  `provides_ko` 로 이 게이트에 걸린 전례 — 게다가 핸드오프 스키마에 없어 게시도 안 됨).
- "무엇을 주는가"는 카탈로그 생성기가 **SQL 최종 SELECT 의 반환 컬럼**에서 뽑는다. 반환 컬럼에
  의미 있는 별칭(`SUM(cnt) AS opened_total`)을 붙이는 것이 곧 문서화다.
