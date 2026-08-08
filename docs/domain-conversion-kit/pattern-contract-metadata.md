# 패턴 계약 메타데이터 구성 (스캐폴드) — 값 없이 구조 정의

usage_pattern 한 건이 계약에 실을 수 있는 **메타데이터 전 필드**를 구조로 정의한다. 값은
도메인이 채우지만, 여기서 (1) 현행 허용 필드 (2) #192 P1/P3 확장 제안 필드 (3) 빈 템플릿을
미리 짜 둔다. #217 [DBT] "패턴 계약 메타" 사전작업.

> ⚠️ **필드는 화이트리스트다.** 목록 밖 필드는 `serving-contract-gate` CI 가
> `usage_pattern_unknown_field` 로 막는다(정본: `ASAC-DBT/serving_contract/schema.yml`
> `usage_pattern_fields`). 커머스가 `provides_ko` 로 이 게이트에 걸린 전례가 있다. 확장 필드
> (§2)는 **계약 스키마 + 게이트웨이를 함께 고친 뒤에만** 라이브 yml 에 넣는다.

---

## 1. 현행 허용 필드 (지금 바로 쓸 수 있음)

정본: `serving_contract/schema.yml:usage_pattern_fields` (2026-08-09 실측).

| 필드 | 필수 | 타입 | 의미 | 소비처 |
|---|---|---|---|---|
| `pattern_id` | ✅ | str `^[a-z0-9_]{1,64}$` | 패턴 식별자(하이픈 금지) | 게이트웨이 실행 키, 카탈로그 |
| `sql` | ✅ | str | 참조 구현 SQL(`:name` 바인딩) | 게이트웨이가 실행, 카탈로그 반환컬럼 추출 |
| `question_ko` | | str | 이 패턴이 답하는 물음 | describe_product·카탈로그 |
| `axes` | | str | 집계·랭킹 축 설명(스위치 허용값 명시) | 카탈로그·사람 리뷰 |
| `requires` | | str[] | 필요한 조회 능력(§requires 어휘) | AS-IS/TO-BE 판정, 소비 측 능력 대조 |
| `verified_rows` | | int | 검증 실행이 반환한 행수 | 드리프트 판정 기준 |
| `verified_at` | | str(ISO8601) | 검증 시각 — **없으면 게이트웨이 실행 거부(409)** | runnable 게이트 |
| `verified_publication_id` | | str | 검증 당시 그 제품 게시본 id | 증거-데이터 정합 |
| `allow_empty` | | bool | 0행이 정상인 패턴인가 | 0행 해석 |
| `insight_sample_ko` | | str | 검증 결과 해석 예시(실측 수치) | 답변 가드레일(환각↓) |
| `d1_table` | | str | 한 모델→다제품 라우팅(미선언 시 모델명) | 게시 라우팅 |

`requires` 어휘(허용값): `select_columns · sort · aggregate · group_by · having · join ·
subquery · window · filter_range · filter_set · filter_null`.

## 2. 확장 제안 필드 (구조만 — 계약+게이트웨이 확장 후 사용) `제안·미적용`

#192 P1(선택 파라미터+기본값)·P3(배열 파라미터)를 계약 메타로 표현하는 구조. **아직 넣지 말 것**
— 먼저 `serving_contract/schema.yml` 의 optional 목록 + 게이트웨이(`index.js`) 처리에 반영돼야 함.

### 2.1 P1 — `param_defaults` / `param_enum` (선택 파라미터·기본값)

```yaml
param_defaults:                  # 미전달 파라미터를 이 상수로 bind (게시자 선언값, 소비자 입력 아님)
  min_biz: 100
  dir: desc
param_enum:                      # 허용값 집합(밖이면 400 — 센티널 오타의 조용한 0행 제거)
  dir: [asc, desc]
  dim: [gu, category, dong]
```
- 게이트웨이 삽입점: `index.js` 626~627(`missing parameter` 분기) 앞에서 `param_defaults` 채움;
  `param_enum` 은 628 타입검사 직후 멤버십 검사.
- 보안: 값이 게시자 상수라 인젝션 표면 0(§4). `param_enum` 은 오히려 안전을 높인다.

### 2.2 P3 — `params`(타입 선언, 배열 IN 1급화)

```yaml
params:                          # 파라미터 타입 선언(선언된 것만; 미선언은 기존대로 스칼라)
  gus: { type: array, item: string, max_len: 100 }   # IN (:gus) 를 ?,?,? 로 전개
  y:   { type: string }
```
- 게이트웨이: `:gus` 를 원소 수만큼 `?` 전개(617 로직 일반화). `max_len` 초과·타입 불일치 400.
  현행 `json_each(:gus)` 관용구도 하위 호환(§관용구).
- 보안: 각 원소는 값 bind(식별자 아님) → 인젝션 불가. `max_len` 이 카티전 팬아웃 상한.

### 2.3 (P6, 고위험 — 별도) `identifier_slots`

```yaml
identifier_slots:                # @{name} 식별자 슬롯 + 필수 화이트리스트(P0 선행 필수)
  order_col: { allow: [lq, share, active_cnt] }
  dir:       { allow: [asc, desc] }
```
- **유일한 인젝션 표면 확장** — 값 bind 가 아니라 문자열 치환. allow 미선언 슬롯은 실행 거부,
  정확 일치만, 치환 후 정적 감사 재파싱(3중 방어). #192 P6 이 정리될 때까지 **넣지 말 것**.

## 3. 빈 템플릿 (복사용 — 현행 필드만)

```yaml
- pattern_id: ""                 # ^[a-z0-9_]{1,64}$
  question_ko: ""
  axes: ""                       # 스위치 쓰면 허용값 명시(:dim ∈ {gu,category})
  verified_rows: 0               # 검증 스크립트가 실측으로 갱신(손 백필 금지)
  insight_sample_ko: ""          # 검증 후 실측 수치로
  sql: |
    -- :param='예시값'           # 첫 줄 예시값 주석(bare 값 허용: -- :area=성수동)
    SELECT ...
    FROM gold_<domain>_<name>    # 자기 도메인 gold_ 테이블만(감사가 강제)
    WHERE ...
  requires: []                   # §requires 어휘에서만
  # allow_empty: true            # 0행이 정상일 때만
  # d1_table: ...                # 한 모델→다제품일 때만
```

## 4. 채운 예시 (조합 관용구 — 값은 예시)

```yaml
- pattern_id: "air_rank_places_on_date_by_metric"
  question_ko: "특정 날짜에 지표(pm10/pm25/통합지수)로 지점을 정렬하면 최악(또는 최선)은?"
  axes: "지점 랭킹 × 지표 스위치(:metric ∈ {pm10,pm25,air_idx}) × 방향(:dir ∈ {asc,desc}) @ 날짜 고정"
  verified_rows: 0
  insight_sample_ko: "2026-07-30 pm10 기준 최악은 …(실측 수치)."
  sql: |
    -- :date=2026-07-30, :metric=pm10, :dir=desc, :n=10
    SELECT area_name, gu, pm10_avg, pm25_avg, air_idx_avg
    FROM gold_citydata_air_daily
    WHERE event_date = :date AND (:gu = 'ALL' OR gu = :gu)
    ORDER BY CASE WHEN :dir='asc' THEN
               CASE :metric WHEN 'pm10' THEN pm10_avg WHEN 'pm25' THEN pm25_avg ELSE air_idx_avg END END ASC,
             CASE WHEN :dir='desc' THEN
               CASE :metric WHEN 'pm10' THEN pm10_avg WHEN 'pm25' THEN pm25_avg ELSE air_idx_avg END END DESC
    LIMIT :n
  requires: [select_columns, sort, filter_range]
  # 확장 후: param_enum: { metric: [pm10,pm25,air_idx], dir: [asc,desc] } ; param_defaults: { dir: desc, gu: ALL, n: 10 }
```

authored-drafts/ 의 230건이 이 구조를 따른다(값은 실측). 확장 필드는 §2 대로 계약이 열린 뒤 얹는다.
