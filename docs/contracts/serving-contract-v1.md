# 도메인 공통 Serving Contract v1

> 정본. 이 문서는 ASAC-DAG #478 [v1 최종 결정](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478#issuecomment-5056366122)과 [v1.1 운영 계약 보강 결정](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478#issuecomment-5065980055)을 규격으로 옮긴 것이다.
> ASAC-DBT의 machine-readable Schema·Validator와 ASAC-DAG 공통 D1 Publisher는 이 문서를 원천으로 구현한다.
> 문서와 구현이 어긋나면 **이 문서가 우선**이며, 변경은 아래 [§8 버전 정책](#8-계약-버전-정책)을 따른다.

## 1. 목적과 범위

각 도메인이 "어떤 Gold를 어떤 기준·규약으로 D1 데이터 제품으로 승격·게시하는가"를 하나의 공통 계약으로 통일한다. 도메인마다 서로 다르던 서빙 표시 필드(`serving_tier`·`external`·`serving_gold_candidate`)와 등록·게시 절차를 `meta.serving.*` 하나로 수렴한다.

**범위 안**: 승격 기준, 필드 규약, Publication(게시+등록+검증) 단위, 마이그레이션.
**범위 밖** (다른 이슈): Gateway/Worker 구조·URL·과금(#476), 특정 도메인 Gold 구현, Worker/Dashboard 구현.

## 2. 책임 경계

| 주체 | 소유 | 비고 |
|---|---|---|
| **dbt YAML** (`meta.serving.*`) | 정적 Serving Contract — 이 문서의 필드 | 정본. 사람·CI·Publisher가 읽는 단일 소스 |
| **ASAC-DBT Validator** | 계약 스키마 검증 · CI Gate | merge 전 계약 위반 차단 |
| **Export DAG (공통 Publisher)** | 실측 · Publication Gate · D1 Write · `_catalog` Upsert · Smoke Test | 런타임 값 기록 |
| **D1 `_catalog` / publication 테이블** | 현재 게시 상태 · Runtime Metadata | 게시된 실측값의 정본 |
| **Gateway / Worker** | API Route · Filter · Limit · 인증 | dbt YAML이 소유하지 않음 (#476) |
| **Watchdog (독립 감시자)** | 게시 후 지속 감시 — `published_at`·`freshness`를 계약과 대조 ([§7.4](#74-운영-감시-책임-operational-monitoring-v11)) | v1.1 신설 책임. 구현은 후속 이슈 |

원칙: **정적 계약은 dbt YAML, 실측값은 런타임 기록, API 형태는 Worker 계약.** 셋을 한 곳에 섞지 않는다.

## 3. 필드 정의

계약은 모델 `config.meta.serving` 아래에 선언한다.

### 3.1 필수 필드 (9) — 하나라도 없으면 게시 불가

| 필드 | 타입 | 허용값 / 형식 | 기본값 | 설명 |
|---|---|---|---|---|
| `enabled` | bool | `true` / `false` | `false` | D1 게시 여부. `false`면 Publisher가 건너뛴다 |
| `external` | bool | `true` / `false` | `false` | 공개 `/catalog`·외부 API 노출 여부. 내부 Agent 전용은 `enabled: true, external: false` |
| `product_id` | string | `^[a-z0-9_]+$`, **전역 유일** | — | 제품 식별자. 도메인 간 충돌 불가 |
| `product_question` | string | 비어 있지 않음 | — | 이 제품이 답하는 질문. 답할 질문이 없으면 승격 대상이 아니다 |
| `grain` | string | 비어 있지 않음 | — | 1행의 의미 (예: `link_id마다 한 행`) |
| `primary_key` | list[string] | 모델의 실제 컬럼, 각 컬럼에 `not_null`·고유성 근거 | — | 단일·복합 모두 list로 선언 |
| `publication_mode` | enum | `snapshot` \| `upsert` \| `append` | — | [§4](#4-publication_mode) |
| `zero_policy` | enum | `fail` \| `retain_last_good` \| `allow` \| `warn` | `retain_last_good` | [§5.1](#51-zero_policy) |
| `publication_trigger` | object | `schedule_cron` 또는 `trigger_type: asset` | — | [§6](#6-publication_trigger) |

> **조건부 필수 (v1.1)**: `event_time`을 선언한 제품은 `freshness_slo_minutes`도 필수다. 미선언 시 Validator FAIL. `event_time`이 없는 명부성 제품은 면제 — 신선도를 잴 시간축이 없는데 강제하면 죽은 메타데이터만 늘어난다(§3.3의 `estimated_*` 제외와 같은 철학). 모든 제품의 "게시 지연" 감시는 이미 필수인 `publication_trigger`가 담당한다.

### 3.2 선택 필드 — 없으면 폴백

| 필드 | 타입 | 허용값 / 형식 | 폴백 | 설명 |
|---|---|---|---|---|
| `contract_version` | string | `v1` | `v1` | 계약 버전 |
| `event_time` | string | 모델의 실제 컬럼 | 없음(시간축 없음) | Worker `from`/`to` 필터축, freshness 기준 컬럼 |
| `retention_or_horizon` | string | 자유 서술 (예: `최근 2일`, `+3일 예보`) | 무제한 | 보존·예보 범위 |
| `partial_policy` | object | `min_publish_ratio`: 0~1 | 검사 안 함 | [§5.2](#52-partial_policy) |
| `freshness_slo_minutes` | int | > 0 | — | `event_time` 최신값 지연 임계. **v1.1: `event_time` 선언 제품은 조건부 필수** (§3.1 참조) |
| `shape` | enum | `wide` \| `rollup` \| `event` | 없음 | 서빙 형태 분류 |
| `reliability` | object | rollup 전용, [§5.3](#53-reliability-rollup-전용) | 없음 | 표본 신뢰도 정책 |

### 3.3 YAML에서 제외 — 실측·타 소유

계약 YAML에 **선언하지 않는다.** Validator는 이 필드가 `meta.serving`에 있으면 실패시킨다.

- `estimated_rows` · `estimated_daily_write_rows` → Export가 실측해 `_catalog`에 기록 (선언형 숫자는 하루만 맞다)
- `api_path` · `allowed_filters` · `default_limit` · `max_limit` · `pagination` → Gateway/Worker API 계약 (#476)

### 3.4 런타임 실측값 — Export가 기록 (YAML 아님)

Publisher가 `_catalog`/publication 테이블에 매 게시마다 기록한다.

`publication_id` · `source_run_id` · `source_row_count` · `published_row_count` · `published_bytes` · `freshness` · `published_at` · `serving_status`

## 4. `publication_mode`

| 값 | 의미 | 재실행 안전성 |
|---|---|---|
| `snapshot` | 전량 교체 (staging 적재 후 원자 전환). 스냅샷형 제품 | 멱등 |
| `upsert` | PK 기준 INSERT OR REPLACE. 부분 갱신 | 멱등 |
| `append` | 최근 구간만 삭제→재삽입 + 신규. 이력·누적형 | 멱등(구간 재적재) |

`snapshot`은 DROP→CREATE 중간 상태를 외부에 노출하지 않도록 staging 적재 후 pointer를 마지막에 전환한다(원자 게시). 게시할 것이 없으면(0행 등) [§5.1](#51-zero_policy)에 따른다.

## 5. 데이터 품질 게이트

### 5.1 `zero_policy`

게시하려는 행 수가 0일 때의 정책.

| 값 | 동작 |
|---|---|
| `fail` | Publication 실패 처리 (예: Weather 현재 스냅샷 0행 = 장애) |
| `retain_last_good` | **기본.** 게시 스킵 + 직전 정상 게시본 유지 (덮어쓰지 않음) |
| `allow` | 0행이 정상 의미를 가질 수 있는 제품 (예: 현재 교통사고 목록 0건) |
| `warn` | 게시하되 `serving_status = degraded` 표시 |

### 5.2 `partial_policy`

```yaml
partial_policy:
  min_publish_ratio: 0.8   # 직전 성공 게시 행수 대비 최소 비율
```

이번 게시 행수 < 직전 성공 게시 행수 × `min_publish_ratio` 이면 **부분 절단**으로 판정하고 `zero_policy`의 `retain_last_good`과 동일하게 직전본을 유지한다. 0행은 ratio 0이므로 `zero_policy`가 먼저 판정한다. (culture HWM 볼륨 계약과 동일 메커니즘)

### 5.3 `reliability` (rollup 전용)

롤업 셀 표본이 얇을 때 신뢰도 정책. `partial_policy`와 섞지 않는다.

```yaml
reliability:
  sample_count_field: base_n
  minimum_sample_count: 30
  insufficient_sample_policy: suppress_row   # suppress_row | flag_degraded | allow
```

| `insufficient_sample_policy` | 동작 |
|---|---|
| `suppress_row` | 표본 부족 행 게시 제외 |
| `flag_degraded` | 게시하되 행 단위 degraded 표시 |
| `allow` | 그대로 게시 (표본 쌓이기 전 임시) |

## 6. `publication_trigger`

`refresh`(원천 수집/dbt 빌드/D1 게시 중 무엇인지 모호)를 대체한다. **D1 게시 기준**만 표현한다.

**cron 기반**:
```yaml
publication_trigger:
  schedule_cron: "40 * * * *"   # 매시 40분 게시
```

**asset(트리거) 기반**:
```yaml
publication_trigger:
  trigger_type: asset
  max_interval_minutes: 90      # 이 간격 넘게 게시 없으면 stale 경보
```

정확히 둘 중 하나만 선언한다 (Validator가 강제).

## 7. Publication — 게시·등록·검증의 단일 단위

D1 적재와 `_catalog` 등록은 **하나의 Publication 완료 조건**으로 묶는다. #477(transit 골드가 D1에 적재됐으나 `_catalog` 미등록으로 404, 적재 잡은 exit 0) 재발 방지.

```
1. Contract Load     — meta.serving 로드·검증 (Validator 통과 계약 신뢰)
2. Publication Gate   — zero_policy / partial_policy / reliability 적용
3. D1 Write           — publication_mode 대로 적재 (snapshot=원자 전환)
4. Row-count Verify   — published_row_count 확인
5. _catalog Upsert    — 자기 도메인 행만 INSERT OR REPLACE (DROP 금지)
6. API Smoke Test     — 대표 product_id 1건 조회 200 확인
```

### 7.1 성공 조건 (모두 충족)

- 게이트 통과 (게시 스킵은 게이트 정책에 따른 정상 종료)
- `published_row_count`가 검증 기준 충족
- `_catalog` upsert 완료, **쓴 테이블 수 == `_catalog` 내 도메인 행 수** (#477 ③ 자기검증)
- 대표 API Smoke Test 200

### 7.2 실패 조건 → 직전 정상본 보호

- 위 중 하나라도 실패 → Publication **미완료**, Airflow task 실패.
- `snapshot`은 staging→pointer 전환 전이라 **직전 게시본 그대로 유지**. 조용한 실패(exit 0) 금지.

### 7.3 런타임 기록

성공·degraded·skip 각 경우에 [§3.4](#34-런타임-실측값--export가-기록-yaml-아님) 값을 `_catalog`/publication 테이블에 기록한다. `serving_status ∈ {published, degraded, skipped_retained, failed}`.

### 7.4 운영 감시 책임 (Operational Monitoring, v1.1)

게시가 끝난 뒤에도 계약은 지켜져야 한다. 선언(§3)과 기록(§7.3)만으로는 **지켜보는 주체**가 없다 — 특히 export DAG 안의 자기보고 경보는 DAG 자체가 죽으면 함께 침묵한다(#477과 같은 계열의 조용한 실패).

- **export DAG 밖의 독립 관찰자(watchdog)** 가 다음 검사쌍 2개를 주기적으로 대조한다:
  1. `_catalog.published_at` ↔ `publication_trigger` 주기(cron 간격 / `max_interval_minutes`) — **D1 미갱신·죽은 DAG** 탐지
  2. `_catalog.freshness` ↔ `freshness_slo_minutes` — **제때 게시됐지만 낡은 데이터** 탐지
- 위반 시 경보를 발생시킨다. (`serving_status` 소비자 노출 연계는 후속 검토)
- 이 조항은 **책임과 검사 대상만** 규정한다. watchdog 구현(위치·소유·알림 경로)은 별도 후속 이슈.
- 계약에 이미 있는 선언·기록만 읽으므로 이 조항으로 인한 추가 스키마 변경은 없다.

## 8. 계약 버전 정책

- `contract_version`은 선택, 폴백 `v1`.
- **하위 호환 변경**(선택 필드 추가, 허용값 추가)은 v1 유지.
- **비호환 변경**(필수 필드 추가·삭제, 허용값 삭제, 의미 변경)은 `v2` 신설 + 이 문서 개정 + 마이그레이션 절차 명시.
- Validator·Publisher는 알 수 없는 `contract_version`을 만나면 ERROR(exit 2)로 멈춘다.

**개정 이력**

- **v1.1** (2026-07-24, [보강 결정](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478#issuecomment-5065980055)): `freshness_slo_minutes` 조건부 필수 승격(§3.1) + §7.4 운영 감시 책임 신설. 필수 규칙 변경이지만 **채택 전 amend**(당시 `meta.serving` 채택 도메인 0, 마이그레이션 비용 0)라 v2가 아닌 v1.1로 처리. Pilot 채택 이후부터는 본 §8을 엄격 적용한다.
- **v1** (2026-07-23, [최종 결정](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478#issuecomment-5056366122)): 최초 확정.

## 9. 마이그레이션 — 기존 메타 → `meta.serving.*`

정본은 `meta.serving.*` 하나. **장기 이중 선언 금지.**

| 도메인 | 기존 | v1 정본 | 소비자 |
|---|---|---|---|
| citydata | `meta.serving_tier` (`d1_direct`/`hold`) | `serving_tier: d1_direct` → `enabled: true` (+ `external`) · `hold` → `enabled: false` | Worker `_catalog`, export DAG |
| culture | `meta.external` (bool) | `external` | 대시보드 `?external=` 필터·'내부' pill |
| culture | `meta.refresh` (`1일`) | `publication_trigger` | 대시보드 '데이터 주기' 칸 |
| culture | `meta.display` (title·summary·caveat) | **유지** — 외부 전시 정본, `product_question`과 별개 |
| transit / traffic_weather | `meta.serving_tier` + `external` + `refresh` (transit #331), `meta.serving_gold_candidate` (+ `serving_gold_catalog.yml`) | `enabled` + `external` + `publication_trigger` + `meta.serving.*` | (PR#324/#328 머지 후 Pilot에서 전환) |

**규칙**:
1. 도메인별 PR에서 dbt 메타와 소비자(대시보드 extract 등)를 **함께** 변경한다.
2. 병합 후에는 `meta.serving`만 정본. 기존 필드는 제거한다.
3. Reader fallback(소비자가 구 필드도 읽음)은 **제거 기한을 명시**한 경우에만 임시 허용. **이중 쓰기(두 필드 동시 기록) 금지.**
4. Validator는 구 필드와 `meta.serving` **이중 선언**을 실패시킨다([§10.2](#102-실패-예제)).

## 10. 예제

### 10.1 올바른 예제

```yaml
# snapshot · 공개 · cron
- name: gold_weather_place_current_outlook
  config:
    meta:
      serving:
        enabled: true
        external: true
        product_id: weather_place_current_outlook
        product_question: 지금 이 장소의 가장 가까운 기상 예보는 무엇입니까?
        grain: place_id마다 한 행입니다.
        primary_key: [product_row_id]
        publication_mode: snapshot
        zero_policy: fail            # 현재 예보 0행 = 장애
        publication_trigger:
          schedule_cron: "10 * * * *"
        event_time: forecast_at
        freshness_slo_minutes: 90    # v1.1: event_time 선언 시 필수
        shape: wide
  columns:
    - name: product_row_id
      tests: [not_null, unique]      # primary_key 근거
```

```yaml
# append · 이력 · rollup 신뢰도
- name: gold_citydata_ppltn_dow_hour
  config:
    meta:
      serving:
        enabled: true
        external: true
        product_id: citydata_ppltn_dow_hour
        product_question: 이 장소는 무슨 요일 몇 시에 붐빕니까?
        grain: 장소×요일×시간마다 한 행입니다.
        primary_key: [area_cd, day_of_week, hour_of_day]
        publication_mode: append
        zero_policy: retain_last_good
        publication_trigger:
          schedule_cron: "40 8 * * *"
        shape: rollup
        reliability:
          sample_count_field: base_n
          minimum_sample_count: 30
          insufficient_sample_policy: flag_degraded
```

```yaml
# 내부 전용 (D1엔 올리되 공개 카탈로그엔 숨김)
- name: gold_traffic_incident_active_latest
  config:
    meta:
      serving:
        enabled: true
        external: false             # 내부 Agent용
        product_id: traffic_incident_active_latest
        product_question: 지금 활성 돌발은 무엇이며 언제 수집되었습니까?
        grain: source_record_id마다 한 행입니다.
        primary_key: [source_record_id]
        publication_mode: snapshot
        zero_policy: allow           # 돌발 0건은 정상
        publication_trigger:
          trigger_type: asset
          max_interval_minutes: 30
```

### 10.2 실패 예제

```yaml
# ❌ external=true 인데 enabled=false — 게시 안 되는데 공개 노출 표시 (모순)
serving: { enabled: false, external: true, ... }

# ❌ product_question 누락 — 답할 질문 없는 제품은 승격 불가
serving: { enabled: true, external: true, product_id: x, grain: ..., ... }

# ❌ primary_key 컬럼이 모델에 없음 / not_null·unique 근거 없음
serving: { primary_key: [nonexistent_col], ... }

# ❌ 허용되지 않은 값
serving: { publication_mode: replace, zero_policy: skip, ... }

# ❌ 이중 선언 — 구 serving_tier 와 신규 meta.serving 공존
meta: { serving_tier: d1_direct, serving: { enabled: true, ... } }

# ❌ estimated_rows / api_path 를 계약에 선언 (실측·Worker 소유 필드)
serving: { estimated_rows: 5000, api_path: /data/x, ... }

# ❌ publication_trigger 에 cron·asset 둘 다 또는 둘 다 없음
serving: { publication_trigger: { schedule_cron: "* * * * *", trigger_type: asset } }

# ❌ v1.1: event_time 을 선언했는데 freshness_slo_minutes 누락 (조건부 필수)
serving: { event_time: forecast_at, ... }   # freshness_slo_minutes 없음 → FAIL
```

## 11. 관련

- ASAC-DAG #478 (v1 최종 결정 · v1.1 보강 결정), #477 (`_catalog` 등록 누락 장애), #476 (진입점 통합), #505 (문서화+Publisher 통합 작업 이슈)
- ASAC-DBT: Serving Contract Validator (Schema·CI Gate), `contracts/engine` 재사용
- ASAC-DAG: `common/serving/` 공통 D1 Publisher
