# 파이프라인 표준 실행로그 통합 — ops 존 카테고리 유지 + D1 집계

- 상태: 초안 (2026-07-30 개정 — 신규 카테고리 신설안 철회, 기존 4 카테고리 유지로 전환)
- 작성일: 2026-07-30
- 이슈: ASAC-DAG#619 / 브랜치: `feat/619-ops-standard-run-log`
- 연계: ASK-Seoul#70 (수명주기·D1 테이블 확정) · ASK-Seoul#60 (R2 경로 규약)

## 배경 · 목표

도메인별 적재 실행(도메인·DAG·시작/종료·소요·행수·호출 API·저장 대상·상태)을 한 스키마로 남기고,
D1 에서 집계·헬스체크·그래프로 쓸 수 있게 만든다.

`dags/common` 에는 이미 관측 코드가 4벌 있으나 도메인별 배선이 서로 겹치지 않아
"전 도메인 한 곳 집계"가 현 구조로는 불가능하다. 착수 전 R2 를 전수 실측해 현황을 확정했다.

**이 문서의 전제: ops 존 카테고리 4종은 유지한다.** 초판에서 `ops/pipeline_runs/` 신규 카테고리를
제안했으나 철회했다 — 실측 결과 기존 카테고리 구조는 대부분 타당하고, 문제는 카테고리 **하나의 경계**에
한정된다(§기존 구조 평가). 신규 카테고리는 그 문제를 해결하지 않고 5번째 이름만 늘린다.

## 실측 결과 (2026-07-30)

### prod 버킷 `seoul` (2026-07-29 ~ 07-30)

| prefix | 구현 | 오브젝트 | 평균 | 쓰는 도메인 |
|---|---|---|---|---|
| `ops/runs/` | `common/ops/run_sink.py` | 5,110 | 392 B | citydata 만 |
| `ops/metrics/` | `common/runmetrics.py` (#188) | 11,636 | 519 B | transit 11,566 · weather 55 · traffic 15 |
| `ops/errors/` | `common/errors/sink.py` (#77) | 44 | 560 B | 전 도메인 (실패 시) |
| `ops/logs/` | `commerce_ops_logship` | 206 (12.6 MiB) | 64 KB | commerce 만 |

일별: `ops/runs` 7-29 1,889 → 7-30 3,221 · `ops/metrics` 7-29 8,043 → 7-30 3,593.

`ops/metrics/` 파일명 형태별 분해:

| 도메인 | 총계 | 태스크런 (`track`) | dbt 노드 | 비고 |
|---|---|---|---|---|
| transit | 11,566 | 1,127 | 10,439 | distinct dbt 노드 147, 노드당 최대 72런 |
| weather | 55 | 0 | 55 | distinct 노드 26 |
| traffic | 15 | 0 | 15 | distinct 노드 8 |

transit 태스크런 내역: subway 509 · parking 304 · bus 153 · loader 151 · master 6 · route_master 2 · maintenance 2.

**`ops/metrics/` 내용의 90%가 이미 dbt 노드 레코드다 (10,439 / 11,636).**

### dev 버킷 `seoul-dev`

- `runs/` (구경로, **ops 존 밖**) **43,676** (7-16~7-29, citydata 43,674 · transit 2, 최대 4,736/일)
- `ops/metrics/` 1,547 (7-29 하루)
- `errors/` (구경로, ops 존 밖) **3,566** (7-03~7-29) — population 1,008 · citydata 947 · transit 808 · traffic 648 · weather 150 · culture 5
  - `population` 은 현재 `dags/domains/` 에 없는 도메인
- `ops/runs/`, `ops/logs/` 비어 있음

`run_sink.runs_prefix()` 가 dev 는 `runs`, prod 는 `ops/runs` 를 반환한다 — **dev 는 ops 존을 안 쓴다.**

### 도메인 × 메커니즘 커버리지

| 도메인 | `ops/runs` | metrics 태스크런 | metrics dbt | `ops/logs` | `ops/errors` |
|---|---|---|---|---|---|
| citydata | ✅ 5,110 | ✗ | ✗ | ✗ | ✅ |
| transit | ✗ | ✅ 1,127 | ✅ 10,439 | ✗ | ✅ |
| traffic | ✗ | ✗ (0) | ✅ 15 | ✗ | ✅ |
| weather | ✗ | ✗ (0) | ✅ 55 | ✗ | ✅ |
| commerce | ✗ | ✗ | ✗ | ✅ 206 | 자체 `log_event` |
| culture | ✗ | ✗ | ✗ | ✗ | ✅ |

`ops/runs` 와 `ops/metrics` 를 **둘 다 쓰는 도메인이 없다.**

### 레코드 내용 실측

- `layer` 값은 `bronze`/`silver` 뿐 — **`raw`·`gold`·`d1` 은 0건** (transit 60건 표본: silver 55 · bronze 5)
- dbt 레코드는 `dag_id=null` (표본 60건 중 55) — dbt artifact 에는 Airflow 식별자가 없다
- transit dbt `rows` 는 표본 40건 **전부 null**
- D1 발행 결과는 `common/serving/publisher.py` 가 D1 `_publication_ledger` 에 별도 적재 — ops 존과 조인 불가

## 기존 ops 구조 평가

### 타당한 것 — 유지한다

1. **`ops/` 존을 데이터 레이어와 분리한 것.** 관측 산출물을 `raw/bronze/silver/gold` 와 섞지 않아
   수명주기·보존기간·비용을 따로 걸 수 있다. 이게 없으면 로그 TTL 이 데이터 TTL 과 엉킨다.
2. **카테고리를 산출물 성격으로 나눈 것.** 실측이 이를 뒷받침한다 — 평균 크기가
   `ops/logs` 64 KB vs 나머지 ~520 B 로 **123배** 차이다. `errors` 는 RFC 9457 Problem 문서로
   스키마 자체가 다르고, `logs` 는 tar.gz 바이너리다. 보존기간도 성격별로 달라야 한다.
   **이 셋을 한 카테고리에 합치면 안 된다. 분리한 판단이 맞다.**
3. **domain-first + `key=value` 파티션 (#60).** 도메인별 조회·수명주기·소유권 경계가 경로에 드러난다.
   `ops/errors/`, `ops/metrics/`, `ops/logs/` 가 이미 이 형태다.

### 불합리한 것 — 딱 하나

**`ops/runs/` 와 `ops/metrics/` 가 같은 grain(태스크런 1건)·같은 사실을 담는다.**
카테고리 경계가 "무엇을 기록하는가"가 아니라 **"어느 코드가 썼는가"**로 그어져 있다.

근거:

- **필드가 겹친다.** `run_sink.build_run_record` 12필드 중 **10필드**(domain, layer, dag_id, task_id,
  run_id, try_number, status, started_at, ended_at, duration_s)가 `runmetrics._RECORD_FIELDS` 21필드에
  이미 있다. 고유한 건 `scheduled_at`(runmetrics 는 `schedule_delay_s` 로 파생 보유)·`error` 요약뿐.
  즉 **`run_sink` 레코드는 사실상 `runmetrics` 레코드의 부분집합**이다.
- **둘 다 붙이면 같은 태스크런이 두 곳에 각각 기록된다.** 집계 시 이중계상이거나 조인이 필요하다.
- **아무 도메인도 둘 다 쓰지 않는다** — 실측. 사실상 도메인마다 둘 중 하나를 임의로 고른 상태다.

이름 자체는 둘 다 합리적이다. 문제는 **경계가 없다**는 것이다.

### 재정리 방향 — 경계를 grain 으로 다시 긋는다

카테고리를 지우지 않고 경계만 명확히 하면 두 이름이 다 살고 중복이 사라진다.

| 카테고리 | grain (기록 단위) | 판단 기준 |
|---|---|---|
| `ops/runs/` | **Airflow 가 아는 실행 단위** — 태스크런 1건 | 오케스트레이터가 식별하는 실행 |
| `ops/metrics/` | **dbt 가 아는 실행 단위** — 노드 1건(모델/테스트) | 변환엔진이 식별하는 실행 |
| `ops/errors/` | 실패 1건 (RFC 9457 Problem) | 실패 상세 — `error_id` 로 위 둘에 조인 |
| `ops/logs/` | run 1건의 태스크 텍스트 로그 (tar.gz) | 사람이 읽는 원문 로그 |

이 경계가 실측과 맞는다: **`ops/metrics/` 의 90%가 이미 dbt 노드 레코드**다. metrics 는 사실상 이미
dbt 상세 저장소로 쓰이고 있고, 태스크런 레코드 1,127건(transit 단독, 전체의 10%)만 이물질이다.
**이동량이 전체의 10% 뿐이고, 신규 카테고리는 필요 없다.**

`dag_id=null` (진단 #4)도 이 경계로 설명된다 — dbt artifact 에는 Airflow 식별자가 없어서 비어 있다.
`dump_dbt_run_results` 호출 시 Airflow context 를 넘겨 채우면 두 카테고리가 조인 가능해진다.

## 진단

### 구조 (1건 — 위 §재정리 방향에서 해결)

| # | 문제 | 근거 |
|---|---|---|
| 1 | `runs`/`metrics` 가 같은 grain·같은 사실 | 12필드 중 10필드 중복, 둘 다 쓰는 도메인 0 |

### 정합성 — 기존 구조 안에서 정렬만 하면 되는 것

| # | 문제 | 근거 |
|---|---|---|
| 2 | 세그먼트 순서 불일치 | `ops/runs` 만 `observed_date=/domain=/` (date-first). errors·metrics·logs 는 domain-first bare (#60 A 구조) |
| 3 | 파티션 키 어휘 2종 | `observed_date=` (runs·metrics·errors) vs `load_date=` (logs) |
| 4 | 파티션 타임존 2종 | KST (runs·logs) vs UTC (metrics·errors) |
| 5 | dev 가 ops 존 밖 | `runs_prefix()` dev→`runs`, prod→`ops/runs`. dev 43,676건이 존 밖에 있다 |
| 6 | 버킷 분기 불일치 | `run_sink` 은 `r2_env_for(target)`(#556), `MetricsR2Sink` 는 `r2_env`(dev 우선). 실측: 7-29 metrics 가 dev·prod 양 버킷 존재 |

### 배선·구현

| # | 문제 | 근거 |
|---|---|---|
| 7 | 레이어 공백 (raw/gold/d1 미관측) | 실측 `layer` ∈ {bronze, silver} |
| 8 | `track()` 배선이 transit 뿐 (12곳) | traffic/weather 는 `*_reliability_report.py` 에 `layer="bronze"` 오라벨, 실측 0건 |
| 9 | `rows` 사실상 공백 | dbt-trino 가 `adapter_response.rows_affected` 미채움 |
| 10 | dbt 레코드에 Airflow 식별자 없음 | `dag_id=null` 90% → `GROUP BY dag_id` 시 조용히 유실. `unique_id` 마지막 세그먼트만 남아 model/test 구분 불가 |
| 11 | 상태 어휘 3종 | `{success,failed,skipped}` / `{SUCCESS,FAILED,PARTIAL}` / 원장 `outcome` |
| 12 | 소비자 없음 (write-only) | `serving/`·`dashboard/` 에 ops 읽기 코드 0. `citydata_ops_digest` 만 R2 LIST + 파일명 파싱 |
| 13 | 재시도 이중계상 | 2회차 성공 시 failed 1 + success 1. digest 성공률이 태스크 기준이 아니라 시도 기준 |
| 14 | at-rest 유출 위험 | `runmetrics.py` `redact()` 호출 0건. `http/auth.py` `PathKey` 는 API 키를 **URL 경로**에 실음 |
| 15 | dbt 노드당 1 PUT | transit 인보케이션 = 147 PUT. 전면 배선 시 Class A 무료 한도 초과 |

### 요구 필드 대조

| 요구 | 현재 | 판정 |
|---|---|---|
| 도메인 | `domain` | ✅ |
| 실행 DAG 이름 | `dag_id` | ⚠ dbt 레코드 null (진단 #10) |
| 시작/종료시간 | `started_at`/`finished_at` | ✅ |
| 소요시간 (시·분·초) | `duration_s` (float) | ⚠ H:M:S 없음 |
| 실제 적재건수 | `rows` | ⚠ dbt 전량 null (진단 #9) |
| 호출 API | `api_calls` (횟수) | ❌ 식별자 없음 |
| 저장 타입 | — | ❌ |
| 저장 경로 | — | ❌ |
| 상태값 표준화 | `status` | ⚠ 어휘 3종 |
| 집계·헬스체크 | — | ❌ 집계 저장소 없음 |

### Airflow 전용인가

**그렇다.** `track()` 은 Airflow context 에서 dag_id/run_id/try_number 를 뽑고, dbt 레코드도
Airflow 태스크가 `dump_dbt_run_results` 를 호출해 남는다. 맨 `dbt run` 이나
`serving/export_gold_to_d1.py` · `serving/export_transit_to_d1.py` 스탠드얼론 실행은 아무것도 남기지 않는다.

## 재사용할 자산

| 모듈 | 재사용 지점 |
|---|---|
| `common/runmetrics.py` | `_RECORD_FIELDS` 21필드 평평한 구조(DB 컬럼 1:1). docstring 이 "DB sink 교체 전제" 명시 — sink 교체가 설계된 경로 |
| `common/ops/run_sink.py` | Airflow success/failure 콜백 배선. `default_args` 로 DAG 단위 일괄 적용 가능 (track 보다 커버리지 확보가 싸다) |
| `common/storage.py` | `Storage` ABC · `R2Storage` (read/list/delete/서버사이드 `copy`) · `r2_env_for(target)` |
| `common/serving/d1_client.py` | D1 HTTP API · `build_insert_statements`/`group_api_batches` (바이트 예산 배치) · `_ensure_catalog_schema` additive ALTER |
| `common/http/core.py` | `note_http_request`/`note_http_retry` contextvar 컬렉터 → API 식별자 확장 지점 |
| `common/serving/publisher.py` | `_publication_ledger` = 사실상 `layer=d1` 로그. 매핑 대상 |

R2 쓰기 경로가 4벌 중복(`R2Storage` / `MetricsR2Sink._put_r2_object` / `R2ErrorSink._put_r2_object` /
`run_sink._put_r2`). 읽기·삭제가 되는 건 `R2Storage` 뿐이다.

## 설계

### 1. ops 존 레이아웃 — 카테고리 4종 유지, 신규 없음

```
ops/runs/<domain>/observed_date=<KST>/dag_id=<dag_id>/<run_id>__<task_id>__try<N>.json
    grain = Airflow 태스크런 1건. 전 레이어(raw/bronze/silver/gold/d1). 표준 레코드.
    변경: 세그먼트 순서를 domain-first bare 로 정렬 (진단 #2 — errors/metrics/logs 와 동일)
          파일명에서 __<status> 제거 (상태는 본문 필드 — 재시도 시 키 예측 가능하게)

ops/metrics/<domain>/observed_date=<KST>/<dag_id>__<run_id>__dbt-<invocation_id>.ndjson
    grain = dbt 노드 1건(모델/테스트). 표준 레코드 + dbt_node_id.
    변경: 노드별 1파일 → 인보케이션당 1 NDJSON (진단 #15, PUT 147→1)
          dag_id/run_id 채움 (진단 #10 — ops/runs 와 조인 가능)

ops/errors/<domain>/observed_date=<KST>/dag_id=<dag_id>/<run_id>__<HHMMSSffffff>_<slug>.json
    변경: 파티션 타임존 UTC→KST 만 (진단 #4). 그 외 불변.

ops/logs/<domain>/observed_date=<KST>/<dag_id>/<run_id>.tar.gz
    변경: load_date= → observed_date= (진단 #3, 신규만). 도메인 세그먼트를 env 하드코딩에서 인자로.
```

- **공통 prefix = `ops/`** — 전체 조회 가능. 도메인별은 `ops/<category>/<domain>/`
- 카테고리 4종 명칭·성격 모두 유지. 새 카테고리 없음
- 태스크런 레코드 1,127건(transit 단독)만 `ops/metrics/` → `ops/runs/` 이동 = **전체의 10%**
- dev 도 `ops/runs` 사용 (진단 #5) — `runs_prefix()` 의 dev/prod 분기 제거
- 모든 카테고리 쓰기를 `r2_env_for(target)` 로 통일 (진단 #6)

### 2. 표준 레코드 v2 — #188 스키마에 가산만

`ops/runs/` 와 `ops/metrics/` 가 **같은 스키마, 다른 grain**을 쓴다. 스키마 단일 출처 = 한 모듈.

```
-- 식별 · 계보
event_id         TEXT PK   sha1(domain|dag_id|task_id|run_id|try_number|dbt_node_id)  결정적, 멱등 upsert 키
schema_version   INTEGER
domain           TEXT
layer            TEXT      raw|bronze|silver|gold|d1          닫힌 enum
grain            TEXT      task_run|dbt_node|publication      카테고리와 1:1 (runs|metrics|publisher)
dag_id, task_id, run_id, try_number                            dbt 레코드도 채운다 (진단 #10)
dbt_node_id      TEXT      dbt unique_id 전체 (model/test 구분). task_run 은 null

-- 시간
started_at, finished_at    UTC ISO8601
duration_s       REAL      집계 정본
duration_hms     TEXT      HH:MM:SS  사람 표시용
observed_date    TEXT      KST YYYY-MM-DD  파티션과 동일값 (일별 집계 기준)
scheduled_at, schedule_delay_s

-- 처리량
rows             INTEGER   실제 적재 행수
rows_source      TEXT      callable|dbt|count_query|d1_readback|manifest   null 사유 설명
bytes            INTEGER

-- 호출 API  (원본 URL 금지 — PathKey/QueryKey 가 키를 URL 에 실음)
api_name         TEXT      정규화 서비스명: seoul.citydata, kma.vilage_fcst …
api_host         TEXT
api_calls, api_retries, api_error_count   INTEGER

-- 저장 대상
sink_type        TEXT      file|iceberg|d1|db|none
sink_uri         TEXT      r2://<bucket>/<prefix> | iceberg://<cat>.<schema>.<table> | d1://<db>/<table>

-- 상태
status           TEXT      success|failed|skipped|partial     닫힌 enum (진단 #11 통합)
is_final_attempt INTEGER   0/1  재시도 성공 시 성공률 왜곡 차단 (진단 #13)
error_id, error_type       ops/errors 문서 조인키
target, hostname, peak_rss_mb, cpu_time_s
```

- `duration_s` 집계 정본 + `duration_hms` 병기 (요구사항의 시·분·초 표기)
- `api_name`/`sink_uri` 는 **저장 직전 `redact()` 필수** (진단 #14)
- 상태 매핑: `raw_manifest` `SUCCESS→success` `FAILED→failed` `PARTIAL→partial`,
  원장 `skipped_retained→skipped`

### 3. 태스크런 grain 의 이중 기록 방지 (진단 #1 의 코드측 해소)

`ops/runs/` 에 쓰는 경로가 둘이다 — 둘 다 살리되 권위를 정한다.

| 경로 | 역할 | 채우는 필드 | 쓰기 방식 |
|---|---|---|---|
| `runmetrics.track()` 데코레이터 | **권위(authoritative)** | 요구 10필드 전부 (rows·api·sink 는 콜러블 반환에서만 얻는다) | upsert (덮어씀) |
| `run_sink.record_run()` 콜백 | **그물망(fallback)** | 상태·시간만 | `INSERT OR IGNORE` (빈 자리만 채움) |

- 같은 태스크런은 같은 `event_id` → D1 에서 자동 dedup. 콜백은 절대 track 레코드를 덮지 않는다
- 콜백은 `default_args` 로 DAG 단위 일괄 적용 → **track 미배선 태스크도 100% 커버리지 확보**
- track 배선은 요구 필드가 필요한 적재 태스크부터 (진단 #8)

### 4. R2 효율 — 카테고리 안에서 배치만

레코드당 1 PUT → **트랜잭션 경계당 1 PUT**. 경로 구조는 그대로, 파일 단위만 바뀐다.

| | 현재 | 배치 후 |
|---|---|---|
| dbt 인보케이션 1회 (transit 147노드) | 147 PUT | **1 PUT** (NDJSON) |
| prod 쓰기 | 11,264 obj/일 (도메인 40% 배선) | ~1.5–3k/일 |
| 6도메인 전면 배선 | 28–35k/일 ≈ **850k–1.05M PUT/월** (Class A 무료 1M/월 초과) | 50–90k/월 |
| 하루치 읽기 | 8,043 GET | ~100–150 GET |

용량은 어느 쪽이든 무의미(5.8 MiB / 2일). 비용을 만드는 건 오브젝트 수와 LIST/GET 횟수다.
태스크런 레코드는 이미 1건 = 1 PUT 이라 배치 대상이 아니다.

### 5. D1 적재 — 폴더 이동 없이

초판은 `pending/`→`loaded/` 상태 세그먼트와 파일 이동을 제안했다. **철회한다.**
그건 R2 를 큐로 쓰는 A안의 부속물이고, 아래 C안에서는 필요가 없다 — 그리고 경로에 상태를 넣는 것은
카테고리 구조를 흔든다.

| 안 | 내용 | 평가 |
|---|---|---|
| A | R2 를 큐로 쓰고 폴링 DAG 가 주기 적재 | 도메인별 주기 차이·미적재 판별·`pending/loaded` 경로 상태·적재기 SPOF 를 전부 떠안는다 |
| B | 태스크 종료 시 D1 직접 write | 흐름 최단. D1 장애가 핫패스 |
| **C (권장)** | **R2 는 항상 쓴다(내구 사본) + 같은 자리에서 D1 upsert 시도.** 실패분은 리컨실러가 채운다 | R2 레이아웃 무변경. 실시간 + 내구성 |

C 의 동작:

1. sink 는 **평소처럼 R2 카테고리 경로에 쓴다** — 경로·이름 규약 변화 없음. R2 가 정본 로그다
2. 같은 호출에서 D1 `INSERT ... ON CONFLICT(event_id)` 를 시도한다. **실패는 무시**(fail-open)
3. `ops_runlog_reconcile` DAG (1일 1회) 가 **누락분만** 채운다:
   - (domain, observed_date) 별로 R2 `list_objects_v2` 카운트 ↔ D1 `GROUP BY` 카운트 비교 (둘 다 값싸다)
   - **수가 어긋난 파티션만** GET 해서 upsert. 일치하면 GET 0건
4. 미적재 판별은 **경로가 아니라 `event_id` 존재 여부**로 한다 — 파일 이동·워터마크·상태 세그먼트 불필요

이 방식의 이점:

- **도메인별 적재/실행 주기가 달라도 무관하다.** 리컨실러는 큐를 소비하는 게 아니라 카운트를 맞춘다
- 늦게 끝난 런도 다음 리컨실 때 카운트 불일치로 잡힌다
- 중복 없음 — `event_id` PK upsert
- R2 오브젝트를 옮기지 않으므로 copy/delete Class A 비용도 없고, 경로가 불변이라
  `citydata_ops_digest` 같은 기존 파일명 파싱 소비자가 깨지지 않는다

근거:

- `runmetrics` 가 "DB sink 교체 전제"로 설계됨 — 설계된 이전 경로
- `d1_client` 에 바이트 예산 배치 삽입이 이미 있음 (dbt 147노드 = D1 batch 1회)
- 물량: 실측 11.3k행/일(40% 배선) → 전면 ~30k행/일. D1 무료 쓰기 100k행/일 안.
  누적 21필드 × 30k행 ≈ 월 5MB (D1 무료 5GB)
- write 는 반드시 fail-open (`write_record` 현재 규약 유지).
  ⚠ `publisher._append_ledger` 는 의도적으로 raise 하지만 표준 로그 sink 는 그 반대여야 한다

### 6. D1 집계 surface

`_catalog`/`_publication_ledger` 와 같은 `_` 접두 규약. **R2 의 4 카테고리를 D1 에서 합친다** —
합치는 지점을 스토리지가 아니라 질의 계층에 두는 게 이 설계의 핵심이다.

| 테이블 | 소스 | 보존 |
|---|---|---|
| `_ops_runs` | `ops/runs/` + `ops/metrics/` (grain 필드로 구분) + 발행 원장 | 90–180일 롤링 |
| `_ops_runs_daily` | 위 롤업: (observed_date, domain, layer) 런수·성공/실패·p50/p95 소요·행수 합 | 영구 |
| `_ops_dag_health` | dag_id 별 최근 상태·연속 실패·최근 성공 경과분 | 현재값 |
| `_ops_dag_registry` | dag_id → domain, layer, `expected_interval_s`, owner | 관리 |

`_ops_dag_registry` 가 헬스체크의 전제다. 실행 주기가 도메인마다 다르므로
(citydata 5분 · transit subway 3분 · commerce 일 1회 · culture 주 1회) 기대 주기 없이는
**"안 돌아서 기록이 없는 것"과 "고장나서 없는 것"을 구분할 수 없다.**
신선도 SLO 는 `expected_interval_s` 배수로 정의한다.

스키마 진화는 additive-only + `schema_version`. 근거: #521 (공통 Publisher 15컬럼 vs 라이브 8컬럼
불일치로 카탈로그 등록 실패). `_ensure_catalog_schema` 의 additive ALTER 적용.

## 3중 검토

### 백엔드 — 계약·API 관점

- 카테고리를 스토리지에서 합치지 않고 **D1 뷰에서 합치는** 게 맞다. 스토리지 레이아웃은 쓰기 최적,
  질의 계층은 읽기 최적 — 둘을 같은 구조로 강제하면 한쪽이 손해다. 기존 카테고리를 유지할 수 있는 이유다.
- `schema_version` + additive-only 진화 필수. 선례 #521.
- `event_id` 없는 재시도는 이중계상(진단 #13). 상태값은 write 시점 검증하는 닫힌 enum.
- track/콜백 이중 기록은 **권위 규칙**으로 푼다(§3) — 둘 중 하나를 지우는 것보다 커버리지가 높다.
- 읽기 경로는 D1 단일화 — 대시보드가 R2 를 LIST 하면 사용자 기능이 오브젝트 스토어 지연·비용에 묶인다.
  현재 `citydata_ops_digest` 가 정확히 그 형태다.
- `api_name`/`sink_uri` 는 redaction 없이 저장 금지(진단 #14). `PathKey` 회귀 테스트 필요.

### 데이터 엔지니어 — 모델링·계보 관점

- **카테고리 경계를 grain 으로 그은 것이 모델링상 옳다.** 팩트 테이블을 grain 으로 나누는 건 정석이고,
  `ops/runs`(오케스트레이터 grain) / `ops/metrics`(변환엔진 grain) 는 그 정의에 맞는다.
  실측이 이미 그 방향(metrics 90%가 dbt 노드)이라 이동량도 10%뿐이다.
- `rows` 가 최난관. dbt-trino 가 `rows_affected` 를 안 채우므로 silver/gold 는
  ① dbt `on-run-end` 훅 + count 질의 ② Iceberg 스냅샷 `summary.added-records`
  ③ 발행 원장 `published_row_count`(이미 채워짐) 중 선택하고 `rows_source` 로 출처를 남긴다.
  dbt test 노드는 행수 개념이 없어 null 이 정상 — 구분에 `grain`/`dbt_node_id` 가 필요하다.
- 파티션 어휘·타임존·세그먼트 순서를 4 카테고리에서 통일한다(진단 #2·#3·#4). 구조 변경이 아니라 정렬이다.
- **새 Iceberg ops 테이블을 만들지 않는다.** `ops.run_metadata` 가 89.2 GiB 로 방치된 선례
  (2026-07-27 카탈로그 실측, 단일 최대 용량 항목)이고, R2 Data Catalog 는 DROP 해도 물리 파일이 남는다.

### DevOps/SRE — 운영 관점

- 관측이 파이프라인을 죽이면 안 된다. C 안의 D1 write 는 fail-open 이 전제조건이고,
  R2 쓰기가 항상 선행하므로 D1 이 죽어도 로그는 남는다.
- **파일 이동을 안 하는 게 운영상 이득이다.** 이동은 copy+delete 2 Class A 이고, 이동 중 크래시 시
  경로 상태와 D1 상태가 어긋난 파편이 남는다. 카운트 비교 방식은 상태를 D1 한 곳에만 둔다.
- 배치는 선택이 아니다(§4). 전면 배선 시 Class A 무료 한도 초과.
- 리컨실러는 전 도메인 blast radius 를 갖는 신규 SPOF — `max_active_runs=1` · 런당 처리량 상한 ·
  자체 알림 · **target-aware** 필수(진단 #6 미해결 시 dev 관측이 prod D1 로 들어간다).
- 정리 대상 동반: dev `runs/` 43,676(ops 존 밖) · dev `errors/` 3,566(죽은 `population` 1,008 포함) ·
  `ops.run_metadata` 89.2 GiB.
- 알림 채널은 늘리지 않고 기존 digest 를 D1 소스로 돌린다.
- 보존기간은 카테고리 성격별로 다르게: `logs` 가 용량 주도(64 KB/건), 나머지는 건수 주도(~520 B).

## 범위 (in / out)

**in**

- `common/ops/runlog.py` — 표준 레코드 v2 스키마·직렬화·redaction·R2 write·D1 upsert 공통 모듈
- 기존 3 sink (`runmetrics` / `run_sink` / `errors`) 가 이 모듈에 위임 (**경로 규약은 유지**)
- ops 존 정합성 정렬: 세그먼트 순서·파티션 키·타임존·dev 존 편입·target 분기
- 6도메인 × 5레이어 배선 + track/콜백 권위 규칙
- dbt 인보케이션 단위 NDJSON 배치
- `ops_runlog_reconcile` DAG (카운트 비교 방식)
- D1 테이블 4종 + 뷰, `citydata_ops_digest` D1 전환·일반화

**out**

- 신규 ops 카테고리 신설 — **철회**
- `pending/`·`loaded/` 상태 세그먼트 및 파일 이동 — **철회**
- 태스크 텍스트 로그를 나머지 5도메인으로 확대 (`ops/logs/`) — 별건
- Airflow 외부 실행 관측 (스탠드얼론 `dbt run`, `serving/export_*_to_d1.py`) — P1 이후 재검토
- `ops.run_metadata` 물리 삭제 — ASK-Seoul#70 합의 후

## 계획

1. **P0 — 표준 레코드 v2 + 공통 sink 모듈**
   `common/ops/runlog.py`. status enum·redaction·`r2_env_for(target)`. 기존 3 sink 위임.
   **이 단계에서 경로는 바꾸지 않는다** (스키마·코드만). 단위테스트: enum 검증·fail-open·`PathKey` 유출 회귀.
2. **P1 — ops 존 정합성 정렬**
   `ops/runs` domain-first 정렬 · 파티션 키/타임존 통일 · dev `runs`→`ops/runs` 편입 ·
   태스크런 레코드를 `ops/metrics`→`ops/runs` 이동(1,127건, transit).
   구경로 병행 읽기 필요 여부는 소비자(digest) 전환 순서와 함께 결정.
3. **P2 — 배선 확대 + 배치**
   `track()` 을 raw/bronze/gold 로 확장(6도메인) + 콜백을 `default_args` 로 일괄 적용.
   dbt 인보케이션 단위 NDJSON + `dag_id`/`dbt_node_id`/`rows_source` 채움.
   `publisher` 발행 원장 → `grain=publication` 매핑. traffic/weather `layer="bronze"` 오라벨 교정.
4. **P3 — D1 집계 surface**
   D1 테이블 4종 + 뷰. `ops_runlog_reconcile`. Worker 엔드포인트. 대시보드 헬스 패널.
   `citydata_ops_digest` → D1 소스 전환·전 도메인 일반화.
5. **P4 — 레거시 정리**
   dev 구경로 `runs/`·`errors/`, `ops.run_metadata`. **P3 완료 후에만** 구경로 쓰기를 끊는다.

## 수용 기준

1. ops 존 카테고리는 4종 그대로다 (신규 없음, 폐기 없음)
2. 4 카테고리가 같은 세그먼트 순서(domain-first)·같은 파티션 키(`observed_date=`)·같은 타임존(KST)을 쓴다
3. 같은 태스크런이 `ops/runs`/`ops/metrics` 양쪽에 중복 기록되지 않는다 (grain 경계 + `event_id`)
4. 6도메인 × 5레이어(raw/bronze/silver/gold/d1) 전부 레코드 존재 (현재 2레이어 3도메인)
5. 요구 10필드 전부 채워지고, `rows` null 시 `rows_source` 로 사유 설명 가능
6. 동일 태스크 재시도가 성공률을 왜곡하지 않는다 (`is_final_attempt`)
7. prod R2 쓰기가 전면 배선 후에도 100k obj/월 미만
8. D1 한 질의로 도메인별 일일 성공률·소요 분포·적재 행수·API 호출량이 나온다
9. 관측 계층 장애(D1/R2)가 어떤 도메인 태스크도 실패시키지 않는다
10. `api_name`/`sink_uri` 에 자격증명이 남지 않는다 (`PathKey`/`QueryKey` 회귀 테스트 포함)

## 리스크 · 열어둔 질문

| 항목 | 내용 |
|---|---|
| `rows` 확보 방식 | dbt silver/gold 의 실제 적재 행수 출처 미결(on-run-end 훅 / Iceberg 스냅샷 / 발행 원장). **가장 큰 미해결 항목** |
| 경로 정렬 시 소비자 파손 | `citydata_ops_digest` 가 `ops/runs` 파일명·세그먼트 순서를 파싱한다. P1(정렬)과 P3(D1 전환) 순서 조정 필요 — 정렬을 P3 뒤로 미루는 것도 선택지 |
| 파티션 타임존 KST 선택 | 일별 도메인 집계를 사람 기준일에 맞추려 KST 로 통일하나, metrics·errors 는 현재 UTC 라 전환일 경계에 하루 겹침이 생긴다. 소급 재파티션 여부는 ASK-Seoul#70 결정 |
| 태스크런 grain 이동 | `ops/metrics`→`ops/runs` 1,127건(transit)은 신규 쓰기만 전환하고 기존 오브젝트는 방치할지 소급 이동할지 미결 |
| D1 직접 write 부하 | 전면 배선 시 ~30k행/일 추정 — 실측 기반 재확인. 초과 시 A안(폴링)으로 후퇴 가능하게 sink 계약 유지 |
| `metrics` 명칭 적합성 | dbt 노드 grain 을 담는 이름으로 `metrics` 가 최적은 아니다. 카테고리 유지를 우선해 이름은 그대로 두고 **경계 정의를 문서화**하는 선택. 재명명이 필요하면 별건 |
| 구경로 전환 시점 | 소비자(P3)를 먼저 옮기지 않으면 `ops.run_metadata` 사고가 반복된다. P3 완료 전 쓰기 중단 금지 |
| 보존기간 | `errors` 180일 · `logs` 30일 제안 — ASK-Seoul#70 결정 대기 |

## 개정 이력

- **2026-07-30 개정** — 초판의 `ops/pipeline_runs/` 신규 카테고리 신설안과 `pending/loaded` 파일 이동안을
  철회했다. 실측 재검토 결과 기존 4 카테고리는 성격 분리(평균 크기 123배 차이)·domain-first 파티션 모두
  타당하고, 실제 결함은 `runs`/`metrics` 의 grain 경계 부재 1건에 한정된다. 신규 카테고리는 그 결함을
  해결하지 않으므로, 경계를 grain 으로 재정의(이동량 10%)하고 통합은 D1 질의 계층에서 하는 방향으로 전환.
  파일 이동은 D1 `event_id` 카운트 비교로 대체해 R2 레이아웃을 불변으로 유지.
- **2026-07-30 초판** — R2 전수 실측 + 진단 + 표준 레코드 v2 제안.

## 실측 재현

세션 스크래치패드의 `audit_ops_zone.py` / `audit_coverage.py` / `audit_layers.py`.
`.env` 의 `R2_*` / `R2_DEV_*` 로 `list_objects_v2` 전수 + 표본 `get_object`. 상태는 변하므로 재실측 전제.
