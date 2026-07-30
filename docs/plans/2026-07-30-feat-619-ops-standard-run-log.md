# 파이프라인 표준 실행로그 통합 — ops 존 단일 스트림 + D1 집계

- 상태: 초안
- 작성일: 2026-07-30
- 이슈: ASAC-DAG#619 / 브랜치: `feat/619-ops-standard-run-log`
- 연계: ASK-Seoul#70 (존 레이아웃·수명주기·D1 테이블 확정) · ASK-Seoul#60 (R2 경로 규약)

## 배경 · 목표

도메인별 적재 실행(도메인·DAG·시작/종료·소요·행수·호출 API·저장 대상·상태)을 한 스키마로 남기고,
D1 에서 집계·헬스체크·그래프로 쓸 수 있게 만든다.

`dags/common` 에는 이미 관측 코드가 4벌 있으나 서로 겹치지 않고 도메인별로 다르게 붙어 있어,
"전 도메인 한 곳 집계"가 현 구조로는 불가능하다. 착수 전 R2 를 전수 실측해 현황을 확정했다.

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

| 도메인 | 총계 | bronze (`track`) | silver (dbt) | 비고 |
|---|---|---|---|---|
| transit | 11,566 | 1,127 | 10,439 | distinct dbt 노드 147, 노드당 최대 72런 |
| weather | 55 | 0 | 55 | distinct 노드 26 |
| traffic | 15 | 0 | 15 | distinct 노드 8 |

transit bronze 내역: subway 509 · parking 304 · bus 153 · loader 151 · master 6 · route_master 2 · maintenance 2.

**dbt 노드별 레코드가 전체 오브젝트의 90% (10,439 / 11,636).**

### dev 버킷 `seoul-dev`

- `runs/` (구경로) **43,676** (7-16~7-29, citydata 43,674 · transit 2, 최대 4,736/일)
- `ops/metrics/` 1,547 (7-29 하루)
- `errors/` (구경로) **3,566** (7-03~7-29) — population 1,008 · citydata 947 · transit 808 · traffic 648 · weather 150 · culture 5
  - `population` 은 현재 `dags/domains/` 에 없는 도메인
- `ops/runs/`, `ops/logs/` 비어 있음

### 도메인 × 메커니즘 커버리지

| 도메인 | `ops/runs` | metrics bronze | metrics silver | `ops/logs` | `ops/errors` |
|---|---|---|---|---|---|
| citydata | ✅ 5,110 | ✗ | ✗ | ✗ | ✅ |
| transit | ✗ | ✅ 1,127 | ✅ 10,439 | ✗ | ✅ |
| traffic | ✗ | ✗ (0) | ✅ 15 | ✗ | ✅ |
| weather | ✗ | ✗ (0) | ✅ 55 | ✗ | ✅ |
| commerce | ✗ | ✗ | ✗ | ✅ 206 | 자체 `log_event` |
| culture | ✗ | ✗ | ✗ | ✗ | ✅ |

두 메커니즘을 다 쓰는 도메인이 없다.

### 레코드 내용 실측

- `layer` 값은 `bronze`/`silver` 뿐 — **`raw`·`gold`·`d1` 은 0건** (transit 60건 표본: silver 55 · bronze 5)
- silver 레코드는 `dag_id=null` (표본 60건 중 55건)
- transit silver `rows` 는 표본 40건 **전부 null**
- D1 발행 결과는 `common/serving/publisher.py` 가 D1 `_publication_ledger` 에 별도 적재 — ops 존과 조인 불가

## 진단

| # | 문제 | 근거 |
|---|---|---|
| 1 | 레이어 공백 (raw/gold/d1 미관측) | 실측 `layer` ∈ {bronze, silver} |
| 2 | `track()` 배선이 transit 뿐 (12곳) | traffic/weather 는 `*_reliability_report.py` 에 `layer="bronze"` 오라벨, 실측 0건 |
| 3 | `rows` 사실상 공백 | dbt-trino 가 `adapter_response.rows_affected` 미채움 |
| 4 | grain 혼재 | silver `dag_id=null` 90% → `GROUP BY dag_id` 시 조용히 유실. `unique_id` 마지막 세그먼트만 남아 model/test 구분 불가 |
| 5 | 파티션·타임존 불일치 | metrics `observed_date=`(UTC) / runs `observed_date=`(KST) / logs `load_date=`(KST) |
| 6 | 버킷 분기 불일치 | `run_sink` 은 `r2_env_for(target)`(#556), `MetricsR2Sink` 는 `r2_env`(dev 우선). 실측: 7-29 metrics 가 dev·prod 양쪽 존재 |
| 7 | 상태 어휘 3종 | `{success,failed,skipped}` / `{SUCCESS,FAILED,PARTIAL}` / 원장 `outcome` |
| 8 | 소비자 없음 (write-only) | `serving/`·`dashboard/` 에 ops 읽기 코드 0. `citydata_ops_digest` 만 R2 LIST + 파일명 파싱 |
| 9 | 재시도 이중계상 | 2회차 성공 시 failed 1 + success 1. digest 성공률이 태스크 기준이 아니라 시도 기준 |
| 10 | at-rest 유출 위험 | `runmetrics.py` `redact()` 호출 0건. `http/auth.py` `PathKey` 는 API 키를 **URL 경로**에 실음 |

### 요구 필드 대조

| 요구 | 현재 | 판정 |
|---|---|---|
| 도메인 | `domain` | ✅ |
| 실행 DAG 이름 | `dag_id` | ⚠ silver null |
| 시작/종료시간 | `started_at`/`finished_at` | ✅ |
| 소요시간 (시·분·초) | `duration_s` (float) | ⚠ H:M:S 없음 |
| 실제 적재건수 | `rows` | ⚠ silver 전량 null |
| 호출 API | `api_calls` (횟수) | ❌ 식별자 없음 |
| 저장 타입 | — | ❌ |
| 저장 경로 | — | ❌ |
| 상태값 표준화 | `status` | ⚠ 어휘 3종 |
| 집계·헬스체크 | — | ❌ 저장소 없음 |

### Airflow 전용인가

**그렇다.** `track()` 은 Airflow context 에서 dag_id/run_id/try_number 를 뽑고, dbt 레코드도
Airflow 태스크가 `dump_dbt_run_results` 를 호출해 남는다. 맨 `dbt run` 이나
`serving/export_gold_to_d1.py` · `serving/export_transit_to_d1.py` 스탠드얼론 실행은 아무것도 남기지 않는다.

## 재사용할 자산

| 모듈 | 재사용 지점 |
|---|---|
| `common/runmetrics.py` | `_RECORD_FIELDS` 21필드 평평한 구조(DB 컬럼 1:1). docstring 이 "DB sink 교체 전제" 명시 — sink 교체가 설계된 경로 |
| `common/storage.py` | `Storage` ABC · `R2Storage` (read/list/delete/**서버사이드 `copy`**) · `r2_env_for(target)` |
| `common/serving/d1_client.py` | D1 HTTP API · `build_insert_statements`/`group_api_batches` (바이트 예산 배치) · `_ensure_catalog_schema` additive ALTER |
| `common/http/core.py` | `note_http_request`/`note_http_retry` contextvar 컬렉터 → API 식별자 확장 지점 |
| `common/serving/publisher.py` | `_publication_ledger` = 사실상 `layer=d1` 로그. 매핑 대상 |

R2 쓰기 경로가 4벌 중복(`R2Storage` / `MetricsR2Sink._put_r2_object` / `R2ErrorSink._put_r2_object` /
`run_sink._put_r2`). 읽기·삭제가 되는 건 `R2Storage` 뿐 — 그래서 적재기가 재사용할 reader 가 없다.

## 설계

### 1. 표준 레코드 v2 — #188 스키마에 가산만

```
-- 식별 · 계보
event_id         TEXT PK   sha1(domain|dag_id|task_id|run_id|try_number|node_id)  결정적, 멱등 upsert 키
schema_version   INTEGER
domain           TEXT
layer            TEXT      raw|bronze|silver|gold|d1          닫힌 enum
grain            TEXT      task_attempt|dbt_node|publication  집계 시 grain 혼재 차단
dag_id, task_id, run_id, try_number
dbt_node_id      TEXT      dbt unique_id 전체 (model/test 구분)

-- 시간
started_at, finished_at    UTC ISO8601
duration_s       REAL      집계 정본
duration_hms     TEXT      HH:MM:SS  사람 표시용
observed_date    TEXT      KST YYYY-MM-DD  파티션·일별 집계 단일 기준
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
status           TEXT      success|failed|skipped|partial     닫힌 enum (3종 어휘 통합)
is_final_attempt INTEGER   0/1  재시도 성공 시 성공률 왜곡 차단
error_id, error_type       ops/errors 문서 조인키
target, hostname, peak_rss_mb, cpu_time_s
```

- `duration_s` 를 집계 정본으로 두고 `duration_hms` 를 병기한다 (요구사항의 시·분·초 표기).
- `api_name`/`sink_uri` 는 **저장 직전 `redact()` 필수**. 진단 #10 참조.
- 상태 매핑: `raw_manifest` `SUCCESS→success` `FAILED→failed` `PARTIAL→partial`,
  원장 `skipped_retained→skipped`.

### 2. 공통 prefix

```
ops/pipeline_runs/<state>/observed_date=<KST>/domain=<domain>/<batch_id>.ndjson.gz
                  state ∈ { pending, loaded }

batch_id:  <dag_id>__<run_id>__<task_id>__try<N>       grain=task_attempt (1행)
           <dag_id>__<run_id>__dbt-<invocation_id>     grain=dbt_node    (N행, 인보케이션 단위)
```

- 전체 조회는 단일 prefix `ops/pipeline_runs/`, 도메인 식별은 `domain=` 세그먼트 — 요구사항 충족
- 적재기는 `pending/` 만 LIST → 작업량이 미적재분에 비례. **도메인별 스케줄이 달라도 워터마크 불필요**,
  늦게 끝난 런도 그냥 pending 에 나타난다
- 적재 후 `R2Storage.copy()` (서버사이드) + `delete()` 로 `loaded/` 이동
- `event_id` PK upsert 이므로 적재 후 이동 실패로 크래시해도 재적재가 중복을 만들지 않는다

### 3. R2 효율 — NDJSON 배치

레코드당 1 PUT → **트랜잭션 경계당 1 PUT**. dbt 인보케이션 하나 = 147 PUT → 1 PUT.
실측상 silver 가 오브젝트의 90% 이므로 이것만으로 대부분이 사라진다.

| | 현재 | 배치 후 |
|---|---|---|
| prod 쓰기 | 11,264 obj/일 (도메인 40% 배선) | ~1.5–3k/일 |
| 6도메인 전면 배선 | 28–35k/일 ≈ **850k–1.05M PUT/월** (Class A 무료 1M/월 초과) | 50–90k/월 |
| 하루치 읽기 | 8,043 GET | ~100–150 GET |

용량은 어느 쪽이든 무의미(5.8 MiB / 2일). 비용을 만드는 건 오브젝트 수와 LIST/GET 횟수다.
`loaded/` 에 lifecycle 만료(90일) 를 걸고 질의는 D1 이 담당한다.

### 4. D1 적재 방식 — 3안 비교

| 안 | 내용 | 평가 |
|---|---|---|
| A | R2 에 쌓고 폴링 DAG 가 주기 적재 | 도메인별 주기 차이·미적재 판별·적재기 SPOF 를 전부 떠안는다 |
| B | 태스크 종료 시 D1 직접 write | 흐름 최단, 적재기·워터마크 불필요, 실시간. D1 장애가 핫패스 |
| **C (권장)** | **B + R2 를 스필/리플레이 버퍼로.** D1 write 가 정상경로, 실패분만 `pending/` 스필, 리컨실러 DAG(1일 1회)가 배수 | 실시간성 + 내구성. 도메인 주기 차이가 무의미해진다 |

C 근거:

- `runmetrics` 가 "DB sink 교체 전제"로 설계됨 — 설계된 이전 경로
- `d1_client` 에 바이트 예산 배치 삽입이 이미 있음 (dbt 147노드 = D1 batch 1회)
- 물량: 실측 11.3k행/일(40% 배선) → 전면 ~30k행/일. D1 무료 쓰기 100k행/일 안.
  누적 21필드 × 30k행 ≈ 월 5MB (D1 무료 5GB)
- 적재기가 정상경로가 아니라 **안전망**이 되어 도메인별 주기 차이 문제가 사라진다
- write 는 반드시 fail-open (`write_record` 현재 규약 유지).
  ⚠ `publisher._append_ledger` 는 의도적으로 raise 하지만 표준 로그 sink 는 그 반대여야 한다

### 5. D1 집계 surface

`_catalog`/`_publication_ledger` 와 같은 `_` 접두 규약.

| 테이블 | 용도 | 보존 |
|---|---|---|
| `_ops_runs` | 팩트 (`event_id` PK) | 90–180일 롤링 |
| `_ops_runs_daily` | (observed_date, domain, layer) 롤업: 런수·태스크수·성공/실패·p50/p95 소요·행수 합 | 영구 |
| `_ops_dag_health` | dag_id 별 최근 상태·연속 실패·최근 성공 경과분 | 현재값 |
| `_ops_dag_registry` | dag_id → domain, layer, `expected_interval_s`, owner | 관리 |

`_ops_dag_registry` 가 헬스체크의 전제다. 실행 주기가 도메인마다 다르므로
(citydata 5분 · transit subway 3분 · commerce 일 1회 · culture 주 1회) 기대 주기 없이는
**"안 돌아서 기록이 없는 것"과 "고장나서 없는 것"을 구분할 수 없다.**
신선도 SLO 는 `expected_interval_s` 배수로 정의한다.

스키마 진화는 additive-only + `schema_version`. 근거: #521 (공통 Publisher 15컬럼 vs 라이브 8컬럼
불일치로 카탈로그 등록 실패) — 같은 사고 반복 금지. `_ensure_catalog_schema` 의 additive ALTER 적용.

## 3중 검토

### 백엔드 — 계약·API 관점

레코드는 Worker/대시보드가 읽는 API 페이로드다.

- `schema_version` + additive-only 진화 필수. 선례 #521.
- `event_id` 없는 재시도는 이중계상 (진단 #9). 상태값은 write 시점 검증하는 닫힌 enum.
- 읽기 경로는 D1 단일화 — 대시보드가 R2 를 LIST 하면 사용자 기능이 오브젝트 스토어 지연·비용에 묶인다.
  현재 `citydata_ops_digest` 가 정확히 그 형태다.
- `api_name`/`sink_uri` 는 redaction 없이 저장 금지 (진단 #10). `PathKey` 회귀 테스트 필요.

### 데이터 엔지니어 — 모델링·계보 관점

- grain 선언 없는 팩트는 집계가 조용히 틀린다. 실측 `dag_id=null` 90% 가 증거.
- `rows` 가 최난관. dbt-trino 가 `rows_affected` 를 안 채우므로 silver/gold 는
  ① dbt `on-run-end` 훅 + count 질의 ② Iceberg 스냅샷 `summary.added-records`
  ③ 발행 원장 `published_row_count`(이미 채워짐) 중 선택하고 `rows_source` 로 출처를 남긴다.
  dbt test 노드는 행수 개념이 없어 null 이 정상 — 구분에 `grain`/`dbt_node_id` 필요.
- 파티션 어휘·타임존 단일화 (진단 #5).
- **새 Iceberg ops 테이블을 만들지 않는다.** `ops.run_metadata` 가 89.2 GiB 로 방치된 선례
  (2026-07-27 카탈로그 실측, 단일 최대 용량 항목)이고, R2 Data Catalog 는 DROP 해도 물리 파일이 남는다.
  이 규모는 D1 이 맞다.

### DevOps/SRE — 운영 관점

- 관측이 파이프라인을 죽이면 안 된다. C 안의 D1 직접 write 는 fail-open + R2 스필이 전제조건이며
  그게 C 의 존재 이유다.
- 비용은 배치가 선택이 아니다 (설계 §3). 전면 배선 시 Class A 무료 한도 초과.
- 리컨실러는 전 도메인 blast radius 를 갖는 신규 SPOF — `max_active_runs=1` · 런당 처리량 상한 ·
  자체 알림 · **target-aware** 필수 (진단 #6 미해결 시 dev 관측이 prod D1 로 들어간다).
- 정리 대상 동반: dev `runs/` 43,676 · dev `errors/` 3,566 (죽은 `population` 1,008 포함) ·
  `ops.run_metadata` 89.2 GiB.
- 알림 채널은 늘리지 않고 기존 digest 를 D1 소스로 돌린다.
- `ops/logs/` (commerce tar.gz) 는 태스크 **텍스트 로그**로 성격이 달라 표준 로그와 합치지 않는다.
  다만 나머지 5도메인은 태스크 로그를 도커 볼륨에 그대로 두고 있어 별건 후속으로 남긴다.

## 범위 (in / out)

**in**

- `common/ops/runlog.py` 신설 — 표준 레코드 v2 단일 sink (D1 우선 → R2 pending 스필, fail-open, redaction)
- 기존 3 sink (`runmetrics` / `run_sink` / `errors`) 를 이 모듈에 위임
- 6도메인 × 5레이어 배선
- `ops_runlog_reconcile` DAG + R2 lifecycle
- D1 테이블 4종 + 뷰, `citydata_ops_digest` D1 전환·일반화

**out**

- 태스크 텍스트 로그 통합 (`ops/logs/`, commerce 외 5도메인) — 별건
- Airflow 외부 실행 관측 (스탠드얼론 `dbt run`, `serving/export_*_to_d1.py`) — P1 이후 재검토
- `ops.run_metadata` 물리 삭제 — ASK-Seoul#70 합의 후

## 계획

1. **P0 — 표준 레코드 v2 + 단일 sink**
   `common/ops/runlog.py`. status enum·파티션(`observed_date=` KST)·타임존·target 분기(`r2_env_for`) 통일.
   redaction 적용. 기존 3 sink 위임. 단위테스트: 키 형식·enum 검증·fail-open·`PathKey` 유출 회귀.
2. **P1 — 전 도메인 배선**
   `track()` 을 raw/bronze/gold 로 확장(6도메인). dbt 경로는 인보케이션 단위 배치 +
   `dbt_node_id`/`rows_source`. `publisher` 발행 원장 → `layer=d1` 레코드 매핑(중복 저장 아님).
   traffic/weather reliability-report 의 `layer="bronze"` 오라벨 교정.
3. **P2 — 리컨실러 + 수명주기**
   `ops_runlog_reconcile` (1일 1회, pending 배수 → loaded 이동, `max_active_runs=1`, 처리량 상한).
   R2 lifecycle: `loaded/` 90일.
4. **P3 — 집계 surface**
   D1 테이블 4종 + 뷰. Worker 엔드포인트. 대시보드 헬스 패널.
   `citydata_ops_digest` → D1 소스 전환·전 도메인 일반화.
5. **P4 — 레거시 정리**
   dev `runs/`·`errors/` 구경로, `ops.run_metadata`. **P3 완료 후에만** 구경로 쓰기를 끊는다.

## 수용 기준

1. 6도메인 × 5레이어(raw/bronze/silver/gold/d1) 전부 레코드 존재 (현재 2레이어 3도메인)
2. `ops/pipeline_runs/` 단일 prefix 로 전 도메인 조회, `domain=` 으로 도메인 식별
3. 요구 10필드 전부 채워지고, `rows` null 시 `rows_source` 로 사유 설명 가능
4. 동일 태스크 재시도가 성공률을 왜곡하지 않는다 (`is_final_attempt`)
5. prod R2 쓰기가 전면 배선 후에도 100k obj/월 미만
6. D1 한 질의로 도메인별 일일 성공률·소요 분포·적재 행수·API 호출량이 나온다
7. 관측 계층 장애(D1/R2)가 어떤 도메인 태스크도 실패시키지 않는다
8. `api_name`/`sink_uri` 에 자격증명이 남지 않는다 (`PathKey`/`QueryKey` 회귀 테스트 포함)

## 리스크 · 열어둔 질문

| 항목 | 내용 |
|---|---|
| `rows` 확보 방식 | dbt silver/gold 의 실제 적재 행수 출처 미결 (on-run-end 훅 / Iceberg 스냅샷 / 발행 원장). **가장 큰 미해결 항목** |
| D1 직접 write 부하 | 전면 배선 시 ~30k행/일 추정 — 실측 기반 재확인 필요. 초과 시 A 안(폴링) 으로 후퇴 가능하게 sink 계약 유지 |
| 구경로 전환 시점 | 소비자(P3) 를 먼저 옮기지 않으면 `ops.run_metadata` 사고가 반복된다. P3 완료 전 쓰기 중단 금지 |
| 파티션 소급 | `ops/logs/` `load_date=` 를 소급 재파티션할지 신규만 정렬할지 — ASK-Seoul#70 결정 대기 |
| 보존기간 | `ops/errors/` 180일 · `ops/logs/` 30일 제안 — ASK-Seoul#70 결정 대기 |
| 3 sink 위임 | 위임 과정에서 기존 키 형식이 바뀌면 `citydata_ops_digest` 파일명 파싱이 깨진다. P3 전환과 순서 조정 필요 |

## 실측 재현

세션 스크래치패드의 `audit_ops_zone.py` / `audit_coverage.py` / `audit_layers.py`.
`.env` 의 `R2_*` / `R2_DEV_*` 로 `list_objects_v2` 전수 + 표본 `get_object`. 상태는 변하므로 재실측 전제.
