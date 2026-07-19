# Weather·Traffic Iceberg Maintenance Partial/OOM 재실행 설계

## 문서 상태

- 대상 이슈: [ASAC-DAG #419](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/419)
- 상태: 설계 승인
- 기준 ASAC-DAG ref: `67d33dc420b1ab1ae3508a61df2dfdf5e6d2db06` (`origin/dev`, #427 merge)
- 범위: Weather·Traffic, dev 전용
- 비범위: Commerce, Citydata, Transit, Culture, OpenLineage timeout 정리, weighted capacity 조정

## 목적

`ask_seoul_iceberg_maintenance`의 중단 run
`manual__local_maintenance_20260717_0421`을 완료로 가정하지 않고 partial effect를
보수적으로 판정한다. 이후 Weather·Traffic dev Iceberg maintenance를 기존 메모리
가드레일 안에서 관찰 가능하고 재개 가능한 실행 단위로 재설계한다.

이 설계는 다음 기존 의도를 보존한다.

- Weather가 소유하는 단일 maintenance DAG를 유지한다.
- table 순서와 table별 `optimize → expire_snapshots → remove_orphan_files` 순서를
  유지한다.
- table과 operation을 병렬 실행하지 않는다.
- Trino 9 GiB container, 약 4.95 GiB heap, query memory 한도와 global concurrency 1을
  올리거나 제거하지 않는다. #426 이후 maintenance는 Weather resource adapter의
  `TRINO_HEAVY_POOL`, 즉 Airflow `trino_weather_heavy` 1-slot을 사용한다.
- 실패를 성공이나 무효과로 추정하지 않는다.

## 조사 경계와 현재 상태

조사는 `.env`와 secret을 읽지 않고, maintenance·recovery·W2·backfill·recollect를
실행하지 않은 상태에서 수행했다.

#426과 연결 root/dbt 변경의 dev merge·배포 뒤 실제 Traffic/Weather 동시 transform을
검증했다. `hardConcurrencyLimit=2`에서는 Weather query memory cap 실패가 재현됐고,
같은 Weather pin은 기본 `hardConcurrencyLimit=1` 복귀 뒤 성공했다. 따라서 #419의
기준선은 domain별 Airflow pool 분리를 수용하되 Trino global concurrency 1과 기존 memory
상한을 그대로 유지한다.

- Airflow DAG: paused
- active run: 0
- dev routing: `iceberg_dev.weather_traffic_bronze`
- 이전 run의 Airflow DagRun/TI/log: 현재 runtime에 없음
- 이전 run 당시 Trino query ID와 peak memory: 현재 runtime에 없음
- Trino 관측 기준선: 약 2.97 GiB / 9 GiB container memory

이전 run의 정확한 task attempt, query 결과, orphan 삭제량은 현재 환경에서 복원할 수
없다. 이후 증거가 추가되지 않는 한 이 항목은 `UNKNOWN`으로 유지한다.

## Canonical 대상

현재 DAG 기본 순서는 다음과 같다.

1. `bronze_kma_vilage_fcst`
2. `bronze_seoul_traffic_incident`
3. `bronze_seoul_traffic_incident_request_audit`
4. `bronze_collection_run_manifest`
5. `silver_kma_vilage_fcst`
6. `gold_weather_forecast_summary`
7. `dim_weather_place`
8. `gold_weather_forecast_by_place`
9. `silver_seoul_traffic_incident`
10. `gold_traffic_incident_summary`

런타임 입력은 위 목록의 subset만 허용한다. 입력 순서와 관계없이 canonical 순서로
정규화한다. 빈 목록, 중복, 비대상 table, 잘못된 identifier는 query 제출 전에
실패시킨다.

현재 dev schema에는 앞의 Bronze 4개만 존재한다. 뒤의 Silver/Gold 6개는 현재
없으며, 이전 run에서 실제로 missing-table check까지 도달했는지는 확인할 수 없다.

## 확인된 사실과 추론

### 확인된 사실

- 현재 구현은 단일 Airflow task가 10개 table과 table별 3개 operation을 처리한다.
- 각 `ALTER TABLE EXECUTE`는 별도 query이며 세 operation 전체가 하나의 transaction은
  아니다.
- table 예외는 문자열 결과로 저장한 뒤 다음 table 처리를 계속한다.
- Airflow task retry는 이미 완료된 operation을 포함해 전체 loop를 다시 시작한다.
- procedure 반환 metric과 Trino query ID를 현재 구현은 보존하지 않는다.
- 현재 `$files`는 current snapshot이 참조하는 파일만 나타낸다. orphan file과 과거
  삭제량을 보여 주지 않는다.

### 추론

- 중단 run과 같은 시간대에 canonical table 순서와 일치하는 네 개의 `replace`
  snapshot이 존재한다. 네 Bronze table의 `optimize`가 실행됐다는 강한 증거다.
- 앞의 세 table은 다음 table의 `replace` snapshot이 존재하므로, 같은 run이 canonical
  code path를 실행했다는 전제 아래 해당 table의 `remove_orphan_files` 호출이 반환됐을
  가능성이 높다.
- 마지막 `bronze_collection_run_manifest.remove_orphan_files`는 issue에 기록된 중단
  지점과 순서가 맞지만, query ID와 procedure metric이 없어 시작·완료·partial deletion
  어느 것도 확정할 수 없다.

위 추론은 run 완료 판정에 사용하지 않는다.

## 이전 run partial effect 판정

| 대상 | `optimize` | `expire_snapshots` | `remove_orphan_files` | 판정 근거와 한계 |
|---|---|---|---|---|
| KMA Bronze | 실행 효과 확인 | 완료 강한 정황 | 반환 완료 정황 | `replace` snapshot과 다음 table 진행 흔적은 있으나 run ID 직접 귀속·삭제 metric 없음 |
| Traffic incident Bronze | 실행 효과 확인 | 완료 강한 정황 | 반환 완료 정황 | 동일 |
| Traffic request audit Bronze | 실행 효과 확인 | 완료 강한 정황 | 반환 완료 정황 | 동일 |
| Collection run manifest Bronze | 실행 효과 확인 | 완료 정황 | `UNKNOWN` | 마지막 존재 table이며 issue의 orphan 중단 기록과 정합. partial deletion 배제 불가 |
| 현재 없는 Silver/Gold 6개 | `UNKNOWN` | `UNKNOWN` | `UNKNOWN` | 현재 missing이지만 이전 run이 skip까지 도달했는지 알 수 없음 |

전체 run 판정은 `PARTIAL_UNKNOWN`이다. 완료도, 무효과도 아니다.

## 현재 table 규모와 위험도

2026-07-18 read-only 관측값이다. 수집 pipeline의 append로 값은 계속 변할 수 있다.

| table | snapshot 수 | current data/delete file 수 | current file bytes | metadata log entries | 위험도 |
|---|---:|---:|---:|---:|---|
| `bronze_kma_vilage_fcst` | 780 | 34 / 0 | 15,644,880 | 87 | 중간 |
| `bronze_seoul_traffic_incident` | 2,174 | 254 / 3 | 2,986,654 | 101 | 높음 |
| `bronze_seoul_traffic_incident_request_audit` | 2,180 | 256 / 5 | 1,327,401 | 101 | 높음 |
| `bronze_collection_run_manifest` | 5,153 | 1,058 / 13 | 2,173,766 | 101 | 매우 높음 |
| 현재 없는 Silver/Gold 6개 | 없음 | 없음 | 없음 | 없음 | 실행 대상 아님 |

metadata JSON과 orphan 후보의 object count/bytes는 Iceberg metadata table만으로 확정할
수 없다. 실행 전 read-only object inventory와 referenced-file set을 비교해야 한다.
이전 run 이전 inventory가 없으므로 과거 삭제량은 끝내 정확히 복원하지 못할 수 있다.

## Operation 의미와 재시도 계약

### `optimize`

작은 data file을 더 적고 큰 file로 다시 쓴다. 논리 row set은 보존되어야 하지만 새
snapshot을 생성할 수 있으므로 strict idempotent가 아니다. ACK 유실 후 재시도는
no-op일 수도 있고, intervening write가 있으면 추가 rewrite를 수행할 수도 있다.

- logical row/fingerprint 불변을 검증한다.
- partition predicate가 안전하게 고정 가능한 table만 optimize slice를 허용한다.
- 모든 slice가 성공해야 같은 table의 snapshot expiration으로 진행한다.

### `expire_snapshots`

retention보다 오래된 snapshot을 metadata에서 제거하고 time-travel/rollback 가능 범위를
영구히 축소한다. 이미 만료된 snapshot에는 정책상 수렴하지만 relative `7d` cutoff는
재시도 시각에 따라 이동하므로 동일 side-effect set을 보장하지 않는다.

- retention은 이번 이슈에서 `7d`로 고정한다.
- `retain_last`를 암묵값으로 두지 않고 실행 evidence에 기록한다.
- current snapshot과 current logical state는 변하면 안 된다.
- pre/post `$snapshots`, `$metadata_log_entries`, `$refs`를 기록한다.

### `remove_orphan_files`

table directory에서 metadata에 연결되지 않고 retention보다 오래된 파일을 물리
삭제한다. 개별 삭제의 집합이며 all-or-nothing이 아니다. 중간 종료 후 재시도는 남은
orphan 삭제로 수렴할 수 있지만, 완료를 추정할 수는 없다.

- Trino procedure에는 partition predicate나 batch parameter가 없다.
- action task 분리는 fault localization을 개선하지만 단일 table scan의 OOM을 줄이지
  않는다.
- preflight budget을 넘는 table은 query를 제출하지 않고 `BLOCKED_OVERSIZE`로 남긴다.
- `processed_manifests_count`, `active_files_count`, `scanned_files_count`,
  `deleted_files_count`, `deleted_bytes`를 보존한다.
- writer 최장 시간보다 retention이 짧거나 path 표현이 불일치할 가능성이 있으면
  실행하지 않는다.

## 권장 구조

하나의 DAG 안에서 `(table, operation)`을 정적 Airflow task checkpoint로 만들고 완전히
직렬 연결한다.

```text
preflight
  → T1.optimize[*bounded slices]
  → T1.expire_snapshots
  → T1.remove_orphan_files
  → T1.gate
  → T2.optimize ...
  → final_report
```

다음 계약을 지킨다.

- DAG ID, 기본 schedule, `max_active_runs=1`, Weather failure callback을 유지한다.
- dynamic task mapping에 의한 병렬 실행과 Weather/Traffic DAG 분리를 금지한다.
- 모든 mutating operation task는 `TRINO_HEAVY_POOL`이 해석하는
  `trino_weather_heavy`, `pool_slots=1`을 사용한다.
- action 성공은 server query 성공과 postcondition을 모두 확인한 경우에만 인정한다.
- table-local terminal error는 해당 table 후속 operation을 skip하고 다음 table 결과는
  계속 수집한다.
- OOM, Trino restart, health loss, connection loss 후 server 상태 불명처럼 infrastructure
  상태가 `UNKNOWN`이면 circuit breaker로 이후 모든 mutating operation을 중단한다.
- circuit breaker는 실패 gate를 예외로 끝내는 방식이 아니라 gate XCom의
  `circuit_open=true` control state로 이후 모든 table gate에 전달한다. 각 gate는 이전
  gate control과 현재 action의 Airflow TI state를 함께 검사한다. 선택된 table에서
  result XCom 없이 task가 `failed`, `upstream_failed` 또는 비정상 terminal state이면
  table-local failure로 추정하지 않고 `UNKNOWN` circuit으로 승격한다.
- gate는 immutable plan을 직접 확인해 `NOT_SELECTED`와 XCom 유실을 구분한다. 이미 열린
  circuit은 다음 table의 첫 action이 Trino 연결 전에 skip하고, 이후 gate가 같은 circuit을
  다시 전달한다. 따라서 중간 `ALL_DONE` gate가 후속 mutation을 되살릴 수 없다.
- gate는 이전 circuit을 table selection보다 먼저 적용한다. non-selected table은 열린
  circuit을 닫거나 `NOT_SELECTED`로 덮을 수 없다.
- final report는 하나라도 `FAILED`, `UNKNOWN`, `BLOCKED_OVERSIZE`가 있으면 DAG를
  success로 만들지 않는다.

## Dev-only preflight

preflight는 두 층으로 나눈다.

Airflow DAG 내부에서는 어떤 Trino 연결이나 query 제출보다 먼저 다음을 검증한다.

- requested target이 정확히 `dev`
- runtime target도 정확히 `dev`
- catalog가 정확히 `iceberg_dev`
- schema가 승인된 Weather·Traffic dev schema
- table이 canonical allowlist subset
- retention이 정확히 `7d`
- immutable plan hash와 exact-table read-only inventory

paused 상태, active run, `trino_weather_heavy`·`trino_traffic_heavy`·legacy
`trino_heavy`의 idle, 실제 Trino running/queued query 0, container
health/restart/OOM, 60초 idle memory baseline은 **DAG trigger 전 외부 보호 gate**에서
검증한다. 실행 중 DAG는 자기 자신을 active
run으로 포함하므로 DAG 내부에서 `active run=0`을 요구하지 않는다. trigger 직전에는 active
run 0, trigger 뒤에는 current run 외 active run 0을 계약으로 사용한다. 실제 mutation 중에는
2초 외부 watcher가 RSS/JVM/health/restart를 감시한다.

prod/shared catalog 또는 schema fallback은 허용하지 않는다.

## Checkpoint와 evidence

Airflow task state를 기본 checkpoint로 사용한다. Iceberg catalog 내부에 새로운
checkpoint table은 만들지 않는다.

각 action은 다음 evidence를 남긴다.

- `plan_id`, immutable `plan_hash`
- canonical ordinal, catalog/schema/table/operation
- retention, `retain_last`, optimize predicate
- Airflow run ID와 attempt
- Trino query ID와 terminal state
- 시작·종료 시각과 duration
- procedure 반환 metric
- pre/post current snapshot ID
- pre/post snapshot/file/metadata fingerprint와 `$refs`
- `optimize` 전후 exact `SELECT count(*)` logical row count. `$files.record_count` 합계는
  delete-file이 있는 table의 logical row invariant로 사용하지 않는다.
- error class와 중단 사유

상태는 다음 중 하나다.

- `PENDING`
- `RUNNING`
- `SUCCEEDED`
- `FAILED`
- `UNKNOWN`
- `SKIPPED_MISSING`
- `BLOCKED_OVERSIZE`

mutation query 제출을 시도한 뒤 `fetchall`, telemetry, post-fingerprint 중 어느 단계에서든
예외가 발생하고 query terminal state를 확인하지 못하면
`UNKNOWN`이다. 성공으로 추정하거나 같은 plan을 자동 재개하지 않는다.

table-local terminal failure는 query ID와 `FAILED` terminal state 및 구조화된 non-memory
user error가 모두 확인된 경우에만 인정한다. 성공은 query state가 `FINISHED`이고 필수
telemetry와 postcondition이 모두 확인된 경우에만 인정한다. `remove_orphan_files`는
`processed_manifests_count`, `active_files_count`, `scanned_files_count`,
`deleted_files_count`, `deleted_bytes` 다섯 metric이 모두 있어야 한다.
procedure output은 Trino의 `metric_name, metric_value` N행을 dict로 정규화하며,
`optimize`의 공식 3개 metric도 모두 non-null numeric이어야 한다. exception evidence에는
판정 phase, query ID/state와 sanitized structured error type/name을 남긴다.

## 메모리 예산과 fail-fast

기존 Trino 설정을 변경하지 않는다.

- container limit: 9 GiB
- JVM max heap: container의 55%, 약 4.95 GiB
- query max memory: 2 GB
- query total memory: 4 GB
- heap headroom: 2 GB
- Trino global hard concurrency: 1
- Airflow `trino_weather_heavy`: 1 slot
- Airflow `trino_traffic_heavy`: 1 slot
- legacy Airflow `trino_heavy`: 1 slot bootstrap 유지

초기 세 번의 관측이 쌓이기 전까지 다음 보수적 runtime gate를 적용한다.

| 신호 | 시작 허용 | 경고·다음 operation 금지 | 즉시 실패·중단 |
|---|---:|---:|---:|
| Container RSS | 60초간 4.5 GiB 이하 | 6.3 GiB 이상 | 7.0 GiB 이상 |
| JVM heap used | 1.5 GiB 이하 | 3.5 GiB 이상 | 4.0 GiB 이상 |
| Trino pool free | reservation 0 | 1 GiB 미만 | 512 MiB 미만 |
| Query peak user memory | 관측 | 1.4 GiB 이상이면 완료 action은 인정하되 circuit open | 1.6 GiB 초과이면 완료 action은 인정하되 즉시 확대 중단 |
| wall time | 관측 | operation 10분 또는 전체 25분 | operation 15분 또는 전체 30분 |

`BLOCKED_OVERSIZE`는 trigger 전 external inventory/capacity gate가 mutation을 제출하지 않은
상태에만 사용한다. query 완료 뒤 peak memory가 기준을 넘은 경우 이미 수행된 action을
`BLOCKED_OVERSIZE`로 오표기하지 않고 `SUCCEEDED_WITH_STOP`과 열린 circuit으로 기록한다.

2초 간격으로 수집한다. 단일 spike라도 8 GiB 이상, container restart,
`OOMKilled`, Trino health loss, memory-limit error가 관측되면 즉시 실패한다.

이 수치는 단순 메모리 증설안이 아니라 현재 상한 안에 추가 안전 여유를 두는 초기
abort 기준이다. 세 번의 비교 가능한 관측 후에만 별도 승인으로 조정한다.

`iceberg.file-delete-threads` 변경은 catalog 전체에 영향을 주며 directory listing
memory를 제한한다는 근거도 없다. #419 구현에 포함하지 않는다.

## 재실행·중단·재개 계약

1. DAG는 paused 상태로 유지한다.
2. 이전 run 판정과 current inventory를 immutable plan evidence로 저장한다.
3. 가장 작은 existing table 하나를 canary 후보로 선택한다.
4. 승인된 한 table의 operation chain만 실행한다.
5. `FAILED`, `UNKNOWN`, memory warning 이상이면 즉시 확대를 중단한다.
6. canary 성공만으로 schedule을 재개하지 않는다.
7. 승인된 전체 Weather·Traffic matrix의 bounded evidence를 확보한다.
8. 팀 승인 후에만 unpause를 별도 결정한다.

현재 inventory에서는 KMA Bronze가 canary 후보지만, 실제 실행은 별도 승인과 보호된
dev capacity gate가 필요하다.

## 작은 dev 검증 계획

### 정적·단위 검증

- non-dev, invalid catalog/schema가 connection 전에 실패한다.
- 빈·중복·비대상 table과 잘못된 retention이 실패한다.
- canonical table/action 순서와 완전 직렬 dependency를 검증한다.
- 모든 mutation task가 `trino_weather_heavy` 1-slot을 사용한다.
- `optimize` 실패 시 같은 table의 expire/orphan이 실행되지 않는다.
- `expire_snapshots` 실패 시 orphan이 실행되지 않는다.
- table-local failure와 infrastructure `UNKNOWN`의 continuation 정책이 다르다.
- hard-kill/XCom 유실 뒤 `ALL_DONE` gate가 다다음 table mutation을 되살리지 않는다.
- query ID와 procedure metric이 보존된다.
- query terminal state, `$refs`, `optimize` exact row count, orphan 5개 metric이 없으면
  성공으로 판정하지 않는다.
- final report가 partial/unknown을 success로 바꾸지 않는다.
- root Trino runtime hardening 계약이 그대로 통과한다.

### 보호된 dev canary

- DAG paused, active run 0, 세 heavy pool 모두 idle, 실제 Trino running/queued query 0을
  확인한다.
- exact root/dags/dbt revision과 dirty 상태를 기록한다.
- 한 existing table의 before inventory를 기록한다.
- memory watcher를 붙여 한 table chain만 실행한다.
- logical row/fingerprint 불변, snapshot/file effect, procedure metric을 검증한다.
- 종료 후 RSS/heap이 idle baseline으로 회복되고 restart/OOM이 없음을 확인한다.

## PR merge 전 OOM·회귀 gate

다음은 merge 차단 조건이다.

- container/heap/query memory 한도 상향
- resource group 또는 Airflow pool concurrency 상향
- mutation task의 pool 누락
- table 또는 operation 병렬화
- canonical 순서 변경
- mutation 제출 이후 전체 task 자동 retry
- target/catalog/schema가 dev임을 증명하지 못함
- query ID, peak memory, scanned/deleted metric 누락
- OOM/restart/health loss 이후 다음 mutation 실행
- logical row 또는 active snapshot reference 손실
- maintenance를 W2/recovery/backfill/recollect와 함께 실행

merge 후에도 DAG는 paused 상태를 유지한다. idle memory 5분 baseline을 기록하고 승인된
matrix를 한 번만 실행한다. 종료 후 10분 안에 RSS/heap 회복, pool reservation 0,
restart count 불변, OOM 없음이 확인되어야 한다.

## 대안과 결정

### A. Table당 하나의 task

변경량은 작지만 orphan 실패 시 같은 table의 optimize와 expire를 다시 실행한다.
operation별 ACK와 partial effect가 계속 불명확하므로 채택하지 않는다.

### B. Table × operation 정적 직렬 task

기존 순서와 단일 DAG를 유지하면서 retry와 evidence 경계를 operation 수준으로 줄인다.
단일 `remove_orphan_files`의 memory peak 자체는 줄이지 못하므로 preflight budget과
`BLOCKED_OVERSIZE`가 함께 필요하다.

**결정: B를 채택한다.**

## 공식 동작 근거

- [Trino Iceberg connector와 maintenance procedure](https://trino.io/docs/current/connector/iceberg.html)
- [Trino resource management](https://trino.io/docs/current/admin/properties-resource-management.html)
- [Trino resource groups](https://trino.io/docs/current/admin/resource-groups.html)
- [Apache Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [Apache Iceberg reliability](https://iceberg.apache.org/docs/latest/reliability/)

## 구현 승인 조건

구현은 이 문서가 사용자 검토를 통과한 뒤 별도 implementation plan으로 진행한다.
구현·canary·maintenance 실행·unpause는 서로 다른 승인 경계다.
