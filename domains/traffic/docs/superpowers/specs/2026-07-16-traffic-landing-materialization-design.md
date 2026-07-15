# Traffic landing·materialization 최소 분리 설계

- 상태: 승인됨
- 기준 이슈: [ASAC-DAG #390](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/390)
- 기준 ref: ASAC-DAG `f3035aa5d4d23877409ce7afb282e412aa7c5b5a`, ASAC-DBT `8b43fe4e5260d373093ce0d25e7a555d6ae7e858`

## 1. 문제와 설계 원칙

현재 `traffic_incident_bronze`는 5분 TOPIS 수집, R2 raw landing, Trino/Iceberg Bronze 적재·검증을 한 run에서 직렬 실행한다. `trino_heavy` 대기나 재시도가 5분을 넘으면 `catchup=False`, `max_active_runs=1` 때문에 scheduled slot 자체가 생성되지 않을 수 있다. 그 결과 raw snapshot 누락과 `missing_run_ledger_entry` 경보가 함께 발생한다.

해결 기준은 “단계마다 DAG/Task를 만든다”가 아니다. 다음 중 하나가 다를 때만 Airflow 경계를 나눈다.

- retry 정책
- pool·worker·실행환경
- 독립 재개 가치
- 병렬 실행 가치
- 운영자가 별도로 대응해야 하는 실패 단위

ledger·manifest 상태 기록, runtime guard, Asset metadata 조립은 lifecycle 내부 구현으로 숨긴다. DAG 파일은 orchestration만 표현하고, 도메인 규칙은 작은 모듈에서 테스트한다.

## 2. 범위

### 포함

- `traffic_incident_landing` 신규 DAG
- `traffic_incident_bronze`를 receipt materializer로 축소
- `traffic_flow_bronze`를 Incident Bronze Asset 기반으로 전환
- `traffic_incident_transform`의 Incident/Flow snapshot pair 선택 보강
- reliability 분모·불연속 gap·알림 fingerprint·backlog 표시 수정
- Traffic/Weather 회귀 테스트와 dev smoke

### 제외

- Weather DAG 구조 변경
- `common/**` 변경
- ASAC-DBT SQL/model 계약 변경
- validation 전용 DAG 또는 failure 수집 전용 DAG 추가
- prod 환경 변경
- manual recollect/backfill 계약 변경

production 변경은 `domains/traffic/**`에만 둔다.

## 3. 최종 DAG 구조

### 3.1 `traffic_incident_landing`

dev에서 기존과 같은 5분 cron을 소유하고 visible task는 하나다.

```text
land_traffic_incident_snapshot
  ledger STARTED
  → runtime guard
  → TOPIS API / R2 raw landing
  → 결정적 LANDED receipt + pending marker
  → ledger SUCCESS
  → Traffic Incident Raw Asset
```

- `catchup=False`, `max_active_runs=2`
- Trino 호출과 `trino_heavy` pool이 없다.
- 실패 시 ledger FAILED를 best-effort로 남기되 원래 예외를 보존한다.
- task retry는 기존 landing checkpoint와 같은 run ID를 재사용한다.
- full snapshot 성공만 Raw Asset을 발행한다.

### 3.2 `traffic_incident_bronze`

Raw Asset이 생기면 즉시 실행하고, 15분 cron fallback이 pending queue 복구를 보장한다. visible task는 하나다.

```text
materialize_pending_traffic_incident_snapshots [trino_heavy]
  pending receipt oldest-first 조회
  → snapshot별 manifest STARTED
  → Bronze load + verify
  → manifest SUCCESS
  → MATERIALIZED receipt (pending 유지)
  → batch의 최신 Incident Bronze Asset 조건부 발행
  → Airflow success callback에서 pending marker 제거
```

- `max_active_runs=1`
- Bronze row와 manifest의 `dag_run_id`는 materializer run ID가 아니라 원래 landing `snapshot_run_id`다.
- 같은 snapshot 재시도는 기존 page delete+insert와 MATERIALIZED receipt로 멱등이다.
- 한 snapshot이 실패하면 뒤 snapshot으로 넘어가지 않고 manifest FAILED 후 task를 실패시킨다.
- fallback run에 pending이 없으면 성공으로 끝나며 Asset을 발행하지 않는다.
- 조건부 발행은 `AssetAlias`를 사용한다. 실제 materialization이 있을 때만 고정 Bronze Asset을 alias에 추가한다.
- pending ack는 Asset event를 포함한 task 성공 메시지가 supervisor에 수락된 뒤 success callback에서 수행한다.
- success callback 전 실패나 callback 실패는 pending을 남겨 다음 실행에서 at-least-once 재처리한다.
- 한 run에서 여러 snapshot을 Bronze까지 처리해도 downstream에는 가장 최신 완료 snapshot 한 개만 발행한다. 중간 Raw/Bronze snapshot은 삭제하거나 `COALESCED` 처리하지 않는다.

### 3.3 `traffic_flow_bronze`

독립 5분 cron을 제거하고 Incident Bronze Asset을 소비한다. API/R2와 Trino의 retry·resource 특성이 다르므로 visible task는 두 개다.

```text
land_traffic_flow_snapshot
→ materialize_verify_publish_traffic_flow [trino_heavy]
```

- 첫 task는 triggering Incident Asset의 `bronze_dag_run_id`를 exact parent로 고정하고 그 snapshot의 link ID만 조회한다.
- raw result와 Flow Asset metadata에 `parent_incident_run_id`를 보존한다.
- 두 번째 task는 Flow Bronze load·verify·manifest lifecycle을 수행한다.
- 완료 시 parent가 더 이상 최신 publishable Incident가 아니면 Raw/Bronze와 SUCCESS manifest는 보존하되 Flow Asset은 발행하지 않는다.
- 조건부 Flow Asset 역시 `AssetAlias`를 사용한다.

### 3.4 `traffic_incident_transform`

Incident Bronze 또는 compatible Flow Bronze 이벤트에 반응한다.

- Incident 이벤트: exact Incident run을 사용하고 Flow var는 비운다.
- Flow 이벤트: `parent_incident_run_id`와 exact Flow run을 함께 dbt vars로 전달한다.
- 여러 Incident 이벤트가 한 run에 모이면 최신 `event_at`을 선택하고 나머지 transform 후보만 manifest `COALESCED`로 표시한다.
- stale Flow 이벤트가 transform 대기 중 뒤처지면 최신 Incident를 Flow 없이 재처리해 Gold snapshot 회귀를 막는다.
- 기존 `traffic_snapshot_dag_run_id`, `traffic_flow_snapshot_dag_run_id` var 계약은 유지한다.

## 4. receipt 계약

R2에는 감사용 terminal receipt와 작은 pending index를 함께 둔다.

```text
traffic-snapshot-receipts/source_id=seoul_traffic_incident/
  pending/<safe-run-id>.json
  snapshot_date=YYYY-MM-DD/run_id=<safe-run-id>/LANDED.json
  snapshot_date=YYYY-MM-DD/run_id=<safe-run-id>/MATERIALIZED.json
```

`LANDED.json` 필수 필드:

- `version`, `source_id`, `producer_dag_id`, `snapshot_run_id`
- `logical_date`, `snapshot_at`, `event_at`
- `raw_result`

`MATERIALIZED.json` 필수 필드:

- `version`, `source_id`, `snapshot_run_id`, `snapshot_at`
- `materializer_dag_id`, `materializer_run_id`
- `row_count`, `raw_object_count`, `event_at`

쓰기 순서는 `LANDED receipt → pending marker`, `MATERIALIZED receipt → Asset event가 포함된 Airflow success → pending marker delete`다. terminal receipt는 보존한다. run ID는 path segment에 percent-encoding해 서로 다른 identity가 같은 key로 합쳐지지 않게 한다. retry가 같은 key에 다른 payload를 쓰려 하면 contract error로 실패한다. secret, endpoint credential, webhook URL은 저장하지 않는다.

## 5. reliability 계약

scheduled cadence SLO는 `traffic_incident_landing` ledger를 본다. publishability와 Bronze freshness는 계속 `traffic_incident_bronze` manifest/table을 본다.

각 expected slot은 정확히 한 상태에 속한다.

```text
success + failed + running + grace = expected
```

- terminal FAILED는 grace 구간이어도 즉시 failed다.
- stale 전 STARTED는 running, stale STARTED는 `run_stalled`다.
- stale 전 missing은 grace, stale 후 missing은 `missing_run_ledger_entry`다.
- failure gap은 5분 연속 slot끼리만 묶어 여러 window로 표시한다.
- 알림 identity는 상태, failure reason/task, 각 연속 window 시작으로 만든다. 같은 장애 window의 끝이 늘어나는 것만으로 재알림하지 않는다.
- pending count와 oldest pending age를 별도로 표시한다.

## 6. 클린 코드 기준

- DAG entrypoint에는 operator 생성, dependency, Airflow context mapping만 둔다.
- receipt 직렬화·queue 상태전이는 Airflow를 import하지 않는 pure domain module로 둔다.
- landing/materialization lifecycle은 dependency injection 가능한 service로 둔다.
- Asset URI·metadata parsing·조건부 발행 계약은 Traffic-owned module 한 곳에서 관리한다.
- scheduled materializer와 manual recollect/backfill entrypoint는 파일을 분리해 서로의 wiring을 숨기지 않는다.
- 이름은 Airflow task ID, receipt 상태, manifest 상태가 같은 업무 용어를 사용한다.
- wrapper를 위한 wrapper, 한 번만 쓰는 추상 base class, 단계 이름만 바꾼 파일 분리는 만들지 않는다.

## 7. 검증과 rollout

1. receipt, lifecycle, ledger 분모, gap grouping, fingerprint를 TDD로 고정한다.
2. Traffic 전체와 Weather 전체 Python tests·compile·DAG import를 통과한다.
3. 변경 파일이 `domains/traffic/**`뿐인지 확인한다.
4. 최신 ASAC-DBT dev에서 parse와 Traffic selector test를 통과한다.
5. DAG PR을 `dev`에 merge한다.
6. clean runtime worktree에서 DAG/DBT를 각각 최신 `origin/dev` exact SHA로 맞춘다.
7. `docker compose up -d --build`로 함께 재배포한다. `scripts/deploy.sh`는 사용하지 않는다.
8. Airflow dashboard에서 Landing → Bronze → Flow → Transform run과 Asset event를 확인한다.
9. R2 receipt, Bronze row, Silver/Gold row와 snapshot ID 일치를 Trino에서 확인한다.
10. 5분 landing cadence와 15분 reliability report를 관찰한다.

## 8. 완료 조건

- Incident scheduled graph가 기존 7 task에서 Landing 1 task + Materializer 1 task로 줄어든다.
- Landing에는 Trino 호출과 `trino_heavy`가 없다.
- 성공한 모든 LANDED receipt가 원래 snapshot run ID로 정확히 한 번 Bronze에 반영된다.
- no-pending fallback은 downstream Asset이나 skipped task를 만들지 않는다.
- Flow가 exact Incident parent를 소비하고 late Flow가 Gold를 과거로 되돌리지 않는다.
- reliability 분모 등식, 불연속 window, stable fingerprint가 테스트로 고정된다.
- production diff는 `domains/traffic/**`에만 있다.
- Traffic/Weather baseline `536 passed` 이상과 Airflow 3.2.2 container import가 통과한다.
- dashboard에서 최신 Landing, Bronze, Flow, Transform 성공 run을 바로 확인할 수 있다.
