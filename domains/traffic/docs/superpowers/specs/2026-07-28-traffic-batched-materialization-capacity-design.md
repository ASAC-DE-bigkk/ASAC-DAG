# Traffic batch materialization 및 2-lane capacity 설계

- 상태: 사용자 승인
- 기준 브랜치: `origin/dev`의 `38c5cdd`
- 이슈: 사용자 지시에 따라 별도 이슈를 만들지 않고 PR로 추적

## 문제와 근거

Traffic landing은 5분마다 약 8초 안에 끝나지만 Incident Bronze materializer는
receipt마다 manifest START, audit/Bronze delete·insert, verification, manifest SUCCESS를
각각 실행한다. 2026-07-28 dev 관측에서 신규 receipt 18개 처리에 40.97분이 걸렸고,
최근 2시간 `trino_traffic_heavy` 점유율은 98%였다. 이 중 Incident Bronze가
88.13분을 사용했다.

`verified-skip`은 불확실한 commit 재시도에는 유효하지만 신규 receipt에는 적용되지
않는다. 현재 schedule은 raw Asset과 15분 cron을 동시에 사용해 active materializer
뒤에 recovery run을 추가로 만들 수 있다. 한편 Bronze, Flow, Silver, Gold가 단일
Airflow pool을 공유해 독립된 snapshot-pinned 단계도 전부 직렬화된다.

## 목표

1. 최대 24개 Incident receipt를 한 번 준비하고 audit/Bronze와 manifest를 set-based
   query로 기록한다.
2. materializer를 KST 기준 15분 cron-only로 실행해 5분 landing 세 건을 한 cycle로
   coalesce한다.
3. 동일 writer는 직렬화하면서 ingest와 transform을 별도 1-slot lane으로 나눈다.
4. 로컬 dev Trino에 최대 동시 query 2개를 허용할 수 있도록 12GB container,
   약 6.6GB heap, query당 2GB·전체 4GB 한도를 사용한다.

## 비목표와 경계

- Traffic raw/Bronze/Silver/Gold schema와 dbt model 계약을 바꾸지 않는다.
- 동일 Incident Bronze writer 또는 동일 transform writer를 병렬 실행하지 않는다.
- snapshot pin, supersession, exact reconciliation, full-history test를 완화하지 않는다.
- Weather 및 다른 도메인의 DAG/DBT 코드는 수정하지 않는다.
- root `.airflowignore`은 수정하지 않는다.
- prod memory·concurrency 값은 변경하지 않는다. root 변경은 로컬 dev harness에만 둔다.

## batch materialization 계약

materializer는 pending receipt를 oldest-first로 읽고 exact preflight를 한 번 수행한다.
acknowledgement가 끝난 stale receipt는 기존 fence로 제거한다. 남은 receipt는 legacy raw
manifest를 먼저 복구하고 모든 raw payload/hash/page 계약을 Trino mutation 전에
검증한다.

```text
pending receipts
  -> exact verified preflight
  -> stale acknowledgement fence
  -> recover legacy manifest + prepare every raw payload
  -> manifest STARTED batch MERGE
  -> audit DELETE/INSERT batch
  -> Bronze DELETE/INSERT batch
  -> exact Bronze+audit grouped verification
  -> manifest SUCCESS batch MERGE
  -> MATERIALIZED receipts
  -> latest asset event
  -> success callback pending acknowledgement
```

Bronze와 audit delete는 대상 `snapshot_run_id` 집합에만 한정한다. insert 또는 verify가
실패하면 pending marker를 유지하고 대상 manifest를 batch FAILED로 기록한다. 다음 retry는
exact preflight로 완전 commit만 skip하며 부분 commit은 다시 set-based replace한다.
따라서 multi-statement Iceberg transaction이 없어도 at-least-once 복구와 멱등성이
유지된다.

## schedule과 asset

`traffic_incident_landing`은 5분 cadence와 raw Asset 감사 계약을 유지한다. Incident
materializer는 dev에서 `*/15 * * * *`만 사용하고 raw Asset이 즉시 run을 만들지 않는다.
한 cycle에서 실제 materialization이 있을 때만 최신 Incident Bronze Asset 하나를
발행하므로 downstream은 최대 15분마다 한 번 실행된다.

## resource lane

- `trino_traffic_ingest`: 1 slot
  - Incident Bronze materializer
  - Flow Bronze materializer
- `trino_traffic_transform`: 1 slot
  - Incident/Flow Silver
  - Traffic Gold
- `trino_traffic_heavy`: 1 slot 유지
  - manual recovery, maintenance, reliability 등 보수적 legacy 작업

각 lane 내부 writer는 직렬이다. lane 사이 동시 실행은 기존 pinned snapshot과
supersession fence를 통과한 작업만 허용한다. 로컬 Trino root resource group은 최대
2 query로 제한해 세 번째 heavy query를 queue한다.

## 오류 처리와 rollback

- batch prepare 실패: Trino data mutation 없이 task 실패
- batch DML/verify 실패: batch FAILED manifest, pending 보존, Airflow retry
- asset callback 실패: pending 보존, 다음 cron에서 verified recovery
- OOM 또는 Iceberg conflict 증가: root hard concurrency를 1로 되돌리고 두 Airflow
  pool은 유지해 논리 lane만 직렬화
- backlog가 줄지 않으면 query duration과 Iceberg commit 수를 다시 측정하고 batch SQL을
  세분화한다.

## 검증

1. batch writer가 여러 receipt를 audit/Bronze 각각 한 DELETE와 한 INSERT로 기록한다.
2. 모든 payload가 준비되기 전에는 cursor를 열거나 mutation하지 않는다.
3. partial evidence는 verified로 인정하지 않고 재적재한다.
4. cron-only schedule과 pool routing을 DAG unit test로 고정한다.
5. Traffic 전체 test, compile, Airflow 3 import를 통과한다.
6. 최신 dev 재배포 후 pending age, materializer duration, pool utilization, Trino memory,
   Incident→Flow→Silver→Gold 새 cycle을 확인한다.
