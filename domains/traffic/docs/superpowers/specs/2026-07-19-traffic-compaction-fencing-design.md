# Traffic Silver compaction fencing 및 transform admission 설계

## 1. 목표

Traffic transform의 반복 실패를 일으킨 외부 Iceberg rewrite 경쟁을 차단하고,
동일 snapshot을 처리하는 asset-triggered run이 Trino 1-slot을 반복 소비하지 않도록 한다.
기존 `source_record_id` canonical grain, publishability, pinned snapshot, 단일 MERGE,
fail-closed preflight와 full-history test 의미는 그대로 유지한다.

## 2. 확인된 장애

- `silver_seoul_traffic_incident`의 Trino MERGE 직후에는 신규 lineage만 남아 있었다.
- 26초 뒤 Airflow와 Trino query history 밖에서 `replace` snapshot이 생성됐다.
- 새 data file 이름은 R2 managed compaction 규칙인 `compacted-` prefix였다.
- rewrite 이후 구 lineage 7행이 복원되어 7개 `source_record_id`가 각각 2행이 됐다.
- 첫 run은 post-MERGE dbt test에서 중복을 발견했고, 이후 run은 target preflight에서
  `null or duplicate source_record_id`로 fail-closed 했다.
- Bronze와 request audit에는 같은 중복이 없으므로 오염 범위는 Traffic Silver다.

`trino_traffic_heavy` 1-slot은 중복을 만들지 않았다. 다만 snapshot pin 전에 여러
Trino gate를 직렬 실행하고 동일한 최신 snapshot을 여러 asset run이 다시 처리해,
실패 run과 후속 run의 queue를 키웠다.

## 3. 절대 범위

- 실행·수정·검증 대상은 dev의 Weather와 Traffic만 허용한다.
- 이번 소스 변경은 `ASAC-DAG/domains/traffic/**`와
  `ASAC-DAG/domains/weather/weather_iceberg_maintenance.py`의 Traffic table pool routing만
  허용한다.
- Commerce, Citydata, Transit, Culture 파일은 수정·생성·테스트·탐색하지 않는다.
- 기존 Traffic Gold가 계약상 소비하는 Citydata snapshot ID는 읽기 전용 외부 입력으로만
  유지하며, Citydata DAG·모델·테이블은 변경하거나 재실행하지 않는다.
- prod, shared schema, full refresh, 광범위 backfill은 금지한다.
- `.env`, secret, API key, webhook 값은 읽거나 출력하지 않는다.
- W2, recovery, backfill, maintenance DAG는 pause 상태를 유지한다.
- 새 GitHub issue는 만들지 않는다. PR base는 `dev`다.

## 4. 보존할 계약

1. Silver canonical key는 `source_record_id`다.
2. Silver 입력은 pinned Incident Bronze snapshot의 publishable row만 허용한다.
3. stale lineage retraction과 upsert는 기존 단일 Trino MERGE로 유지한다.
4. target/temp null·duplicate preflight를 완화하거나 삭제하지 않는다.
5. Gold exact-set reconciliation, idempotency, event/ingest time 의미를 변경하지 않는다.
6. full-history test를 최근 window, tolerance, skip으로 축소하지 않는다.
7. transform 실패는 기존 failure callback을 통해 즉시 Discord에 알린다.
8. Traffic/Weather 일일 reliability는 09:00 KST 1회 계약을 유지한다.

## 5. 검토한 접근

### A. 외부 compaction 차단 + Airflow 소유 writer fence + early admission

선택한다. 외부 writer를 제거해 정합성 원인을 차단하고, 현재 OOM 보호 한도를 유지하면서
불필요한 run을 admission 단계에서 종료한다.

### B. `trino_traffic_heavy`와 Trino 동시 실행 수만 증가

선택하지 않는다. R2 managed compaction은 Airflow pool 밖에서 실행되므로 정합성 문제를
해결하지 못한다. root `dev`의 `hardConcurrencyLimit=1`은 OOM 방지를 위해 도입됐으므로,
메모리 실측 없이 2 이상으로 올리지 않는다.

### C. preflight 완화, arbitrary dedup, full refresh 또는 COW 전체 rewrite

선택하지 않는다. 오염을 숨기거나 원자성·full-history 의미를 낮춘다. Cloudflare catalog의
rename/replace 제약과 local memory 비용 때문에 매 run 전체 table rewrite도 기본 경로로
사용하지 않는다.

## 6. 선택 설계

### 6.1 R2 control-plane containment

Cloudflare Dashboard의 기존 로그인 세션을 사용해 dev catalog의
`silver_seoul_traffic_incident`만 automatic compaction을 비활성화한다. catalog 전체 설정,
다른 domain table, snapshot expiration 설정은 변경하지 않는다. 설정 변경 전후 상태만
기록하고 token이나 credential은 조회하지 않는다.

R2 automatic compaction은 vendor fix와 position-delete 동시성 재현 검증이 끝날 때까지
비활성 상태를 유지한다. 기존 `compacted-` file은 설정 변경만으로 삭제하지 않는다.

### 6.2 현재 7개 stale row 복구

복구 직전에 다음 조건을 다시 확인한다.

- duplicate key가 정확히 기존 7개인가
- 각 key가 구 lineage 1행과 현재 canonical lineage 1행으로만 구성되는가
- Bronze의 해당 두 run은 각각 unique key인가
- 구 lineage가 latest publishable manifest가 아닌가
- R2 automatic compaction이 대상 table에서 비활성인가

모든 조건이 맞을 때만 구 lineage와 7개 key의 exact conjunction을 조건으로 단일
`DELETE` commit을 실행한다. 신규 canonical row는 이미 존재하므로 delete+insert 두 단계로
만들지 않는다. 조건이 달라졌으면 자동 일반화하지 않고 복구를 중단한다.

복구 후 duplicate 0, latest-publishable mismatch 0, non-publishable lineage 0, canonical row
count와 current Iceberg snapshot ID를 기록한다. local maintenance는 실행하지 않는다.

### 6.3 Traffic maintenance writer fencing

#419가 만든 table × operation checkpoint DAG 구조는 유지한다. 변경은 pool routing에만 둔다.

- Traffic table의 `optimize`, `expire_snapshots`, `remove_orphan_files` task는
  `trino_traffic_heavy`를 사용한다.
- Weather table task는 기존 `trino_weather_heavy`를 유지한다.
- `silver_seoul_traffic_incident` maintenance와 Traffic transform MERGE는 같은 1-slot을
  사용하므로 동시에 실행되지 않는다.
- canonical allowlist, immutable plan hash, circuit breaker, per-operation fingerprint와
  partial-effect 판정은 #419 구현을 그대로 보존한다.

maintenance DAG는 이번 PR 검증에서도 pause 상태를 유지하며 실제 run하지 않는다.

### 6.4 Transform DAG 책임 분리

기존 `traffic_incident_transform`은 Incident Silver 책임만 남기고 DAG ID는 유지한다.
새 `traffic_gold_transform`은 Gold와 외부 snapshot 결합만 담당한다.

#### `traffic_incident_transform`

1. dev runtime 검증
2. test tier 선택
3. Incident snapshot resolve 및 pin
4. 동일 Incident snapshot의 성공 marker와 Silver canonical health를 확인하는 admission
5. source freshness·availability·pinned Bronze contract
6. Silver run
7. Silver targeted/full tier test
8. Silver Asset 발행
9. 성공 marker와 metrics 기록

Incident Bronze Asset만 이 DAG를 trigger한다. Flow 또는 Citydata snapshot을 읽지 않는다.

#### `traffic_gold_transform`

1. dev runtime 검증
2. test tier 선택
3. Silver Asset과 compatible Flow snapshot을 pin
4. 기존 read-only Citydata crowding snapshot ID를 pin
5. 동일 `(incident, flow, citydata)` 성공 marker admission
6. 필요한 seed/common dimension phase
7. Gold run과 tier별 Gold test
8. 성공 marker와 metrics 기록

Silver Asset 또는 compatible Flow Bronze Asset이 이 DAG를 trigger한다. 외부 Citydata는 기존
Gold contract를 위한 snapshot ID만 읽고 Citydata 파일·DAG·table은 변경하지 않는다.

두 DAG 모두 `max_active_runs=1`과 기존 classified failure callback을 유지한다. dbt selector,
model, macro는 변경하지 않는다.

### 6.5 Early admission과 성공 marker

snapshot resolve는 `dbt deps`, source freshness, seed, dimension보다 먼저 수행한다. 성공 marker는
Airflow Variable에 versioned JSON으로 저장하며 다음 identity를 포함한다.

- Silver: Incident Bronze run ID
- Gold: Incident run ID, optional Flow run ID, Citydata snapshot ID

marker는 해당 DAG의 모든 write/test가 성공한 뒤에만 갱신한다. 같은 identity가 다시 들어오면
cheap canonical-health check를 먼저 수행한다. health가 정상일 때만 downstream task를 skip한다.
health가 비정상이거나 marker가 없으면 기존 idempotent reconciliation을 실행한다. Airflow metadata
초기화로 marker가 사라져도 한 번 더 멱등 실행될 뿐 데이터 유실은 만들지 않는다.

### 6.6 Silver post-commit snapshot fence

Traffic 전용 pure module이 Iceberg `$snapshots`와 `$files` 결과를 다음 구조로 검증한다.

- current snapshot ID
- committed_at
- operation
- current `compacted-` data file path의 정렬된 fingerprint

Silver run task는 dbt 실행 직전에 baseline evidence를 잡고 MERGE 완료 직후 다시 읽는다. Silver test
task는 dbt test 전후로 current evidence를 읽는다. 기존 `compacted-` file은 baseline에 포함해 허용하되,
baseline 이후 예상하지 않은 `replace`가 개입하거나 신규 managed-compaction file path가 나타나면
generic dbt failure 대신 `EXTERNAL_COMPACTION_RACE`로 fail-closed 한다. 기존 dbt duplicate와
publishability test도 계속 실행하므로 fence가 correctness test를 대체하지 않는다. fence 수집은
기존 Silver run/test task 내부 adapter에서 수행해 별도 Airflow task 수를 늘리지 않는다.

### 6.7 Trino capacity

이번 PR은 다음 값을 유지한다.

- Airflow `trino_traffic_heavy`: 1 slot
- Trino root resource group `hardConcurrencyLimit`: 1
- existing query memory cap과 spill 설정

병목은 slot 증가가 아니라 early admission, Silver/Gold 분리, static prerequisite의 tier cadence로
먼저 줄인다. 배포 후 24시간 동안 queue, duration, peak memory, skipped duplicate identity를 기록한 뒤
동시 실행 확대 여부를 별도 판단한다.

## 7. 오류 처리

- external compaction 설정을 확인할 수 없으면 data repair를 실행하지 않는다.
- duplicate 집합이나 lineage가 예상과 다르면 repair를 실행하지 않는다.
- marker가 malformed면 skip하지 않고 fail-closed 한다.
- snapshot fence telemetry가 없으면 성공으로 간주하지 않는다.
- Silver 실패는 Gold Asset을 발행하지 않으므로 Gold DAG가 시작되지 않는다.
- Gold 실패는 Silver 성공을 되돌리지 않으며 같은 pinned tuple로 독립 재시도한다.

## 8. 테스트 및 검증

### 소스 검증

- Traffic snapshot resolver/admission pure unit test
- Silver/Gold DAG task graph·schedule·pool·failure callback test
- Traffic maintenance table별 pool routing test
- snapshot fence normal/replace/compacted-file/malformed telemetry test
- 기존 Traffic transform, Bronze, reliability와 #419 maintenance regression test
- Python compile과 Airflow DAG import
- `.airflowignore` Weather/Traffic allowlist 확인

### dev 데이터 검증

1. R2 대상 table automatic compaction disabled 확인
2. exact 7-row stale lineage repair
3. Traffic Silver duplicate/publishability/latest snapshot targeted query
4. dbt parse/compile
5. Traffic Silver targeted test
6. Incident Silver DAG 작은 asset checkpoint
7. Gold DAG 동일 pinned tuple checkpoint
8. 최종 Silver/Gold row count와 lineage query
9. maintenance/W2/recovery/backfill pause 확인

Commerce, Citydata, Transit, Culture DAG/model은 parse·run·test하지 않는다.

## 9. Git 및 배포

- ASAC-DAG `feat/traffic-compaction-fencing` 한 브랜치에서 구현한다.
- ASAC-DBT와 root source 변경이 필요하다는 증거가 생기면 범위를 자동 확장하지 않고 보고한다.
- PR은 `dev` base로 생성하고 issue는 연결하지 않는다.
- PR check와 Weather/Traffic targeted regression이 통과하고 다른 domain 파일 diff가 0일 때만 merge한다.
- merge 후 clean local deployment worktree에서 DAGS와 DBT를 각각 최신 `origin/dev` exact revision으로
  맞추고 `docker compose up -d --build`로 재배포한다.
- 재배포 후 Weather/Traffic allowlist, DAG import, pool, paused maintenance/recovery, Traffic Silver/Gold
  smoke를 확인한다.

## 10. 완료 기준

- R2 managed compaction이 대상 Traffic Silver table에서 비활성이다.
- Traffic Silver duplicate와 stale non-publishable lineage가 0이다.
- local maintenance가 Traffic transform과 같은 writer fence를 사용한다.
- 동일 snapshot asset run이 heavy phase를 반복하지 않는다.
- Silver와 Gold가 별도 DAG로 실행되고 실패 알림·pinned lineage 계약을 유지한다.
- 기존 dbt MERGE, full-history correctness, idempotency를 낮추지 않는다.
- 금지 domain 파일 diff·parse·run이 0이다.
- PR merge 후 최신 dev revision으로 local 재배포와 targeted smoke가 완료된다.
