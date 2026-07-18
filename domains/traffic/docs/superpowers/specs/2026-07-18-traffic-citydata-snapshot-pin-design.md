# Traffic Citydata Iceberg snapshot pin 설계

- 상태: 승인됨
- 기준 ref: ASAC-DAG `5b0a574b8210bd3c3749034fde928a81679c6b1a`, ASAC-DBT `b20d4bd50fc962cf378fe5a40e66f62bcee15a6e`
- 대상 환경: dev
- 대상 파이프라인: `traffic_incident_transform`의 Traffic 소유 cross-domain Gold

## 1. 문제와 실행 증거

dev run `asset_triggered__2026-07-17T18:56:20.191180+00:00_gNQVHjQa`에서 Gold build는 성공했지만 Gold test 171개 중
`assert_gold_traffic_incident_x_citydata_crowding_current_hourly_latest_per_area_reconciles` 하나가 38행 차이로 실패했다.

시간 순서는 다음과 같다.

```text
19:51:44.691 UTC  Traffic Gold crowding model query 시작
19:52:02.425 UTC  외부 gold_citydata_ppltn_by_time 새 Iceberg snapshot commit
19:52:20.915 UTC  Traffic Gold crowding model query 종료
이후               reconciliation test가 새 snapshot을 읽어 38행 차이 검출
```

같은 test SQL에서 외부 source를 model 실행 당시 snapshot
`8738321387624398062`로 `FOR VERSION AS OF` 고정하면 차이는 0행이었다. 따라서 실패 원인은 Traffic Bronze pin, Gold SQL 계산,
full-history test 또는 OOM이 아니라 서로 다른 statement가 이동 중인 외부 Iceberg table의 서로 다른 snapshot을 읽은 것이다.

## 2. 보존할 기존 의도

- ASAC-DAG `1c92946`은 preflight 이후 publishable Traffic Incident/Flow run을 고정하고 pinned phase를 우선 실행한다.
- ASAC-DAG PR #424는 `dbt_run_gold`와 exact Gold test 사이에 local Bronze write가 끼지 않도록 1-slot priority fence를 둔다.
- ASAC-DBT `ba4f449`는 Traffic 소유 cross-domain Gold를 의도적으로 추가하고 장소별 최신 crowding reconciliation을 exact test로 고정했다.
- full-history 범위, latest-per-area tie-break, canonical Traffic grain, event/ingest time, null semantics는 축소하거나 완화하지 않는다.

이번 변경은 위 계약을 유지하며 외부 source statement snapshot만 run 단위로 고정한다.

## 3. 선택한 설계

### 3.1 DAGS: resolver가 외부 snapshot도 고정

기존 `resolve_traffic_snapshot_run`은 Incident/Flow Bronze pair를 검증한 직후 다음 metadata query로
`iceberg_dev.<SEOUL_CITYDATA_SCHEMA>.gold_citydata_ppltn_by_time`의 최신 snapshot ID를 읽는다.

```sql
SELECT snapshot_id
FROM <catalog>.<schema>."gold_citydata_ppltn_by_time$snapshots"
ORDER BY committed_at DESC, snapshot_id DESC
LIMIT 1
```

식별자는 기존 dev runtime과 identifier validator를 사용한다. query 연결 오류는 일반 예외로 남겨 기존 Airflow retry 1회를 허용한다.
metadata row가 없거나 snapshot ID가 양의 정수가 아니면 재시도로 복구되지 않는 계약 오류로 명시적으로 실패한다.

검증된 ID는 resolver task XCom key `traffic_citydata_crowding_snapshot_id`에 정수로 기록한다. resolver의 기존 string return은
Incident snapshot run ID로 유지해 downstream 및 failure watcher 계약을 깨지 않는다.

### 3.2 DAGS → DBT 변수 전달

resolver 이후 모든 pinned dbt phase는 기존 변수와 함께 다음 정수 변수를 받는다.

```json
{
  "traffic_snapshot_dag_run_id": "<incident run id>",
  "traffic_flow_snapshot_dag_run_id": "<optional flow run id>",
  "traffic_citydata_crowding_snapshot_id": 8738321387624398062
}
```

pinned phase에서 외부 snapshot XCom이 없거나 유효하지 않으면 dbt를 실행하기 전에 fail-closed한다. preflight phase는 새 변수를 요구하지 않아
`dbt deps`, source freshness와 availability gate의 기존 실행 순서를 유지한다.

### 3.3 DBT: 단일 fail-closed source macro

Traffic 전용 macro는 parse 단계에서 source dependency를 정상 등록한다. 실제 compile/run/test 단계에서는
`traffic_citydata_crowding_snapshot_id`가 양의 숫자인지 검증하고 아래 relation을 생성한다.

```sql
<traffic_citydata_gold.gold_citydata_ppltn_by_time relation>
FOR VERSION AS OF <traffic_citydata_crowding_snapshot_id>
```

Gold model과 exact latest-per-area reconciliation test는 직접 `source()`를 쓰지 않고 같은 macro를 사용한다. 변수 누락, 문자열 값,
0 이하 값은 compiler error로 실패하며 unversioned source로 조용히 fallback하지 않는다.

### 3.4 데이터 lineage

`gold_traffic_incident_x_citydata_crowding_current_hourly`의 모든 행에
`citydata_crowding_snapshot_id bigint`를 기록한다. 별도 singular test는 이 값이 run pin과 정확히 같은지 검증한다.
Traffic `product_row_id / admin_dong_code / hour_at` grain과 기존 crowding 집계 값은 바뀌지 않는다.

실패 recovery record에도 snapshot ID를 포함해 Discord 실패 알림과 재현 정보에서 Bronze run ID와 외부 source version을 함께 확인할 수 있게 한다.

## 4. 오류·재시도·idempotency

- external metadata query의 일시적 Trino/R2 Catalog 오류: resolver task의 기존 retry 1회 적용.
- snapshot metadata 없음 또는 잘못된 ID: fail-closed, Silver/Gold 미실행, 즉시 기존 실패 알림 경로 사용.
- pin 이후 새 Citydata commit: 현재 run에는 영향 없음. 다음 Traffic transform run이 새 snapshot을 선택한다.
- pin이 실행 중 expire된 경우: `FOR VERSION AS OF`가 명시적으로 실패하며 최신 source로 fallback하지 않는다.
- 같은 Incident/Flow/external pin으로 재시도하면 model과 test가 같은 relation version을 읽어 결과가 재현된다.

## 5. 검증

1. DAGS unit test에서 metadata SQL, XCom 정수 pin, 변수 전달, 누락·비정상 ID fail-closed를 RED→GREEN으로 검증한다.
2. DBT contract test에서 model과 exact test가 같은 macro를 사용하고 unversioned direct source가 남지 않음을 검증한다.
3. `dbt parse`는 일반 project parse로 통과하고, 대상 compile은 pin 누락 시 실패·유효 pin 사용 시 두 SQL 모두 같은
   `FOR VERSION AS OF`를 포함해야 한다.
4. targeted model/test를 dev에서 실행해 latest-per-area mismatch 0행과 lineage pin 일치를 확인한다.
5. Traffic transform 전체 Gold test 171개 이상이 통과하고 DAG run이 성공해야 한다.
6. merge 후 clean dev 재배포, `.airflowignore` Weather/Traffic allowlist, paused maintenance/W2/recovery를 재확인한 뒤
   `traffic_incident_transform`만 재활성화한다.

## 6. 배포·호환 순서

1. ASAC-DAG PR을 먼저 merge한다. 기존 DBT는 추가 `--vars` key를 무시하므로 중간 상태가 호환된다.
2. ASAC-DBT PR을 merge한다. Traffic transform은 두 merge가 배포될 때까지 pause 상태를 유지한다.
3. clean deploy root에서 두 `origin/dev` exact ref로 재배포하고 dev smoke를 수행한다.

## 7. 범위와 비목표

- production code 수정은 ASAC-DAG `domains/traffic/**`, ASAC-DBT `domains/traffic_weather/**`의 Traffic 소유 파일로 한정한다.
- 외부 Citydata DAG/model을 실행·수정·백필하지 않는다. Traffic Gold가 이미 소비하는 단일 Iceberg table의 metadata와 pinned data만 읽는다.
- Weather, Commerce, Transit, Culture 파이프라인은 실행·수정하지 않는다.
- maintenance, W2, recovery, backfill, full refresh를 실행하지 않는다.
- 09:00 KST reliability cadence와 실패 시 즉시 Discord 알림 계약은 변경하지 않는다.
- 새 GitHub issue는 만들지 않는다. ASAC-DAG #419와 ASAC-DBT #117은 별도 미완료 작업이므로 닫지 않는다.

## 8. 완료 조건

- 동일 Traffic transform run의 Gold model과 exact reconciliation test가 같은 external snapshot ID를 사용한다.
- model/test 사이 외부 commit이 있어도 latest-per-area reconciliation 결과가 0행이다.
- Gold row와 failure evidence에서 external snapshot lineage를 확인할 수 있다.
- Traffic Raw/Bronze 수집은 계속되고 Silver/Gold transform이 다시 unpaused되어 자동 Asset trigger로 성공한다.
- Weather/Traffic 외 도메인은 parser 실행 대상이나 runtime 검증 대상에 포함되지 않는다.
