# Traffic Gold full-history reconciliation 실행 fence 설계

- 상태: 승인됨
- 기준 이슈: [ASAC-DAG #420](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/420)
- 기준 ref: ASAC-DAG `3003724bec4dde0871095168a2b26f7c72917406`, ASAC-DBT `b20d4bd50fc962cf378fe5a40e66f62bcee15a6e`
- 대상 환경: dev

## 1. 문제와 실행 증거

`traffic_incident_transform`의 dev smoke run
`asset_triggered__2026-07-17T16:17:00.545635+00:00_JM25jivE`에서 다음 결과를 확인했다.

- snapshot resolver, Silver run, Silver test가 모두 성공했다.
- Gold run은 약 298초 후 성공했고 OOM이 발생하지 않았다.
- Gold test 171개 중 170개가 통과했다.
- `assert_gold_traffic_incident_collection_coverage_5m_reconciles`만 2행 차이로 실패했다.

실패 시각은 다음 순서였다.

```text
17:14:37.770810 UTC  dbt_run_gold 종료
17:15:39.388182 UTC  Incident Bronze materialization 시작
17:17:38.512923 UTC  Incident Bronze materialization 종료
17:17:38.552417 UTC  dbt_test_gold 시작
```

즉 Gold 모델이 읽은 Bronze와 Gold test가 읽은 Bronze 사이에 새 snapshot이 반영됐다.
실패한 두 행은 이 변경으로 생긴 missing/extra aggregate 한 쌍이다. 이는 Gold SQL의 OOM,
모델 실행 실패, selector 누락이 아니라 `dbt_run_gold`와 `dbt_test_gold` 사이의 source race다.

## 2. 보존해야 하는 계약

ASAC-DBT `ff51ae2`에서 Gold coverage 모델과 exact reconciliation test를 함께 도입했다.
이 테스트는 현재 retained Bronze 전체와 materialized Gold 전체가 일치하는지 검증한다.

이번 변경은 다음 계약을 낮추지 않는다.

- full-history 범위를 최근 시간대나 특정 snapshot으로 축소하지 않는다.
- missing/extra row 허용치나 시간 지연 허용치를 추가하지 않는다.
- Gold canonical grain과 dedup 기준을 변경하지 않는다.
- event time, ingest time, snapshot identity를 재해석하지 않는다.
- Gold run과 Gold test를 한 Airflow task로 합치지 않는다.
- 재실행 시 같은 입력에 대해 같은 결과를 만드는 idempotency 계약을 유지한다.

## 3. 선택한 설계

ASAC-DAG만 변경한다. `DBT_PHASE_SPECS`에서 `dbt_test_gold`를
`pin_critical=True`로 지정한다.

모든 Traffic transform dbt task는 기존처럼 다음 실행 자원을 공유한다.

- Airflow pool: `trino_heavy`
- pool slot: 1
- `weight_rule`: `absolute`

우선순위는 다음처럼 유지·변경한다.

| task | priority | 결정 |
| --- | ---: | --- |
| snapshot resolver | 10 | 기존 유지 |
| `dbt_run_silver` | 10 | 기존 유지 |
| `dbt_test_silver` | 10 | 기존 유지 |
| `dbt_run_gold` | 1 | 기존 유지 |
| `dbt_test_gold` | 10 | 이번 변경 |
| Incident/Flow Bronze materialization | 1 | 기존 유지 |

최종 실행 순서는 다음과 같다.

```text
preflight(priority 1)
  -> snapshot resolver(priority 10)
  -> Silver run(priority 10)
  -> Silver test(priority 10)
  -> Gold run(priority 1)
  -> Gold test(priority 10)
  -> metrics
```

Gold run이 `trino_heavy` slot을 반납하면 Gold test와 대기 중인 Bronze가 함께 실행 가능해진다.
Airflow scheduler는 같은 pool의 실행 가능 task를 absolute priority로 정렬하므로 Gold test를
먼저 선택한다. Gold test가 slot을 점유하는 동안 Incident/Flow Bronze materialization은 시작할
수 없다. 따라서 Gold run 직후의 source 상태가 Bronze write에 의해 test 전에 바뀌는 구간을
닫는다.

Gold run 자체는 priority 1로 둔다. Gold rebuild가 긴 동안 이미 대기하던 Bronze까지 계속
선점하지 않게 하고, exact validation에 필요한 짧은 구간만 fence로 보호하기 위해서다.
Gold run이 slot을 획득하기 전에는 다른 priority 1 task와 기존 방식대로 경쟁한다.

## 4. 지연과 운영 영향

실측 Gold test 실행 시간은 약 147초였다. 따라서 Gold test와 동시에 실행 가능해진 Bronze는
통상 약 2~3분 추가 대기할 수 있다. 사용자는 정확한 full-history reconciliation을 위해 이
지연을 허용했다.

- Traffic Incident raw landing은 `trino_heavy`를 사용하지 않으므로 계속 수집된다.
- pending receipt와 materializer 계약은 바뀌지 않으므로 raw 유실을 만들지 않는다.
- 추가 대기는 Gold test 구간에만 한정된다.
- `trino_heavy`를 공유하는 Weather의 priority 1 task도 이 구간에는 최대 Gold test 실행 시간만큼
  대기할 수 있다. Weather 코드·schedule·데이터 계약은 변경하지 않는다.
- daily 09:00 KST reliability report와 24시간 lookback 계약은 변경하지 않는다.
- 15분 주기는 Traffic Bronze pending 복구 fallback이며 reliability 알림 주기로 되돌리지 않는다.

## 5. 실패와 재실행

- Gold test 실패는 기존 task failure로 남고 Weather/Traffic 공용 failure callback을 통해 즉시
  Discord 알림 경로를 탄다.
- test SQL이나 실패 판정은 바꾸지 않는다.
- Gold test가 진짜 mismatch를 발견하면 Bronze는 test 종료 후 다시 진행한다.
- Gold run 이후 Bronze가 이미 변경된 상태에서 Gold test만 수동 재시도하면 exact test가
  실패하는 것이 올바르다. 이 경우 `dbt_run_gold`부터 downstream을 다시 실행한다.
- Gold run 또는 Gold test가 실패해도 pool slot은 해제되므로 raw/Bronze 수집을 영구 차단하지
  않는다.

## 6. 검증 전략

### 정적·단위 검증

1. 테스트를 먼저 변경해 `dbt_test_gold`만 새 critical priority를 요구하도록 한다.
2. 변경 전 테스트가 실패하는 RED를 확인한다.
3. `DBT_PHASE_SPECS`의 최소 변경으로 GREEN을 만든다.
4. Weather/Traffic 전체 Python test, Ruff, compileall을 통과시킨다.
5. 변경 파일이 `domains/traffic/**`에만 있는지 확인한다.
6. Airflow DagBag import와 Weather/Traffic allowlist를 다시 확인한다.

### dev runtime 검증

1. DAGS/DBT 최신 `origin/dev` exact SHA로 clean deploy root를 맞춘다.
2. `docker compose up -d --build`로 dev 환경만 재배포한다.
3. `traffic_incident_transform`만 unpause하고 새 Asset-triggered run을 관찰한다.
4. Gold run 종료 시 대기 중인 priority 1 Bronze보다 `dbt_test_gold`가 먼저 시작하는지 확인한다.
5. Gold test 실행 중 Incident/Flow Bronze materialization이 시작되지 않는지 확인한다.
6. Gold run과 Gold test가 성공하고 Gold test 171/171이 통과하는지 확인한다.
7. run ID, task 상태·시간, row count, 영향 table을 `LessonRun.md`에 기록한다.

## 7. 대안과 기각 이유

### Gold run과 test를 한 task로 결합

하나의 task가 pool slot을 계속 점유하므로 fence는 강하지만, 모델 build와 validation의 독립
재시도·실패 가시성·artifact 계약을 잃는다. 기존 task 분리 의도와 맞지 않아 채택하지 않는다.

### DBT에 Iceberg snapshot 또는 watermark 계약 추가

run/test가 같은 source snapshot을 명시적으로 읽게 할 수 있지만 ASAC-DBT source contract와
모든 관련 테스트를 바꾸는 큰 작업이다. 현재 race는 기존 Airflow pool 안에서 해결 가능하므로
이번 범위에는 포함하지 않는다.

### exact test 축소 또는 삭제

최근 snapshot만 검사하거나 tolerance를 두면 현재 retained history와 Gold의 불일치를 놓친다.
`ff51ae2`의 full-history correctness 의도를 깨므로 금지한다.

## 8. 범위와 비목표

production 변경은 `domains/traffic/**`에만 둔다. ASAC-DBT 모델·test·selector는 변경하지 않는다.

다음은 이번 작업의 비목표다.

- Weather transform 로직 변경
- Commerce, 시티데이터, Transit, 문화정보 파이프라인 탐색·실행·수정
- W2 recovery, Weather OOM, Iceberg maintenance 재개 또는 수정
- prod·공유 schema 실행
- full refresh 또는 대량 backfill
- source watermark 아키텍처 재설계

## 9. 완료 조건

- `dbt_test_gold`만 priority 10 fence를 갖고 `dbt_run_gold`는 priority 1을 유지한다.
- exact full-history DBT test는 변경 없이 유지된다.
- Weather/Traffic 전체 회귀 테스트와 DAG import가 통과한다.
- dev smoke에서 Gold run과 test 사이에 Bronze write가 끼지 않는다.
- Gold test 171/171과 Traffic transform DAG run이 성공한다.
- forbidden domain pipeline은 parser와 실행 대상에 포함되지 않는다.
- PR 본문에 `Closes #420`을 넣고 merge 직후 issue가 실제 `closed`인지 확인한다.

## 10. 이슈 수명주기

이번 변경을 위해 새 issue를 만들지 않는다. 현재 작업 브랜치와 PR은 기존 ASAC-DAG #420을
사용하며, merge와 동시에 자동 종료되도록 연결한다. 자동 종료가 동작하지 않으면 merge SHA와
검증 결과를 남기고 `completed` 사유로 직접 닫는다.

현재 open인 ASAC-DAG #419는 maintenance partial/OOM 재실행 설계가 미해결이고,
ASAC-DBT #117은 지연·백필 Silver repair 계약이 미구현이므로 이번 PR에서 닫지 않는다.
향후 각 issue의 acceptance criteria를 충족한 PR은 `Closes #...`로 연결하고 merge 직후 상태를
확인한다.
