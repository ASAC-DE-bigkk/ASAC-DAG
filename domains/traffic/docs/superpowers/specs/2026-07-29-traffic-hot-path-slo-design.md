# Traffic 15분 주기 hot path 단축 설계

- 상태: 확정
- 기준 revision: ASAC-DAG `98902de`, ASAC-DBT `2e07dd0`
- 대상: Traffic 도메인의 활성 수집·변환·서빙 파이프라인
- 배제: Weather와 그 밖의 모든 도메인 코드·데이터·검증
- 배포 목표: `dev` 병합 후 최신 revision 재배포와 실제 Traffic asset 1 cycle 검증

## 1. 목적

Traffic의 5분 수집 주기와 15분 end-to-end 목표를 지키면서도 데이터 정합성, 멱등성,
asset ordering과 last-known-good publication 계약을 유지한다. 매 cycle마다 전체 계약을
반복 실행하던 구조를 다음 두 lane으로 분리한다.

1. **hot path**: 현재 publication을 안전하게 승인하는 최소 계약만 실행한다.
2. **daily assurance**: 전체 Bronze/Silver/Gold 계약을 매일 09:00 신뢰성 감사에서 실행한다.

계약을 삭제하거나 실패를 숨기지 않는다. 전체 계약의 실행 위치와 실패 영향만 분리한다.

## 2. 확인된 병목

최근 실제 asset run `asset_triggered__2026-07-29T11:33:45.180187+00:00_kDsvqarq`의
`traffic_incident_transform`은 약 585초가 걸렸다.

- source freshness: 약 79초
- Bronze 전체 preflight 71 tests: 약 197초
- Silver 단일 build: 약 250초
- 그 밖의 admission/publish/metric: 약 60초

최근 24시간 성공 run의 p95는 Incident Transform 약 1,853초, Gold Transform 약
1,486초였다. Gold는 매 cycle마다 seed, 공통 axis build/test, 162개 gate test와
Traffic schema 안의 cross-domain leaf model까지 함께 실행해 15분 주기 여유를 소진했다.

## 3. 성능 목표

steady-state의 정상 asset cycle에 다음 SLO를 적용한다.

| 단계 | 목표 |
|---|---:|
| Incident Landing | 30초 미만 |
| Incident / Flow Bronze | 각각 4분 미만 |
| Incident / Flow Transform | 각각 3분 미만 |
| Traffic core Gold | 5분 미만 |
| D1 serving export | 2분 미만 |
| Traffic end-to-end | 15분 미만 |

backlog 복구와 명시적 recovery run은 steady-state SLO에서 분리해 기록한다. 초과 즉시
running task를 강제 종료하지 않고 09:00 리포트에서 RED로 표시해 데이터 손실 위험을
피한다.

## 4. dual-lane 계약

### 4.1 hot path publication receipt

hot path의 hard gate는 pinned asset identity에 대해 다음을 한 번에 확인한다.

- 원본 Bronze manifest가 `SUCCESS`이고 publishable이다.
- run id, source id, payload hash, row count가 pinned asset metadata와 일치한다.
- 최신 허용 시각과 ordering fence를 통과한다.
- Silver/Gold 출력에 필수 key, event time, source-native id의 null/중복이 없다.
- pinned 입력의 missing, extra, mixed-lineage row가 없다.

동일 테이블을 여러 generic test가 반복 스캔하지 않도록 단계별 compound singular
contract 하나로 묶는다. 모델 materialization과 contract를 동일 `dbt build` selector에서
실행해, 모델 성공만으로 publication이 승인되는 틈을 만들지 않는다.

receipt는 `(dag_id, source asset run id, model contract version)`을 identity로 갖는다.
동일 identity의 성공 receipt는 재실행 시 O(1) admission으로 heavy phase를 skip한다.
실패 또는 불완전 receipt는 성공으로 간주하지 않는다.

### 4.2 09:00 daily assurance

`traffic_bronze_reliability_report`에 Traffic 전용 전체 dbt audit를 추가한다.

- 기존 Incident Bronze 71 tests
- Incident/Flow Silver 전체 계약
- Traffic Gold 전체 계약과 static axis 계약
- hot path receipt와 최신 published row count의 교차 검증
- 단계별 runtime과 SLO 초과 내역

감사 계약 실패, Trino 오류, artifact 누락은 모두 리포트 RED 사유다. 리포트 생성과
전달은 계속 실행해 실패가 운영자에게 보이게 한다. daily audit RED는 이미 검증된
last-known-good serving snapshot을 폐기하거나 다음 hot path를 자동 차단하지 않는다.

## 5. DAG별 변경

### 5.1 Incident Transform

현재 `source freshness -> 71-test preflight -> dbt build`를 다음으로 축소한다.

```text
asset admission
  -> O(1) manifest/identity/freshness fence
  -> Incident Silver model + compound publication contract 단일 dbt build
  -> receipt publish
```

기존 latest-only admission, pinned Bronze run, fail-closed publish, lineage 기록은 유지한다.
수동 transform run은 Bronze asset event가 없으면 계속 실패해야 한다.

### 5.2 Flow Transform

Incident parent와 Flow snapshot pair의 기존 pin 계약을 유지한다. 모델과 critical
publication contract를 한 번의 `dbt build`로 실행하고, 별도 전체 test scan은 daily
assurance로 이동한다. pair가 불완전하거나 다른 lineage가 섞이면 publication하지 않는다.

### 5.3 Gold Transform

매 cycle의 seed, 공통 admin axis rebuild/test를 제거하고 이미 성공한 axis
version/receipt를 O(1)로 pin한다. hot selector에는 여섯 Traffic serving product에 필요한
Traffic core model과 그 critical publication contract만 포함한다.

hot selector는 여섯 Traffic D1 제품을 직접 생산하는 Traffic 소유 model로 한정한다.
이 중 `gold_traffic_incident_x_weather_current_hourly`는 이미 발행된 Weather Gold를
읽지만 Weather DAG나 model을 실행·수정하지 않는다. 여섯 제품과 무관한 Citydata,
Culture, Transit, Commerce cross-domain leaf는 hot selector와 이번 1 cycle 검증에서
제외한다. 기존 incident-only/flow-present trigger-aware selector, incremental MERGE,
pin revalidation과 fail-closed publish는 유지한다.

### 5.4 Bronze와 Landing

Landing 데이터 경로는 바꾸지 않는다. Bronze는 raw snapshot을 버리거나 latest-only로
축약하지 않는다. 이미 materialized된 receipt는 strict evidence 확인 후 O(1) skip하고,
미처리 receipt만 기존 set-based batch에 포함한다. Flow에도 동일한 verified receipt
admission을 적용한다. API 재호출로 backlog를 해결하지 않는다.

### 5.5 D1 export

D1 publisher의 byte-budget batch, staging, gate, `_catalog`, last-known-good 교체 계약은
그대로 유지한다. Gold publication receipt가 없는 run은 export하지 않는다.

## 6. 실패 처리와 관측

- manifest, pin, critical contract 중 하나라도 실패하면 해당 publication task를 실패시킨다.
- 성공 receipt가 없으면 downstream asset과 D1 export를 발행하지 않는다.
- 각 run에 admission/build/contract/publish duration과 skip 사유를 기록한다.
- 신뢰성 리포트는 steady-state run의 단계별 p50/p95와 SLO 초과 run id를 포함한다.
- 429, Trino OOM, queue wait는 모델 실행 시간과 분리해 원인을 구분한다.
- backlog/recovery는 별도 표기로 남겨 정상 주기 p95를 왜곡하지 않는다.

## 7. 구현 경계

- ASAC-DAG: `domains/traffic/**`만 수정한다.
- ASAC-DBT: `domains/traffic_weather/**` 중 Traffic selector/test만 수정한다.
- 다른 도메인 DAG, model, test, source는 수정·실행·검증하지 않는다.
- root `.airflowignore`, 개인 `AGENTS.md`, `LessonRun.md`,
  `engineering-decision-log.md`는 커밋하지 않는다.
- 이슈는 사용자 지시에 따라 생략하고, DAGS와 DBT 각각 `dev` 대상 PR을 만든다.

## 8. 검증과 완료 조건

### 8.1 정적·회귀 검증

- 변경 전 실패하는 unit/selector test를 먼저 추가한다.
- Traffic DAG 전체 pytest와 Python compile을 통과한다.
- Traffic DBT contract test, `dbt parse`, `dbt ls` exact node-set을 통과한다.
- hot selector에는 Traffic core node와 critical contract만 포함되어야 한다.
- daily selector는 hot path에서 이동한 기존 전체 계약을 빠짐없이 포함해야 한다.
- 다른 도메인 파일이 PR diff에 없음을 확인한다.

### 8.2 병합·재배포

DBT PR을 먼저 병합하고 DAGS PR을 병합한다. clean local dev harness에서 두 repo를 최신
`origin/dev` merge commit으로 맞춘 뒤 기존 dev override를 사용해 Airflow를 재배포한다.
root submodule pointer PR과 `scripts/deploy.sh`는 사용하지 않는다.

### 8.3 실제 Traffic asset 1 cycle

재배포 후 scheduler가 만든 실제 Traffic asset event로 다음 한 cycle을 검증한다.
필요한 수동 trigger는 반드시 `scripts/safe-trigger-dag.sh`를 거치며, transform DAG를
asset event 없이 직접 trigger하지 않는다.

```text
Traffic Landing
  -> Incident/Flow Bronze
  -> Incident/Flow Transform
  -> Traffic core Gold
  -> Traffic D1 serving export
```

각 DAG run id, task 성공 여부, 단계별 duration, published row count, D1 `_catalog`와
last-known-good gate를 기록한다. 한 cycle이 각 단계 budget과 15분 end-to-end 목표를
충족하면 배포 검증을 완료한다. p95 달성은 한 cycle로 확정하지 않고 이후 09:00
리포트의 누적 표본으로 판정한다.

## 9. 롤백

계약 누락, row mismatch, publication 오류 또는 성능 회귀가 확인되면 DAGS/DBT를 직전
`origin/dev` merge commit으로 되돌려 재배포한다. D1 last-known-good snapshot과 Bronze
raw/history는 변경 전후 모두 보존되므로 publication되지 않은 실패 run은 serving에
노출되지 않는다.
