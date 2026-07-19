# Traffic Flow Silver 소유권 분리 설계

## 1. 목표

`traffic_flow_bronze`가 발행한 publishable Flow Bronze를 별도 DAG가
`silver_seoul_traffic_flow`로 materialize한 뒤에만 Flow Gold 경로를 열도록 한다.
Incident Silver와 Gold의 기존 책임 분리, pinned run, canonical grain, incremental MERGE,
fail-closed pre-hook과 idempotency는 유지한다.

## 2. 확인된 근본 원인

- feature smoke의 Flow run
  `asset_triggered__2026-07-19T08:01:21.167998+00:00_h7PpERI9`은 manifest에서
  `SUCCESS`, `is_publishable=true`이고 Bronze에 6행이 있다.
- 같은 run ID의 `silver_seoul_traffic_flow`는 0행이다.
- 현재 Flow Silver의 마지막 materialization은 2026-07-15이며, 이후 Flow Bronze run은
  계속 존재한다.
- 분리 전 통합 `traffic_incident_transform`은 같은 dbt invocation에서 Silver를 먼저 실행한
  뒤 Gold를 실행했다.
- 분리 후 `traffic_incident_transform`은 Incident Bronze만 소비하고,
  `traffic_gold_transform`은 Flow Bronze를 직접 소비한다. 따라서 Flow Silver를 만드는
  실행 소유자가 사라졌다.

Flow XCom이 존재할 때 full Gold selector를 고르는 현재 routing은 Bronze publishability만
증명할 뿐, pinned Flow Silver materialization은 증명하지 못한다. Gold의
`traffic_flow_assert_pinned_incremental_rows()`는 이 상태를 의도대로 fail-closed 했다.

## 3. 선택한 구조

새 `traffic_flow_transform` DAG를 추가한다.

```text
Incident Bronze Asset
  -> traffic_incident_transform -> Incident Silver Asset
  -> traffic_flow_bronze         -> Flow Bronze Asset

Flow Bronze Asset AND matching Incident Silver Asset
  -> traffic_flow_transform
  -> silver_seoul_traffic_flow run/test
  -> Flow Silver Asset

Incident Silver Asset OR Flow Silver Asset
  -> traffic_gold_transform
```

`traffic_flow_bronze`는 기존처럼 API 호출, R2 raw, Bronze MERGE와 manifest publishability를
소유한다. 새 DAG는 API와 raw/Bronze를 다시 처리하지 않고 Flow Silver만 소유한다.
`traffic_gold_transform`은 Silver를 쓰지 않고 Gold만 소유한다.

## 4. Asset 계약과 순서

### 4.1 입력

`traffic_flow_transform`은 다음 두 Asset의 AND 조건으로 시작한다.

- `iceberg://traffic/flow/bronze`
- `iceberg://traffic/incident/silver`

resolver는 가장 최신 Incident Silver event를 고정하고, 그 `incident_run_id`와 정확히 같은
`parent_incident_run_id`를 가진 Flow Bronze event만 허용한다. Flow manifest도 같은
`flow_dag_run_id`가 publishable인지 다시 확인한다. 일치하는 pair가 없으면 fail-closed 한다.

### 4.2 출력

Flow Silver model과 전용 test가 모두 성공한 뒤에만
`iceberg://traffic/flow/silver` Asset을 발행한다. metadata는 다음 exact 필드를 가진다.

- `source_id=seoul_traffic_flow`
- `flow_run_id`
- `flow_dag_run_id`
- `parent_incident_run_id`
- timezone-aware `event_at`
- `is_publishable=true`
- `contract=traffic_flow_silver.v1`

Flow Silver가 0행이면 전용 pinned-row test가 실패하므로 Asset을 발행하지 않는다.

### 4.3 Gold

Gold DAG의 Flow 입력을 Flow Bronze Asset에서 Flow Silver Asset으로 교체한다.

- Incident Silver만 도착: incident/cross-domain Gold 17개와 대응 test 실행
- 같은 parent의 Flow Silver 도착: 기존 full Gold 21개와 대응 test 실행
- stale/incompatible Flow Silver: Flow identity를 `None`으로 유지

Gold resolver는 Flow Silver Asset을 신뢰하되 Bronze manifest publishability를 다시 검증하고,
기존 Flow Gold pinned-row pre-hook도 유지한다. 따라서 Asset 계약과 dbt pre-hook이 이중으로
materialization을 보호한다.

## 5. 재실행과 오류 처리

- Flow Silver는 `(link_id, dag_run_id)` incremental MERGE를 그대로 사용한다.
- Airflow task retry와 같은 pair 재실행은 같은 key를 MERGE하므로 중복을 만들지 않는다.
- Flow Silver Asset 재발행으로 같은 Gold tuple이 들어와도 기존 Gold success marker와
  admission이 heavy phase를 skip한다.
- Flow Silver DAG의 모든 task failure는 기존 Traffic classified failure callback과 즉시
  Discord 알림 경로를 사용한다.
- Flow Silver 전용 success marker는 추가하지 않는다. 고유 Flow run마다 실제 새 데이터를
  처리해야 하며, retry idempotency와 downstream Gold admission으로 필요한 중복 방지가 이미
  충족된다.

## 6. 검토한 대안

### Gold DAG 내부에서 Flow Silver 실행

변경량은 작지만 Gold DAG가 Silver write를 다시 소유해 책임 분리 의도와 사용자의 DAG 분리
요구를 깨므로 선택하지 않는다.

### Flow Silver 0행이면 incident-only로 강등

Gold 실패는 피하지만 Flow Silver를 생성하는 소유자가 계속 없으므로 Flow Gold가 영구히
갱신되지 않는다. 증상을 숨기므로 선택하지 않는다.

### Flow Bronze DAG에서 Silver까지 실행

수집·raw·Bronze와 dbt Silver lifecycle을 결합해 Bronze DAG 수행시간과 실패 범위를 다시
키우므로 선택하지 않는다.

## 7. 범위와 검증

- 코드 변경은 ASAC-DAG `domains/traffic/**`와 ASAC-DBT Traffic monoproject selector/test에만
  둔다.
- Commerce, Citydata, Transit, Culture 파일은 수정·생성하지 않는다. 기존 Gold의 source read만
  유지한다.
- dev만 사용하고 maintenance, W2, recovery, backfill은 pause 상태를 유지한다.
- `.env`, secret, `.omc`, `.omx`, `__pycache__`, `.pytest_cache`를 읽거나 stage하지 않는다.
- DBT exact selector set, DAG graph/schedule/Asset metadata, full Traffic regression, Airflow import,
  feature revision smoke를 통과한 뒤 draft PR 두 개를 ready로 전환해 `dev`에 merge한다.
- merge 뒤 두 submodule을 exact `origin/dev` revision으로 맞추고 세 compose file을 명시한
  dev 명령으로 재배포한다.
