# Traffic Transform Contract Gates Design

## Goal

교통 transform이 Silver를 시작하기 전에 traffic Bronze source generic test와 공용 행정동 축 seed test를 명시적으로 검증하고, selector drift 또는 gate 실패가 Silver·기존 Gold·canonical Gold로 전파되지 않도록 한다.

## Current constraints

- `dbt_source_freshness`와 `dbt_test_traffic_incident_availability`는 기존 운영 검증으로 유지한다.
- `dbt_seed_asac_axes`, `dbt_run_common_admin_dong_dimension`, `dbt_test_common_admin_dong_dimension`은 ASAC-DAG #333에서 이미 추가되어 있으므로 중복 생성하지 않는다.
- canonical Gold와 same-target fresh parse 계약은 ASAC-DAG #333의 현재 동작을 유지한다.
- cutoff capture, repair parameterization, full-refresh/backfill은 ASAC-DAG #168 및 ASAC-DBT #117 범위로 남긴다.
- 수정 파일은 `dags/domains/traffic/**` 아래에만 둔다.

## Design

### 1. Bounded Bronze source gate

새 `dbt_test_traffic_bronze_source_contract` task는 `test --select source:traffic_bronze`를 실행한다. 실행 직전에 같은 dbt target/profile/vars로 `dbt ls --resource-type test --select source:traffic_bronze --output json`을 호출하고, JSONL 결과를 `(resource, column, test_name)` tuple로 정규화한다.

승인 allowlist는 traffic source schema의 generic test 40개다. actual set이 exact set과 다르면 dbt test를 실행하지 않고 명시적 validation error로 실패한다.

### 2. Admin-axis seed gate

새 `dbt_test_asac_axes_seed_contract` task는 다음 세 seed만 선택한다.

```text
asac_axes.seoul_admin_dong_crosswalk
asac_axes.seoul_admin_dong_boundary
asac_axes.seoul_gu_boundary
```

같은 방식으로 `dbt ls` 결과를 정규화하고 승인된 7개 tuple과 exact-set 비교한 뒤 `dbt test`를 실행한다. 기존 `dim_admin_dong` materialization/test는 그대로 선행시키고, seed contract gate는 그 결과와 Silver 사이에 둔다.

### 3. DAG dependency and failure propagation

실행 순서는 다음과 같다.

```text
source freshness
  -> incident availability
  -> Bronze source contract
  -> asac_axes seed
  -> common dim materialization
  -> common dim test
  -> asac_axes seed contract
  -> Silver run/test
  -> existing summary + canonical Gold run/test
```

두 신규 task는 기존 run/task/try별 artifact와 failure callback을 사용하는 `dbt_task` 경로를 재사용한다. 두 gate를 transform failure watcher에도 포함해 실패가 `upstream_failed`로 남도록 한다.

### 4. Test strategy

- DAG unit test로 task ID, command, exact selector set, task 순서를 고정한다.
- JSONL normalization unit test로 source/seed dbt `ls` output의 실제 필드 조합을 검증한다.
- source/seed allowlist drift가 test 실행 전에 실패하는지 검증한다.
- failure propagation set에 신규 task가 포함되고 Silver/Gold보다 먼저 실행되는지 회귀 검증한다.
- 기존 canonical Gold, same-target fresh parse, common admin stale-Gold exclusion 테스트를 유지한다.

## Alternatives considered

1. 기존 availability/dimension test에 계약을 합친다. task 수는 줄지만 실패 원인과 운영 재실행 범위가 섞이므로 선택하지 않는다.
2. 모든 pre-Silver 검증을 하나의 Python task로 감싼다. 실패 지점 관측성이 낮아지고 기존 dbt artifact 계약과 어긋나므로 선택하지 않는다.
3. **독립적인 두 dbt gate와 공통 exact-set helper를 추가한다.** 실패 원인·재실행 단위·selector drift를 분리하면서 기존 DAG 실행/복구 계약을 보존하므로 채택한다.
