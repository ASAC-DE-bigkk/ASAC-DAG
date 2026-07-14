# Traffic Transform Contract Gates Runbook

Traffic scheduled transform은 Silver 이전에 두 개의 독립 계약 gate를 통과해야 한다.

## Gate order

```text
dbt_source_freshness
  -> dbt_test_traffic_incident_availability
  -> dbt_test_traffic_bronze_source_contract
  -> dbt_seed_asac_axes
  -> dbt_run_common_admin_dong_dimension
  -> dbt_test_common_admin_dong_dimension
  -> dbt_test_asac_axes_seed_contract
  -> dbt_run_silver
```

두 gate는 모두 `resolve_traffic_snapshot_run`이 고정한 동일한 publishable Bronze snapshot을 사용한다. gate 실패 시 Silver, 기존 summary Gold, canonical Gold, metrics publish 이전의 transform failure watcher가 실행되며 downstream transform은 `upstream_failed`로 남아야 한다.

## Gate selectors

| Task | dbt test selector | dbt ls exact-set |
|---|---|---:|
| `dbt_test_traffic_bronze_source_contract` | `source:traffic_bronze,test_type:generic` | 40 tuples |
| `dbt_test_asac_axes_seed_contract` | `asac_axes.seoul_admin_dong_crosswalk asac_axes.seoul_admin_dong_boundary asac_axes.seoul_gu_boundary` | 7 tuples |

각 gate는 실제 `dbt test` 전에 동일 target/profile/vars/isolated target path로 `dbt ls --resource-type test --output json`을 실행한다. JSONL 결과를 `(resource, column, test_name)` tuple로 정규화한 뒤 승인 allowlist와 exact-set 비교한다. missing 또는 unexpected tuple이 있으면 selector drift로 간주하고 test 실행 없이 실패한다.

## Failure handling

1. `dbt_test_traffic_bronze_source_contract` 실패
   - 먼저 `dbt ls` 결과의 missing/unexpected tuple을 확인한다.
   - dbt source schema 변경이 의도된 것인지 확인하고, 의도된 변경이면 allowlist와 DAG unit test를 같은 PR에서 갱신한다.
   - tuple drift가 없으면 source generic test 실패 원인을 고친 뒤 같은 dev snapshot 조건으로 해당 transform task를 재실행한다.

2. `dbt_test_asac_axes_seed_contract` 실패
   - seed 파일 변경, `axis_coverage`/bbox/unique/null 위반 여부를 확인한다.
   - 공용 `dim_admin_dong` materialization 문제와 seed 자체 계약 문제를 구분한다.
   - seed와 dimension이 정상화된 뒤 같은 dev snapshot 조건으로 gate부터 재실행한다.

3. gate를 통과하지 않은 실행의 Silver/Gold 결과는 유효한 transform 결과로 취급하지 않는다. gate 실패를 우회하기 위해 `--exclude`, full-refresh, destructive delete, 과거 cutoff/backfill을 사용하지 않는다. repair/backfill이 필요하면 ASAC-DAG #168 및 ASAC-DBT #117 운영 경로를 따른다.

## Validation evidence

PR에는 다음을 기록한다.

- DAG run id와 pinned `traffic_snapshot_dag_run_id`
- 두 gate task의 상태와 소요 시간
- source 40개/seed 7개 exact-set 결과
- Silver/Gold downstream 실행 여부
- dev catalog/schema와 최종 row count
