# Weather W2 용신동 경량 Recovery v3 설계

## 1. 배경

기존 `weather_w2_observation_recovery`는 2GB Trino 제한을 지키기 위해 6시간 창을 사용하지만, 각 창마다 Silver observation/grid와 Canonical Gold를 다시 실행하고 winner 8분할·lineage 4분할 검증을 반복한다. 실측 처리 시간은 창당 약 19~21분이었다.

2026-07-27 안전 중단 시점에는 기존 checkpoint v2가 7개 창을 완료했고 다음 창은 `2026-07-12 18:00:00.000000`이었다. 용신동(`admin_dong_code=1123053600`, `nx=61`, `ny=127`)의 입력은 `silver_kma_vilage_fcst_grid`에 존재하며 Canonical Gold에는 1,014개 grain이 부족하다. 따라서 Silver 변경 없이 Gold 범위만 복구할 수 있다.

## 2. 목표

- 기존 Silver relation을 읽기 전용으로 사용한다.
- 용신동만 6시간 단위로 별도 staging relation에 누적한다.
- 각 창은 가벼운 안전 계약 통과 후 checkpoint v3에 기록한다.
- 전체 범위를 staging에서 winner·lineage 검증한 뒤 한 번의 target-scoped `MERGE`로 Canonical Gold에 반영한다.
- 비대상 행의 row count와 checksum이 전후 동일함을 증명한다.
- 기존 checkpoint v2와 처리 이력은 수정하거나 삭제하지 않는다.
- 실패 후 동일 checkpoint ID로 멱등 재개할 수 있다.

## 3. 비목표

- Bronze 또는 Silver rebuild/full refresh
- Silver schema·contract 변경
- 전체 Weather Gold rebuild
- 운영 D1 snapshot 생성·publish·`_catalog` 갱신·Worker 전환
- 기존 checkpoint v2를 v3로 자동 승격
- 공용 Serving 규약 자체 변경

## 4. 실행 식별자와 고정 입력

새 실행은 기존 `yongsin-0716-0723-v2`가 아닌 별도 checkpoint ID를 사용한다.

```text
checkpoint_id = yongsin-0711-0724-light-v3
target_admin_dong_code = 1123053600
expected_nx = 61
expected_ny = 127
range = 2026-07-11 00:00:00.000000 ~ 2026-07-24 00:00:00.000000
```

첫 실행에서 다음 값을 한 번 resolve하고 checkpoint에 저장한다. 재개 시 최신값을 다시 선택하지 않고 저장값을 재사용한다.

- `admin_dong_crosswalk_pin_snapshot_id`
- `weather_w2_bridge_pin_snapshot_id`
- `weather_w2_silver_grid_pin_snapshot_id`
- 시작 시 Canonical Gold snapshot ID
- 비대상 Gold row count와 전체-column checksum
- pinned Silver grid의 `max(published_at)` source watermark
- publishable anchor가 존재하는 selected window 목록

## 5. checkpoint v3 계약

Airflow Variable payload는 다음 구조를 사용한다.

```json
{
  "contract_version": 3,
  "mode": "staged_gold_only",
  "range": {
    "start_at": "2026-07-11 00:00:00.000000",
    "cutoff_at": "2026-07-24 00:00:00.000000"
  },
  "target": {
    "admin_dong_code": "1123053600",
    "nx": 61,
    "ny": 127
  },
  "pins": {
    "admin_dong_crosswalk_snapshot_id": 1,
    "weather_bridge_snapshot_id": 1,
    "silver_grid_snapshot_id": 1,
    "gold_baseline_snapshot_id": 1
  },
  "baseline": {
    "non_target_row_count": 1,
    "non_target_checksum": "hex",
    "source_watermark": "2026-07-27 02:33:08.181476"
  },
  "selected_windows": [],
  "completed_windows": [],
  "known_gaps": [],
  "state": "staging"
}
```

허용 상태 전이는 다음과 같다.

```text
staging
  -> prepublish_validated
  -> published
  -> verified
```

- 창 staging 또는 창 계약이 실패하면 `completed_windows`를 갱신하지 않는다.
- 재실행된 창은 staging merge의 winner ordering으로 downgrade 없이 멱등 처리한다.
- final validation 실패 시 Canonical Gold를 수정하지 않고 staging과 checkpoint를 보존한다.
- publish 후 검증 실패 시 `published` 상태를 유지해 자동 재-publish를 막고 운영자 확인을 요구한다.

## 6. DBT staging relation

새 private incremental model:

```text
weather_w2_observation_recovery_stage
```

grain:

```text
checkpoint_id + admin_dong_code + forecast_at + category
```

입력:

- `silver_kma_vilage_fcst_grid FOR VERSION AS OF <silver_grid_snapshot_id>`
- `bridge_weather_admin_dong_grid FOR VERSION AS OF <bridge_snapshot_id>`
- `asac_axes.pinned_dim_admin_dong()` with pinned crosswalk
- 기존 publishable manifest anchor

필터:

```sql
bridge.source_admin_code = '1123053600'
and bridge.nx = 61
and bridge.ny = 127
and grid.published_at between repair_start_at and repair_cutoff_at
```

stage merge는 동일 checkpoint·natural key에서 `weather_w2_grid_winner_order_key`가 더 최신인 행만 update한다. 창 재실행이나 순서 변경으로 기존 stage winner가 downgrade되지 않는다.

## 7. 창마다 유지하는 경량 검증

한 번의 dbt test invocation에서 다음 실패 행을 합쳐 반환한다.

- checkpoint ID가 요청과 일치
- 모든 행이 용신동 코드와 `(61,127)`에 한정
- `product_row_id`, `admin_dong_code`, `forecast_at`, `category` null 없음
- lineage 필수 컬럼 null 없음
- stage natural key 중복 없음
- pinned Silver+bridge에서 계산한 expected key가 stage에 모두 존재
- stage payload가 pinned input의 winner와 일치
- 해당 창 밖의 stage 행을 창 결과로 오인하지 않음

crosswalk·bridge·Silver snapshot 값은 checkpoint에 고정되므로 창마다 최신 snapshot 조회나 전체 Silver 계약 검증을 반복하지 않는다.

## 8. 최종 pre-publish 검증

모든 selected window가 완료된 뒤 staging만 대상으로 수행한다.

1. 전체 범위 expected key reconciliation
2. winner no-downgrade 8분할
3. exact lineage 4분할
4. 용신동 공식 코드·이름·revision·`nx/ny`·`source_grid_place_id` 확인
5. 자연키 unique와 필수 컬럼 not-null
6. 요청 범위의 known gap과 stage row count 기록
7. 현재 비대상 Gold fingerprint가 baseline과 동일함을 재확인

이 단계가 모두 통과한 뒤에만 checkpoint 상태를 `prepublish_validated`로 변경한다.

## 9. Canonical Gold 반영

`weather_w2_publish_recovery_stage` dbt operation은 checkpoint 상태가 `prepublish_validated`일 때 호출된다. SQL source는 해당 checkpoint의 용신동 stage 행뿐이며 target은 `gold_weather_forecast_by_admin_dong`이다.

한 번의 Iceberg `MERGE`에서:

- natural key가 없으면 insert
- natural key가 있고 stage winner가 기존보다 같거나 최신이면 payload가 다른 경우에만 update
- 비대상 admin_dong은 source에 포함되지 않음
- delete와 full refresh는 수행하지 않음

publish 후:

- 전 동 bridge FULL reconcile selector의 missing=0, extra=0,
  invalid_actual_bridge=0
- 비대상 row count/checksum baseline 일치
- `gold_weather_current_wide_by_admin_dong` Serving projection에 용신동 포함

을 확인하고 checkpoint 상태를 `verified`로 변경한다.

`latest_grid_record`, `repair_no_downgrade`, `repair_reconciles`를 포함한
Canonical W2 전체 10종 계약은 recovery가 `verified`된 뒤 canonical DAG를
unpause하고 다음 정상 Bronze asset-triggered transform에서 검증한다. recovery가
정지된 동안 누적된 live Silver와 Gold의 정상 drift를 staged recovery 실패로
오인하지 않으며, v2 observation publishability/count final contract는 Silver를
변경하지 않는 `staged_gold_only` 경로에서 실행하지 않는다.

## 10. DAG phase

```text
validate runtime
-> resolve/init v3 checkpoint pins and baseline
-> dbt deps
-> for each selected 6h window:
     run stage model
     test lightweight window contract
     save completed window
-> build final lineage workset from stage
-> test final reconciliation
-> test winner buckets 0..7
-> test lineage buckets 0..3
-> verify pre-publish non-target fingerprint
-> mark prepublish_validated
-> run-operation target-scoped Gold merge
-> post-publish FULL bridge reconcile
-> non-target/Gold/serving verification
-> mark verified
```

## 11. 실패·재개

- 수동 실행은 반드시 `scripts/safe-trigger-dag.sh weather_w2_observation_recovery`를 사용한다.
- canonical transform은 recovery가 `verified`가 될 때까지 paused 상태를 유지한다.
- stage run 성공 후 checkpoint 저장 전 실패하면 같은 창을 다시 merge한다.
- checkpoint 저장 후 실패하면 완료 창을 skip한다.
- 기존 v2 Variable은 보존되며 v3 Variable과 staging checkpoint ID가 독립적이다.
- actual publish 전까지 staging relation만 변경되므로 Canonical Gold rollback은 필요하지 않다.

## 12. 예상 시간

기존 방식은 남은 45개 창 기준 약 15시간이었다. v3는 창당 DBT model 1회와 test 1회만 실행하고, 12개 bucket 검증은 마지막에 한 번만 수행한다.

첫 두 창을 benchmark하여 계속 진행 여부를 결정하며 목표 범위는 다음과 같다.

```text
창당 1~3분
전체 staging 45~135분
최종 검증·publish 15~30분
총 1~3시간
```

목표 범위를 넘으면 publish하지 않고 phase timing을 보고한다.
# 운영 보완: pinned source gap 계약

2026-07-27 실데이터 실행에서 publishable manifest anchor가 존재해도 같은 6시간
창의 pinned Silver grid `(61,127)` 행이 0개일 수 있음이 확인됐다. dbt stage의
0행/null/중복/범위이탈 가드는 유지한다. DAG는 dbt 실행 전에 pinned Silver와
해당 창의 최신 publishable manifest를 결합해 target source row count를 확인한다.

- 1행 이상이면 기존 stage model과 창 계약을 실행하고 `completed_windows`에 기록한다.
- 0행이면 dbt를 실행하지 않고 `known_gaps`에
  `no_target_source_rows_at_pinned_snapshot` 사유를 기록한다.
- `known_gaps`는 완료 데이터가 아니며 `completed_windows`와 겹칠 수 없다.
- pre-publish 전에는 모든 selected window가 completed 또는 known gap 중 정확히
  하나로 해소되어야 한다.
- 재개 시 두 집합을 모두 skip하므로 같은 영구결손 창을 반복 실행하지 않는다.
- snapshot pin과 source watermark가 바뀌지 않으므로 이 판정은 해당 checkpoint
  안에서 재현 가능하다.
