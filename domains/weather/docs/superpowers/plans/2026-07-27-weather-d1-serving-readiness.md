# Weather D1 Serving 준비 및 게시 승인 Runbook

## 1. 대상 제품

- dbt model/relation: `gold_weather_current_wide_by_admin_dong`
- product ID: `weather_current_by_admin_dong`
- grain/PK: `admin_dong_code`마다 한 행 / `admin_dong_code`
- 공통 시간축: `forecast_at`
- publication mode: `snapshot`
- zero policy: `retain_last_good`
- D1 relation 이름: `gold_weather_current_wide_by_admin_dong`
- 공통 Publisher smoke 경로: `/data/gold_weather_current_wide_by_admin_dong`
- 계약 버전: Serving Contract schema v1.1, model `contract_version: v1`
- 용신동 포함 조건: `admin_dong_code = '1123053600'`

기존 후보 재선정 문서는 W2 upstream이 0행이던 시점에는 이 모델을 보류하고,
W2를 복구한 뒤 다시 채택하는 대안 B를 명시했다. 이번 경량 recovery가 그
선행조건을 충족시키는 경로다.

## 2. 선언과 연결

`ASAC-DBT`의 `_gold.yml`에 `meta.serving`을 선언한다.

- `freshness_slo_minutes: 240`
- intended cron: `50 2,5,8,11,14,17,20,23 * * *`
- Worker 소유 필드인 `api_path`, `allowed_filters`, 예상 행 수는 선언하지 않는다.

`ASAC-DAG`의 `weather_serving_export.py`는 공통
`build_serving_export_dag`만 호출한다. 공통 Publisher의 계약 탐색, Gold 추출,
snapshot swap, PK/row-count 확인, `_catalog`, smoke 로직은 복제하지 않는다.

현재 wrapper는 `schedule=None`이다. 아래 최종 승인 Gate 전에는 자동 게시를
활성화하지 않는다.

## 3. 공개 projection

현재 Gold relation의 전체 컬럼을 그대로 snapshot으로 내보낸다.

`product_row_id`, `admin_dong_code`, `forecast_at`, `admin_dong`, `gu_code`,
`gu`, `admin_dong_revision_date`, `issued_at`, `temp_c`, `humidity_pct`,
`wind_ms`, `wind_dir_deg`, `precip_prob_pct`, `sky_code`, `sky_label`,
`pty_code`, `pty_label`, `is_precipitating`, `pcp_raw`,
`pcp_representation`, `pcp_mm`, `pcp_lower_mm`, `pcp_upper_mm`, `sno_raw`,
`sno_representation`, `sno_cm`, `forecast_lead_hours`, `published_at`

API key, request ID, raw object key 같은 내부 수집 lineage는 이 serving projection에
노출하지 않는다.

## 4. Recovery와 Gold 검증

다음 SQL/계약이 모두 통과해야 한다.

```sql
select count(*) as missing_yongsin_rows
from (
  -- dbt test assert_gold_weather_forecast_by_admin_dong_bridge_exclusions_reconcile
  -- 의 용신동 expected-minus-actual 범위
) gaps;
```

실행 증거:

- v3 checkpoint state가 `verified`
- 선택된 6시간 창이 모두 `completed_windows`에 기록
- stage final reconcile 통과
- winner 8 bucket 통과
- lineage 4 bucket 통과
- 비대상 Gold row count/checksum 불변
- `dbt_test_w2_canonical_contracts` 통과
- 용신동 canonical Gold 및 current WIDE 존재

## 5. C1 및 dry-run

```powershell
$env:PYTHONUTF8='1'
python -m pytest -q serving_contract/tests -p no:cacheprovider
python serving_contract/validate_serving_contract.py `
  --source "domains/**/*.yml" "packages/**/*.yml" `
  --format text
python -m pytest -q `
  domains/traffic_weather/tests/weather/test_weather_serving_contract.py
```

```powershell
python -m pytest -q `
  common/serving/tests `
  domains/weather/tests/test_weather_serving_export.py `
  -p no:cacheprovider
```

그 다음 dbt `parse/compile`, source row count, PK 중복, 필수 컬럼 null,
용신동 포함을 확인한다. 이 단계에서는 Cloudflare D1 API를 호출하지 않는다.

## 6. Worker / `_catalog` 호환성 차단점

공통 Publisher의 현재 `_catalog` 컬럼은 다음과 같다.

`name`, `product_id`, `external`, `description`, `product_question`, `tests`,
`time_axis`, `columns`, `row_count`, `serving_status`, `publication_id`,
`source_run_id`, `published_bytes`, `freshness`, `exported_at`

현재 `ASK-Seoul/dev` 트리에는 Worker `/catalog` 구현 소스가 없어 최신 Worker
계약을 저장소에서 재현할 수 없다. 인계된 운영 Worker는 legacy
`serving_tier`를 기대하지만 공통 v1.1 `_catalog`에는 그 필드가 없다.

따라서 다음 중 하나가 검증되기 전에는 게시하지 않는다.

1. Worker가 위 v1.1 컬럼을 직접 읽도록 갱신되어 호환성 테스트가 통과한다.
2. 공통 규약 이슈 #503/#337의 팀 결정으로 호환 adapter가 확정된다.

이 작업에서는 공통 규약이나 Worker를 임의로 수정하지 않는다.

## 7. 최종 게시 승인 Gate

아래 조건을 모두 만족하고 사용자가 명시적으로 승인해야 한다.

1. recovery checkpoint `verified`
2. 용신동 canonical/Gold/current WIDE 존재
3. 비대상 Gold 불변
4. canonical 및 Weather Gold dbt test 통과
5. recovery 이후 신규 Bronze→canonical 정상 주기 최소 2회
6. Serving Contract C1 통과
7. projection PK/null/row count 및 용신동 포함 확인
8. Worker와 `_catalog` v1.1 호환성 테스트 통과
9. D1 secret은 환경에만 존재하고 로그/코드에 노출되지 않음
10. 사용자 명시 승인

승인 전 금지:

- 운영 snapshot 생성
- D1 upsert/replace
- 운영 `_catalog` 갱신
- Worker route 전환

승인 후에도 수동 실행은 반드시 root 가드레일을 거친다.

```bash
scripts/safe-trigger-dag.sh weather_serving_export
```
# 로컬 Airflow 등록 상태

`weather_serving_export`는 2026-07-27 로컬 Airflow metadata에 등록됐고
`is_paused=true`, `schedule=None`으로 확인했다. thin wrapper에는 Airflow
safe-mode 탐지를 위한 `Airflow`/`DAG` 토큰 회귀 테스트가 있다. 이는 등록
검증일 뿐 실제 snapshot 생성, D1 publish, `_catalog` 갱신을 승인하지 않는다.
