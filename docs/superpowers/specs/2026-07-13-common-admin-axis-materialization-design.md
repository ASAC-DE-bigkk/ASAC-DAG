# Weather·Traffic 공용 행정동 차원 재물질화 설계

## 목적

Weather·Traffic 정상 transform이 매 실행마다 도메인별 `asac_axes.dim_admin_dong` view를 현재 공용 원천인 `iceberg_dev.common.bronze_admin_dong_master` 기준으로 재생성하고, 그 계약을 검증한 뒤 Silver/Gold로 진행하게 한다.

## 관측된 문제

- ASAC-DBT `b904a99`는 `axes_bronze` 기본 schema를 `COMMON_SCHEMA/common`으로 옮겼다.
- 두 정상 transform은 `dbt deps`와 `dbt seed --select asac_axes`만 실행하고 package model은 실행하지 않는다.
- 그 결과 이미 생성된 Weather·Traffic `dim_admin_dong` view는 과거 개인 dev schema를 계속 참조한다.
- Weather W1 smoke와 Citydata 정상 transform은 `dim_admin_dong`을 명시적으로 실행하는 안전한 선례를 제공한다.

## 선택한 설계

두 정상 DAG에 같은 두 phase를 추가한다.

1. `dbt_run_common_admin_dong_dimension`: `run --select asac_axes.dim_admin_dong`
2. `dbt_test_common_admin_dong_dimension`: `test --select asac_axes.dim_admin_dong`

순서는 각 도메인에서 다음처럼 고정한다.

```text
dbt_seed_asac_axes
  -> dbt_run_common_admin_dong_dimension
  -> dbt_test_common_admin_dong_dimension
  -> 기존 도메인 seed 또는 Silver 단계
```

Weather는 기존 BashOperator와 Weather 실패 알림/Problem callback을 그대로 사용하고, 새 task의 알림 단계명을 `공용 행정동 차원 실행/검증`으로 분류한다. Traffic은 기존 `dbt_task` factory를 사용해 pinned snapshot 변수, artifact 격리, retry 가능성 분류, 실패 기록 계약을 그대로 유지한다. Dimension phase는 Silver가 아직 저장되기 전이므로 `silver_persisted=False`를 유지한다.

`dim_admin_dong`은 공용 Bronze의 최신 revision을 읽는 live view다. 따라서 dimension test가 끝난 직후 공용 Bronze가 갱신되면 뒤이은 Silver가 더 새로운 revision을 볼 수 있다. 현재 계약은 정확한 transform-run 재현성보다 최신 canonical 축 반영을 우선하며, 공용 축 revision을 한 run에 고정하지 않는다. 향후 exact run consistency가 필요해지면 별도 이슈에서 axes revision을 resolve·전달하고 dimension test와 Silver가 같은 revision을 소비하도록 확장한다.

## 고려한 대안

### Silver selector에 부모 선택 연산자(`+`) 추가

선택 범위가 package seed·source까지 암묵적으로 넓어지고, 기존에 명시적으로 분리한 run/test phase와 실패 원인 분류가 흐려져 채택하지 않는다.

### `dbt build` 한 task로 합치기

간단하지만 현재 두 DAG의 run/test 분리 관례와 단계별 실패 가시성을 잃는다. 기존 운영 패턴을 유지하기 위해 run과 test를 분리한다.

### 공용 schema에 dimension relation 하나만 배포

현재 `asac_axes` package model은 소비 프로젝트 target schema에 materialize되는 계약이다. 이 티켓에서 relation ownership과 schema 생성 규칙까지 바꾸면 ASAC-DBT 변경과 전 도메인 migration이 필요하므로 범위를 벗어난다.

## 검증 기준

- 새 task가 없어서 실패하는 unit test를 먼저 확인한 뒤 최소 DAG 배선으로 통과시킨다.
- Weather·Traffic task order, 정확한 selector, Weather 알림 단계명, 기존 failure callback/pinned snapshot 계약을 테스트로 고정한다.
- 관련 pytest와 changed DAG compile을 통과시킨다.
- 승인된 DEV 실행에서 두 view가 `iceberg_dev.common.bronze_admin_dong_master`를 참조하는지 확인한다.
- 두 view가 각각 426행이고 `admin_dong_code` null/duplicate가 0건이며 revision이 공용 원천 최신 revision과 일치하는지 확인한다.

## 범위 제외

- ASAC-DBT 공통축 SQL 또는 source 변경
- 기존 Weather·Traffic Silver/Gold grain·selector 변경
- Weather W2, Traffic analytic Gold, cross-domain consumer 재배선
- prod catalog/schema write, destructive full-refresh, 과거 데이터 backfill
