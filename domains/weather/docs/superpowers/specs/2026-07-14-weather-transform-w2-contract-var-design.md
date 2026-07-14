# Weather transform W2 canonical revision 계약 설계

## 목표

Weather transform DAG가 최신 DBT W2 public Gold 계약에 필요한
`weather_w2_canonical_revision_date=2025-04-01`을 일관되게 전달해 공용 행정동 차원 검증의 컴파일 실패를 막는다.

## 범위와 경계

- 변경 대상은 Weather transform DAG와 해당 회귀 테스트뿐이다.
- `dbt deps`는 모델을 파싱하거나 실행하지 않으므로 contract var를 전달하지 않는다.
- 나머지 Weather dbt source freshness, seed, run, test 명령은 중앙 helper를 통해 같은 고정 contract var를 받는다.
- DBT model, R2, Trino table, Traffic, prod, backfill은 변경하지 않는다.

## 배포와 검증

- `AIRFLOW__API__BASE_URL=http://localhost:30585`는 root `origin/dev`에 이미 병합되어 있으므로 공통 Airflow 컨테이너를 재생성해 적용한다.
- 코드 PR 병합 뒤 현재 Airflow가 마운트한 Weather DAG에도 동일한 병합본을 반영한다.
- dev에서 실패했던 `dbt test --select asac_axes.dim_admin_dong`을 동일 var와 함께 재실행하고, TaskInstance log URL이 30585를 쓰는지 읽기 검증한다.
