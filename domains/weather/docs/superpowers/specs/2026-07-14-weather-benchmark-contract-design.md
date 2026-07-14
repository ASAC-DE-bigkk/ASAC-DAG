# Weather benchmark 반복·실행 지문 계약 설계

## 목표

Weather 소유 비용 대리 지표 benchmark가 최소 3회 반복과 동일 실행 지문을 강제하고, Weather Gold·Traffic canonical Gold까지 읽기 전용으로 비교한다.

## 결정

- benchmark는 계속 `ASK_SEOUL_TARGET=dev`에서만 실행하고 `dbt compile`, `EXPLAIN ANALYZE`, reliability 조회만 수행한다.
- `--repeat`의 최소값은 3이다. 반복 수가 부족하면 Trino 연결 전에 `ValueError`로 실패한다.
- 실행 지문은 suite 이름·domain·workload 종류·model/report·source tables·dbt vars·compile command·target/catalog/schema/DBT_BIN으로 구성한다. 생성 시각과 query metrics는 지문에 포함하지 않는다.
- Weather Gold는 `gold_weather_forecast_by_place`, Traffic Gold는 `gold_traffic_incident_current_by_admin_dong_hourly`을 측정한다. Traffic Gold는 기존 Traffic publishable snapshot var를 재사용한다.
- source fingerprint나 execution fingerprint가 다르면 결과를 비교하지 않는다. benchmark는 Traffic 코드·table·raw object를 변경하지 않는다.

## 경계와 검증

- 변경·생성 파일은 `domains/weather/**`만 허용한다.
- Python unit test와 `py_compile`을 먼저 수행한다.
- Traffic transform이 같은 dev schema에 쓰는 동안에는 실제 before/after collection 결과를 확정하지 않고, 결과가 `fingerprint_mismatch`면 비교 불가로 기록한다.
