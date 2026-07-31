# Weather·Traffic prod 승격 P0 DAG 설계

## 목적

prod 승격을 막는 두 데이터 경로 회귀를 제거한다. 제품 관측은 제품별·게시별 이벤트를 유실 없이 target에 맞는 R2 bucket에 남기고, raw landing은 한 DAG run의 모든 object와 completion manifest를 하나의 KST `load_date`에 묶는다.

## 변경하지 않는 경계

Dashboard, Worker API, dbt Gold SQL, schedule, 실제 prod run과 backfill 실행은 변경하지 않는다. payload의 `collected_at`은 실제 API 호출 시각으로 계속 보존하고, D1 Publisher의 last-known-good·gate·write 순서도 바꾸지 않는다.

## 관측 이벤트 계약

`common.runtime_guard.resolve_runtime_target()`가 `DBT_TARGET`을 runtime target의 authoritative source로 해석한다. 값이 없거나 `dev`·`prod` 밖이면 예외를 발생시키며, legacy `ASK_SEOUL_TARGET`가 존재할 경우에는 `DBT_TARGET`과 같은 값일 때만 호환 입력으로 허용한다. 따라서 target이 없는 Airflow callback은 dev로 추측하지 않는다.

`build_product_event()`는 단일 `product_id`와 nullable `publication_id`를 포함한 canonical identity를 JSON으로 정렬해 SHA-256 digest를 `event_id`로 만든다. R2 key는 `event_id=<digest>.json`을 포함한다. 같은 event identity는 retry에 같은 key를 사용하고, 다른 product 또는 publication은 다른 key를 사용한다. 기존 `product_ids` 배열은 event payload의 호환 정보로 남기되, D1 publisher는 이미 제품별 호출하므로 각 호출의 단일 product id가 identity가 된다.

제품 event·health write는 resolver 오류와 R2 오류를 application task에 전파하지 않는다. 대신 Airflow Stats counter `product_observability.write_failed`와 error kind가 들어간 구조화 경고를 남긴다. reconciler는 이 counter와 warning kind를 수집해 관측 write 자체의 실패를 data-path 성공과 분리해 경보할 수 있다.

## Landing partition 계약

Weather·Traffic Incident의 `RunIdentity`는 optional `landing_load_date`를 가진다. collect 시작 시 명시값, 이미 기록된 checkpoint anchor, 현재 KST 날짜 순서로 값을 확정하며 첫 network 요청 전에 checkpoint에 저장한다. raw key 생성과 completion manifest는 모두 이 값을 사용한다. retry는 checkpoint anchor를 재사용한다.

Traffic Flow도 collect 시작 시 같은 방식으로 `landing_load_date`를 한 번 계산해 모든 per-link raw key와 manifest에 전달한다. `collected_at`은 각 link의 실제 수집 시각으로 유지한다. replay/backfill은 명시 `load_date`를 받거나 입력 raw key의 한 partition을 추론하며 둘 이상의 partition은 validation error로 거부한다.

## 오류 처리

잘못된 `landing_load_date`와 cross-partition replay 입력은 manifest를 쓰기 전에 실패한다. checkpoint 구 버전에는 anchor가 없으므로 이미 저장된 object key의 `load_date`가 하나일 때만 그것을 복구하고, 없으면 새 run anchor를 기록한다. 관측 실패는 fail-open이지만 target resolution 오류도 counter와 structured warning으로 남긴다.

## 테스트

- 한 task/run에서 두 D1 product record를 만들면 서로 다른 key와 event_id가 생성된다.
- 같은 product/publication retry는 같은 key가 생성된다.
- prod environment와 params 없는 callback이 prod target을 `_put_r2`에 전달한다. target 불명은 dev write 없이 failure counter를 증가시킨다.
- Weather, Traffic Incident, Traffic Flow의 clock fixture가 23:59:59와 00:00:01 KST를 반환해도 모든 raw key·manifest의 `load_date`가 첫 anchor와 같다.
- checkpoint retry와 replay의 explicit load_date가 같은 partition을 보존하고 cross-partition replay를 거부한다.
