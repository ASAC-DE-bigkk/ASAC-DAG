# Weather·Traffic 운영 이벤트 행수 출처 계약 설계

- 상태: 사용자 승인
- 작성일: 2026-07-31
- 기준 이슈: ASAC-DAG #619
- 기준 확정안: issue comment `5139672393`
- 대상 저장소: ASAC-DAG

## 목적

Weather와 Traffic의 기존 `product_observability.py` 배선을 확장해 raw, Bronze,
D1 제품 전이 이벤트에 실제 처리 행수와 그 정본 출처를 함께 기록한다.

새로운 운영 기록 체계를 만들지 않는다. 기존 raw manifest, Bronze run manifest,
D1 publication ledger를 단계별 정본으로 재사용하며, D1 `_ops_*` 적재와
reconciler, ops-dashboard 연결은 공통 후속 작업으로 분리한다.

## 배경

현재 `product_observability.py`에는 다음 장치가 이미 있다.

- SHA-256 기반 결정적 `event_id`
- 닫힌 `layer`와 `status` 목록
- R2 관측 실패가 본 작업을 실패시키지 않는 fail-open 경계
- Weather와 Traffic의 raw, Bronze, Gold, D1 제품 전이 기록

그러나 Weather와 Traffic의 raw와 Bronze 성공 콜백은 런타임에 실제 행수를
이미 알고 있으면서도 공통 콜백에 전달하지 않는다. 그 결과
`ops/product-events`의 `row_count`가 NULL로 남고, NULL이 미측정인지 실제 0건인지
조회자가 구분할 수 없다.

## 설계 원칙

1. `row_count=NULL`은 "측정하지 못함"이고, `row_count=0`은 "실제 0건"이다.
2. 행수에는 반드시 측정 방법을 나타내는 `rows_source`를 함께 기록한다.
3. 도메인별 XCom 구조는 도메인 DAG가 해석한다. 공통 모듈이 task ID나
   도메인별 payload 구조를 알지 않는다.
4. 행수 관측을 위해 새 API 호출, R2 GET, Trino COUNT 쿼리를 추가하지 않는다.
5. 콜백의 XCom이 누락되거나 손상돼도 본 task의 성공을 실패로 바꾸지 않는다.
6. `event_id` 계산에는 행수와 출처를 포함하지 않아 재시도 멱등성을 유지한다.

## 공통 계약

`common/ops/product_observability.py`의 스키마 버전을
`product-observability/v2`로 올리고 `rows_source`를 추가한다.

허용값은 다음과 같다.

| 값 | 의미 |
|---|---|
| `raw_manifest` | raw landing manifest와 같은 런타임 수집 명세 |
| `bronze_run_manifest` | 검증 완료 후 Bronze run manifest에 기록한 실측값 |
| `iceberg_snapshot` | Iceberg snapshot summary 기반 값, 후속 Gold 배선용 |
| `count_query` | 제한된 COUNT 쿼리 기반 값, 후속 Gold 폴백용 |
| `publication_ledger` | D1 publication ledger의 게시 결과 |
| `not_observed` | 행수를 측정하지 못했거나 실패 이벤트라 값이 없음 |

검증 규칙은 다음과 같다.

| `row_count` | `rows_source` | 판정 |
|---|---|---|
| NULL | `not_observed` | 허용 |
| 0 이상 정수 | 정본 출처 | 허용 |
| NULL | 정본 출처 | 거부 |
| 0 이상 정수 | `not_observed` | 거부 |
| 음수 또는 boolean | 모든 값 | 거부 |
| 모든 값 | 미지원 출처 | 거부 |

기존 호출자와 실패 콜백의 호환성을 위해 `row_count`와 `rows_source`를 생략하면
`row_count=NULL`, `rows_source=not_observed`로 기록한다.

## Weather 배선

| 단계 | 이벤트 행수 | 정본 출처 |
|---|---|---|
| raw 성공 | `raw_objects[].row_count` 합계 | `raw_manifest` |
| Bronze 성공 | `verify_kma_bronze_runtime` 반환값 | `bronze_run_manifest` |
| D1 | `ProductRecord.published_row_count` | `publication_ledger` |
| 실패 | NULL | `not_observed` |

Weather raw 값은 KMA parser가 각 raw object에 기록한 `row_count`의 합계다.
Bronze 값은 적재 후 검증을 통과하고 run manifest의 `actual_rows`에 기록되는 값과
같다.

## Traffic 배선

| 단계 | 이벤트 행수 | 정본 출처 |
|---|---|---|
| Incident raw 성공 | landing 결과의 `parsed_rows` | `raw_manifest` |
| Incident Bronze 성공 | 처리한 snapshot들의 검증 행수 합계 | `bronze_run_manifest` |
| Flow raw 성공 | landing 결과의 `expected_rows` | `raw_manifest` |
| Flow Bronze 성공 | materializer의 검증 `row_count` | `bronze_run_manifest` |
| D1 | `ProductRecord.published_row_count` | `publication_ledger` |
| 실패 | NULL | `not_observed` |

Incident materializer의 반환 계약에는 기존 `processed`와 `snapshot_run_ids`를
유지하면서 집계 `row_count`를 가산한다. receipt 기록, acknowledgement,
publishability 판단, Asset 발행 순서는 변경하지 않는다.

## 콜백 실패 경계

도메인별 성공 콜백은 현재 task의 XCom을 읽어 행수와 출처를 결정한다.
XCom이 없거나 예상 타입과 다르면 예외를 다시 던지지 않고
`row_count=NULL`, `rows_source=not_observed` 이벤트를 기록한다.

`record_product_event()`의 기존 R2 write와 target resolution 실패 처리도 그대로
유지한다. 관측 실패는 Stats와 warning으로 드러내되 원래 데이터 작업 결과를
바꾸지 않는다.

## 테스트 설계

공통 계약 테스트:

- 관측된 0건이 0으로 유지되는지 확인
- NULL과 `not_observed` 조합 허용
- 행수와 출처가 모순되면 거부
- 음수, boolean, 미지원 출처 거부
- 행수 변경에도 같은 런의 `event_id`가 유지됨
- D1 이벤트가 `publication_ledger`를 사용함

Weather 테스트:

- raw object별 행수 합계가 raw 이벤트에 전달됨
- Bronze 검증 반환값이 Bronze 이벤트에 전달됨
- 누락되거나 손상된 XCom이 `not_observed`로 기록됨

Traffic 테스트:

- Incident raw `parsed_rows`가 raw 이벤트에 전달됨
- Incident materializer가 snapshot 검증 행수 합계를 반환함
- Incident Bronze 콜백이 집계 행수를 전달함
- Flow raw `expected_rows`가 raw 이벤트에 전달됨
- Flow Bronze 검증 행수가 전달됨
- 누락되거나 손상된 XCom이 `not_observed`로 기록됨

테스트 데이터는 모두 로컬 가짜 payload와 XCom을 사용하며 실제 API 키나 운영
자격증명을 사용하지 않는다.

## 변경하지 않는 경계

- Gold의 Iceberg snapshot 또는 COUNT 쿼리 배선
- D1 `_ops_*` 테이블과 직접 upsert
- `ops_runlog_reconcile` 또는 별도 reconciler DAG
- ASK-Seoul-Serving의 ops-dashboard 연결
- R2 prefix 이동, dual-read, lifecycle 설정
- Traffic checkpoint, receipt, run ledger, recovery evidence
- Weather와 Traffic 외 도메인 DAG 배선

## 수용 기준

1. Weather와 Traffic raw, Bronze, D1 성공 이벤트가 정본 기반 행수와 출처를 갖는다.
2. 실패 또는 미측정 이벤트는 NULL과 `not_observed`를 사용한다.
3. 실제 0건이 NULL로 바뀌지 않는다.
4. 기존 `event_id` 멱등성과 fail-open 동작이 유지된다.
5. Incident receipt와 acknowledgement 순서가 유지된다.
6. 관련 단위테스트가 실제 secret 없이 통과한다.
