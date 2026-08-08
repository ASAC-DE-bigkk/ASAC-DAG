# Collection slot receipt materializer 구현 계획

## 목적

R2 `ops/control/state/collection_slots/v1` receipt를 dev/prod target이 명시한 Iceberg
`weather_traffic_bronze` namespace의 expected/event 테이블로 배치 투영한다. collector hot path에는
Trino/Iceberg write를 추가하지 않고, receipt 재실행은 같은 identity를 중복 생성하지 않으며
동일 identity의 다른 문서는 fail-closed 한다.

## 변경 경계

- 변경: 공통 receipt document 검증, materializer 서비스, Iceberg sink, 별도 Airflow DAG와 테스트
- 변경하지 않음: 기존 raw path, Traffic run/snapshot manifest, Weather/Traffic collector,
  D1 Publisher, serving product, prod bucket/schema
- 입력: `CollectionSlotReceipts`가 만든 expected/event JSON
- 출력: `bronze_collection_expected_slot`, `bronze_collection_slot_event`

## 단계

1. receipt batch를 deterministic하게 읽고 `ExpectedSlot`/`CollectionOutcome` 재검증
2. pre-rollout Traffic event에 한해 결정론적 expected receipt 복구; 해석 불가능한 고아는 실패
3. expected/event sink의 schema·table 생성과 idempotent append 구현
4. `collection_slot_materializer` Airflow DAG를 dev-safe runtime guard와 함께 연결
5. fake sink 단위 테스트, canonical container import, dev table materialization·재실행 검증

## 완료 조건

- malformed JSON, identity hash mismatch, 해석할 수 없는 event→expected 참조, conflicting duplicate는 task 실패
- pre-rollout Traffic event는 같은 `expected_slot_id`를 재현할 때만 R2 expected receipt를 복구
- 동일 R2 receipt로 materializer를 두 번 실행해도 expected/event row 수가 증가하지 않음
- 두 테이블이 없으면 additive DDL로 생성되고, 재실행은 기존 행과 canonical document를 비교
- materializer 오류는 empty 성공으로 변환하지 않고 Airflow task failure로 남음
- dev에서 실제 table row count와 source receipt count가 일치하고, DBT macro가 해당 relation을 읽을 수 있음

## 롤백

새 DAG를 pause하고 새 테이블을 소비하는 DBT model을 선택하지 않으면 기존 pipeline은 영향받지 않는다.
R2 receipt/raw는 삭제하지 않는다.
