# D1 게시 경합과 Weather 정시 stale publication 보강 설계

## 목적

공용 D1 Publisher를 사용하는 모든 도메인의 쓰기를 하나의 Airflow pool로 직렬화하고, 일시적인 Cloudflare D1 오류는 멱등성이 증명된 요청에서만 짧게 재시도한다. 동시에 Weather 전체 transform이 시간 경계를 넘어 끝날 때 이전 시간대 snapshot Asset이 게시되는 것을 차단하고, API smoke와 D1 오류에 비밀값 없는 진단 증거를 남긴다.

## 변경하지 않을 경계

- KMA 단기예보 수집 주기와 원천/Bronze/Silver/Gold 계약은 바꾸지 않는다.
- D1 물리 schema와 Worker API 응답 계약은 바꾸지 않는다.
- append-only publication ledger, staging 전환, last-known-good 복구와 기존 Airflow task 재시도는 유지한다.
- 다른 도메인 DAG 파일은 수정하지 않는다. 공용 Publisher pool의 효과만 전체 도메인에 적용한다.
- prod 재배포, DAG pause/unpause, 수동 trigger는 이 구현 범위에 포함하지 않는다.

## 관측된 문제

### 공용 D1 쓰기 경합

Traffic 게시 실패 시점에 Culture의 대형 게시가 같은 D1 데이터베이스에 중첩됐다. Traffic의 첫 시도는 D1 code `7500 internal error`로 write 단계에서 실패했고 Airflow task 재시도는 성공했다. 현재 `publish_to_d1`은 `default_pool`을 사용하므로 도메인 family가 달라도 하나의 D1에 동시에 쓸 수 있다.

### Weather 시간 경계 경쟁

`weather_vilage_fcst_transform`은 시작 시 `serving_as_of_hour`를 고정한 뒤 긴 dbt run/test를 수행한다. 정시를 넘어 완료되면 이전 시간대 Gold가 terminal Asset을 먼저 발행하고, hourly refresh는 같은 Weather heavy pool을 기다린다. 그 사이 Worker는 `quality_snapshot_not_current`로 `503`을 반환하고 Publisher smoke가 실패한다.

### 진단 손실

API smoke는 모든 비정상 응답을 `failed`로 축약하고 D1 client도 status와 Cloudflare 요청 식별자를 보존하지 않는다. 따라서 `401`, `429`, `503`, timeout과 D1 내부 오류를 과거 로그만으로 구분할 수 없다.

## 결정

### 1. 모든 공용 Publisher를 단일 D1 writer pool로 직렬화

`common/pools.py`에 slot 1인 `serving_d1_publish`를 등록하고 `build_serving_export_dag()`가 만드는 `publish_to_d1` task에 적용한다. ASK-Seoul의 최신 `airflow-init`은 이 registry를 import하므로 별도 Compose 하드코딩은 추가하지 않는다.

### 2. 멱등성이 확인된 D1 요청만 제한적으로 재시도

- 최대 3회, exponential backoff와 jitter를 사용한다.
- HTTP `429`, `5xx`, D1 error code `7500`, timeout/connection 오류만 일시 오류로 분류한다.
- `SELECT`/`PRAGMA`, `IF EXISTS`/`IF NOT EXISTS` DDL, 자연키 upsert, 멱등 delete와 PK 검증 뒤 staging `INSERT OR REPLACE`만 로컬 재시도를 허용한다.
- append insert, append-only ledger insert, snapshot 활성화/보상 `ALTER TABLE` batch는 결과가 불확실한 상태에서 재전송하지 않는다.
- 비멱등 실패는 기존 Airflow task 재시도와 last-known-good 절차가 담당한다.

### 3. Weather stale terminal Asset을 차단

- 전체 transform marker는 고정한 `serving_as_of_hour`가 marker 실행 시점의 KST hour보다 과거면 `AirflowSkipException`으로 끝내 Asset을 발행하지 않는다.
- hourly refresh marker는 동일 조건에서 fail-closed하고 Asset을 발행하지 않는다.
- 정상 Asset metadata에 `serving_as_of_hour`를 추가한다.
- hourly refresh dbt run/test에 높은 절대 priority를 부여해 shared Weather writer pool에서 다음 실행 순서를 우선한다.
- 기존 정시 schedule과 pool 직렬화는 유지한다.

### 4. 비밀값 없는 smoke/D1 진단

- smoke 진단은 status, API error code, blocker 목록, `CF-Ray`, latency, exception type만 보존한다.
- 토큰, Authorization header, SQL, 응답 원문은 기록하지 않는다.
- 기존 `passed`/`failed`/`not_evaluated` 상태는 유지한다.
- 실패 reason에는 정제된 요약을 넣고 XCom publication record에는 구조화된 진단을 추가한다.
- D1 retry 로그와 최종 오류는 HTTP status, D1 code, `CF-Ray`, attempt만 포함한다.

## 오류 처리와 안전성

- D1 pool은 도메인 간 쓰기 경합을 제거하지만 읽기와 Worker API 요청은 제한하지 않는다.
- local retry는 요청 종류별 opt-in이다. 안전성이 불명확하면 재시도하지 않는다.
- staging insert는 source PK 검증 후 실행되므로 `INSERT OR REPLACE` 재전송이 최종 행 집합을 바꾸지 않는다.
- Weather stale full transform은 실패로 위장하지 않고 publication만 skip한다. hourly refresh stale은 현재 공개 snapshot 복구가 필요하므로 실패로 드러낸다.
- smoke 실패 시 기존 snapshot compensation과 ledger 기록 순서는 바꾸지 않는다.

## 검증 기준

1. pool registry가 `serving_d1_publish=1`을 정확히 출력한다.
2. 공통 factory의 모든 `publish_to_d1` task가 새 pool을 사용한다.
3. transient + retry-safe 요청만 최대 3회 재시도하고 비멱등 요청은 1회만 실행한다.
4. retry 로그와 예외에 token, Authorization, SQL, 응답 원문이 없다.
5. smoke의 `503 product_not_ready`와 blocker가 구조화되어 failure reason/XCom에 전달된다.
6. full transform의 이전 시간대 marker는 Asset을 발행하지 않고 skip한다.
7. hourly refresh의 이전 시간대 marker는 Asset을 발행하지 않고 실패한다.
8. 현재 시간대 marker는 `serving_as_of_hour` metadata를 포함해 기존 Asset을 발행한다.
9. 관련 공통/Weather pytest, Python compile, DAG import 검증이 통과한다.

## 롤백

- 코드 롤백은 새 pool assignment, retry helper, smoke diagnostic, Weather marker guard를 해당 커밋에서 되돌린다.
- pool registry의 잔존 entry는 사용 task가 없으면 동작에 영향을 주지 않는다.
- D1 schema·제품 데이터 migration이 없으므로 데이터 롤백은 필요하지 않다.
