# Weather·Traffic 비용 대리 지표 및 SLO/Watchdog 설계

**상태:** 구현 계획 수립 전 승인된 설계
**작성일:** 2026-07-13
**대상:** Weather, Traffic 도메인 (dev 우선)

## 1. 결정 요약

실제 Cloudflare 청구액 대신, 동일한 dev 데이터와 실행 조건에서 얻는 **비용 대리 지표**로 성능·비용 효과를 판단한다. 우선순위는 다음과 같다.

1. 측정 가능한 기준선(baseline)을 먼저 확보한다.
2. 가장 큰 비용 대리 지표를 보이는 Weather/Traffic 처리 경로만 최소 범위로 최적화한다.
3. 같은 조건으로 재측정하고, 데이터 정합성 게이트를 통과한 경우에만 변경을 유지한다.
4. Watchdog는 기존 도메인별 reliability report를 확장하며, 자동 재실행은 하지 않는다.
5. 전후 결과와 한계를 회고록에 수치로 기록한다.

이 설계는 실제 비용 절감을 주장하지 않는다. 대신 실제 비용과 상관성이 큰 실행량·스캔량·자원 사용량을 일관된 방식으로 비교해, 이후 청구 데이터가 생겨도 검증 가능한 근거를 남긴다.

## 2. 배경과 현재 근거

- Weather dbt source freshness는 `warn_after: 30h`, `error_after: 48h`인 반면, KMA 수집 주기는 3시간 단위다. 이는 장애 감지 기준으로 지나치게 느슨하다.
- Traffic은 manifest freshness를 `30m/2h`로 감시하지만, reliability report의 기본 freshness SLO는 15분이다. 같은 도메인 안에서도 감시 기준과 알림 기준의 역할을 명확히 나눌 필요가 있다.
- 두 도메인 모두 Bronze reliability report와 Discord 전달 경로는 이미 존재한다. 새 공통 프레임워크를 만들기보다 이 경로를 활용한다.
- 루트 dev에는 Trino spill/heap 설정이 이미 있다. 설정을 중복으로 바꾸기보다 실제 query metric으로 병목 여부를 확인한다.
- 현재 로컬 WIP에는 Weather/Traffic transform 및 Gold 정합성 변경이 있다. 특히 Traffic 30분 lookback의 late-arrival 누락 위험과 Weather late-repair 이슈가 남아 있으므로, 스캔 비용만 줄이기 위해 lookback을 축소하지 않는다.

## 3. 범위와 비범위

### 포함

- Weather와 Traffic의 dev 기준 성능·비용 대리 지표 수집
- 동일 조건 baseline/after 비교와 결과 문서화
- 기존 Weather/Traffic reliability report의 freshness·coverage·publishability 관찰 보강
- Watchdog 쿼리 자체의 스캔량·실행 자원 측정 및 비용 상한 검증
- 측정으로 확인된 상위 1~2개 병목의 최소 범위 최적화

### 제외

- 실제 Cloudflare/R2 청구액 조회 또는 비용 알림
- 단일 PostgreSQL serving layer 작업
- cross-domain mart 작업
- model-level orchestration/lineage 전환
- 다른 도메인까지 강제하는 공통 운영 표준화
- Watchdog의 자동 recollect, 자동 backfill, 자동 retry
- 데이터 정합성 보장이 없는 incremental lookback 축소

## 4. 비용 대리 지표

비교 단위는 `도메인 × benchmark suite × 동일 데이터 fingerprint × 동일 실행 설정`이다. 각 실행에서 가능한 항목을 수집하되, 사용할 수 없는 항목은 0으로 보정하지 않고 `unavailable`로 남긴다.

| 우선순위 | 지표 | 비용과의 관계 | 수집 우선 경로 |
| --- | --- | --- | --- |
| 1 | Trino physical input bytes / input rows | R2·Iceberg 스캔량의 가장 직접적인 대리 지표 | Trino query statistics |
| 2 | Trino CPU time | 컴퓨팅 소비량 대리 지표 | Trino query statistics |
| 3 | physical written bytes / output rows | Iceberg write·R2 저장/쓰기 증가 감시 | Trino query statistics |
| 4 | spilled bytes / peak user memory | 메모리 부족으로 인한 지연·추가 I/O 신호 | Trino query statistics |
| 5 | query wall time / queued time | 사용자 체감 지연과 자원 경합 신호 | Trino query statistics |
| 6 | Airflow task duration / task 수 | 오케스트레이션 실행량과 운영 비용 신호 | Airflow task instance/log |
| 7 | R2 raw/bronze 객체 수·bytes 변화 | 저장량과 요청 증가의 보조 지표 | 객체 메타데이터 또는 run manifest |

`physical input bytes`와 `CPU time`을 주된 판정 지표로 사용한다. wall time 단축만 있고 위 두 지표가 증가하면 비용 최적화 성공으로 기록하지 않는다.

## 5. 비교 가능성을 보장하는 측정 규약

### 5.1 데이터와 실행 환경 fingerprint

각 baseline/after 결과에는 다음을 함께 남긴다.

- root, `dags`, `dbt`의 Git revision과 dirty-WIP 여부
- `ASK_SEOUL_TARGET`, catalog, schema, dbt selector, 실행 파라미터
- 대상 Bronze/manifest/audit 테이블의 row count, 최신 `collected_at`/`event_at`, 사용 partition 또는 snapshot 식별자(지원 시)
- 실행 시작/종료 시각, 동시 실행 여부, Trino/컨테이너 설정

데이터 fingerprint가 달라졌거나 동시 부하를 통제할 수 없으면 결과를 `non-comparable`로 표시한다. 수치를 억지로 전후 비교하지 않는다.

### 5.2 benchmark suite

외부 API 수집은 benchmark에 포함하지 않는다. KMA 429나 TOPIS 응답 변동을 비용 실험에 섞지 않기 위해, 고정된 dev Bronze 스냅샷을 입력으로 사용한다.

초기 suite는 다음 세 경로다.

1. Weather transform의 Silver/Gold 대상 dbt selector
2. Traffic transform의 Silver/Gold 대상 dbt selector
3. Weather/Traffic reliability report가 실행하는 Trino 집계 쿼리

materialization 비용까지 확인해야 하는 경우에는 공유 dev 테이블을 덮어쓰지 않는 격리 schema에서 실행한다. source와 target을 충분히 분리할 수 없는 환경이면, 먼저 read-only query plan/통계 수집만 수행하고 write benchmark는 보류한다.

### 5.3 반복과 집계

- 같은 fingerprint에서 최소 3회 실행한다.
- 기준값은 중앙값(median)으로 기록하고, 최소·최대 범위도 함께 남긴다.
- 10회 이상 실행된 suite만 p95를 보조 지표로 기록한다.
- 완료 query의 통계는 실행 직후 수집한다. Trino가 query history를 보존하지 않으면 해당 항목은 `unavailable`로 기록하고 Airflow task duration으로 대체하지 않는다.

## 6. 비용 우선 최적화 원칙

### 6.1 우선순위

1. **측정 경로 자체의 비용을 먼저 가시화**한다.
2. `physical input bytes` 또는 `CPU time`이 큰 transform/query 한두 개만 선택한다.
3. 파티션 predicate, 불필요한 전체 스캔, 중복 변환, 불필요한 task 실행, 큰 공간 변환식의 재계산 여부를 query plan과 metric으로 확인한다.
4. 변경 후 동일 suite를 재실행해 효과가 없는 변경은 유지하지 않는다.

### 6.2 보호 규칙

- Traffic late-arrival 해결 경로 없이 30분 lookback을 더 짧게 만들지 않는다.
- Weather의 late-repair 또는 canonical grid grain을 훼손하는 증분/파티션 변경은 하지 않는다.
- 현재 WIP의 Weather/Traffic transform 및 Gold 정합성 변경과 같은 파일을 건드릴 때는 먼저 diff를 대조한다. 충돌 가능성이 있으면 별도 patch 또는 별도 브랜치로 분리한다.
- 이미 설정된 Trino spill/heap 값은 benchmark에서 병목임이 확인되기 전까지 다시 튜닝하지 않는다.

### 6.3 유지 판정

최적화 변경은 아래를 모두 만족할 때만 유지한다.

1. 기존 정합성·freshness·coverage·publishability 테스트가 통과한다.
2. 영향을 받은 핵심 suite에서 `physical input bytes` 또는 `CPU time`이 중앙값 기준 15% 이상 감소한다.
3. `physical written bytes`, spilled bytes, peak memory, 결과 행 수에 설명되지 않는 회귀가 없다.

15%에 못 미치더라도 spill 제거처럼 명확한 운영 안정성 효과가 있으면 예외 사유와 수치를 회고록에 남긴 뒤에만 유지한다. 그렇지 않으면 되돌리거나 후보에서 제외한다.

## 7. Freshness/SLO 및 Watchdog 설계

### 7.1 관찰 축 분리

한 개의 PASS/FAIL만으로 상태를 표현하지 않는다. Weather와 Traffic 모두 아래 신호를 분리해 report와 Discord 메시지에 포함한다.

| 신호 | 의미 | 실패 시 해석 |
| --- | --- | --- |
| freshness | 마지막 수집/manifest 이벤트의 경과 시간 | 수집 지연 또는 중단 가능성 |
| publishability | 최근 run이 SUCCESS이고 publishable인지 | Bronze가 downstream에 노출되지 않음 |
| coverage | 예상 grid/page/row 범위를 충족하는지 | partial 또는 pagination 누락 가능성 |
| zero-row | 원천이 정상적으로 빈 결과를 준 횟수 | 가용성 실패와 구분할 정보 |
| late-arrival | 허용 window 밖으로 늦게 들어온 레코드 여부 | incremental 누락 위험 신호 |

Traffic의 `zero-row`는 availability 실패로 자동 분류하지 않는다. Weather의 grid/page coverage도 freshness와 독립적으로 보고한다.

### 7.2 초기 SLO 기본값

기본값은 환경 변수로 조정 가능하게 유지한다. dbt source freshness와 reliability report의 심각도를 같은 cadence model에 맞춘다.

| 도메인 | 예상 cadence | 경고 | 오류 | 비고 |
| --- | --- | --- | --- | --- |
| Weather | 3시간 | 4시간 초과 | 6시간 초과 | 한 주기 지연을 빠르게 보되, 단발성 수집 지연과 구분 |
| Traffic | 5분 | 15분 초과 | 30분 초과 | 현재 report의 15분 SLO를 경고 기준으로 승격 |

Weather의 기존 `30h/48h`, Traffic manifest의 기존 `30m/2h`는 이 값으로 정렬하는 후보다. 실제 변경 전에는 최근 dev 실행 이력에서 false-positive 여부를 확인한다.

### 7.3 Watchdog 비용 상한과 스케줄

- 기존 domain reliability report를 재사용한다. 새 공통 DAG나 전 도메인 framework는 만들지 않는다.
- Traffic watchdog는 15분, Weather watchdog는 1시간 이하의 기본 간격을 후보로 한다. 각 쿼리는 최근 필요한 partition/manifest/audit 범위로 제한한다.
- report가 전체 Bronze를 반복 스캔하는 경우, 우선 query rewrite로 대상 partition과 최신 run으로 범위를 줄인다.
- 하루 동안 watchdog가 소비한 `physical input bytes`와 `CPU time`은 각각 해당 도메인의 **동일 지표 기준 예상 일일 transform 실행량**(transform 1회 중앙값 × 일일 예정 실행 횟수)의 5% 이내를 목표로 한다. 서로 다른 단위인 bytes와 시간을 합산하지 않는다. 어느 한 지표라도 넘으면 스케줄·쿼리 범위를 다시 줄이거나 summary 원천을 사용한다.
- 알림은 상태 변화와 오류를 우선 전달하고, 자동 recollect/backfill은 수행하지 않는다.

## 8. 산출물과 회고 기록

각 benchmark 실행은 사람이 읽는 표와 기계 판독 가능한 원본을 동시에 남긴다.

- `docs/benchmarks/YYYY-MM-DD-weather-traffic-cost-proxy.md`: 전후 비교 표, fingerprint, 결론
- `artifacts/benchmarks/`: query/task 단위 원본 JSON 또는 CSV (저장 정책·용량을 확인한 뒤 필요한 것만 추적)
- `retrospective-2026-07-09.md`: 기존 본문을 보존하고 아래 항목을 새 섹션으로 추가

회고 섹션에는 반드시 다음을 포함한다.

1. 비교 대상 revision과 데이터 fingerprint
2. baseline/after의 median, 범위, 변화율
3. 측정 불가 지표와 그 이유
4. 정합성 게이트 결과
5. 유지·되돌림 결정 및 남은 리스크
6. 실제 청구액이 아닌 비용 대리 지표라는 한계

## 9. 검증 및 롤백

### 검증

- 변경 전후에 Weather/Traffic 관련 Python unit test와 dbt parse/compile을 실행한다.
- 영향받은 dbt model/test selector를 실행해 row count, unique key, publishable run, freshness/coverage 계약을 확인한다.
- benchmark suite가 동일 fingerprint임을 확인한 뒤에만 결과를 비교한다.
- `git diff --check`와 변경 파일별 regression test를 통과해야 한다.

### 롤백

- 비용 대리 지표가 개선되지 않거나 정합성이 약화되면 해당 최적화 patch만 되돌린다.
- Watchdog는 알림/보고 전용이므로, false-positive가 많으면 threshold 또는 schedule만 이전 값으로 복원한다.
- 기존 raw 데이터, Bronze manifest, recollect/backfill 경로는 이 작업에서 삭제하거나 변경하지 않는다.

## 10. 구현 순서

1. benchmark collector와 결과 스키마를 만든다.
2. 고정된 dev fingerprint에서 Weather/Traffic baseline을 수집한다.
3. 비용이 큰 상위 병목을 선정하고 최소 변경을 적용한다.
4. 동일 조건 after 측정과 정합성 검증을 수행한다.
5. reliability report의 SLO/Watchdog 쿼리를 비용 상한 내에서 보강한다.
6. watchdog 포함 after 재측정, 회고록 업데이트, 남은 제한사항 기록 순으로 마무리한다.
