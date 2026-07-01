# culture 도메인 — 변경 로그 (change-log)

설계·구조에 영향을 준 변경만 **최신순**으로 기록한다(사소한 수정 제외).
형식: 날짜 · 무엇 · 왜 · 영향 파일. 참조는 PR/이슈 번호.

## 2026-07-01 — 멘토 코드리뷰 4건 (수집 신뢰성·안전성)

- **run_report coverage 분모를 plan 기준으로** (#39, PR #40) — 실패 데이터셋이 리포트에서
  사라져 coverage가 늘 ~100%로 보이던 문제. `_report`가 `plan` 태스크 출력으로 `expected`를
  잡고, 예외로 사라진 실패를 실패 summary로 복원. → `culture_bronze_ingest.py`
- **수집 target 검증(fail-closed) + 기본값 dev 통일** (#41, PR #42) — `{dev,prod}` 외 값이
  조용히 prod로 새던 fail-open 제거. `normalize_target` 추가, `_plan`에서 fail-fast.
  → `common/config.py` · `common/warehouse.py` · `source/ingest.py` · `culture_bronze_ingest.py`
- **bronze INSERT 분할을 문자 수 → UTF-8 바이트로** (#21, PR #43) — 한글 `record_json`이 문자
  기준으론 한도 미만이어도 UTF-8 바이트로는 Trino 쿼리 한도를 넘던 문제. → `common/warehouse.py`
- **SeoulClient.list_pages 첫 윈도우도 max_rows 존중** (#44, PR #45) — 첫 페이지가 항상
  1~1000이라 샘플/드라이런 행수 제어가 안 되던 문제. → `source/clients.py`

## 2026-06-30 — 수동 트리거 호환 + 대용량 INSERT

- **data_interval_end KeyError 폴백** (#32, PR #33) — Airflow 3 수동 트리거는 data interval이
  없어 `_plan`/`_report`가 KeyError. `_interval_end` 헬퍼로 `run_after`/now 폴백.
  → `culture_bronze_ingest.py`
- **bronze 로더 INSERT를 SQL 길이 기준 배치** (#21, PR #22) — 큰 `record_json`에서
  `QUERY_TEXT_TOO_LARGE` 회피(이후 #43에서 바이트 기준으로 정정). → `common/warehouse.py`
- **raw → bronze Iceberg 로더 (Trino HTTP)** (#17, PR #18) — R2 raw를 파싱해
  `iceberg[_dev].culture.bronze_*`에 적재하는 `BronzeWarehouse` 추가.
  → `common/warehouse.py` · `source/ingest.py`

## 2026-06-29 — 초기 프레임워크 + DAG

- **bronze 원본 적재 프레임워크 + Airflow DAG** (#8, PR #15) — `culture_ingest` 패키지
  (common/source 계층), 12데이터셋 레지스트리, `plan→map→report` DAG, 수집 계약 v0,
  R2 파티션 규약. → `domains/culture/` 전체
