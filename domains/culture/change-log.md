# culture 도메인 — 변경 로그 (change-log)

설계·구조에 영향을 준 변경만 **최신순**으로 기록한다(사소한 수정 제외).
형식: 날짜 · 무엇 · 왜 · 영향 파일. 참조는 PR/이슈 번호.

## 2026-07-03 — culture_transform DAG (#103)

- **Asset 트리거 dbt 변환 DAG 신설** (#103) — `schedule=[Asset("iceberg://culture/bronze")]`로
  #102가 심은 **load_bronze outlet을 구독**해 자동 기동. bronze가 실제 갱신됐을 때만 변환이 돌아,
  cron이 낡은 bronze 위에서 헛도는 **우연 결합**을 제거. → `culture_transform.py`
- **체인 `dbt_source_freshness → dbt_seed → dbt_run → dbt_test`** — freshness를 맨 앞 게이트로
  둬 계약(에러 48h)을 실측하고 **error면 여기서 멈춘다** → 낡은 입력으로 silver/gold를 오염시키는
  대신 수집부터 고치게 신호. seed(sema_branch_gu)는 매 run 멱등, run이 silver 9종+gold 3종을 빌드.
- **논리 Asset URI 단일 진실 원천** — outlet(#102)·schedule(#103)이 `config.py`의
  `CULTURE_BRONZE_ASSET`을 공유(target 무관). → `culture_ingest/common/config.py`

## 2026-07-03 — fetch/load 태스크 분리 (#102)

- **DAG를 `plan → fetch_raw ×12 → load_bronze → report`로 재배선** (#102) — 실시간 API 응답은
  재현 불가라 **raw 박제까지를 fetch_raw**로, raw에서 재생 가능한 bronze Iceberg 적재를
  **load_bronze** 단일 태스크로 분리. bronze만 깨진 run은 API 재호출 없이 load_bronze만
  재시도/backfill(population #94와 동일 패턴). → `culture_bronze.py` · `source/ingest.py`
- **`write_iceberg` 파라미터 삭제** — bronze 적재가 옵션이 아닌 **매 run 상시 경로**가 됨.
  CLI `--write-iceberg`는 fetch→load 순차 실행으로 유지. → `culture_bronze.py` · `scripts/`
- **load_bronze에 Asset outlet** — 성공 시 Asset 갱신으로 culture_transform(dbt, 후속 #103)
  자동 기동. all_done이라 부분 실패 run도 성공분으로 발행. → `culture_bronze.py`
- **run 리포트에 `load_failed` 반영** — fetch가 성공해도 bronze 미갱신이면 `slo_passed=false`
  + Discord에 명시 표기(초록 리포트 뒤 침묵 방지). → `source/ingest.py` · `common/notify.py`

## 2026-07-02 — 팀 컨벤션 정렬 (리네임 2건)

- **DAG 파일/ID `culture_bronze_ingest` → `culture_bronze`** (PR #73) — 팀 DAG 네이밍 규칙
  `<domain>_<dataset>_<stage>` 정렬(#74). 이 문서들의 과거 항목이 말하는
  `culture_bronze_ingest.py`는 현재의 `culture_bronze.py`다. → `culture_bronze.py`
- **env 키 `SEOUL_OPENAPI_KEY` → `SEOUL_API_KEY_CULT`** (PR #70) — 도메인별 서울 API 키
  네임스페이스 통일. → `source/config.py`

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
