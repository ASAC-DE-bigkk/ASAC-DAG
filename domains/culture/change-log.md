# culture 도메인 — 변경 로그 (change-log)

설계·구조에 영향을 준 변경만 **최신순**으로 기록한다(사소한 수정 제외).
형식: 날짜 · 무엇 · 왜 · 영향 파일. 참조는 PR/이슈 번호.

## 2026-07-08 — bronze pyiceberg 직접 적재 (엔진 스위치) (#203)

- **26분 병목 근본 해법** — Trino `INSERT VALUES`의 커밋 고정비(1행도 4~7초)를 우회.
  `PyicebergBronzeWarehouse`가 Arrow 로 데이터셋당 **append 1회**(커밋 1회) — 26분→2~3분
  추정 + 스냅샷 216→12/일(옛 metadata.json 증식 동시 해소). 컬럼 계약(_COLUMNS)·멱등
  (ingest_ts delete-then-append)은 Trino 경로와 동일 → silver/gold·조회 무영향.
  → `common/warehouse.py`
- **엔진 스위치(롤백 레버)** — `build_warehouse`가 env `CULTURE_BRONZE_ENGINE`로 분기,
  **기본 trino**. pyiceberg 는 opt-in(이미지 준비+검증 후 전환). commerce 패턴 차용.
  → `source/ingest.py`
- **lazy import(파싱 안전)** — pyiceberg/pyarrow 를 함수 안에서만 import → 이미지에
  pyiceberg 없어도 모듈 import·DAG 파싱 무손상(컨테이너 실측 import_errors 0). weather
  `RestCatalog` 패턴 재사용, R2_DATA_CATALOG_* env 기존.
- **인프라 의존성은 별도** — `Dockerfile.airflow`에 `pyiceberg[s3fs]` 추가(sample 인프라
  레포, 멘토 승인)는 이미지 리빌드가 공유 액션이라 분리. 이미지 준비+행수 대조 후 엔진 전환.

## 2026-07-07 — HTTP 전송 계층 common.http 전환 (#152)

- **culture 가 루트 `common/http`(#78) 소비자로** (#152) — 6/6 도메인 완성, 마지막 잔여 중복 해소.
  KOPIS = `HttpCore`+`QueryKey`(키가 URL 문자열에서 사라져 #144 노출 표면 자체 제거),
  서울 = `SeoulOpenApiClient`(PathKey) 합성. 예외는 `requests.HTTPError` →
  `HttpProblemError`(자체 redact + #77 typed 적재). → `source/clients.py` · `common/http.py`
- **재시도 분담 정리** — 429/5xx·연결 오류 = core(backoff+jitter+Retry-After, 기존 1회보다
  강화) / **자정 rate-limit 400 1회 재시도(#146)만 도메인 잔류**(core 는 400 을 정당하게
  비재시도). 페이징·probe(#147)·오버슛=끝(#84)·행 카운트는 culture 유지. 테스트 스텁은
  session → **Transport 경계**로 이전(진짜 HttpCore 통과). detail 단건 관용 경계에
  `HttpProblemError` 추가. → `source/ingest.py` · `tests/`

## 2026-07-07 — 자정런 전멸 핫픽스 + DAG 배선 정적 검증 (#182)

- **culture_bronze import 누락 수정** (#182) — #148이 `_plan`에 `load_baselines_for_target`
  호출을 넣으며 import는 `load_baselines`로 남겨 7/7 자정런이 NameError로 전멸(bronze 12테이블
  0행, report가 all_done 리프라 run은 success로 위장). → `culture_bronze.py`
- **회귀 그물: DAG 전역 이름 배선 정적 검증** — DAG 파싱은 함수 몸통을 실행하지 않아 이 부류
  ("import한 이름 ≠ 호출한 이름")를 못 잡는다. 호스트 pytest(airflow 없음)에서 소스를 compile만
  하고 바이트코드 LOAD_GLOBAL ⊆ 모듈 바인딩∪builtins 를 검사 — culture DAG 3파일 전부 커버.
  → `tests/test_dag_global_wiring.py`

## 2026-07-06 — silver/gold 재설계 완료 (ASAC-DBT#50 · PR ASAC-DBT#52)

- **silver 9모델 + gold 3마트 재구축** — #48 공통축 canonical(공간 5컬럼·KST 시간·계보 사전) 전면 적용.
  dedup 은 `load_date desc` 우선(proxy/백필 관측 역전 방지 — 7/1 proxy 라이브 검증). 설계·계획 문서는
  `docs/design/2026-07-06-culture-silver-gold-redesign*.md`.
- **culture_transform 에 `--exclude package:asac_axes`** — 패키지 자체 dim 이 타 레포 로더(#154) 의존이라
  미적재 환경에서 ERROR → culture 는 자기 모델만 빌드/테스트(패키지 seed·매크로·제네릭 테스트는 사용).
  → `culture_transform.py`
- **datasets.py scd2_dim 주석 갱신** — silver v1 은 최신본 dim 보류(설계 §3-C), bronze 박제로 소급 가능.

## 2026-07-06 — culture_maintenance DAG (#157)

- **주간 Iceberg 유지보수 DAG 신설** (#157) — `maintain >> storage_cleanup`, 일요일 04:30 KST
  (population 04:00·_shared 04:00과 시차). 대상 = bronze 12(레지스트리 파생) + **silver 9·gold 3
  선등록**(재설계 전 'skipped (missing)', 테이블 생기면 자동 편입). → `culture_maintenance.py`
- **maintain은 자체 HTTP 클라이언트로** — #155 `_shared/maintenance.py`와 같은 작업(optimize/
  expire_snapshots/remove_orphan_files)이지만, Airflow 이미지 전 컨테이너에 `trino` 패키지가
  없어(실측) _shared의 `trino.dbapi`가 ImportError → `TrinoClient`(HTTP)로 우회. retention은
  SQL 삽입이라 형식 강제(`^[0-9]+[dhm]$`). → `culture_ingest/common/maintenance.py`
- **storage_cleanup(#156 population 적응)** — R2 카탈로그가 원리상 못 잡는 ①옛 metadata.json
  증식(delete-after-commit 무효) ②버려진 디렉터리(`__dbt_tmp` — 7/6 수동 GC 102폴더의 자동화판)
  를 boto3로 정리. 판정은 순수 함수 `_classify`(테스트 대상), culture UUID 프리픽스로 범위 제한,
  `dry_run` 지원. **dev 실증: 옛 metadata 760개/51MB 정리, 2회차 0건(멱등), 테이블 무손상.**

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
