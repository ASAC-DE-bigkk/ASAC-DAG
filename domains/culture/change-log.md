# culture 도메인 — 변경 로그 (change-log)

설계·구조에 영향을 준 변경만 **최신순**으로 기록한다(사소한 수정 제외).
형식: 날짜 · 무엇 · 왜 · 영향 파일. 참조는 PR/이슈 번호.

## 2026-07-09 — KOBIS 일별 박스오피스 bronze 편입 (#197)

- **신규 소스 `kobis`** — 영화진흥위원회 오픈API `searchDailyBoxOfficeList`(JSON,
  단일 GET 스냅샷). `KobisClient`(HttpCore + `QueryKey("key")` — 키 URL 미노출 #144),
  faultInfo→`KobisError`(redact). → `source/clients.py`
- **데이터셋 2벌** — `kobis_boxoffice_nation`(전국) + `kobis_boxoffice_seoul`
  (`wideAreaCd=0105001`). "전국 집계를 서울 소비 온도로" 프록시 오류를 상영지역
  필터로 보정 — 진짜 서울 영화소비 시계열 축. 실측: 같은 날 전국 1위≠서울 1위.
  snapshot_append · min_rows=5(top10 고정) · volume 0.5. → `source/datasets.py`
- **targetDt=load_date−1**(전일 확정분) — ingest `kobis_boxoffice` 분기가 실행일에서
  계산. `parse_records` kobis 분기(`boxOfficeResult.dailyBoxOfficeList`) 신규.
  `SourceKeys.kobis`(`KOBIS_SERVICE_KEY`) 필수화 · `Clients.kobis` 배선. →
  `source/config.py` · `common/records.py` · `source/ingest.py`
- 테스트 신규 4파일(client·records·dataset·redaction surface) — 디스패치 그물에
  `res.error==""` 단언(#196 params 실버그 재발 방지). SLO expected 13→15 자동 반영.
  → 설계 `docs/design/2026-07-09-culture-kobis-boxoffice-bronze{,-plan}.md`

## 2026-07-09 — KCISA 한눈에보는문화정보 서울 행사 bronze 수집 (#196)

- **신규 소스 `kcisa`** — 국립기관 최신 전시 구멍(서울 API 자발등록 사각) 보강. `KcisaClient`
  (HttpCore+QueryKey("serviceKey"), 키 URL 미노출) + `kcisa_seoul_event`(area2, sido=서울,
  현재 활성 스냅샷 ~498). 빈 페이지=끝(KOPIS 400 오버슛과 대조). `parse_records` XML 분기 공용화.
  → `source/clients.py` · `source/datasets.py` · `source/ingest.py` · `common/records.py` · `source/config.py`
- **계약**: min_rows 300 실측 하한, volume_drop 0.7. 좌표 gpsX/gpsY·sigungu 내장(silver 지오코딩 불요).
- silver 편입(seq dedup·seoul_cultural_event 중복·gold)은 후속 PR. 설계:
  [docs/design/2026-07-09-culture-kcisa-event-bronze.md](docs/design/2026-07-09-culture-kcisa-event-bronze.md)

## 2026-07-09 — culture_bronze 스케줄 자정→03:00 KST 이동 (#201)

- **자정 400 창 이탈** — KOPIS 가 자정 직후 00:00~00:02 창에서 간헐 400 을 뱉는다
  (cause B, 시간의존·낮/새벽엔 정상, 4차 재발까지 관찰). 방어 3겹(#146 재시도·#147 HWM·
  retries=2)이 흡수 중이지만 매 자정런이 헛재시도를 한 번씩 사고, 400 창이 retries 총
  ~4분보다 길어지는 날은 전멸 위험. `@daily`(자정) → `0 3 * * *`(03:00 KST)로 옮겨
  노출 자체를 제거 — 가장 값싼 완화(#201 후보 ③). → `culture_bronze.py`
- **무영향 근거** — freshness SLA 30h 라 시각 여유 충분, 하류 `culture_transform` 은
  asset 트리거(고정 시각 의존 없음). cron 은 DAG 타임존(KST) 해석. 회귀 그물
  `test_bronze_schedule.py`(자정 스케줄 재도입 차단, airflow 없이 소스 검증).

## 2026-07-08 — #206 facility 상세 주간 크롤 분리

- `Dataset.refresh`("daily"/"weekly") 추가, `kopis_facility_detail`을 weekly로 —
  자정런 제외(KOPIS -200콜/일, #201 압력↓). 선택 로직은 `plan_dataset_names`로
  추출(airflow 없이 테스트 가능). → `source/datasets.py` · `culture_bronze.py`
- `culture_facility_refresh` DAG 신규(일 05:30 KST) — culture_bronze를 목록+상세
  전수(`max_detail=2000`)로 트리거. 커버리지 200/1,686(11.9%)→전량.
- `load_baselines` 다중 리포트 병합(최신 5건) — 부분 run(주간·백필) 리포트가
  다음 자정런 볼륨 HWM을 가리던 결함 수정. → `source/ingest.py`
- 설계: [docs/design/2026-07-08-culture-facility-weekly-refresh.md](docs/design/2026-07-08-culture-facility-weekly-refresh.md)

## 2026-07-08 — load_bronze 진행 로그 (26분 블랙박스 해소) (#202)

- **26분 블랙박스 해소** — 7/8 실측: load_bronze = run 30분의 87%, 원인은 Trino INSERT
  커밋 고정비(~216쿼리 × 평균 7.2초, 1행 INSERT도 4~7초). 그런데 시작~끝 사이 진행
  로그가 0줄이라 어느 데이터셋이 병목인지 안 보였다. 배치(=Iceberg 커밋)마다 진행
  로그 + 데이터셋별 완료 로그(행수·소요) 추가 — 세종 88MB≈13분 병목이 눈에 보인다.
  → `common/warehouse.py` · `source/ingest.py`
- **속도 개선은 #203(pyiceberg)에 위임** — 병렬화(ThreadPool, 26분→~13분)를 초안에
  넣었다가 뺐다: pyiceberg 직접 write(#203)가 26분→2~3분으로 병렬화를 무의미하게
  만든다(엔진당 데이터셋 적재가 이미 수 초). 버릴 코드를 안 만들고, 이 PR 은 어느
  엔진에서도 살아남는 **관측(로그)** 만 남긴다. #203 은 이미지 의존성이 멘토 게이트.

## 2026-07-08 — 전멸 run 정직성: report 가 상류 전멸 시 스스로 실패 (#185)

- **slo_passed 공허 참 교정** — plan 전멸이면 expected=0 이라 "실패 0"이 공허하게
  참이 되어 7/7 사고 리포트가 `slo_passed=true` 로 나왔다. 기대가 없으면 통과도
  없다(`expected_total > 0` 조건 추가). Discord embed 색도 이 판정을 따라 빨강으로.
  → `source/ingest.py`
- **report 리프의 success 위장 차단** — report(all_done)가 리포트 저장·알림 발송을
  마친 **뒤**, 전멸(`expected==0` 또는 `landed==0 and failed>0`)이면
  `AirflowFailException` 으로 스스로 실패해 run 을 UI 에서 빨갛게 만든다(재시도 없음
  — 재시도해도 결과 동일 + 알림 중복 방지). 부분 실패·전부 의도적 skip 은 기존 동작
  유지. 판정은 순수 함수 `annihilation_reason(coverage)` 로 분리해 단위 테스트.
  → `culture_bronze.py` · `source/ingest.py` · `tests/test_report_annihilation.py`

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
