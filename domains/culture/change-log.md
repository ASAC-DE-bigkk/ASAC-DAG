# culture 도메인 — 변경 로그 (change-log)

설계·구조에 영향을 준 변경만 **최신순**으로 기록한다(사소한 수정 제외).
형식: 날짜 · 무엇 · 왜 · 영향 파일. 참조는 PR/이슈 번호.

## 2026-07-29 — 첫 volume_hwm 쓰기를 레거시 리포트로 부트스트랩 (#582)

- **결함** — `write_volume_hwm` 의 첫 쓰기는 `previous={}` 라 **이번 run 에 rows>0 인
  데이터셋만** 남는다. 누적 장부는 두 번째 쓰기부터 "부분 run 이 최신이어도 나머지는
  과거에서 보충된다"는 성질을 갖지만, 첫 쓰기에는 물려받을 과거가 없다.
- **왜 조용히 굳나** — `load_baselines` 는 HWM 이 **비어 있을 때만** 레거시로 폴백한다.
  14개짜리 장부는 "비어 있지 않음"이라 폴백이 다시 안 타고, 빠진 데이터셋의 볼륨
  가드(#147)가 꺼진 채로 굳는다. fail-open 이라 로그 한 줄 외엔 아무 신호도 없다.
- **하필 걸리기 쉬운 데이터셋** — `kopis_facility_detail` 은 야간 top-up 대상이 0건이면
  `skipped`(rows=0)로 끝나는 게 **정상 경로**다(#466·#562). dev 실측에서 최근 4개 리포트 중
  2개가 그랬다(7/26·7/28 rows=0 / 7/25 1700 · 7/27 3).
- **해법** — `previous.datasets` 가 비면 `_load_baselines_legacy` 로 부트스트랩. 레거시
  리포트를 정리한 뒤에는 `{}` 를 돌려주므로 자연히 no-op 이 된다(폴백 제거의 전제).
  대안인 "`load_baselines` 를 `{**legacy, **control}` 병합"은 드물게만 rows>0 인
  데이터셋 때문에 옛 리포트를 오래 남겨야 해서 정리가 늦어진다. → `source/ingest.py`
- **dev 는 수동 재시딩 완료** — 레거시 5건 병합(15개)으로 채워 레거시와 차이 0.
  prod 는 아직 HWM 이 없어 이 수정이 첫 런에 적용된다. → `tests/test_reports_ops_zone.py`(+2)

## 2026-07-29 — run 리포트를 raw 밖으로, 볼륨 HWM 은 control 존 분리 (#579)

- **`write_run_report` 목적지 이동** — `raw/culture/_reports/load_date=…` →
  `ops/reports/culture/observed_date=…`. raw 는 "영구 보존·이동 금지" 구역(#60 약속 ②)이라
  관측 산출물이 거기 살면 그 규칙이 가변물까지 영구 보존한다. 날짜 키가 `observed_date` 인
  이유는 이 파일의 날짜가 원본 수신일이 아니라 **관측일**이고 자동 삭제·감사가 그 기준으로
  돌기 때문. → `source/config.py`(존 상수 3종) · `source/ingest.py`
- **리포트를 통째로 옮기면 안 되는 이유 = `run_report.json` 의 역할이 둘** — SLO·대시보드가
  읽는 관측 기록이면서 동시에 **#147 볼륨 가드의 기준선(HWM) 공급원**이다. `ops/reports/` 는
  TTL 대상 구역이라 거기서만 기준선을 읽으면 **lifecycle 이 걸리는 순간 가드가 조용히
  꺼진다** — `load_baselines` 가 fail-open 이라 에러도 안 나고 로그 한 줄만 남는다. #60 이
  `ops/control/` 을 "HWM·커서·diff 기준"이라 콕 집어 쓴 게 이 경우다.
- **`write_volume_hwm` 신설** — `ops/control/state/culture/volume_hwm.json` 단일 최신본
  (누적 장부). 데이터셋별 "가장 최근 성공 run 의 rows". 리포트 5건을 훑던 종전 방식과 결과는
  같지만 **창이 없어** 부분 run 이 연속돼도 기준선이 밀려나지 않는다. 두 가지를 일부러 안
  한다: ⓐ 에러난 데이터셋 미반영(실패 런 부분 rows 가 기준선을 끌어내리면 다음 날 진짜
  급락이 정상으로 보인다) ⓑ 과거 `ingest_ts` 는 기존 값을 덮지 않음(백필·재실행 방어).
- **과도기 dual-read** — `load_baselines` 는 control HWM 우선 → 없으면 옛 리포트 스캔 폴백.
  없으면 **전환 첫날 볼륨 가드가 통째로 꺼진다.** `scan_new_reports` 는 신·구 prefix 양쪽을
  훑고 배치 안에서도 `ingest_ts` 로 중복을 접는다 — 같은 리포트가 두 존에 있는 상황이
  실제로 있다(prod 승격 중 복사된 61건). → `slo/loader.py` · `culture_bronze.py`
- **기존 62건은 이동 0건** — #60 "기존 객체 이동 0건", 전환은 새 쓰기부터. 물리 이사 대상으로
  명시된 건 가변 상태 3건(`_diff_target`·`_backup`·`_checkpoints`)뿐이고 culture 리포트는
  거기 없다. → `tests/test_reports_ops_zone.py`(11건), `docs/storage.md`, `docs/architecture.md`

## 2026-07-29 — 완결 확인서에 #60 필수 6필드 (#577)

- **`_manifest.json` 에 `completed_at`·`status`·`expected_count`·`actual_count` 추가.**
  ASK-Seoul#60 약속 ③ R2 가 정한 필수 6필드 중 3개가 비어 있었다 — dev·prod raw 전수
  실측(각 23,763객체) 결과 **확인서 561건 전부** 동일하게 누락. `rows`·`pages`·`bytes` 는
  실제값만 있어 "기대 vs 실제" 대조가 안 됐고, `completed_at`·`status` 부재로 R1(확인서 =
  완료 표시)의 근거가 **파일 유무 하나**뿐이었다. → `source/ingest.py`
- **`status` 가 필요한 진짜 이유** — 확인서는 위반이 있어도 쓰인다. 볼륨 급락(#147)은
  확인서를 **쓴 뒤** `result.error` 로 승격되므로, 확인서가 있으면서 그 run 은 실패인
  랜딩이 실제로 남는다. 즉 "확인서가 있다 = 온전하다" 가 성립하지 않는다. 위반 목록은
  확인서 작성 **전에** 확정돼 있어(`evaluate_landing`) 정확히 판정 가능.
- **`expected_count` 를 원천 총계로 두지 않았다** — 서울 openapi 의 `list_total_count` 는
  신뢰 대상이 아니다(#147: 실제 19,377행에 3,925를 `INFO-000` 으로 반환 → 80% 조용한
  누락). 그 값을 기대치로 삼으면 검증 장치가 거짓말을 정답으로 삼는 구조가 된다. 그래서
  기대는 **계약 하한(`min_rows`) + 직전 good 런(HWM)** 으로 정의하고, 확인서가 그 정의를
  자기 안에 밝힌다. 같은 재료가 `checks` 에도 있지만 일반 소비자가 culture 의 checks
  스키마를 몰라도 읽을 수 있게 최상위로 승격. → #60 확정 시 문구 수정 제안 예정.
- **추가만 · 소급 수정 0건** — 확인서를 읽는 기존 소비자 2곳(`_ids_from_landed_list`,
  prod 백필 스크립트)이 쓰는 필드는 그대로. 기존 561건은 손대지 않는다(#60 "기존 객체
  이동 0건"과 같은 원칙). → `tests/test_manifest_contract.py`(6건), `docs/storage.md`
- **`finished_ts` 재사용 불가** — `DatasetResult.finished_ts` 는 `ingest_dataset` 의
  `finally` 에서 채워져 확인서 조립 시점엔 비어 있다. 확인서 작성 시점에 직접 찍는다.

## 2026-07-29 — culture_transform 체인 맨 앞에 dbt deps (#564)

- **`dbt_deps` 태스크 신설** — 체인이 `dbt_deps → dbt_source_freshness → dbt_seed →
  dbt_run → dbt_test` 가 됐다. dbt 는 `packages.yml` 선언 수와 `dbt_packages/` 설치 수가
  어긋나면 **파스 단계**에서 죽어(`dbt found N package(s) specified ... but only M
  installed`) 네 태스크가 전부 시작조차 못 한다. → `culture_transform.py`
- **어긋나는 경로가 둘** — ① `packages.yml` 에 패키지를 추가하는 PR(대기 중인 ASAC-DBT#347
  이 서빙 계약 #346 의 복합 PK 근거로 `dbt_utils 1.3.1` 추가) ② `dbt_packages/` 유실
  (gitignore 대상이라 `git clean -fdx`·컨테이너 재생성으로 사라지는데 **아무도 다시 설치해
  주지 않았다**). ②는 #347 과 무관한 기존 취약점으로, culture 가 로컬 패키지 하나
  (`asac_axes`, 심볼릭 링크)로 버텨 와서 드러나지 않았을 뿐이다.
- **실측 근거** — scheduler 컨테이너에 culture 프로젝트를 복사해 재현: `dbt_packages` 삭제 시
  `1 specified / 0 installed`, #347 의 packages.yml 적용 시 `2 specified / 1 installed`
  로 각각 `Compilation Error`. `dbt deps` 를 먼저 돌리면 둘 다 해소되고 parse 성공.
  컨테이너에서 dbt hub API·GitHub tarball 도달 확인(HTTP 200), `dbt deps --target dev`
  플래그 호환 확인.
- **선례** — citydata `install_deps: True` + `_dbt("deps")`, traffic `dbt_deps` 태스크.
- **영향 범위** — 실패 시 `culture_transform` 전체(silver 10 + gold 14 + 계약 테스트)와
  Asset 하류 `culture_slo`. 수집(`culture_bronze`)은 무관해 데이터 유실은 없고 신선도만 정지.

## 2026-07-27 — culture D1 서빙 export DAG 신설 (#520)

- **`culture_serving_export` — 공통 Publisher factory 소비자 1호** — 외부 gold 7종을
  Serving Contract v1.1(`meta.serving`, ASAC-DBT#346)에 따라 Cloudflare D1(팀 공용
  `ask-seoul-dev-d1`)에 전량 스냅샷 게시. 스케줄 `30 4 * * *` KST(transform ~03:22 후 ·
  culture_slo 05:01 전), 계약의 `publication_trigger.schedule_cron` 과 일치(§6).
  파이프라인은 전부 공통(#505): Gate→D1 Write→Verify→`_catalog` Upsert→Smoke→자기검증.
  → `culture_serving_export.py` (도메인 쪽은 이 얇은 파일 하나)
- **윈도우(append) 방식 기각 근거** — 공통 append lookback 이 `last_good_max` 기준이라
  미래 날짜 행 13.1만을 가진 `activity_by_dong` 에서 창이 깨진다. 수정은 `common/serving`
  변경 = 멘토 게이트라, Workers Paid 예산(7종 ~28.1만/일 ≈ 8.6M/월, 포함량의 ~22%) 안에서
  전량 스냅샷(A안) 채택. → `docs/design/2026-07-27-culture-serving-export-d1.md`

## 2026-07-27 — 공연 상세 야간 안티조인 전환 (#518)

- **`kopis_performance_detail` → `missing_only_nightly`** — 야간엔 신규 공연만
  top-up. 상한 `max_detail=200`이 목록(1,200~1,300/일) 앞을 매일 자르던 탓에 27일
  누적 5,000+ 크롤이 distinct 608건에 그쳤고, 신규 id 는 평균 15건/일 — **200콜의
  약 92%가 전날 것 재수집**이었다. 상세에서 쓰는 값은 `mt10id` 하나뿐이고 608건 중
  변경 0건이라 재크롤 정보량은 0. → `source/datasets.py`
- **품질 영향 없음 근거** — #206 으로 시설 좌표가 전량 확보돼 이름 폴백이 상세
  미크롤분을 받는다(`facility_match`: name 1,487 · detail_id 608 · 미연결 1 =
  좌표 99.95%). 이 변경은 품질 수정이 아니라 **#201 400 의 호출 표면 축소**다.
- **known-id 를 데이터셋별로 조회** — `load_known_detail_ids` 신규. 종전엔
  `load_existing_detail_ids(target)` 를 기본 인자로 1회 호출해 모든 top-up 데이터셋에
  시설 `mt10id` 집합을 공유했다. 공연에 그대로 켜면 교집합 0 → 목록 전체가 신규 판정
  → cap 이 차집합 뒤라 앞 200건 재크롤 유지 = **절감 0 인 조용한 no-op**. 웨어하우스는
  1회 생성해 재사용, fail-open 은 데이터셋 단위. → `source/ingest.py` · `culture_bronze.py`
- **부수 효과: 커버리지 회복(라이브 실측)** — 절감만 예상했으나 안티조인이 앞자르기가
  한 번도 닿지 않던 **목록 후미**를 집어낸다. 전환 시점 목록 1,260건 중 크롤 이력은
  608건뿐(백로그 652건)이라 첫 런은 200건을 긁었고 누적이 808건이 됐다. 하루 200씩
  3~4일이면 목록 전량 → 상세 커버리지 29%→~100%, `facility_match` 가 이름 폴백에서
  `detail_id`(더 신뢰 가능한 경로)로 이동. 절감 효과는 백로그 배수 후부터 나타난다.
- 회귀 그물: `tests/test_detail_topup.py` — 데이터셋별 `id_field` 조회 · 집합 누출 ·
  개별 fail-open · 공연 안티조인/무신규 skip.

## 2026-07-17 — SLO dag_run enrichment v2: PostgresHook + Connection (#411)

- **`load_dag_runs` 접속 획득 교체** — v1 의 `create_engine(env)` 는 Airflow 3
  태스크 격리(#303)가 env 를 차단해 상시 스킵이었다(→ `bronze_culture_dag_runs`
  0행 → 7/17 freshness 사고 DBT#238). v2 는 태스크 허용 경로인
  `PostgresHook(postgres_conn_id="airflow_metadb").get_sqlalchemy_engine()` 로
  교체 — SQL·14일 윈도우 멱등 쓰기는 무변경, Connection 미등록 시 스킵 안전망 유지.
- **Connection 은 CLI 1회 등록**(컨테이너 env 재사용, 접속 문자열 무노출) —
  재등록 절차는 `docs/operations.md` 런북. 공유 인프라(compose) 무수정.
- **배포 순서 게이트**: 적재 확인 후에만 DBT freshness 재활성(별도 PR,
  `from_iso8601_timestamp(start_at)` 교정 포함) — 빈 테이블 감시 금지(#238 교훈).
- 영향: `culture_ingest/slo/io.py`, `tests/test_slo_io_wiring.py`(신규),
  `docs/operations.md`, `docs/design/2026-07-17-slo-dag-run-enrichment-v2.md`.

## 2026-07-16 — culture_slo DAG + SLO bronze 로더 (#257)

- **신규 `culture_slo` DAG (05:00 KST)** — run_report(R2 `_reports/`)와 Airflow
  `dag_run`(메타DB, culture 4 DAG)을 SLO bronze 2표로 흘린다. 본류 03:00 bronze →
  ~04:00 transform 뒤·05:30 facility_refresh 앞 슬롯. Asset outlet 없음(스케줄
  구동 — 본류 무수정 원칙 §3).
- **로더** — `culture_ingest/slo/{loader,io}.py`. 순수 로직(스캔·행빌드)은 loader
  (호스트 pytest), 부수효과(트리노 write·Airflow meta 읽기)는 io. `bronze_culture_run_report`
  는 warehouse 엔진 디스패치(trino) 재사용(리포트별 `ingest_ts` 멱등), `bronze_culture_dag_runs`
  는 타입드 별도 CREATE + 14일 윈도우 delete+insert(첫 실행은 전체 이력). 로더는
  도메인 파라미터화(§6.2 `_shared` 승격 대비).
- **본류 한 줄 수정** — `culture_transform` dbt `run`/`test` 에 `--exclude ... tag:slo`
  추가(SLO 모델 소유권 = culture_slo, 야간 transform 이 미존재 소스 빌드하다 깨지는 것 방지).
- 영향: `culture_slo.py`(신규), `culture_ingest/slo/`(신규), `culture_transform.py:86,90`.
  dbt 마트(silver 3 + gold_culture_slo_daily)는 ASAC-DBT#110. 라이브 검증·머지는 게이트 후.

## 2026-07-14 — DeadlineAlert 침묵 감시 (#259)

- **#259 culture_bronze 침묵 감시** (2026-07-14): 기존 알림은 전부 "실패한 run"
  반응형이라 "run 이 없거나 끝나지 않는" 침묵(행·스케줄러 정지·지연 폭주)이
  사각지대였다. `DeadlineAlert(DAGRUN_QUEUED_AT, 2h, SyncCallback)` 층 추가.
  콜백은 스파이크 실측 제약(콜백 호스트 sys.path = plugins 뿐)에 따라 dags 루트
  `plugins/culture_deadline.py` 에 **자기완결형**(stdlib urllib + env 웹훅,
  `CULTURE_DISCORD_WEBHOOK_URL`→`DISCORD_WEBHOOK_URL` 폴백)으로 배치 — culture
  Notifier·common.discord 재사용 불가. 전제 인프라 = ASK-Seoul#23 plugins compose
  마운트(머지됨). `culture_transform` 은 보류: DAGRUN_QUEUED_AT 은 run 생성 후
  카운트라 "asset 트리거 자체가 안 옴"은 못 잡고, 그 침묵은 상류 bronze 감시가
  커버한다. 영향: `culture_bronze.py`, `plugins/culture_deadline.py`,
  루트 `.airflowignore`(plugins 스캔 제외, 신설).

## 2026-07-09 — bronze pyiceberg 직접 write 전환 (#203)

- **#203 bronze pyiceberg 직접 write 전환** (2026-07-09): Trino `INSERT VALUES`
  (SQL 텍스트 운반·800KB 배치 상한 → 216커밋/일·load_bronze 26분)를 pyiceberg
  `delete+append` 트랜잭션(데이터셋당 커밋 1회)으로 교체. `engine` 파라미터
  (기본 pyiceberg, trino=롤백 레버 — 자정런 7회 연속 성공 후 일몰), R2 Data
  Catalog REST 설정 `build_catalog_settings`(+토큰 redaction #144). 스냅샷
  216→12/일 — culture_maintenance 가 지우던 옛 metadata.json 의 생산자 제거.

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
  넣었다가 뺐다: pyiceberg 직접 write(#203)가 26분→약 5분(dev 실측 303s, 2026-07-09)으로
  병렬화를 무의미하게 만든다(엔진당 데이터셋 적재가 이미 수 초). 버릴 코드를 안 만들고, 이 PR 은 어느
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
