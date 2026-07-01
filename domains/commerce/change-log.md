# Change Log — commerce 번들

`dags/domains/commerce/` 의 변경 이력. **작성일·순서 기준 내림차순**(최신이 위).
각 항목은 **최종 반영된 형태**를 기준으로 정리한다(중간에 되돌린 단계는 합쳐서 기술).

---

## 2026-07-02

### 18. 재수집 규칙 변경 — 동일자 성공분 제외·KST 일자 가드·한 파일 관리 (feat/59-recollect-rule-change)
request:
- **동일자 수동 재실행**: 같은 날짜에 수동 실행 이력이 있으면, 실행 전에 **이미 성공한 API 는 제외**하고
  수집한다.
- **recollect run_id 관리**: 실패분을 재수집할 때 (1) run_id 를 동일하게 맞춰 재수집하거나, (2) 기존 실패
  파일을 삭제하고 별도 run_id 로 재수집하여 **하나의 파일로 관리**되게 한다.
- **KST 일자변경 가드**: recollect 라도 **한국시간(UTC면 보정, KST면 그대로) 기준 일자가 바뀌면** 사실상
  다른 일자 정보라서 그 정보는 재수집하지 않는다.
- 브랜치 feat/59-recollect-rule-change (feat/58 기반).
response:
- **markers.py**: `run_date`(run_id→KST 날짜), `completed_shorts_on_date`,
  `plan_excluding_same_day_completed`(동일 KST 일자 completed 제외),
  `recollect_targets_same_day`(최근 run 의 incomplete 중 **KST 오늘과 같은 날짜만**, 날짜 바뀌면 빈 리스트),
  `cleanup_incomplete`(성공 run 제외 **같은 KST 일자** 실패 파편 삭제 — option2 한 파일). `common/storage.py`
  에 `delete` 추가(ABC/Local/R2).
- **DAG 배선**: daily `plan_all_targets`→동일자 성공분 제외, recollect `find_incomplete_targets`→KST 가드,
  `ingest_one`→성공 시 같은 일자 실패 파편 `cleanup_incomplete`(한 파일).
- **검증**: 단위테스트 **6 통과**(run_date·동일자 제외·전날 미제외·KST 가드·cleanup 유지/타일자 보존). DAG 구문 OK.
- **미검증(정직)**: end-to-end 는 실제 DAG 실행(Airflow) 필요 — 오프라인 단위테스트까지.

### 17. bronze 증분화 코어 — 정렬·검증키·diff (feat/58-commerce-bronze-sort-diff)
request:
- bronze 수집이 매 실행 전체를 다시 받는 문제를 해소한다. 각 API 파일에서 UPDATEDT 존재를 먼저
  확인하고(모두 있으면) **UPDATEDT 내림차순 row 정렬**(날짜가 숫자가 아니면 숫자키로 치환). 정렬은
  **전량 RAM 금지 → 파일 단위 스트리밍**, 퀵정렬보다 낮은 Big-O 가 있으면 그 방식. 저장 포맷은
  **row-NDJSON(UPDATEDT desc)로 전환**. **API 단위 해시 검증키**를 만들고, 수집 파일 삭제 로직은
  재검증 통과 뒤 **맨 마지막(step4)**에만 적용. 브랜치 feat/58-commerce-bronze-sort-diff.
- (합의 Q&A) **정렬 알고리즘** = 외부 병합 정렬(스트리밍·바운디드 RAM). **저장 포맷** = row-NDJSON(UPDATEDT desc).
- (합의 Q&A — 증분 저장/비교기준 모델, 크리티컬) 구 데이터 소실·버전이력 유실 방지를 위해 **2계열**:
  ① **save(증분 영구 저장)** — 첫 수집=full, 이후=신규/변경분만 누적(이력 보존, 삭제 안 함).
  ② **diff-target(롤링 최신본)** — 다음날 비교 기준. 첫 수집 시 save 와 **같은 내용으로 따로 생성**.
  매일: 오늘본 vs diff-target diff → 신규분을 save 로 증분 저장 → **diff-target 을 오늘본으로 교체(구 diff-target 삭제)**.
response:
- **-1단계 확인(실데이터)**: 39종 전부 UPDATEDT 100% `datetime` → 14자리 정수키로 정렬 가능 확인.
- **include/bronze/incremental.py**: `external_merge_sort`(청크→임시파일→heapq 병합, 스트리밍·바운디드
  RAM, O(n log n)), `verification_key`(정렬본 순서민감 sha256), `diff_new_rows`(정렬 병합 스트리밍 diff
  — 같은 키는 정규화 문자열 직접비교로 hot loop 경량). **파일 브리지**: `sort_rows_to_file`(정렬→row-NDJSON+키),
  `read_rows`, `build_increment`(첫수집=full / 동일=증분없음 / 상이=diff 신규분 — 모델 그대로 구현).
- 단위테스트 **13 통과**(정렬·순서민감키·diff 4종 + 파일브리지 first/identical/changed + orchestration
  first→identical→changed).
- **DAG 통합**: `common/paths.py`에 diff-target 경로(`_diff_target/<short>.jsonl` + `.key` 사이드카).
  `incremental_store`(스토리지 브리지: 전날 target 다운로드→비교→증분 업로드→target 롤링 교체).
  `bronze_tasks._write_bronze`가 **status==ok 일 때만** 페이지→row 파싱→증분 저장(중간 중단은 미저장),
  마커에 `verification_key/increment_mode/increment_count/sorted_row_count` 기록. page-NDJSON → row-NDJSON.
- **step0**: `seed_diff_target`(1회성 diff-target/검증키 시드). 미실행이어도 첫 수집이 self-seed 하므로 선택.
- **step4**: 본 모델은 raw 페이지가 휘발(메모리)이라 "수집 파일 삭제" 별도 대상 없음 → "미저장(status!=ok) +
  재검증"으로 갈음(단위테스트로 first/identical/changed 재검증).
- **docs**: [docs/pipeline/bronze/incremental-sort-diff.md](docs/pipeline/bronze/incremental-sort-diff.md)
  (모델·정렬·검증키·diff·수집흐름·step0·검증). 단위테스트 **14 통과**.
- 커밋·푸시(feat/58). **미검증(정직)**: DAG 배선 end-to-end 는 실수집(서울 API 호출) 필요 — 오프라인
  단위테스트까지. **다운스트림(dbt 로더) row-NDJSON 대응은 feat/58 밖**.
- (부수) CLAUDE.md 영어 통일 + Change Log Rule 에 request:/response: 규격 명시(별도 커밋).

## 2026-06-30

### 16. 보안 대응 전용 패키지 + 단일 포인트 종합검증 도입 (`include/security/`)
- **배경**: 로그/예외(특히 `requests` 네트워크 실패 메시지)에 서울 OpenAPI 인증키가 박힌 URL 이
  들어가, 로그뿐 아니라 **bronze 마커 JSON(error 필드)으로 키가 영구 저장(at-rest 누출)**될
  위험이 있었다. 그 외 흔한 공격/누출 경로(알림 전송, 경로 주입, 하드코딩 키, `.env` 추적,
  `yaml.load`/`eval`/`verify=False`/timeout 누락)도 함께 상정해 종합 대응.
- **추가**: 이식 가능한 **stdlib-only 독립 패키지** `include/security/`:
  - `redaction.py` — literal(env 시크릿 실제값) + structural(서울 URL 경로키·`Bearer`·`AKIA`·
    `secret=`/`token=` 등) **2중 마스킹**. `redact()` 는 str/dict/list/예외 재귀.
  - `log_filter.py` — `install_log_redaction()` 가 루트/airflow 로거·핸들러에 마스킹 필터 부착
    (msg/args/traceback 마스킹, idempotent).
  - `inputs.py` — `assert_iso_date`/`assert_safe_segment`(경로 주입 차단).
  - `audit.py` — 정적 점검 7종 + 런타임 자기검증(redactor/log).
  - `verify.py`(+`__main__.py`) — **단일 포인트** `run_security_verification()`/`assert_secure()`
    및 CLI `python -m security`(exit code=차단 이슈 유무).
- **적용**: DAG 는 env 적재 직후 `install_log_redaction()` 호출 + `resolve_observed_date` 에
  `assert_iso_date()`. bronze `clients.py`(예외/경고 로그)·`bronze_tasks.py`(마커 error·실패 로그)·
  `common/notify.py`(알림 message/context)에 `redact()` 적용(이중 방어).
- **검증**: 전체 단위테스트 58 통과(보안 30 신규 — 마스킹/로그필터/입력검증/정적감사 +
  **bronze 마커 at-rest 키 비노출 end-to-end**), `python -m security` 차단 이슈 0.
- **이식성**: `include/security/` 디렉터리 복사 + DAG 한 줄(`install_log_redaction()`) + 누출
  지점 `redact()` 로 타 번들/프로젝트에 일괄 적용. 시크릿은 env 이름 규칙으로 자동 식별.
- **점검/연결 구조(거버넌스)**: 에이전트(Claude/Codex)가 수시로 불러오고 적용·점검하도록 연결.
  CLAUDE.md **§20 Security Gate**(Recall/Apply 트리거/Check) + §18 Final Quality Gate 에 보안 항목 +
  §19 CLAUDE-chain 에 `security` 포함(세션 이동에도 따라옴). Share.md **§5 보안** 섹션.
  타 프로젝트 이식 가이드 `docs/security/adoption.md`(복사-붙여넣기 프롬프트 포함) 신설.
- 파일: `include/security/*`(신규), `seoul_commerce_dag.py`, `include/bronze/clients.py`,
  `include/bronze/bronze_tasks.py`, `include/common/notify.py`, `tests/test_security.py`(신규),
  `docs/security/{README,security,adoption}.md`(신규), `docs/README.md`·`Share.md`·`README.md`·`CLAUDE.md`(인덱스/규약).

### 15. silver 가공을 bronze DAG에서 분리 — DAG 라인은 원본 수집(bronze) 전용
- **배경**: `seoul_commerce_daily`/`seoul_commerce_recollect` 의 공통 흐름(`_wire`)이 bronze 수집과
  silver 적재를 한 DAG 안에 묶고 있었다. bronze 는 "원본 수집"만 담당해야 한다는 역할 경계에 맞춰
  silver 를 DAG 오케스트레이션에서 **완전히 분리**.
- **변경**: DAG 파일에서 `from silver import silver_tasks` 임포트, `build_silver_one` 태스크,
  `_wire` 의 `build_silver_one.expand(...)` 결선을 제거. 흐름은
  `… → ingest_one.expand → finalize_run` 로 단순화. `finalize_run` 은 ingest 요약만 집계(불변).
- **보존**: silver **로직은 그대로 유지**(`include/silver/silver_tasks.py`·`validators.py` 무수정).
  사용자 결정에 따라 **별도 silver DAG 는 생성하지 않음** — 로직만 보존하고 오케스트레이션은 비움.
  `observed_date` 파라미터/파생값은 여전히 silver 파티션 키 의미로 남는다.
- 검증: `seoul_commerce_dag.py` 구문 검사 통과 + 잔여 silver 참조는 docstring 설명뿐(임포트/결선 없음).
- 파일: `seoul_commerce_dag.py`(docstring 다이어그램·임포트·태스크·`_wire`).

### 14. bronze 경로에 연/월/일 파티션 추가 (`/<YYYY>/<MM>/<DD>/run_id=…`)
- bronze 저장 구조를 `…/bronze/commerce/run_id=<ts>/…` → **`…/bronze/commerce/<YYYY>/<MM>/<DD>/run_id=<ts>/…`**
  로 변경. 연/월/일은 **run_id 날짜에서 파생**(별도 인자 없음) → 같은 날 실행이 같은 날짜 폴더에 모인다.
- 구현은 `paths.bronze_run_dir` 한 곳(+ `_run_date_dir` 헬퍼, 날짜 형식 아니면 방어적으로 파티션
  생략). object/marker/`_RUN` 키가 전부 따라옴. `markers.list_run_ids` 는 `run_id=` 부분문자열로
  추출하므로 **무수정 동작**(신·구 레이아웃 모두 인식). silver 경로(observed_date 파티션)는 불변.
- 검증: 단위테스트 28 통과 + R2 실적재로 `bronze/commerce/2026/06/30/run_id=…/food_cold_storage.jsonl`
  + 마커 확인, `latest_run_id` 정상 인식.
- 파일: `include/common/paths.py`, `include/bronze/bronze_tasks.py`(docstring),
  `tests/test_markers.py`·`tests/test_bronze_tasks.py`, `README.md`,
  `docs/architecture/storage.md`, `docs/pipeline/common_info.md`.
- ⚠️ 기존 구레이아웃 run(`…/run_id=2026-06-30_160452_591/`, 전환 전 적재)은 그대로 남는다 —
  다음 수집부터 신규 레이아웃. 필요 시 구 run 정리.

### 13. 인허가 39종 전 종 수집 — 잔여 14종 service_name 채움(25→39)
- **배경**: 레지스트리 39종 중 14종이 `service_name: null`(코드 미입력) → `enabled_for_schedule()`
  가 코드 채워진 것만 job 으로 만들어 **25 job 만 수집**되고 있었다. (job 단위 = API 단위 =
  `ingest_one[<short>]` 1 인스턴스. category 는 그룹 라벨일 뿐 job 수와 무관.)
- **해소**: 사용자 전달 **포털(data.seoul.go.kr) 정본 LOCALDATA 코드 14종**을 레지스트리에 입력하고
  14종 전부 API 호출로 검증(`INFO-000` + 업태 일치). → `enabled('daily')=39, pending=0`.
  - 공중위생 5: 미용 `051801`·이용 `051901`·세탁 `062001`·소독 `093011`·목욕 `114401`
  - 축산 5: 판매 `072204`·가공 `072205`·포장 `072206`·보관 `072224`·운반 `072225`
  - 관광 2·건기식일반·숙박: `072401`·`072402`·`072203`·`031103`
- **정정**: 이전 분석의 *"공중위생은 비-LOCALDATA 코드"* 주장은 **오류**였다 — 자동 스캔이 prefix
  01/02/03/07 만 봐서 못 찾았을 뿐, 공중위생도 정상 LOCALDATA(05/06/09/11)를 쓴다.
- **호출량 재산정**: 39종 = **데이터 1,360 + 게이트 1 = 1,361회/수집**, 약 134만 건(실측 2026-06-30).
  (이전 25종 1,049회에서 증가.)
- 파일: `config/dataset_registry.yaml`, `docs/pipeline/common_info.md`(카탈로그),
  `docs/pipeline/bronze/{api-call-volume,uncollectable-datasets,resolve-worklist,README}.md`,
  `docs/pipeline/{README,non-license-datasets}.md`, `Share.md`.

### 12. R2 적재 복구 — `R2Storage` boto3 전환 · `STORAGE_BACKEND=r2`
- **증상**: Airflow 실행은 됐으나 R2 에 적재 이력 없음. **원인 2가지** — (1) `.env.commerce` 의
  `STORAGE_BACKEND=local` + `R2_BUCKET` 공백 → 컨테이너 휘발성 볼륨(`/opt/airflow/data`)에만 적재,
  (2) `R2Storage` 가 `s3fs` 기반인데 호스트 이미지에 **s3fs 미설치**(boto3/pandas/pyarrow 는 있음).
- **해결**: `R2Storage` 를 **boto3** S3 클라이언트로 재구현(path-style·SigV4·region `auto`) — 이미지에
  이미 있는 boto3 만 사용해 **번들 안에서 자립 해결**(호스트 이미지 변경 불필요). `.env.commerce` 를
  `STORAGE_BACKEND=r2` + R2 블록(`R2_BUCKET=${R2_DEV_BUCKET_NAME}` 등 루트 `.env` dev 키 참조)으로 복구.
  → **로컬(도커)에서 실행해도 R2(`seoul-dev`)에 적재**된다.
- **검증**: 컨테이너에서 boto3 R2 write/read/list 확인 + `food_cold_storage`(50행, 실키) bronze 1건을
  `bronze/commerce/run_id=…/food_cold_storage.jsonl` + `_markers/...completed` 로 R2 적재 후 정리.
- 의존성 문서 정정: R2=boto3·silver=pandas/pyarrow 는 **이미지에 이미 포함**(추가 설치 불필요),
  s3fs 는 미사용. (이전 "패키지 미포함/설치 필요" 서술 수정.)
- 파일: `include/common/storage.py`, `.env.commerce(.example)`, `requirements.txt`, `CLAUDE.md`,
  `README.md`, `docs/architecture/storage.md`, `docs/configuration/{configuration,environments}.md`,
  `docs/operations/{deploy-dev,deploy-prod}.md`.

### 11. 재수집 DAG · 알림 인터페이스 · API별 진행 가시성 · change-log 규칙
- **재수집 파이프라인**: `seoul_commerce_recollect` DAG(6h) 추가 — 최근 run 의 마커를 읽어
  **미완료(incomplete/미시도) API만 재수집**. 대상이 없으면 수집 진행 안 함(빈 매핑 → run 폴더
  미생성). 마커 조회 헬퍼 `bronze/markers.py`, `paths.bronze_root()`. `finalize_run` 은 빈 실행 시
  `_RUN` 마커 생략. DAG 정의를 공통 태스크(모듈 레벨) 공유 + daily/recollect 2개로 정리.
- **API별 진행 가시성**: `ingest_one`·`build_silver_one` 에 `map_index_template="{{ short }}"` →
  Airflow Grid/Graph 에서 매핑 인스턴스가 **API 이름**으로 표시(성공/실패/대기 가시화). 실측 확인.
- **알림 인터페이스(비활성)**: `common/notify.py` — `Notifier`/`NoopNotifier`/`notify_exception`.
  예외 로그를 알림으로 보낼 수 있는 인터페이스만 제공(**기본 no-op, 미와이어링**).
- **change-log 규칙**: 대단위 변경은 `change-log.md` 에 작성일·순서 내림차순으로 기록하도록
  CLAUDE.md(§19 Change Log Rule)에 명시. 경로는 Share.md §4·docs/README.md 로 인덱싱.
- 파일: `seoul_commerce_dag.py`, `include/bronze/markers.py`(신규)·`include/common/notify.py`(신규),
  `include/common/paths.py`, `tests/test_markers.py`·`tests/test_notify.py`(신규),
  `docs/operations/recollect-and-alerts.md`(신규), `CLAUDE.md`, docs 인덱스/architecture/operations/README.

### 10. 인허가 외 2종 격리 · monthly/irregular DAG 비활성 (41 → 39종)
- `medical_location`(병의원 위치정보)·`food_hygiene_status`(식품위생업소 현황)은 LOCALDATA
  인허가 표준이 아니어서 **수집 대상에서 제외(격리)**.
- 레지스트리에서 제거 → `config/non_license_datasets.yaml` 로 파킹. 사유/재활성 절차는
  `docs/pipeline/non-license-datasets.md`.
- 두 주기에 인허가 대상이 0종이라 **`SCHEDULES = {"daily"}`** 로 축소 → `seoul_commerce_daily`
  1개만 생성(monthly/irregular DAG 비활성).
- 결과: 인허가 레지스트리 **39종 = 해석 25 / 미해석 14**.
- 파일: `config/dataset_registry.yaml`, `config/non_license_datasets.yaml`(신규),
  `seoul_commerce_dag.py`, `docs/pipeline/non-license-datasets.md`(신규), 카탈로그/uncollectable/
  worklist/caveats/api-call-volume 갱신.

### 9. DAG 명칭 통일: `seoul_license_*` → `seoul_commerce_*`
- 파일 `seoul_license_dag.py` → **`seoul_commerce_dag.py`**, DAG id `seoul_license_{daily,monthly,
  irregular}` → `seoul_commerce_*`, 태그에서 중복 `license` 제거.
- 모든 문서의 DAG id/경로 참조 일괄 변경.
- 파일: `seoul_commerce_dag.py`(이름변경), 전체 docs/README/Share.

### 8. `.airflowignore` glob 전환 (Airflow 3.x 호환)
- Airflow 3.x 기본 `dag_ignore_file_syntax=glob` 인데 regexp(`^include/`)라 무효 → 번들 내부
  (`include/`·`config/`·`tests/`·`docs/`)가 DAG 파일로 오스캔되던 문제 수정.
- glob 패턴(`include/**` 등)으로 변경. 컨테이너에서 dag-processor가 DAG 파일만 파싱 확인.
- 파일: `.airflowignore`.

### 7. bronze 저장 구조 재설계 — run_id 스냅샷 · API당 1파일 · 마커
- **DAG 실행 1회 = `run_id=<YYYY-MM-DD_HHMMSS_mmm>` 폴더 1개**. API당 **1파일**
  (`<short>.jsonl`, 줄=원본 페이지 NDJSON).
- 수집 상태는 **API당 마커 1개**(`_markers/<short>.completed | .incomplete`) + 실행 마커
  (`_RUN.*`). 리니지는 마커 JSON 에 포함.
- **외부 매니페스트 제거**(`commerce/_manifest/manifest.json`) — bronze 는 run_id 폴더 안에서만
  파일 생성. 중복 제거는 silver 가 `MGTNO` 로. force 파라미터/스킵 제거(매 실행 전체 수집).
- `COMMERCE_STORAGE_PREFIX` 추가(`{prefix}/bronze/commerce/…`). silver 는 단일 NDJSON 키를 읽음.
- 파일: `include/common/paths.py`, `include/bronze/bronze_tasks.py`, `include/silver/silver_tasks.py`,
  `include/common/settings.py`, `seoul_commerce_dag.py`, `include/bronze/manifest.py`(삭제),
  tests, storage/architecture/operations/common_info 등 갱신.

### 6. 미해석 데이터셋 코드 해석 13종 (의료·동물)
- `sample` 키 실호출 + BPLCNM 식별로 의료/약무 `0101xx`·의료기사 `0102xx`·동물 `0203xx`
  계열 **13종**의 `service_name` 확정(병원/의원/부속/산후조리/안전상비/약국/안마/안경/치과기공/
  동물병원/동물약국/동물용의료용구/가축). `resolve.verify` 통과.
- 모호한 14종은 **후보 코드 + 워크리스트**로 정리(오수집 방지 위해 미입력).
- 파일: `config/dataset_registry.yaml`, `docs/pipeline/bronze/uncollectable-datasets.md`,
  `docs/pipeline/bronze/resolve-worklist.md`(신규).

### 5. bronze 수집 주의사항 문서(caveats)
- 실호출에서 발견한 특이사항을 **API별 + `[bronze]`/`[silver]` 단계 태그**로 정리(정렬 키
  없음·날짜 공백 패딩·상태 in-place·UPTAENM 공란·비-LOCALDATA 스키마·대용량 등).
- 파일: `docs/pipeline/bronze/caveats.md`(신규).

### 4. docs 주제별 폴더 재분류 + 인덱싱
- 평면 문서를 `architecture/` · `configuration/` · `operations/` · `pipeline/`(+`bronze/`)로
  분류. 마스터/폴더별 README 인덱스 작성, 모든 상대 링크 갱신.
- 파일: `docs/**`.

### 3. bronze 실호출 분석 문서
- 페이지네이션 정렬(위치 기반·안정이나 정렬 기준 컬럼 없음 → `MGTNO` dedupe), API 호출량
  (수집 1회 호출 수 산정), 영업상태 추적 모델(업장당 1행 in-place), 수집 불가 원인 분석.
- 파일: `docs/pipeline/bronze/pagination-ordering.md`·`api-call-volume.md`·
  `status-tracking-model.md`·`uncollectable-datasets.md`(신규).

### 2. `SEOUL_MAX_PAGES` 무제한 기본값
- 일반 API 는 호출 횟수 제한이 없으므로, **값이 없으면(미설정/빈값/0/음수) 무제한**(`None`).
  양수만 부분 수집 캡. `settings._env_limit` 추가.
- 파일: `include/common/settings.py`, `include/bronze/bronze_tasks.py`, `.env.commerce(.example)`, docs.

### 1. 환경변수 자립화 + 현행 환경 반영
- 번들 자체 환경파일 **`.env.commerce`** + 로더(`include/common/env.py`, `load_commerce_env()`)
  도입 — DAG 임포트 시 `os.environ` 에 setdefault. 루트 `.env` 와 겹치는 R2 값은 **`${VAR}`
  참조**로 불러옴(중복 저장 X). 시크릿은 gitignore, 템플릿만 추적.
- `requirements.txt`(번들 의존성 명세) 추가. CLAUDE.md 에 **작업 경계**(번들 안에서만) 명시.
- 문서를 **현행 환경**(LocalExecutor·단일 루트 `.env`·`elt-infra` compose·UI :30585)에
  맞춰 전면 갱신(기존 CeleryExecutor/serving DB/`.env.local·dev·prod` 서술 정리).
- 파일: `.env.commerce`(신규)·`.env.commerce.example`(신규)·`.gitignore`(신규)·
  `include/common/env.py`(신규)·`requirements.txt`(신규)·`seoul_commerce_dag.py`·
  `include/bronze/resolve.py`·`CLAUDE.md`·`Share.md`·`README.md`·`docs/**`.
