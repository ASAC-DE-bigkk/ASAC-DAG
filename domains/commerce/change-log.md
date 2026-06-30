# Change Log — commerce 번들

`dags/domains/commerce/` 의 변경 이력. **작성일·순서 기준 내림차순**(최신이 위).
각 항목은 **최종 반영된 형태**를 기준으로 정리한다(중간에 되돌린 단계는 합쳐서 기술).

---

## 2026-06-30

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
