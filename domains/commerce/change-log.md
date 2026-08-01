# Change Log — commerce 번들

`dags/domains/commerce/` 의 변경 이력. **작성일·순서 기준 내림차순**(최신이 위).
각 항목은 **최종 반영된 형태**를 기준으로 정리한다(중간에 되돌린 단계는 합쳐서 기술).

---

## 2026-08-01

### 90. 운영 기록 적재 첫 실행 검증 + 환경 지정 env 키 전면 제거 (#654, #655)

request:
- "648 complete" / "어떤 테스트를 진행했고 어떤 결과물이 나왔고 그걸로 어떤 결론을 얻었는지
  이슈 정리하고 PR 진행할 것" / "검증 작업 진행할 것" / "해당되는 부분은 모두 참조하지 않도록
  변경하고, 변경에 대한 테스트도 같이 진행할 것"

response — **검증(#655)**:
- **운영 Airflow DagBag 파싱**: commerce 9개 DAG `import_errors 0`, 9/9 파싱. 콜백 배선 실측 —
  전 DAG `success_cb=True`, `commerce_collect_raw`·`commerce_recollect_raw` 는 `failure_cb=2`
  (기존 실패상세 + 신규 실행기록이 **교체가 아니라 병존**함을 실증).
- **실제 D1 첫 실행**: `_ops_run_event`·`_ops_daily_metric`·`_ops_pipeline_state`·
  `_ops_pipeline_expectation` **4종 생성 확인**. 기록 **52건 적재**(citydata 35 · weather 6 ·
  traffic 7 · transit 4), 기대 주기 **9행** 등록.
- **중복 판정 2단 관문 실증**: 같은 범위 재실행 시 훑은 127건 중 **48건은 파일을 열지도 않고**
  건너뛰고(키 기준 1차), 2건은 읽은 뒤 `event_id` 로 걸러(2차) **loaded 0**. 파일 이동·표식 0.
- **설계대로 나온 것**: 적재된 52건 전부 `layer` 없음(실패상세·복구·제품상태 문서에 단계 칸이
  없다) → 단계별 집계표는 비어 있고, 영수증이 도메인별 건수를 보고한다. commerce 는 배선을
  끝냈으므로 다음 실행부터 채워진다.
- **실질 결함 발견**: 전 구간(약 22,000건) 적재가 **30분 넘게 0행**. 원인은 적재기가 **전부 읽은
  뒤에야 쓰는** 구조(직렬 GET + all-or-nothing)라 진행이 보이지 않고 중단 시 전량 폐기된다.
  평상시에는 1차 관문이 이미 넣은 파일을 읽지 않으므로 무해하고, **최초 전량 적재에서만** 걸린다.
  → 날짜 단위 분할 커밋 제안(#655 결정 요청).

response — **환경 지정 env 키 제거(#654)**:
- 호스트는 ENV2 개편에서 이미 `R2_DEV_*` 를 없앴고(`trino/iceberg.properties` 는 canonical
  `R2_*` 한 세트만 읽는다) `dags/` 코드에만 옛 분기가 남아 있었다. 카탈로그·스모크 스키마까지
  같은 패턴이라 전부 제거했다 — 카탈로그 키 11곳, R2 자격증명 잔여 6곳.
- **부수로 기존 결함 1건 해소**: culture 의 두 안전 테스트가 서로 모순이라 하나가 계속 깨져
  있었다(선언값 존중 vs prod 폴백 금지). 판정을 키가 아니라 **값**으로 옮겨 둘 다 만족시켰다 —
  선언값이 prod 창고 이름일 때만 dev 기본으로 되돌린다. culture 222 → 223 통과.
- **강제 장치**: `common/tests/test_env_key_scoping.py` — 실코드가 폐지된 키를 이름으로 참조하면
  실패. 유예 목록은 자기 규약(구 규약 박스 지원)을 가진 citydata·culture config 2건뿐이고
  전환은 도메인 오너 몫. 죽은 줄이 남아도 실패한다.
- 검증: common 205 · commerce 433 · culture 223 · transit 119 통과. traffic 5 · weather 2
  실패는 변경 전과 동일한 기존 실패(로컬 `airflow.providers.standard`·`trino` 미설치).
  `python -m security` PASS.

response — **부수 확인**:
- **ASAC-DAG#603(flow 3종 소급 미반영) 종결.** 모델이 `append` → `delete+insert` + 워터마크
  기반 소급 재계산으로 바뀌었고(`#603, 사용자 확정`), prod 실측에서 `src_collected_at` 컬럼
  3종 실재 + 워터마크가 silver 원천 최신 수집시각(`2026-07-31 02:22:46.145315`)과 **정확히
  일치**함을 확인해 닫았다. 이슈가 제안한 별도 스윕 DAG 는 신설되지 않았고 더 나은 방식으로 해결됐다.
- 서브모듈 포인터는 **dev 브랜치에서도 갱신해 왔다**(`5d03181`·`11697dd` 등) — "main 만 갱신"은
  사실이 아니다. 갱신 시점은 이 PR 병합 이후.

### 89. R2_DEV_* 폐지(키가 배포 영역을 지정하지 않게) + Y-7 89 GiB 판정 + 운영 기록 흐름 문서

request:
- "`R2_DEV_*` 는 패치로 이제 접근하지 않는 설정일 텐데? **현재 사용하는 곳이 없다면 혼동하지
  않도록 완전히 드랍**하고, 사용하는 환경이 있다면 **key 단위에서 배포 영역을 지정하지 않고
  배포 환경에 따라 key 의 '값'을 따라가도록** 일괄 설정해 달라."
- "`seoul-dev` 는 **운영 설정이 완전히 종결되면 다시 되돌아갈 곳**이므로, 운영 이관이 완전히
  종결되면 그때 조치할 것임."
- "prod 카탈로그에 80기가 있다는 것이 **정말 commerce 소유인지**, 현재 **prod·seoul 환경에
  영향을 미치는 상태로 잔여**하는지, **다른 도메인 자료는 아닌지** 명확히 확인하고, 모든 게
  명확하면 드롭 시행할 것."
- "로그 생성과 적재가 어떻게·어떤 주기로 이뤄지는지 정리해 **문서로 남겨** 달라."

response:
- **`R2_DEV_*` 는 이미 호스트에서 폐지된 상태였고, `dags/` 코드에만 남아 있었다.** 근거:
  `docker-compose.yml` 주석에 "ENV2 가 `R2_DEV_*` 를 없애자 dev 카탈로그 파일만 참조 불가가 되어
  Trino 가 기동 실패했다 → 파일을 하나로 합쳤다"는 이력이 있고, 실제 `trino/iceberg.properties`
  는 canonical `R2_*`(`R2_DATA_CATALOG_URI`·`R2_ENDPOINT` 등) 한 세트만 읽는다. 루트 `.env` 에도
  활성 `R2_DEV_*` 가 없다. 즉 **호스트는 이미 사용자가 요구한 형태**(키는 환경 무관, 값이 배포를
  따라감)였고 코드만 옛 분기를 이고 있었다.
- **코드에서 환경 지정 키를 제거**(런타임 무변경 — 어차피 그 키가 없어 전부 `R2_*` 로 수렴하고
  있었다). 분기를 남겨 두면 누가 `R2_DEV_*` 를 채우는 순간 같은 날짜 기록이 두 버킷으로 갈리는
  통로가 된다(#78 `Z-7`).
  - `common/storage.py` — `r2_env()` 의 dev 우선 규칙 삭제. `r2_env_for(name, target)` 은 호출측
    5곳(run_sink·citydata 4개 DAG) 호환을 위해 남기되 **target 으로 분기하지 않는 별칭**으로.
  - `common/runtime_guard.py` — `_R2_TARGETS`(타깃별 키 세트) → `_R2_CREDENTIAL_KEYS`(canonical
    한 세트) + `_R2_EXPECTED_BUCKET`(타깃별 **기대 값**). 타깃이 가르는 건 키가 아니라 값이다.
  - `common_dbt_smoke.py` — `r2_env_name()`(dev 키 치환) 제거, `R2_DEV_RAW_PREFIX` → `R2_RAW_PREFIX`.
  - 호스트 `.env.example` — `R2_DEV_*` 8줄 삭제 + "되살리지 말 것(Z-7)" 경고와 되돌리는 올바른
    방법(값 교체) 명시. **호스트 파일 변경이라 사용자 지시에 따라 수행.**
  - 테스트 갱신: `R2_DEV_*` 가 있어도 무시되고, 그것만으로는 자격증명이 채워지지 않으며,
    `r2_env_for` 의 target 이 더는 분기하지 않음을 회귀로 고정.
- **`seoul-dev` 서술 정정.** 직전 §88 에서 "폐기 대상"으로 적었으나 사용자 확인 결과 **운영 이관
  종결 시까지 보존하는 동결된 롤백 지점**이다. `environments.md` §3 · `deploy-dev.md` 배너 ·
  `deploy-prod.md` 를 "삭제 대상 아님 / 되돌릴 때는 키가 아니라 값을 바꾼다"로 고쳤다.
- **Y-7(89 GiB) 판정 — 드롭하지 않는다. prod 에 존재하지 않고 commerce 소유도 아니다.** 실측:
  - prod 카탈로그 `iceberg` 의 스키마는 citydata·commerce·common·culture·ops_smoke·traffic·
    transit·weather·weather_traffic_bronze 뿐 — **`ops` 스키마가 없다**(=`ops.run_metadata` 없음).
  - prod 창고 `seoul/__r2_data_catalog/` **전체가 141,878객체 38.27 GiB** — 89 GiB 객체가 물리적으로
    존재할 수 없다. 최대는 citydata `bronze_seoul_citydata` 8.67 GiB, commerce 최대는
    `silver_license_history` 1.34 GiB.
  - 소유도 commerce 가 아니다. `ops.run_metadata` 는 전 도메인 공통 실행기록 테이블이었고
    (`citydata_ops_digest` 주석: "과거엔 `ops.run_metadata`(Iceberg)를 읽었으나 citydata DAG 들이
    `record_run` 으로 옮기며"), commerce 는 이 테이블을 읽거나 쓴 적이 없다(참조 0).
  - 대상은 dev 카탈로그(`iceberg_dev`/`seoul-dev`)이고, 이 Trino 스택에는 그 카탈로그가 아예
    등록돼 있지 않다(`SHOW CATALOGS` = iceberg·jmx·memory·system·tpcds·tpch).
  - → **prod·seoul 에 미치는 영향 0.** seoul-dev 는 롤백 지점으로 보존하기로 했으므로 드롭은
    운영 이관 종결 후로 미룬다. #78 `Y-7` 의 commerce 배정은 **오배정**이며 #78 에 정정 요청.
- **운영 기록 흐름 문서 신설**: [docs/operations/ops-records.md](docs/operations/ops-records.md).
  생성(무엇이 언제 어디에) · 적재(01:30 2태스크 순서·3일 창의 이유·상한·최초 백필) · 주기 표 ·
  보관 기간 · 중복 방지(파일 이동 없음) · 실패 시 동작 · 관측 공백 표기 · 확인 쿼리.
  **04~06시 라인의 기록은 다음 날 01:30 에 실린다**는 시점 관계를 명시했다(3일 창의 근거).
  `docs/operations/README.md` · `docs/README.md` 색인 반영.
- 검증: 634건 통과 · `python -m security` PASS.

### 88. 환경 현황 문서를 운영(prod) 기준으로 재작성 + 적재기 결함 2건 수정

request:
- "현재 전체 설정을 seoul·prod·iceberg 로 바꾼 것으로 아는데, 왜 구 영역인 seoul-dev·dev·
  iceberg_dev 로 검토하고 있는지 이해되지 않는다. **코드와 환경 설정을 정확히 읽고 그에 따라
  현황 문서 파일을 정확히 갱신**하고, 그 바탕으로 현황을 재파악한 뒤 실질적인 작업이 필요한
  영역을 검토해 달라. **개발 영역은 현재 운영과 저장구조가 전혀 다른 레거시**다."

response:
- **지적이 맞다.** §87 작업 중 dev 를 "prod 와 짝을 이루는 현행 환경"으로 전제한 서술이 여러
  곳에 있었다(잔재 추정, 문서의 dev/prod 이원 서술). 루트 `.env` 활성값과 운영 버킷을 직접
  읽어 전부 바로잡았다.
- **환경 실측(루트 `.env` 활성 줄)**: `COMMERCE_STORAGE_BACKEND=r2` · `R2_BUCKET_NAME=seoul` ·
  `TRINO_ICEBERG_CATALOG=iceberg` · `DBT_TARGET=ASK_SEOUL_TARGET=COMMERCE_DBT_TARGET=prod` ·
  `COMMERCE_STORAGE_PREFIX=`(빈 값). **`R2_DEV_*` 와 `TRINO_DEV_ICEBERG_CATALOG` 는 활성 설정이
  하나도 없다** → `common.storage.r2_env` 의 dev 우선 규칙이 있어도 전부 `R2_*`(=`seoul`)로
  수렴한다. 즉 #78 `Z-7`(개발·운영 기록 혼입)은 **이 환경에서는 발생하지 않는다.**
  `.env.commerce` 매핑은 8줄뿐이고 R2 엔드포인트·자격증명은 이름이 같아 매핑 없이 상속된다.
- **운영 버킷(`seoul`) 전수 실측**: 최상위 존 `raw/`·`ops/`·`reference/`·`__r2_data_catalog/`
  (#78 §1 구조와 일치, `dev/` 샌드박스 없음). ops 존 밖 구경로(`runs/`·`errors/`·`metrics/`)
  **0건**. `raw/commerce/` 는 `load_date=` 파티션 **28개뿐, 금지 표기 0건**(`P-1`·`P-2` 준수).
  ops 관측 계열 **36,536건**(control 7,908 별도), 최근 일별 8,000~10,000건대.
  카테고리별 축·날짜 칸은 [docs/configuration/environments.md](docs/configuration/environments.md) §2 표.
- **적재기 경로 판독률 36,536/36,536 = 100%** — 실패 0. dual-read 설계가 실제 운영 배치를 전부
  커버함을 실증했다(도메인 우선 5종 · 날짜 우선 3종 · `observed_date=`/`load_date=`/`date=` 혼재).
- **적재기 결함 2건(실측으로 드러남) 수정**:
  1. **매 실행마다 구간 전체를 다시 GET 했다.** `event_id` 대조를 파일을 **읽은 뒤** 했기 때문에,
     3일 창이면 하루 22,589건을 매일 다시 내려받는 구조였다(PR 본문의 "이미 있는 것은 읽지 않고
     건너뛴다"는 설명도 사실과 달랐다). 저장 시 남기는 `source_key` 로 **읽기 전 1차 관문**을
     추가했다(`known_source_keys_statement`) — 판정 근거는 여전히 "DB 에 있는지"이고 파일은
     건드리지 않는다(`C-6`). `event_id` 대조는 2차 관문으로 유지(원천 발급 id·키 변경 대응).
  2. **오브젝트 상한 기본값 20,000 이 실측 물량보다 낮았다.** 최근 3일 22,589건 · 전량 36,536건이라
     매 실행 조용히 잘렸다. `MAX_OBJECTS_PER_RUN=50,000` 으로 올리고 근거를 상수 주석에 실측으로
     못박았다. 상한 회귀 테스트 추가(실측 건수 미만이면 실패).
- **현황 문서 갱신**(dev 를 현행 환경으로 서술하던 것 전부):
  - `docs/configuration/environments.md` — **전면 재작성**. 운영 단일 환경 표 + 버킷 실측 표 +
    "dev 는 레거시" 절 + 타깃 이름이 셋인 이유(셋을 함께 바꾼다).
  - `docs/operations/deploy-dev.md` — 문서 머리에 **폐지 배너**. 그대로 실행하면 dev 우선 규칙이
    되살아나 운영 기록이 두 버킷으로 갈린다(`Z-7`)는 경고 포함. 본문은 이력 참고용으로 격하.
  - `docs/operations/deploy-prod.md` — 존재하지 않는 버킷명 `seoul-prod` → `seoul`. "dev 와 분리"
    서술을 현행 구성 요약으로 교체. `R2_DEV_*` 를 채우지 말 것을 명시.
  - `docs/configuration/configuration.md` — `.env.commerce` 가 R2 엔드포인트·자격증명을
    `${R2_DEV_*}` 로 매핑한다는 서술이 실제 파일과 달랐다(그런 줄이 없다). 실제 매핑으로 교체.
  - `docs/architecture/storage.md` · `README.md` — 버킷 서술을 단일 운영 버킷으로 정리, raw
    날짜 표기 실측 결과 반영.
- 테스트: 622건 통과(신규 2건 포함) · `python -m security` PASS.
- **판단**: `seoul-dev` 정리는 "쓰는 쪽을 되돌리는" 문제가 아니라 참조 0 확인 후 삭제(`Y-2`)
  대상이고, 저장 구조가 현행과 달라 현황 수치로 인용하지 않는다. 별도 이슈에서 다룬다.

### 87. 운영 기록 단일 관문 + ops 존 → 조회 DB(D1) 적재 (ASK-Seoul#78)

request:
- ASK-Seoul#78「저장소·운영 기록 적용 규약 v1」이 commerce 담당분으로 지목한 항목이 적용됐는지
  확인하고 수정 조치.
- `logship` 이 "해당 위치에 쌓일 데이터를 특정 주기로 D1 에 적재하려는 기능"으로 보이는데,
  **이 기능 하나로** 규약이 명시한 위치의 파일을 감지해 D1 로그 테이블에 **같은 규격·같은
  포맷**으로 적재되도록 설계돼 있는지 확인하고 개발할 것.
- 중복 데이터가 처리되지 않도록 파일 경로 이동 로직이나 중복 체크·파일 저장 규약도 적용.
  (그런 규약이 문서에 있는지 먼저 확인하고, 없으면 대응)
- 이번 개정 반영은 commerce 담당 파트뿐 아니라 **규약에 명시된 경로에 저장되는 모든 도메인의
  로그류를 통합해 DB 에 싣는 것**. commerce 는 내 소관이니 각 파트가 그 정보를 파일로 기록해
  적재할 수 있도록 준비.
- 모든 모듈이 **같은 형태의 로그 모듈**을 쓰게 하고, 공용 위치의 것이 부족하면 공용 로그 모듈을
  따로 만들어 다른 도메인이 그것을 쓰도록 강제. 필수 기입 정보가 빠지면 에러를 내거나 즉각
  피드백. 경로도 용도에 따라 틀린 곳에 쓰는 것을 막게 **선택지를 사전에 좁혀** 두고, 추후
  문제가 생겨도 **이 한 가지 관문만 통과하면 되게** 설계 반영.

response:
- **현행 감사 결과(수정 전)**: `logship` 은 D1 적재 기능이 아니었다 — 태스크 텍스트 로그를
  tar.gz 로 R2 에 옮기는 것이 전부였고 조회 DB 배선은 0. 규약 대비 위반·미구현은 다음과 같다.
  - `P-4` 위반 — 관측 계열인 `ops/logs/commerce/` 가 `load_date=` 를 씀(#78 §2 "현재 어긋난 곳"
    표에 @Exisign 소관으로 명시).
  - `P-9` 위반 — `LOGS_LAYER` 기본값에 도메인명이 박혀 있고 키 빌더가 도메인을 인자로 받지 않음
    (#78 이 `commerce_ops_logship` 을 지목).
  - `C-2`·`C-3`·`C-4`·`C-7`, `D-1`~`D-4`, `F-1`~`F-6` **전부 미구현**(조회 DB 자체가 없었다).
  - `V-4` 결함 — 공통 모듈 `_VALID_LAYERS` 에 `silver` 가 빠져 정제 단계 기록이 검증에서 튕김
    (#78 §16 정정 2).
  - commerce 는 ops 존에 **상태값과 로그 압축본만** 남기고 실행 기록을 남기지 않아, 조회 DB
    관점에서 도메인 전체가 관측 공백이었다.
- **중복 처리 규약은 문서에 이미 있고, 사용자가 상정한 "파일 이동"과는 반대 방향이다.**
  `C-6` 은 *"어디까지 적재했는지는 경로가 아니라 DB 안에 그 기록이 있는지 없는지로 판단한다.
  파일 이동도 별도 표시도 만들지 않는다"* 이고, `C-4` 는 대조 기준을 `event_id`(축은
  `source_path_date`), `D-4` 는 자연키 범위 갱신, `G-3` 은 전환일 중복을 `event_id` 로 제거다.
  → 파일을 옮기는 로직은 **만들지 않았다**. `event_id` 를 PRIMARY KEY 로 두어 재적재가 자연히
  멱등해지게 했고, 적재기는 저장소를 **읽기만** 한다(테스트의 저장소 스텁이 write/delete/copy
  호출 시 실패하도록 강제).
- **공용 관문 `common/ops/contract.py` 신설** — 사용자가 요구한 "한 가지 관문". 규약 중 기계가
  강제할 수 있는 전부를 코드로 옮겼다.
  - 경로는 **만들 수 없고 요청만 가능**(`ops_key`). 관측 계열은 `observed_date=` 필수, 상태
    계열은 날짜 칸 금지 — 용도에 안 맞는 조합은 그 자리에서 거부(P-4·P-5·P-6·P-9).
  - 선택지를 미리 좁힘: `OpsCategory`(10종 닫힌 집합, R-1) · `Layer`(V-4, **silver 포함**) ·
    `Grain`(V-5) · `RunStatus`(V-1) · `ManifestStatus`(V-2) · `RowsSource`(N-5) · `SinkType` ·
    `Environment`(Z-7). 값 집합 세 벌(V-1/V-2/V-3)은 이름이 비슷해도 **섞지 않는다**.
  - 필수 항목 누락은 `OpsContractError` 로 즉시 실패하고, 메시지에 **무엇이 어느 규칙 때문에
    필요한지**를 담는다. grain 별 필수 항목 표(`_REQUIRED_BY_GRAIN`)가 판정 기준.
  - `NULL ≠ 0` 강제(F-3·F-6·N-5): `row_count=None` ↔ `rows_source='not_observed'` 쌍이 아니면
    거부하고, 측정값에 `not_observed` 를 붙이는 것도 거부.
  - `X-1` 강제: `api_name` 에 URL 형태가 오면 거부. 서울 열린데이터 API 는 인증키를 URL 경로에
    싣기 때문에 그대로 저장하면 시크릿이 남는다. 저장 직전 `redact()`(X-2).
  - `F-5` 날짜 축 두 개: `observed_date_kst`(기록 내용 기준·정본) + `source_path_date`(경로
    날짜 그대로, 대조 전용). 소급 재정리를 안 하기로 한 이상(G-2) 이게 없으면 과거 전 구간이
    매일 어긋난다.
  - `event_id` 는 원천이 발급했으면 그대로 쓰고(원천 충실), 없을 때만 식별 항목 sha256 으로
    파생 — `product_observability` 와 같은 해시 규칙.
- **조회 DB 스키마 `common/ops/d1_ops.py`** — #78 §8 테이블 4종(`_ops_run_event` ·
  `_ops_daily_metric` · `_ops_pipeline_state` · `_ops_pipeline_expectation`).
  - **이 모듈은 `DROP TABLE` 문장을 아예 만들지 않는다**(D-6). 대시보드 마이그레이션이
    `DROP TABLE IF EXISTS` 로 시작해 팀 데이터를 지우던 사고와 같은 계열을 코드 수준에서 차단.
    테스트가 모든 산출 문장에 `DROP`/`DELETE`/`TRUNCATE` 가 없음을 검사한다.
  - 갱신은 전부 `ON CONFLICT(<자연키>) DO UPDATE`(D-4). 스키마 진화는 `ADD COLUMN` 만(D-3).
  - `_ops_daily_metric` 은 파이썬 슬라이스가 아니라 **`_ops_run_event` 에서 다시 계산**한다 —
    적재가 나눠 일어나므로 슬라이스 집계는 나중 적재분을 덮어써 조용히 과소 계상된다.
  - `layer` 는 **nullable**. 관문 이전 기록(errors 의 Problem 문서 등)에는 단계 정보가 아예
    없고, 없는 것을 추측해 채우면 그게 거짓말이 된다(F-3). 대신 단계별 집계에서 제외하고
    (`layer IS NOT NULL`) 그 건수를 도메인별로 영수증·경고 로그가 보고한다.
- **감지·정규화·적재 `common/ops/ingest.py`** — 규약이 정한 위치의 파일을 **전 도메인** 대상으로
  감지해 기록 형식 한 벌로 접는다.
  - `parse_ops_key` 가 현재 살아 있는 경로 배치를 전부 읽는다(G-1·G-4 dual-read): 도메인 우선
    bare(`ops/errors/commerce/observed_date=`) · 전환 전 날짜 칸(`load_date=`·`type=…/date=`) ·
    날짜 우선(`ops/runs/observed_date=…/domain=`) · `domain=` key=value · 관문 신규 경로.
    상태 계열(control·receipts)과 ops 존 밖은 대상에서 제외(R-4).
  - 기록기 5벌(run_sink·runmetrics·errors.sink·product_observability·관문)의 서로 다른 모양을
    F 표 한 벌로 정규화. 관문이 쓴 기록은 경로 정보만 덧붙여 그대로 통과.
  - `ops/logs/` 의 tar.gz 는 "기록 1건"이 아니라 텍스트 압축본이라(R-1) **행으로 만들지 않고**,
    같은 (dag_id, run_id) 실행 기록에 `log_bundle_key` 포인터로 붙인다 — 새 `grain` 값을 만들어
    닫힌 집합(V-5)을 깨지 않으면서 화면에서 실행 → 로그 원문으로 갈 수 있다(F-1 추가는 허용).
    run_id 안전화 규칙이 전환 전후로 한 번 바뀌었으므로 양쪽 규칙으로 되짚는다.
  - 영수증(`IngestReceipt`)이 **말하지 않은 누락이 없도록** 건너뛴 것을 전부 센다: 파싱 불가,
    정규화 실패(카테고리별), `layer` 없음(도메인별), 이미 DB 에 있어 건너뜀, 상한 절단 건수.
    조용히 자르면 "전부 봤다"로 읽힌다(C-9).
  - `reconcile()` 은 저장소와 DB 를 `event_id` 기준·`source_path_date` 축으로 대조해 **빠진 것만**
    짚는다(C-3·C-4). 묶어 쓰기(C-5) 전제라 파일 수와 행 수를 같다고 보지 않는다.
- **commerce 배선** — 각 파트가 규약대로 기록을 파일로 남기게 했다.
  - `commerce_core/observability.py`: Airflow 콜백에서 관문을 호출. `ops_default_args(layer)`
    한 줄로 그 DAG 의 모든 태스크가 `ops/runs/commerce/observed_date=…` 에 기록을 남긴다.
    9개 DAG 전부 배선(raw·recollect=raw, bronze, silver, gold, gold_refresh, serving_export=d1,
    watchdog=raw, logship=d1). `commerce_collect_raw` 의 기존 실패 상세 콜백(ops/errors)은
    **교체하지 않고 뒤에 붙였다** — 담는 단위가 다르다(V-6).
  - 행 수는 태스크 반환 dict 의 관례 키(`row_count`/`rows`/`inserted`/`loaded`)에서만 읽고,
    못 읽으면 `not_observed`. 아무 숫자나 행 수로 승격하지 않는다.
  - `is_final_try` 는 판단 근거가 없으면 `None` — 관측 공백은 `False` 가 아니다(C-7).
  - 예외는 **타입명만** 싣는다(`error_ref`) — 메시지에 URL·자격증명이 섞일 수 있다(X-1·X-2).
    본문은 `ops/errors/` 문서의 몫이고 실행 기록은 그 위치만 가리킨다.
  - 관측 실패는 태스크를 죽이지 않고(C-2), **관문 거부는 `log.error`** 로 남긴다 — 배선이
    규약을 못 지키고 있다는 뜻이라 조용히 넘기면 관문이 아니다.
- **`commerce_ops_logship` 이 두 일을 한다** (사용자 요구 "이 기능 하나로").
  1. `ship_logs` — 경로를 관문이 만든다(`observed_date=`, 도메인 인자). 구경로(`load_date=`)에
     이미 올라간 번들을 보고 중복 업로드를 건너뛴다(G-4) — 못 보면 같은 로그를 두 번 올리고
     로컬만 두 번 지운다. 구경로 조회는 **그 시점의 명명 규칙**(`+` 보존)으로 되짚는다.
  2. `load_ops_to_d1` — 전 도메인 ops 관측 파일 감지 → 정규화 → D1 자연키 적재 + 일별 집계
     재계산 + 파이프라인 상태(C-9 완전/부분/**미확인**) + commerce 기대 주기 사본 등록.
     기본 조회 구간 3일(`COMMERCE_OPS_INGEST_LOOKBACK_DAYS`), 도메인 스코프는
     `COMMERCE_OPS_INGEST_DOMAINS`(비우면 전 도메인).
- **`commerce_core/ops_expectations.py`** — #78 §9 commerce 표(오너 확인 @Exisign)를 코드로.
  정본은 DAG 선언이고 이 표는 사본이다(S-1). 상류 이벤트형은 트리거·상류·최대 허용 지연으로
  등록(S-3), 수동 전용 `commerce_load_gold_refresh` 는 감시 제외(S-4). 테스트가 DAG 파일의
  `schedule=` 선언을 실제로 파싱해 사본과 양방향 대조하므로, 스케줄만 바꾸고 표를 안 고치면 막힌다.
- **공통 모듈 수정 2건**(전 도메인 영향, 둘 다 순수 추가):
  - `common/ops/product_observability.py` — 값 집합 3종의 출처를 관문으로 위임. `_VALID_LAYERS`
    에 **`silver` 가 들어갔다**(#78 §16 정정 2 — 이대로 전 도메인에 적용하면 정제 단계 기록이
    검증에서 튕긴다).
  - `common/serving/d1_client.py` — `HttpD1Client.execute()` 공개 seam 추가(적재기가 private
    `_query` 에 손대지 않게). 기존 동작 변경 0.
- **관문 우회 금지 강제** (`common/tests/test_ops_gate_enforcement.py`): `dags/` 전체를 훑어
  `"ops/…"` 문자열로 경로를 조립하는 코드를 찾아 **유예 목록에 없으면 실패**시킨다. 유예 목록은
  관문보다 먼저 있던 기록기 15개 파일이고 각 줄에 경로·사유·오너를 적었다 — 이 목록이 곧
  **남은 전환 작업의 정본**이며, 전환이 끝나 목록에 죽은 줄이 남아도 실패한다.
- **부수 수정(기존 결함)**: `include/bronze/bronze_tasks.py` 의 완료 로그가
  `rows=%d/%d` 에 `list_total_count or "?"` 를 넘겨, 총계를 모르는 run 에서 포맷 `TypeError` 로
  **그 로그 줄이 통째로 유실**되고 있었다(`%s` 로 수정). 공통 스위트와 함께 돌릴 때만 드러나
  지금까지 안 잡혔다.
- 테스트: 신규 4파일 **63건**(관문 30 · 적재기 31·강제검사 3 → 관문 30, ingest 31, 강제 3,
  commerce observability 12, 기대주기 6, logship 6). 전체 **620건 통과**
  (commerce 428 + common 192) · `python -m security` **PASS**(차단 0, 경고 3건은 런타임 설치
  확인 항목으로 정적 실행 시 상시 표시).
- **`P-1`·`P-2` 는 이미 적용돼 있다 — #78 §2 「현재 어긋난 곳」 표가 최신이 아니다.** commerce
  raw 의 `2026/06/30` 표기는 §79(2026-07-28, #60 약속①)에서 전환했다: `make_bronze_run_id` 가
  Asia/Seoul 실행시각으로 run_id 를 만들고 `paths._run_date_dir` 가 거기서 `load_date=YYYY-MM-DD`
  파티션을 파생하며, `%Y/%m/%d` 표기는 코드에 없다. **운영 버킷 실측(§88)에서 `raw/commerce/`
  가 `load_date=` 파티션 28개뿐이고 금지 표기 0건임을 확인했다.** #78 에 표 수정 요청함.
- **스코프 밖으로 남긴 것**: `Y-7`(89 GiB DROP) · 타 도메인 기록기의 경로/타임존/환경 분리
  전환(`P-4`·`P-7`·`P-8`·`Z-7`) · `D-6` 대시보드 증분 전환. 관문·적재기는 이것들이 정리되기
  전에도 동작하도록 dual-read 로 만들었다.

---

## 2026-07-31

### 86. 수집 0종 실행도 완료 알림 발송 — 침묵 구간 제거

request:
- "commerce raw 수집이 정상적으로 종료되지 않아서 discord 메세지를 못받은 것 같은데. 확인해서
  원인 분석 진행해줘. 조치는 일단 하지 말아봐." → 원인 분석만 선행.
- 이어서 "수집 건수가 0건이더라도 실행 완료가 되면 알림을 주도록 코드 수정해줘."

response:
- **원인(실측)**: `commerce_collect_raw` 는 실패한 게 아니라 **대상 0종으로 정상 종료**했다.
  `plan_all_targets` 가 동일 KST 일자 completed 를 제외(feat/59)해 대상 0 → `ingest_one` 0개 매핑 →
  `finalize_run` 이 `if not summaries:` 에서 **리포트 전송 코드보다 위에서 조기 return**. DAG 상태는
  success 라 실패 알림도 안 나가 **성공인데 아무 메시지도 없는 침묵 구간**이 됐다(2026-07-31 실행
  3건 전부 동일). 그날 152/152 를 끝낸 run 은 `2026-07-31_014858_879` 로, 로컬 메타DB·로그에 없는
  **다른 Airflow 인스턴스**가 공유 prod 마커 존에 남긴 것.
- **조치**: 0종이어도 완료 리포트 1건을 보낸다. run 마커(`_RUN.*`)는 종전대로 쓰지 않는다 —
  "마커 생략"과 "알림 생략"을 분리했다.
  - `commerce_raw.py`: 리포트 전송을 `_send_report()` 로 추출해 정상 경로와 0종 경로가 같은 코드를
    타게 하고, 0종 분기에서 `_no_target_reason()` 사유 섹션과 함께 전송. `_dag_stage()` 로
    dag_id/stage 해석 일원화.
  - `bronze/markers.py` `same_day_completed_summary()` — 동일 KST 일자의 완료 short 합집합 +
    **run 별 완료 종수**. 0종 알림에 "어느 run 이 이미 끝냈는지"를 싣기 위한 근거 조회.
  - `commerce_core/run_report.py` `no_target_section()` — 사유 문구(collect=동일자 완료 N/152종 +
    완료 run_id, recollect=미완료 없음/일자변경 가드). 근거 조회 실패 시에도 알림은 나간다(사유 축약).
  - **잔여 감지**: 동일자 완료로 설명되지 않는 API 는 `scope_shorts` 로 넘겨 **⛔ 미수집 + 경고색**
    으로 뜨게 했다 → 게이트 실패 같은 상류 스킵이 '초록 0종'으로 묻히지 않는다.
- 정책 반영: `docs/PROJECT.md` §2 에 "실행 1회 = 알림 1건, 0종도 발송 + 사유 명시" 추가(§7 변경 이력
  기재). 운영 문서 `docs/operations/recollect-and-alerts.md` §1-b 신설.
- 검증: 단위 테스트 8건 추가/갱신(`test_markers.py` 1 · `test_run_report.py` 4) 전량 통과,
  스케줄러 컨테이너 DagBag 파싱 import_errors 0, 실 마커 존 대상 dry-build 로 문구 실측
  ("동일 KST 일자(2026-07-31)에 이미 완료 152/152종 · 완료 run: `2026-07-31_014858_879` 152종").
  보안 게이트 `python -m security` PASS(차단 0), `test_security.py` 162건 통과.

---

## 2026-07-30

### 85. 서빙 게시 — 무변경 스킵 게이트(payload 지문) (#601)

request:
- "일단위 집계가 아니라 주, 월 등등 단위 집계들이면 gold와 d1의 테이블을 drop할 필요가 없을 것
  같은데. 해당되는 부분 검토하고 수정 적용해줘."
- 이어서 "gold를 통해서 재적재 할 때 해당 정보(핸드오프 메타)도 다 지우고 다시 생성해야 하는지",
  "dbt 전체 도메인 규정에서 벗어나거나 위반되는 게 없는지 재확인" 요청.

response:
- **전제 검증 결과 사용자 전제를 그대로 쓰면 오답이 된다**(실측). 그레인 표기는 변경 여부와
  일치하지 않는다 — 연 그레인 `churn_yearly` 는 행수 5,700 고정인데 매일 212행(3.7%) 변하고
  `dong_summary` 는 417행 중 153행(36.7%) 변한다. 반대로 `flow_monthly`/`flow_yearly` 는
  append-only 증분(`ym > max(ym)`)이라 월/연 경계와 full-refresh 외에는 물리 변경이 0이다
  (dev 6일 연속 일일 run 동안 스냅샷 생성 0회, prod 도 동일). 따라서 판정축을 그레인·달력이
  아니라 **게시 payload 지문**으로 두었다.
- 스냅샷 id 게이트도 기각: 일일 `maintain_silver_gold_tables` 의 OPTIMIZE 가 내용 불변인 채로
  `replace` 스냅샷을 만든다(2026-07-29 20:38Z flow_monthly, added=deleted=total=1,338,717,
  체크섬 2821BCF4E001A158 동일) → 절감이 사라진다.
- 구현(`include/gold/serving_export.py`):
  - `_payload_fingerprint(ddl, colnames, rows)` — `_lit()` 직렬화(=D1 로 보낼 표현 그대로) 기반
    행 지문을 정렬해 합친 순서 무관 해시. DDL·컬럼명·행수 포함(스키마만 바뀐 경우도 잡는다).
  - `d1_publish_state`(commerce 소유, upsert 전용·DROP 금지) — table_name/payload_hash/
    row_count/publication_id/written_at/checked_at. 공유 `d1_meta` 는 positional
    `INSERT OR REPLACE ... VALUES` 라 컬럼을 늘리지 않는다.
  - 게이트: 지문 동일 **and** 상태 행수 일치 **and** `SELECT count(*)` 실측 일치일 때만 스킵.
    지문 부재·조회 실패·행수 불일치는 전부 재기록(fail-open) — 배치 INSERT 중도 실패로 잘린
    테이블을 스킵이 고착시키지 않는다.
  - 스킵해도 **메타는 매 run 갱신**: `_catalog` upsert(`exported_at`·`source_run_id` 전진,
    `serving_status='published'`), `d1_meta`(`snapshot_at`=now, `build_status='ready'`),
    핸드오프 보조 4종 정상 재생성 → 26h 미게시 감시축(`publication_trigger.
    max_interval_minutes: 1560`)이 계속 유효해 **dbt 계약 수정이 필요 없다.**
  - `publication_id` 는 내용이 바뀔 때만 새로 발급(같은 게시가 계속 서빙 중임을 표현).
  - 밴드 게이트 스킵과 **경로·상태 분리**: `stale`·`serve.d1_rowcount_alert`·Discord 경고
    아이콘은 밴드 스킵 전용. 무변경은 리포트 별 줄("무변경 — 행 재기록 생략")과
    `result.status='ok'` 로 처리(정상 스킵이 매일 DAG 를 경고 상태로 만들지 않게).
  - `_write_serve_state` 수리: 이번 run 미게시분의 직전 마커 항목을 이어 싣는다(전량 덮어써서
    서빙 중 스냅샷 기록이 사라지던 문제).
- 규정 점검: 공유 `publication_mode` enum(snapshot/upsert/append)·`gate.py` 상태값 계약 위반
  없음 — 데이터 테이블 무접촉이라 `zero_policy: retain_last_good` 이 그대로 충족되고,
  `serving_status='skipped_retained'` 는 **쓰지 않는다**(내용이 실제로 최신인데 소비 측에 불신
  신호가 되므로). 공유 스키마 변경 0.
- 절감 실측(2026-07-30 D1): 일 261,807행 → 58,813행(**−77.5%**), INSERT 요청 약 2,628 → 597.
  대상은 `d1_flow_monthly`(181,435) · `d1_flow_yearly`(21,559) 2종이며, 하드코딩 명단이 아니라
  22제품 균일 적용으로 자동 판정된다(materialization 이 바뀌면 절감도 자동으로 따라온다).
- 테스트: `tests/test_serving_unchanged_gate.py` 11건(지문 순서 무관·값/스키마/행수 민감,
  무변경 스킵 시 메타 갱신·publication_id 재사용, 행수 불일치·상태 부재·조회 실패 fail-open,
  밴드 스킵 경로 불변). 전 스위트 398 통과 · `python -m security` PASS.
- **적대 검증에서 잡힌 자체 회귀(같은 PR 에서 수정)**: 지문 커밋(`_upsert_publish_state`)을 루프
  밖에 한 번만 두면, 데이터를 이미 쓴 뒤 공유 `_catalog` upsert 등에서 죽었을 때 **D1 은 새 내용 ·
  상태는 옛 지문**이 된다. 이후 원천이 옛 내용으로 되돌아오면(운영자 full-refresh 복구가 정확히
  이 형태) 지문이 일치해 **영구히 스킵**된다 — 행수가 같은 채 값만 바뀌는 건 이 제품군의 정상
  변경 형태라 `_d1_row_count` 도 못 잡고, `exported_at` 은 매 run 전진하므로 26h 감시축·리포트가
  오히려 '정상 최신'으로 읽는다. 패치 이전 코드는 매 run 무조건 재기록이라 다음 성공 run 이
  자가치유했으므로 **이 변경이 만든 회귀**다.
  수정: 파괴적 쓰기 **전에** 지문을 무효화(`payload_hash=''`)하고 INSERT 성공 직후 실제 지문을
  커밋한다 → 불변식 **커밋된 지문 ⊆ D1 실물**. 무효화가 실패하면 DROP 이전이라 D1 무손상(fail-open
  방향 유지). 비용은 재기록 테이블당 쓰기 2회(하루 최대 44행).
  실증: sqlite 백엔드로 `export_to_d1` 을 실제 구동해 예산 컷오프 3~12 스윕 — 수정 전 3·4·5 에서
  영구 고착 재현, 수정 후 전 구간 무손상. 순서 회귀 테스트 2건 추가(총 13건).
  지문 계산 실측 비용: 181,440행 **0.412s**(태스크 타임아웃 60분의 0.011%).
- **별건으로 남긴 결함**(이 변경과 독립): `flow_monthly/yearly/daily` 모델 주석은 소급 도착을
  "정기 full-refresh 스윕(`commerce_load_gold_refresh`)"이 흡수한다고 서술하지만 그 DAG 는
  `schedule=None` 이고 `GOLD_READY_ASSET` outlet 도 없다. 실측상 D1 `d1_flow_monthly` 는 이미
  181,435행 중 72행이 최신 silver 재계산과 다르고(합계 4,616,291 vs 4,616,354) 이 부채는 수동
  트리거까지 누적된다. **매일 재게시해도 1행도 고쳐지지 않는다**(append-only 라 gold 자체가
  과거 구간을 다시 쓰지 않음) → 게시 빈도와 무관한 별도 이슈로 제기.

---

## 2026-07-29

### 84. MCP/API 핸드오프 계보 — D1 보조 테이블 3종 게시 (#593)

request:
- MCP/API 를 직접 개발하는 게 아니라, 개발담당자가 **D1 정보만으로** MCP description·각 key
  역할을 처리할 수 있게 처리 체계/계보를 이어달라는 지시.

response:
- 실측: 타 도메인 `_catalog.columns` 는 name/type 관행(공유 필드 확장 배제), dbt yml 컬럼 설명
  271/271 완비(계보 정본 존재) → **운반 자동화**로 해결.
- serving_export 확장 — 계보 `dbt yml → manifest → export → D1(commerce 소유)`:
  `d1_catalog_columns`(ordinal·타입·description_ko) · `d1_catalog_ext`(source_model·tier·
  grain·primary_key·rollup_rule·time_axis) · `d1_usage_patterns`(meta.serving.usage_patterns
  선언분, 다제품 모델은 d1_table 필터). 매 export 전량 교체(멱등), 공유 메타 무접촉.
- 스왑 스킵 제품 메타 보존(후속 수리): 보조 테이블은 DROP+CREATE 라 성공분만 재삽입하면
  밴드 게이트로 스킵된 제품의 컬럼 설명·질의 예시가 사라진다. 그 제품의 D1 데이터와 `_catalog`
  행은 retain_last_good 로 남으므로 메타만 없어지면 '데이터는 있는데 설명이 없는' 상태가 된다.
  `_preserve_skipped_handoff()` 로 직전 행을 이어 싣는다(현재 gold 스키마로 재생성하지 않는다 —
  미게시 스키마를 설명하면 데이터와 어긋남). 용어사전은 제품 스코프가 아니라 전량 재생성.
  회귀 테스트 4건 추가(tests/test_serving_handoff.py).
- 검증: export 실기록 — columns **222행(설명 221/222)**, ext 22행(grain/PK 정확).
  usage_patterns 는 22제품 패턴 채굴(D1 실행 검증 — 진행분 8제품 77패턴) 후 dbt 선언→재export.
- 추가(용어 한국어 정리 지시): `d1_catalog_glossary`(field·code·label_ko·source) — D1 롤업에
  코드만 실리는 열거값(major/category/event_type/gu_code)의 한국어 라벨을 웨어하우스 실데이터
  (gold `*_ko` 컬럼·행정동 참조)에서 파생해 게시. 하드코딩 없음.

### 83. 처리로그 ops/logs 존 일단위 적재 — commerce_ops_logship 신설(도커 볼륨 무잔존)

request:
- 각 과정 파생 처리로그를 도커(컨테이너/볼륨)에 두지 말고 #60 존(`ops/logs/<domain>/`)에
  일단위로 적재, 적재 후 도커 볼륨에 남지 않게 하라는 지시.

response:
- 신규 DAG `commerce_ops_logship`(매일 01:30 KST): `dag_id=commerce_*` 의 **종결(success|failed)
  run** 로그를 run 단위 tar.gz 로 `{COMMERCE_LOGS_LAYER}/load_date=<run 시작일 KST>/<dag_id>/
  <run_id>.tar.gz` 에 업로드(존재 검증) 후 로컬 삭제. 실행 중/미기록 run 은 보존, 타 도메인
  로그 무접촉. 순수 로직 `commerce_core/logship.py`(+tests/test_logship.py).
- env: `COMMERCE_LOGS_LAYER=ops/logs/commerce`(루트 .env — #60 ops 존, 카테고리별 TTL 가능).
- 실검증: 종결 run **206개 번들(13.2MB) 적재·검증·로컬 삭제, 실패 0** — 실행 중 2 run 만 보존.
  security log_event 영수증(masked JSON) 반환.

### 82. serving _catalog 를 타 도메인 형식과 동형화 — time_axis/freshness 채움 + prod 첫 서빙 실측 2건 처리

request:
- D1 적재 시 선행 이슈(#478/#77)의 카탈로그 등록이 시행됐는지 확인하고, 빠진 부분은 원격지
  업데이트를 검토해 타 도메인과 같은 형식으로 정합하라는 지시. D1 100만행+ 제품은 제외 판정.

response:
- **행수 감사(제외 판정)**: SERVING_SPEC 22종 전수 실측 — 최대 181,435행(flow_monthly), 100만+
  0건(290만 원장 flow_daily 는 기존 설계상 iceberg_api 제외 유지), 밴드 이탈 0건 → 전량 포함.
- **첫 D1 적재**: 22종 스냅샷(총 261,802행) — D1 행수 = 감사치 22/22 일치, `_catalog` 22행
  upsert(타 도메인 행 무손실 — 이후 30행 변화는 transit 자체 갱신 실측 확인), `d1_meta` 22행,
  serve state 는 ops 존.
- **동형화 갭**: 타 도메인은 시간축 보유 제품에 `time_axis`/`freshness` 를 채움(citydata 12/17,
  weather·traffic 10/10) — commerce 는 #77 유보로 NULL. 원격(common/serving) 검토 결과 정본
  `publisher._freshness`(= str(max(event_time))) 존재 → serving_export 에 `_freshness_of` 이식,
  dbt meta.serving 에 `event_time` 선언(flow_monthly=ym·flow_yearly=y·churn_yearly=y — 순환·
  코호트 축 제품은 미선언 관행 준수). 재export 실측: `2026-06`/`2025`/`2025` 채움, 스냅샷 19종
  NULL 유지.
- **prod 첫 gold 사건 기록**: (a) dbt seed 부재로 21모델 전멸(#567 로 구조 수리), (b) seed 직후
  재실행 배치에서 4모델(cohort_survival/lifespan/address_succession/env_facility)이 **성공-0행**
  — 입력·SQL 정상(직접 실행 7,502/2,867/99/51행)이라 신생 taxonomy 테이블에 대한 Trino 메타
  캐시 정황(비결정, 17모델은 정상). 재실행으로 복구, gold 교차검증 22/22 정상(+6.59% = 복구분
  전파). **후속 권고: gold 모델 최소행수 dbt test 부재**(0행 스냅샷이 test 통과) — 밴드 게이트가
  D1 단에서 방어하나 gold 단 방어 없음.

### 81. dbt seed 부트스트랩 — silver DAG 에 매 run 멱등 재적재 태스크 신설 (#567, prod from-zero 실측 갭)

request:
- prod from-zero 적재 중 gold 21/22 모델이 `commerce_dataset_taxonomy` 부재로 동시 실패(실측).
  향후 환경 변경/이관 재적재에서도 같은 에러가 나지 않게, 적합한 레이어를 검토해 seed 가
  자동 적재되도록 조치하라는 지시.

response:
- 원인: 분류 체계 참조가 dbt seed 인데 어떤 DAG 에도 seed 단계가 없어 dev 의 과거 수동 1회
  시드에 잠재 의존. 소비자 = gold 22종 중 21종 + silver `silver_license_detail_health`(파킹).
- 레이어 판단: dbt 진입 레이어인 **silver DAG** 가 정위치(사용자 판단 일치) — silver 에서
  적재되면 gold 는 파이프라인 순서상 구조적으로 보장.
- 구현: `commerce_load_silver.dbt_seed_taxonomy` 태스크 — 매 run `dbt seed
  --target {COMMERCE_DBT_TARGET or DBT_TARGET}` 멱등 실행(존재 시 전량 교체·152행·수 초).
  배선 `[dbt_seed_taxonomy, enrich_admin_dong_ref, enrich_fill_jibun, ensure_silver_marker]
  >> seed_silver_if_empty >> dbt_silver`.
- 검증: `airflow tasks test` 실실행 `{'seeds_loaded': 1, 'target': 'prod'}` success ·
  DAG import 무에러. 운영 선조치로 prod 수동 seed(152행=dev 일치) 후 gold run 재개 정상.
- 동류 갭 기록: `raw/common/admin_dong` 원천 부재 → silver enrich 실패(운영 조치: dev 스냅샷
  138개 복사, 리더는 max(load_date) 자동 선택이라 원천 오너 정기 적재 시작 시 자연 대체).

## 2026-07-28

### 80. run 마커를 control 존으로 재배치(#60 오너 해석) + 재감사 확정 갭 5건 수리

request:
- 사용자 재검토 지시: `raw/…/run_id=…/_markers/` 도 control 레이어로 옮겨야 할 것으로 보임 — 파일
  이동 누락과 로직 누락을 재검토 후 작업. **오너 해석 확정**: #60 약속③은 마커의 '내용' 규약일 뿐,
  요지는 "수집 등 중간 과정이 읽는 **지시 파일**은 control 하위로 모은다" → run 마커도 이동 대상.
- 부가 명시: 행정동 코드 원천은 `raw/common` 유지(#60 부록의 reference/ 이사 초안 미적용) —
  commerce 는 이미 `raw/common/admin_dong/…` 를 읽으므로(enrich_tasks) 코드 무변경.

response:
- **마커 존 신설**: `COMMERCE_MARKERS_LAYER`(prod `ops/control/state/commerce/markers`, 미설정 시
  구 위치 폴백). 경로는 run 폴더와 `load_date=/run_id=` 1:1 미러. paths(markers_run_dir/
  run_index_root/markers_date_prefix) · markers(list_run_ids 가 마커 존 스캔 — identical run 은
  마커만 남기므로 단일 소스) · watchdog(_RUN 탐지 파일명 기준) 정합. 기존 마커 3,667개를
  `scripts/relocate_run_markers.py`(dry-run/--apply, 복사→크기검증→원본삭제)로 seoul 내 재배치.
- **멀티에이전트 재감사(14 에이전트, 적대 검증)**: 파일 이동 누락 **0건** 재확인(5,075개 키·바이트
  집합차 대조 — 초과 1건은 스모크 마커, receipts/silver state/_backup/watchdog 구 마커 skip 은
  전부 정당 판정). 로직 갭 5건 확정·수리:
  - **F5**: `_full/` 고아 랜딩(랜딩~diff 사이 중단 시 영구 잔존) — cleanup_incomplete 가 동일자
    성공 시 랜딩까지 정리 + 마커 없는 중단 run 을 raw 일자 파티션에서도 발견(_same_day_run_ids).
  - **F7**: 같은 run 재시도로 completed·incomplete 공존 가능 — completed 기록 **후** 잔존
    incomplete 삭제(상호배타 계약 유지, 역방향 금지).
  - **B2**: purge_v2 의 purge_raw 가 ops 존 diff_target/마커를 못 찾음 — 존 스캔 추가
    (classify_zone_keys, stem 정확 일치).
  - **B3**: purge_v2 purge_gold 가 폐사한 `gold.pg` import 로 런북 전체 크래시 — fail-soft skip
    (gold=Iceberg, silver purge 후 다음 gold run 반영)으로 교체.
  - **B9**: cleanup_orphan_warehouse_dirs 가 무조건 dev 카탈로그를 감사 — COMMERCE_DBT_TARGET
    우선 해석으로 정합(프로드 감사 시 orphan 오분류 방지).
  - (B7) 구 위치를 서술하던 독스트링(seed_diff_target/incremental/resort/load_plan) 정정.
- migrate 스크립트 매핑에 run_marker 분기 추가(재실행 시에도 마커는 존으로). 테스트 381→
  스위트 전체 통과(신 존 왕복·고아 GC·배타·매핑 케이스 추가), security PASS.
- 문서: storage.md(마커 존 절·prod R2 블록)·common_info·data-model·README·configuration.md 정합.

### 79. R2 저장 위치 개편 — #60 규약(load_date= 파티션·ops 존) + seoul(프로드) 버킷·iceberg 카탈로그 전환 + 이력 이관 (ASK-Seoul#60)

request:
- ASK-Seoul#60(경로 규약 — 약속 3개 + 존 정리)을 토대로 저장 위치 변경. 단 목적지는 dev 가 아니라
  **`seoul/`(프로드 버킷)**. 다른 properties 는 필요 시에만 명시·허가 후 변경, commerce 저장경로는 변경 OK,
  하드픽스 지양(env 조합). seoul-dev 의 기존 raw 를 새 구조로 seoul 에 적재 가능하게 이관.
- Q&A 확정: **Iceberg 카탈로그도 seoul(prod `iceberg`)로 전환** · 공유 `TRINO_ICEBERG_CATALOG` 는
  `iceberg_dev` 덮어쓰기(멘티 안전망)를 **진짜 prod(`iceberg`)로 복원 승인** · bronze 적재 실행은 **보류**
  (적재 전 생성물·위치 프리뷰 확인까지 완료, 실행만 대기).

response:
- **약속① (`load_date=` key=value 날짜)**: `paths._run_date_dir` 가 `YYYY/MM/DD` → `load_date=YYYY-MM-DD`
  파티션 생성. 리더(markers/load_plan/incremental)는 `run_id=` 부분문자열·run_id 문자열 기반이라 무변경 —
  신·구 공존 판독. watchdog 의 일자 스캔은 신설 `paths.raw_date_prefix()` 사용.
- **약속② (가변 상태 raw 밖 → ops 존)**: diff-target 를 `COMMERCE_DIFF_TARGET_LAYER`(신설 env, 미설정 시
  구 위치 폴백)로, watchdog 가드를 `COMMERCE_WATCHDOG_STATE_LAYER`(신설, 구 `state/commerce/watchdog`
  하드코딩 제거)로. 기존 env 3종(BRONZE/SILVER/SERVE_STATE_LAYER)은 **값만** `ops/control/state/commerce/
  {bronze,silver,serve}` 로. 약속③(완결성 마커)은 기존 준수.
- **prod 전환(env 조합, 코드 하드픽스 없음)**: `.env.commerce` `R2_BUCKET=${R2_BUCKET_NAME:-seoul}`
  (R2 자격증명은 루트 동일 이름 직접 상속으로 단순화). `warehouse._is_dev()` 가 `COMMERCE_DBT_TARGET`
  우선(공유 `DBT_TARGET=dev` 무변경 — silver/gold DAG 와 동일 규약, gold/serving 은 `_qualified()` 경유
  전파). 루트 `.env`: `COMMERCE_DBT_TARGET=prod` + 레이어 5종 + `TRINO_ICEBERG_CATALOG=iceberg` 복원(승인).
- **이력 이관**: 신규 `scripts/migrate_raw_to_prod_bucket.py`(purge_v2 패턴 — dry-run 기본/`--apply`,
  서버사이드 copy, 멱등, 소스 무삭제). 실측: dated run 4,771개(2.67GB)→`load_date=` 파티션,
  diff-target 304개(2.74GB)→ops 존, `_backup` 78개·테스트 잔재 2개 제외 — **5,075개 복사, 실패 0·불일치 0**.
  상태(워터마크/영수증)는 **이관 안 함**(prod 는 from-zero 적재 예정이라 dev 워터마크 이관 금지 — 리셋).
- **검증**: pytest 372 passed(레이아웃·ops 존·매핑 신규 테스트 포함) · `python -m security` PASS ·
  컨테이너 실측 ALL GREEN(`r2_bucket=seoul`·`_qualified()=iceberg.commerce`·워터마크 키 ops 존·공유
  `DBT_TARGET=dev` 유지) · Trino `SHOW CATALOGS`/PyIceberg REST 로 prod 카탈로그 접근 확인(기존 토큰 충분) ·
  수집 스모크(resort_complex) → `raw/commerce/load_date=2026-07-28/run_id=…` + ops 존 diff-target 실기록.
- **적재(보류 상태)**: 적재 계획 프리뷰 실측 — base 152건/2,631MB(PyIceberg)+증분 954건/18MB(Trino),
  run 35개(6/30~7/28), 152/152종 base 확보. **실행은 사용자 보류** — 실행 시
  `COMMERCE_LOAD_LOOKBACK_DAYS=0` 1회 오버라이드로 `commerce_load_bronze` 트리거(기본 3 창은 base 를
  배제해 오적재 위험). 그때까지 **load 3종(bronze/silver/gold)+watchdog pause 유지**(빈 워터마크에서
  기본 창 실행 방지), `commerce_collect_raw` 만 재개(신규 수집은 신 레이아웃으로 정상 기록).
- 호스트측: 루트 `.env.example` 에 신규 키 반영(별도 PR). seoul-dev 는 완전 보존(롤백 = env 되돌림).

### 78. commerce env 를 루트 `.env` `commerce 전용값` 블록 단일 소스로 이관 + `.env.commerce` 매핑 레이어화 (호스트 env 계약 변경)

request:
- 호스트 프로젝트 오너 지시: commerce 의 env 값 중 루트 `.env` 를 상속하지 않던 것을 모두 루트 `.env`
  기준으로 상속하도록 바꾸고, commerce 에만 관리되던 값은 루트 `.env` 에 `commerce 전용값` 블록으로 묶어 추가.
- generic 이름 네임스페이스 방식 Q&A → **`COMMERCE_` 접두(옵션 B)** 채택(타 도메인 공유 env 오염 방지).

response:
- **계약 반전**: 기존 규약("commerce 변수는 루트 `.env` 에 넣지 않고 `.env.commerce` 로 공급, 번들 자립")을
  뒤집어 **루트 `.env` 의 `commerce 전용값` 블록**을 단일 소스로 삼는다. `dags/` 는 서브모듈, 루트
  `.env`/`.env.example` 는 호스트 프로젝트 파일이라 변경이 두 리포로 나뉜다(호스트 인프라 파일
  `docker-compose.yml`/`Dockerfile.airflow` 무접촉 규약은 유지, env 값만 예외).
- **루트 `.env`/`.env.example`**: `commerce 전용값` 블록 신설. generic 이름(`SEOUL_PAGE_SIZE`·
  `STORAGE_BACKEND`·`LOCAL_DATA_ROOT`·`SCHEMA_VERSION`·`R2_REGION`·`SEOUL_MAX_PAGES`·
  `SEOUL_REQUEST_DELAY_SECONDS`)은 `COMMERCE_` 접두로 네임스페이스, 이미 안전한 `COMMERCE_*`/`JUSO_*` 는
  동일 이름. `JUSO_CONFM_KEY` 시크릿도 루트로 이관(루트 `.env` 는 gitignore).
- **`.env.commerce`(실파일)/`.env.commerce.example`**: 얇은 매핑 레이어로 재작성 — generic 이름은
  `${COMMERCE_<KEY>:-기본}` 으로 코드 이름 복원, R2 는 기존대로 `${R2_DEV_*}` 매핑. `:-기본` 은 루트
  `.env` 없이 단독 실행 시 settings.py 기본값과 동일(빈문자열 footgun 방지). `.env.commerce` 에서 시크릿 제거.
- **코드 무변경**: settings.py 는 여전히 generic 이름을 읽고 `.env.commerce` 가 되돌리므로 include/ 코드
  무변경. 충돌 실측 — 루트 승격 대상 generic 이름의 타 도메인 사용처는 상수/독스트링/동일 기본값
  (`R2_REGION=auto`)뿐이라 무해.
- **검증**: 실제 `commerce_core.env` 로더로 (1) compose 시나리오(루트 `.env` 주입) → `settings` 8/8 값
  변경 전과 동일, (2) 단독 실행 시나리오 → generic 키가 코드 기본값으로 fallback(`SCHEMA_VERSION` 공백
  footgun 없음) 확인.
- **문서 정합**: CLAUDE.md(Working Scope·부트스트랩), configuration.md(§1 주입·§3 대응표),
  environments.md, configuration/README.md, README.md, project_setting.md(포팅 체크리스트 포함)를 새 모델로 갱신.
- **트레이드오프**: 번들 자립/이식성이 낮아짐 — 포팅 시 대상 호스트 `.env` 에 `commerce 전용값` 블록을
  함께 옮겨야 한다(project_setting.md 이식성 체크리스트에 명시).

### 77. D1 서빙을 도메인 공통 Serving Contract v1(#478) 규격에 정합 — `_catalog` 8→15컬럼 + dbt 확정 필드 (#493 보강 · ASAC-DBT#334 보강)

request:
- 현재 D1 적재가 ASAC-DAG **#478 확정 필드(`meta.serving.*`)** 와 **`_catalog` 공통 규약
  (8→15컬럼)** 을 따르는지 확인하고, 다른 부분을 규격에 맞게 수정.

response:
- **`_catalog` 15컬럼 정합(export)**: 자체 8컬럼 스키마(`serving_tier` 포함, 정본에 없는 컬럼)를
  버리고 **`common/serving/d1_client.py` 의 `CATALOG_COLUMNS`/`CATALOG_DDL`(15컬럼, #478 §3.4)** 를
  lazy import 로 단일 소스 소비. 상호운용 버그 해소 — 정본 15컬럼 `_catalog` 가 이미 있으면 8-value
  bare `INSERT` 는 즉시 실패하고, commerce 가 먼저 돌면 8컬럼 테이블을 만들어 타 도메인을 깨뜨렸음.
  upsert 는 **명시 컬럼 리스트**로 전환(컬럼 순서 드리프트 안전). 신규 기록값: `product_id`
  (`commerce_*` = d1\_\* 1:1 파생) · `external`(dbt 계약) · `product_question` · `serving_status=
  'published'` · `publication_id`(uuid4) · `source_run_id`(export dag_run_id) · `published_bytes`
  (json bytes, 정본 산식) · `freshness`/`time_axis`(v1 은 event_time 미선언 → NULL). 밴드 게이트
  스킵분은 `_catalog` 무접촉(직전 published 행 = 서빙 중 스냅샷 서술 유지, 스킵 상태는
  `d1_meta.build_status='stale'` 담당).
- **dbt 계약 #478 확정 필드 정합(ASAC-DBT, 짝 커밋)**: 22 gold `meta.serving` 을 재작성 —
  `enabled`/`external` 추가(flow_daily 는 둘 다 false — iceberg_api 직조회, D1 미게시),
  `contract_version: v1`, `publication_mode: snapshot`(구 iceberg/rollup 은 enum 위반 — 실제 메커니즘이
  전량 교체 스냅샷), `zero_policy: retain_last_good`(구 keep_prior 동의어), `partial_policy:
  {min_publish_ratio: 0.5}`(행수 밴드 ±50% 하한의 계약 표현), `refresh` → `publication_trigger:
  {trigger_type: asset, max_interval_minutes: 1560}`(gold Asset 트리거·26h stale 감시축),
  `product_id` → `commerce_*` 도메인 접두(전역 유일), `shape` 추가. commerce 확장(`serving_tier`/
  `d1_table`/`d1_rollup`)은 추가 필드로 유지(Validator 는 미지 필드 허용). **PK 근거** — 복합키 21종에
  자체 매크로 `unique_combination_of_columns`(dbt_utils 무의존) 모델 테스트 + grain 축 `not_null`
  보강(선언 PK 22종 전부 모델 SQL `GROUP BY` 실측과 일치 확인).
- 검증: #478 Validator 규칙 로컬 재현 → **22모델 0 findings**(serving-contract-gate 통과 형상) ·
  export import + 15컬럼 upsert SQL 렌더 검증 · `py_compile` OK · `python -m security` PASS(차단 0).
  D1 실적재 재검증(행수 밴드 재보정)은 배포 후 후속 그대로.
- **구 적재 잔재 삭제(2026-07-28 실측·실행)**: 공유 D1 에 남아 있던 commerce 구 반복 잔재
  `gold_license_dong_summary` 테이블 + `_catalog` 행 1개를 삭제(신규 규약은 `d1_dong_summary` 라
  새 export 가 지우지 않는 고아). 삭제 후 검증 — commerce 잔재 0 · `_catalog` 타 도메인 23행
  (transit 6·citydata 17) 무손실. `d1_meta`·`d1_*` 는 애초 미존재(PR 미배포).
- **⚠ 배포 블로커(팀 이슈 필요)**: 실측 결과 공유 `_catalog` 는 아직 **구 8컬럼 스키마**
  (`serving_tier` 포함)로 transit·citydata 가 서빙 중 — 15컬럼 정본(`CATALOG_DDL`)은
  `IF NOT EXISTS` 라 기존 테이블을 못 바꾸므로, **8→15 마이그레이션(정본 컬럼 순서로 재생성) 전에는**
  commerce 신규 export 의 `_catalog` upsert 도, 공통 Publisher 의 bare 15-value INSERT 도 실패한다.
  마이그레이션은 타 도메인 행·Worker 소비(`serving_tier` 의존 가능)에 영향 → 도메인 공통 결정
  (#478 계열 이슈)로 제안할 것. commerce 단독 수행 금지.

---

## 2026-07-23

### 76. gold→D1 서빙 export 분리 DAG(commerce_serving_export) + dbt serving_tier 계약 (#493 · ASAC-DBT#334)

request:
- dags 에서 SQLite(=D1) 적재를 **기존 gold 라인에서 분리**해 신규 DAG 로 올린다. 기존 gold 에서
  **지정한 품목만** D1 에 갱신되도록 구성. 위 변경에 맞춰 dbt 계약도 함께 반영. 구현 깊이=**코어
  서빙셋**(direct 15 + rollup 7 + d1_meta + 행수 게이트 + _catalog/R2 마커, dim·current-period 후속),
  기동=**gold 완료 Asset 자동 트리거**(사용자 확정).

response:
- **분리 DAG `commerce_serving_export`**: gold 빌드 라인(`commerce_load_gold`)과 서빙 export 를
  분리(spec §1.4 의 "gold DAG 내 편입" 대신). `commerce_load_gold` 에 `mark_gold_ready`
  (outlet Asset `iceberg://commerce/gold`, 기본 all_success — **dbt_gold 성공 시에만** 발행) 추가 →
  export DAG 는 `schedule=[Asset(...)]` 로 자동 기동(분리 유지 + 신선도). Asset 상수는 번들 자립
  `include/gold/assets.py`(공유 `common.assets` 미변경).
- **export 모듈 `include/gold/serving_export.py`**: 지정 품목(dbt `meta.serving.serving_tier ≠
  iceberg_api`)을 공유 D1(`ask-seoul-dev-d1`, citydata·transit 와 동일 DB)에 **전량 교체 스냅샷**.
  direct 15(SELECT * + 동적 DDL) + rollup 7(§1.3 GROUP BY 파생 — flow_m/y·churn 비율 재산출·
  geo overview/detail·age_band·uptae share). commerce 소유 `d1_*` 만 DROP+CREATE, 공유
  `_catalog`/`_request_log`/`d1_meta` 는 **upsert(DROP 금지 — transit 규약 승계)**. 스왑 전 **행수
  밴드 게이트**(0행/2배 가드 — 밖이면 스킵 + `build_status='stale'` 직전 유지). R2 export 마커
  `commerce_serve_state/_export_state.json`(재개·감사 정본, silver/bronze state 대칭). D1 HTTP API 는
  `security.http_post`(timeout·TLS·예외 마스킹), 토큰 `CLOUDFLARE_API_TOKEN`(자동 마스킹), Trino 는
  `bronze.warehouse`(번들 자립).
- **dbt 계약(ASAC-DBT#334)**: `_commerce_gold__models.yml` 22모델 `meta.serving` 재정리 — 행수캡
  `enabled` → `serving_tier`(d1_direct 15 / d1_rollup 6 / iceberg_api 1) + `d1_table` + rollup 축.
  `gold_license_flow_daily`(원장 290만행)=iceberg_api(D1 금지, Trino 직조회). export `SERVING_SPEC`
  (22 D1 테이블)과 1:1 대조.
- **env**: `.env.commerce.example` 에 `CLOUDFLARE_API_TOKEN`·서빙 account/DB id·`COMMERCE_SERVE_STATE_LAYER` 추가.
- **검증**: py_compile 4파일 OK · `python -m security` PASS(차단 0) · export 모듈 import·SERVING_SPEC 22
  (중복 0)·롤업 SELECT 포맷 OK · dbt yml YAML 파싱 22모델(로컬 `dbt-trino` 어댑터 부재로 `dbt parse` 는
  컨테이너/CI 이관). D1 실적재는 컨테이너(Trino·D1 토큰) 필요 — 배포 후 행수 밴드 실측 재보정.
- 후속(별도 이슈): dim 4종 · current-period 2종(`agg_license_daily/monthly`) · 행수 밴드 실측 보정 ·
  D1 스왑 원자성(`*_next` 스테이징).

---

## 2026-07-20

### 75. from-zero 재빌드 드릴 — bronze/silver/gold 결함 3건 수정 + 태스크 개명 (#458·#459·#460)

request:
- dev 원격 기준으로 bronze/silver/gold 를 **완전히 0부터** 재실행되는지 검증(raw 는 보존, 3레이어만
  전량 삭제). 마지막에 raw 수집을 켜 오늘 미수집분까지 전구간 온전 검증. weather 전용 airflowignore
  스코프 가드는 오적용이니 교정. 집계의 년/월/주 증분 불가 단위 대응 여부 확인. 완료 후 bronze/silver/
  gold 단위 이슈·PR. 유지보수 태스크 이름이 실제 대상과 어긋나니 개명. 드릴 결과 change-log 기록.

response:
- **airflowignore 교정(선반영)**: root `.airflowignore` 의 weather/traffic 전용 스코프 가드 제거 →
  전 도메인 재파싱(dev 에서 #442 로 이미 동일 교정돼 rebase 시 흡수). commerce DAG 8종 재노출 확인.
- **Teardown(dev 전용, prod 없음 확인)**: bronze 4 + silver 83 + gold 22 = **109 테이블 drop**
  (PyIceberg, purge 없음 — #74 안전 경로) + bronze/silver 상태 gate 파일 4개만 리셋(receipts 보존).
  commerce_dataset_taxonomy(seed)·meta_detail_catalog·raw(불변) 보존. R2 write rate-limit
  (TooManyRequests) 대응 재시도·페이싱 필요.
- **재빌드 순서**: bronze(commerce_load_bronze, raw 21 run 06-30~07-19 → 152종) → silver
  (commerce_load_silver, 청크 seed cold build — history window OOM 회피) → gold
  (commerce_load_gold_refresh, 24모델 full-refresh). from-scratch 는 수동 `dbt --full-refresh`
  금지, DAG 청크 seed 경유가 정본(#59 원칙 — 무거운 layer 는 이제 silver).
- **결함 3건 수정(전부 cold rebuild 에서만 발현)**:
  - **① bronze first_load 워터마크 스킵**(`bronze/load_plan.py`, #458): base 파일 run 이 lookback
    창 밖일 때 창 안 identical run 이 워터마크를 최신으로 전진 → base 영구 skip(11개 dataset 실측,
    1회차 2,274행/80ds 만 적재). 수정: first_load 인데 base 미배정이면 워터마크 전진 보류. 회귀
    테스트 10/10.
  - **② silver 청크 대형경로 원형 누락 + entity_history 전역 워터마크**(#459): 대형 dataset 버킷
    경로가 history·current 만 돌고 entity/entity_history 를 건너뜀(대형 4종 entity 0행 — entity
    증분 전환으로 표면화) + entity_history 의 전역 collected_at 워터마크가 청크 배치에서 dataset 을
    통째로 탈락시킴(152→5종). 수정: `chunked_run.py` 에 원형 phase 추가 + entity_history 를
    include_datasets 스코프 워터마크로. 실제 재현→복구 검증. dbt 측 수정은 ASAC-DBT PR.
  - **③ gold refresh 완주 불가**(`gold/report.py`, #460): commerce_load_gold_refresh 가
    send_gold_report(catalog=, load=) 로 호출하나 시그니처에 없어 매 실행 TypeError → **이 DAG 는
    끝까지 성공한 적이 없고 Discord 알림도 발송된 적 없음**. 수정: catalog/load 선택 인자 수용.
    회귀 테스트 4/4.
  - (부수) silver_license_entity **table→incremental 전환**(ASAC-DBT): 매 run 523MB sibling 고아
    원천 제거(#74 계열). 이 전환이 ②를 표면화시킨 계기 → 두 리포 PR 함께 머지 필요.
- **운영 노트(코드 아님)**: 전량 재적재 중 PyIceberg base 적재가 Trino delete 파일의
  `DELTA_LENGTH_BYTE_ARRAY` 인코딩을 PyArrow 가 못 읽어 실패(`Not yet implemented: … DeltaLength
  ByteArrayDecoder`). 전 재적재 전 `ALTER TABLE bronze_localdata_license EXECUTE optimize(
  file_size_threshold => '10GB')` 로 delete 파일 강제 병합(기본 threshold 는 0파일 처리로 무효).
- **태스크 개명**: silver DAG 의 `maintain_silver_gold_tables` → **`maintain_silver_gold_tables`**. 실제
  대상은 silver 원형 2 + silver detail 78 + gold 집계 22 + meta 1(= silver+gold 혼합, **bronze
  아님** — bronze 유지보수는 commerce_load_bronze 의 iceberg_maintenance). 이름이 gold 전용으로
  읽혀 오독 유발하던 것 교정. commerce_load_gold docstring 참조도 갱신.
- **검증(dev 실측 정본)**: bronze 152/152 ds·2,795,606행·데이터단위 746=발행가능 매니페스트 746 /
  silver entity=current=2,788,155·entity_history=history=2,795,295·둘 다 152ds·무마킹 0·마커 746 /
  gold refresh success 51/51·22종·flow 3종 재실행 INSERT 0 rows 멱등·Discord 알림 정상 발송. 테스트
  load_plan 10/10 · silver_chunked 5/5 · gold_report 4/4.
- **집계 년/월/주 증분(확인 요청)**: flow daily/monthly/yearly 는 **완결기간만 append**(당일/당월/당해
  원천 배제) → 미완결 현재기간은 별도 d1_current 트랙(전량 교체 스냅샷 + 조회시점 상대기간 계산)이
  담당하는 설계로 **올바르게 분리**됨. 단 d1_current(agg_license_daily/monthly)·D1 export 는
  설계·준비완료 상태이고 **미구현**(별도 과제). 주(week) 전용 집계 테이블은 없음 — '이번주'는 daily
  롤링 400일 조회시점 롤업(본질상 미완결이라 append 대상 아님, 정합).
- **PR/이슈**: 이슈 ASAC-DAG #458(bronze)·#459(silver)·#460(gold). PR ASAC-DAG #461(결함 3건 +
  orphan 감사 수정 + 개명 + 언어규정) · ASAC-DBT #302(entity 증분 + entity_history 워터마크). 두 PR
  함께 머지. 드릴 후 commerce DAG 8종 paused(standby) 복원(§7.3 계약).

---

## 2026-07-15

### 74. R2 orphan(__dbt_tmp) 전량 정리 + gold description(culture 스타일) + 정리 도구 제도화

request:
- silver/gold 단계에 불필요한 데이터(메타 등) 잔여 확인 후, 남지 않도록 조치. 이어서 gold 테이블의
  기능·역할 description 을 culture 도메인 gold 참조해 부여.

response:
- **잔여물 실측**: dbt `table` materialization 이 매 실행 `<model>__dbt_tmp-<uuid>` 물리 디렉터리를
  남기고 RENAME(메타 이동)으로 테이블화 → 카탈로그엔 없지만 **R2 물리 파일 누적**(commerce ns
  410 디렉터리 중 258 __dbt_tmp). remove_orphan_files 는 location 내부만 봐서 sibling 못 지움.
- **orphan 삭제는 불안전으로 판명 → 리포트 전용으로 회귀(중요)**: 네임스페이스 수준 orphan 삭제를
  시도했으나 **R2 Data Catalog 에서 dbt 생성 테이블의 현재 metadata.json 물리 디렉터리를
  location/metadata_location 만으로 신뢰성 있게 식별 불가** → keep-set 이 라이브 메타를 orphan 으로
  오판·삭제해 **gold 11종을 2회 파손**. 게다가 **Trino 메타 캐시가 삭제 직후 count(*) 검증을
  통과시켜 파손을 은폐**(캐시 만료 후 "Metadata not found" 로 발현). 매번 PyIceberg drop + silver
  재빌드로 **전량 복구(데이터 손실 0 · bronze/silver 원천 전 과정 무손상)**. 결론: 이 워크로드에서
  sibling orphan 자동삭제는 순이익이 아니므로 **`scripts/cleanup_orphan_warehouse_dirs.py` 를
  삭제 기능 제거·리포트(감사) 전용으로 재작성**. 누적 관리는 (a) 전체 재빌드, (b) 테이블별
  Iceberg `remove_orphan_files`(location 내부·스냅샷 보존)로. metadata.json 은
  ensure_metadata_retention(previous-versions-max=50)이 이미 상한(#71). 스냅샷·데이터 행 미삭제.
- **gold description(culture 참조)**: 22종 전부 culture `_culture_gold__models.yml` 스타일
  (그레인 — '대표 질의(기능·역할)'. 핵심 특징)로 재작성. gu_specialization 설명의 미인용 `예:`
  (콜론+공백)이 schema.yml YAML 을 깨 dbt run 이 실패하던 것 발견 → 값 따옴표로 수정.
- **gold 서빙 설계 문서 2종 신설**(Ultracode 워크플로 — 6 화면 테마 팬아웃→종합→비평):
  dbt `docs/DB/gold/serving-design.md`(지표 의도·용도 + 22종→화면→서빙tier 매핑) +
  `opus-serving-build-instructions.md`(D1/Iceberg 이원화 아키텍처·D1 DDL·API 명세·화면 명세).
  대용량(flow_daily 2.9M 등)은 Iceberg 직조회 또는 소형 롤업만 D1, 소형은 D1 직접 export.
- 검증: pytest 359 · dbt run(gold 22 전수 빌드) 0 에러 · 서빙 문서 완결성 비평 PASS(22 전수 매핑).

### 73. gold 전량 재적재(R2 삭제→재빌드) + flow append-only 전환(재실행 0건 멱등 확인)

request:
- 이전까지 적재된 commerce gold 아웃풋을 R2 에서 삭제하고 silver 산출물로 gold 전체 재적재.
  적재 후 gold 재실행 시 **업데이트 0건**으로 올바르게 처리되는 것까지 확인(확인될 때까지 반복).

response:
- **flow 3모델(daily/monthly/yearly) append-only 전환**: 기존 "지연보정 창 delete+insert"는
  재실행마다 창을 재삽입해 0건이 안 나옴 → **기적재 최대기간 초과 완결분만 append**(문자열
  워터마크 비교)로 변경. 지연 도착 소급분은 --full-refresh 스윕으로 이관(트레이드오프 문서화).
- **전량 재적재**: gold 22종 DROP(R2/Iceberg 삭제, 잔존 0 확인) → `dbt run --select tag:gold`
  전량 재빌드(22/22, silver_license_entity/history/detail+taxonomy 기반). 삭제 전/후 행수 일치
  (자정 경계 이동분 flow_daily +12 등 미세차는 완결일 boundary 이동으로 정상).
- **멱등 확인(사용자 요구 — 반복 검증)**: gold 재실행 → flow 3모델 **INSERT (0 rows)** ·
  22 테이블 행수 **완전 일치**(diff 0). flow 만 3회차 재실행까지 0건 재현. table 모델은 CREATE
  OR REPLACE(현재상태 스냅샷 — 결정적 재계산이라 행수·내용 동일).
- 검증: pytest 359 · dbt parse 0.

### 72. gold 인사이트 소진 탐색 — 신규 14모델 실증(총 집계 20종) + 명단 정본화

request:
- silver 결과 테이블로 gold 로 뽑을 수 있는 **모든 의미있는 지표**를 소진 탐색·실증하라
  (공통 조건 집계면 무엇이든, 지역 축 포함, 더 없을 때까지 계속 발굴·반영).

response:
- **다각도 discovery**(8렌즈 병렬 + 비평) → 후보 104+α → 자체 랭킹으로 실증. **신규 14모델**
  (전부 실빌드 + 실데이터 유의미성 검증):
  survival: lifespan(즉석판매 조기폐업 69.5%)·cohort_survival(2018 보건 5년 38.9%) ·
  temporal: seasonality(개업 성수기 1·3·4월) · stock: stock_age_band ·
  geo: gu_specialization(성동 축산 LQ 5.77)·dong_category_matrix·geo_grid(top 격자=가산·선릉·강남역) ·
  churn: churn_yearly(2024 food 18.9%)·address_succession(food→food 9.9만, 중위 8일) ·
  quality: data_quality · change: status_transition(씨앗)·change_activity(씨앗) ·
  detail: detail_area_profile(23-way union, 음식점 116㎡)·multi_site(동물약국 74.6%).
- **drop 판정(사유 기록)**: yoy(flow 파생쿼리)·dtl_status_mix(dataset 스코프)·suspension_episode
  (3.7K 희소)·HHI(LQ 갈음)·요일 패턴(행정처리일 왜곡)·단일 dataset 특수지표(범용성).
- **명단 정본화**: report.AGG_TABLES 1곳 → DAG select(정기/refresh)·유지보수·메타 사본 상한이
  전부 소비. 총 gold 집계 20종.
- 검증: pytest 359 · security PASS · DAG/dbt parse 0 · 씨앗 지표(전이/개명)는 이력 축적
  (6/30 수집 개시)에 따라 성장함을 문서 명시.
- **최종 소진 라운드(비평 재판정)**: 확실 2건 추가 구축 — `gold_license_phone_succession`
  (연쇄창업: 동일 전화 폐업→개업 업종전환, food→food 2.0만·중위 90일) ·
  `gold_detail_uptae_mix`(업태 축 — 일반음식점 내 한식 5.1만, 19-way union). 조건부
  colocation 은 geo_grid(업종 차원 보유) self-join 파생쿼리라 drop. **총 gold 집계 22종 —
  엔티티 축(시간·공간·업종·상태·면적·식별자연결) 실질 소진 판정.**
- **부수 발견(후속 조치 대상)**: 카탈로그 재측정으로 payload 가 확장돼도 기존 detail 테이블에
  ALTER ADD COLUMN 이 없어 **카탈로그-실물 스키마 드리프트** 발생(silver_lodging_detail 의
  uptaenm 부재 실측) — loader 에 컬럼 동기화 ensure 필요.

### 71. gold 인사이트 집계 세트 + 메타 사본 상한(무손실) + 유지보수 재승인 복원

request:
- 메타가 왜 계속 쌓이나 — 메타는 통합하고 본 데이터를 합치며 관리 못 하나(OOM 위험?).
- silver(원형)에서 뽑을 수 있는 인사이트를 전부 gold 집계로: 업종별 일/월/연 단위(**이미 적재된
  기간은 재적재하지 않는 증분**), 기간 조건 추이, 지역 3축(시군구/행정동/법정동), 영업시간·
  영업요일 축, 이력 상태 변화의 지속기간(분야별 비교). 향후 D1(SQLite) 적재 시 중복 없는 조치.

response:
- **메타/데이터 관리(질의 대응)**: ① "메타 통합+데이터 병합" = expire_snapshots+optimize 가
  정확히 그것(데이터 행 0 삭제 — history 가 동일 정책으로 매일 관리되며 6/30 부터 전량 보존 실증).
  OOM 근거: 최중량 history(289만×record_json) 일일 optimize 무사고 → `maintain_silver_gold_tables`
  복원(재승인). ② metadata.json **파일 사본** 상한: PyIceberg 로
  write.metadata.delete-after-commit+previous-versions-max=50 — 82테이블 적용(set 81·fail 0,
  스냅샷 수 불변 실증). Trino 는 해당 속성 차단 → ensure_metadata_retention()이 매 실행 ensure.
- **인사이트 gold 5모델(dbt·Iceberg — 전부 실빌드 검증)**:
  - `gold_license_flow_daily/monthly/yearly` — 개업/폐업 흐름 × 업종 3단(taxonomy) × 지역 3축.
    **완결 기간만**(당일/당월/당해 제외) + 지연보정 창(90일/3개월/1연도) delete+insert —
    "이미 적재된 기간 재적재 없음·중복 불가"를 dbt 증분으로 구현. 빌드 291만/134만/44.6만 행,
    grain 유니크 3/3, **재실행 = 창만 교체·총행수 불변** 실증. D1 적재는 D1 max(기간키) 초과분만
    append(문서 계약).
  - `gold_license_status_duration` — 이력 상태 전이 세그먼트의 지속일 요약(업종·상태군·진행중,
    avg/p50/p90/max). 591행. 예: mail_order_sale 휴업 평균 117일.
  - `gold_env_facility_operation` — 가동 시간·일수 축(환경 v2 2종). **영업시간/영업요일 필드는
    LOCALDATA 원천 전체에 부재(실측 0건)** — 실존 축만 제공, 타 업종은 외부 원천 결합 후속 과제.
  - 워터마크 버그 1건 수정: 전 행 date 변환이 원천 무효 날짜(2006-02-29)에서 폭발 → 문자열
    max 후 1건만 변환.
- 배선: GOLD_SELECT 6모델(정기/refresh)·report AGG_TABLES·유지보수·메타 상한 대상 확장.
  pytest 359 · security PASS · dbt parse·DAG exit 0.

### 70. 레이어 재분류 — 원형은 silver(파이프라인 편승)·gold 는 집계 전용 + 명칭 전면 정리

request:
- 레이어 정의 확정: Raw/Bronze=원본, **Silver=결측 처리·표준화·중복 제거·테이블 단위 정리·JOIN
  모델링(원형)**, **Gold=업무 목적 집계·지표·인사이트만**, Serve=조회 최적화(D1). 현재 gold 에
  있던 테이블 원형을 silver 로 내리고, 집계형만 gold 유지. 원형 생성용 파생 테이블은 별도 단위
  관리. **명칭만이 아니라 수집 파이프라인도 silver 에 편승**시킬 것. (선행 지시: gold_ 접두 통일
  → 재분류로 silver_/meta_ 확정)

response:
- **물리 리네임(총 81, 실패 0 — Iceberg REST RENAME 지원 실측)**: detail 78
  `commerce_*_detail`→(경유 `gold_*`)→**`silver_*_detail`** · `gold_license_entity(+_history)`→
  **`silver_license_entity(+_history)`** · `gold_catalog`→**`meta_detail_catalog`**(파생 과정 메타,
  별도 meta_ 단위). 집계 `gold_license_dong_summary` 만 gold 유지. 카탈로그 rows object 도 치환
  (UPDATE 78행).
- **파이프라인 편승(commerce_load_silver)**: SILVER_SELECT 에 entity 2종 추가(Cosmos ref 체인
  자동 순서) + `build_detail_catalog`·`load_details`(마킹 **뒤** — detail 실패가 run 마킹을 막지
  않음, 자체 워터마크 재개) 태스크 신설. **유지보수(expire 등) 편입은 사용자 지시로 취소** —
  실버 스냅샷은 테이블 단위 생성 데이터로 보존, **중복적재 방지(문장 원자성·워터마크 스냅샷·
  defend)만** 적용. (신설 테이블은 #226 유지보수 대상 아님 — 스냅샷 축적 모니터링만.) silver 리포트에 "원형 detail 적재
  N객체·신규 M행" 섹션 추가.
- **commerce_load_gold = 집계 전용 재작성**: dbt_gold(dong_summary run+test) → report_gold
  (집계 현황 행수·미빌드 실패색). build_catalog/load_details 제거. refresh DAG 는 원형+집계
  전량 재구축 진입점으로 유지(docstring 명시).
- 코드 정리: catalog_rules 명명 silver_ · loader CATALOG_TABLE=meta_detail_catalog ·
  report.py 집계 전용 재작성. dbt 모델 gold/→silver/ 이동(+dong_summary ref 갱신).
- 문서: PROJECT.md §4.1 을 사용자 4계층 정의로 대체(+§4.3 명칭), 집계 쿼리 문서 명칭 전면 치환.
- **검증**: 리네임 후 증분 스모크 +0(워터마크 생존) · 카탈로그 78 specs(silver_) · pytest 359
  전건 · security PASS · dbt parse·DAG 3종 exit 0.

### 69. gold detail 버킷 적재 유실 수정 — 워터마크 스냅샷 + 중단 방어(defend)

request:
- 현행 DB 상태에서 detail 로 최신 버전 상태값 확보 가능 여부 확인(사용자 질의) 중 발견.

response:
- **버그(실측)**: detail 버킷 적재(멤버 행수 > COMMERCE_GOLD_DETAIL_BUCKET_ROWS)에서 워터마크가
  correlated 서브쿼리라 **버킷0 커밋 후 재평가** → 버킷1~k 행이 조용히 걸러짐. mail_order_sale
  75%(93.5만→23.4만)·general_restaurant 50%(53.6만→26.8만) 유실. k=1 멤버는 무영향.
- **수정(loader.py)**: `member_watermark()` — 멤버당 **1회 스냅샷** 후 전 버킷이 같은 창을
  바인딩 파라미터로 공유(서브쿼리 제거). `defend_member()` — 적재 전 워터마크 초과 잔재 선삭제
  (이전 실행이 버킷 도중 죽은 경우의 부분 커밋 정리, PROJECT.md §3 중단 방어 패턴).
- **백필**: 영향 dataset 2개 행 삭제 후 재적재 — 정합 검증 **detail 78 합계 = history 총행수
  (2,898,579 = 2,898,579)**. entity ⋈ detail 현재 버전 조인 커버리지 100%·팬아웃 0 실측.
- 테스트: 스냅샷 바인딩 계약 + 버킷 간 동일 창 회귀 테스트로 고정.

## 2026-07-14

### 68. 서빙 레이어 전면 개편 — gold=Iceberg(RDB 모델링 승계) · 서빙 Postgres 폐기 · D1(SQLite) 예정

request:
- 서빙은 Postgres 가 아니다 — 변경 + 기존 리소스 삭제. 대상은 D1(SQLite). PROJECT.md 에
  "DB 특성에 따른 gold·서빙 레이어 설계 필요"를 명시. gold 를 bronze/silver 처럼 **Iceberg
  카탈로그 구조**로 만들고 특정 몇 가지만 SQLite 로 제공(예정). **재심의**: 집계 테이블만으로
  가지 말 것 — API 마다 컬럼이 크게 상이하므로 기존 RDB 구조(관계형 모델링)를 버리지 말고,
  gold 를 통해 **서빙 가능한 단위**로 데이터를 뽑아낼 수 있어야 함.

response:
- **정책(PROJECT.md §4 신설 + §7 변경 이력)**: 아키텍처 2단(gold=Iceberg 정본 → D1 선별 export
  예정) · **DB 특성별 설계 원칙 표**(§4.2 — 용량 상한→소형만 export, 단일 writer→스냅샷 재생성,
  시퀀스 없음→자연키, 엣지 읽기→사전 집계/평탄화) · gold 구조 표(§4.3).
- **gold(Iceberg) 구조 — RDB 관계형 모델링 승계(재심의 반영)**:
  - 코어(dbt): `gold_license_entity`(현재)·`gold_license_entity_history`(이력, collected_at 증분)
    ·`gold_license_dong_summary`(집계 — D1 1순위 예시). 실빌드 검증: 289만·290만·417행, OOM 없음.
  - **detail(API 별 상이 컬럼 평탄화) 승계**: 카탈로그 구동 유지 — `gold_catalog`(Iceberg, 78 specs
    실측 기록)를 정본으로 `commerce_<domain>_detail` DDL ensure + **멤버별 증분
    `INSERT INTO SELECT`**(Trino 단독 — 행이 Python 을 안 거침, 문장 원자). 워터마크 = detail
    테이블 자체의 멤버별 max(collected_at)(별도 마커 없음). 스모크: 858행 적재 + 멱등 재실행 0행.
  - 서빙 단위 추출 = entity ⋈ detail(자연키 조인) → D1 export 는 대상별 선별(§4.2).
  - 미승계: 뷰 320(Iceberg REST 뷰 제약 — detail 직접 조회로 대체)·entity_seq/commerce_entity_key
    (bigserial — 자연키 전환)·dim 3종(entity 에 코드·명 병기)·code_value(후속 포팅 후보).
- **DAG 재작성**: `commerce_load_gold`(06:00) = build_catalog → dbt_gold(Cosmos, 모델당 run+test)
  → load_details → report_gold. `commerce_load_gold_refresh`(트리거 전용) = full_refresh(dbt)
  + detail 전량 재적재(force_full). 리니지는 Cosmos 네이티브 OL(기존 Asset inlets/outlets 불요).
- **Postgres 리소스 삭제(사용자 지시)**: include/gold 의 ddl/pg/code_values.py + loader·report
  전면 재작성(Iceberg), compose `serving-postgres` 서비스·볼륨 정의 제거, **컨테이너·볼륨 물리
  삭제**(데이터는 silver 에서 전량 재생 가능), `.env.commerce.example` GOLD_PG/PG 튜닝 노브 정리
  (신규 노브 `COMMERCE_GOLD_DETAIL_BUCKET_ROWS`). silver_state `_watermark.json` 은 관측용 유지
  (구 gold 조기 스킵 소비처는 dbt 증분이 대체).
- **검증**: commerce pytest 358 전건(폐기 테스트 정리 + gold report/loader 테스트 재작성),
  `python -m security` PASS, dbt parse/컴파일·gold 3모델 실빌드, 카탈로그+detail 스모크(멱등),
  전 DAG import 오류 0(Cosmos 그래프 11노드 렌더).

### 67. silver 리포트 지표 변경 — 현재행수(누적) → 이번 실행 신규 처리행

request:
- silver report 는 현행(누적) 상태가 아니라 **실제로 silver 과정을 거치며 몇 건이 적재됐는지만**
  표기할 것.

response:
- **정책 갱신(PROJECT.md §2 + §변경 이력)**: silver 리포트 지표 = 이번 실행 신규 처리행(누적 현황
  폐기). collect(increment_count)/bronze(rows_loaded)와 동일한 "실제 신규분" 계열로 통일.
- **구현(`quality_tasks.report_silver_run`)**: `silver_license_current` 전량 집계를 제거하고,
  **이 DAG run 중 DONE 마킹된 run 의 history 적재행**을 dataset(API)별 집계 —
  `silver_load_run_marker` 에서 `marked_at >= run 시작` ∧ `marker_source in (dbt_test_silver,
  processed_no_rows)`(복원 restore_r2_snapshot·부트스트랩 bootstrap_history 는 처리가 아니라 제외)
  로 이번 실행 마킹 run 을 뽑아 history 와 조인. **seed 청크빌드·Cosmos 증분 모두 같은 마킹
  경로라 포괄**(seed 가 마킹한 날 downstream mark 가 0건이어도 정확). dedup 0행 run 은 new=0
  (변경내역 없음)으로 표기. count_label "현재"→"신규". run 시작시각은 DAG 가 `run_started_at`
  으로 전달(미상이면 now 폴백=0건 — 과대보고 방지). mark_silver_done XCom 의존 제거.
- **검증**: 신규 테스트 2건(이번 run 마킹만 집계·경계/소스 필터/바인딩, 마킹 0건→'신규 처리 run
  0건' 요약) + 렌더러 테스트 라벨 갱신 — commerce pytest 369 전건 통과, `python -m security` PASS.
  **실 Trino 검증**: 오늘 재수집 창(08:00Z~)으로 실행 → 정확히 13종·13 runs·36,125행(그라운드
  트루스 일치). 컨테이너 DAG 파싱 오류 0.

### 66. v2(환경 13종) 오탐 누적 데이터 전 계층 삭제 + raw 재수집 (dev 실행 완료)

request:
- #65 진단에 따라 v2 환경 데이터는 중복이 계속 쌓였을 것으로 판단 — **전 계층의 모든 v2
  데이터를 삭제하고 raw 부터 수집 라인을 재실행**할 것.

response:
- **실측(삭제 전, dev)**: 가설 검증 — bronze Iceberg **65,289행 vs 고유 ~36,125**(중복 ~29K;
  air_pollution 23,172행=고유 5,579의 4.2배, garbage_bag 3.5배, manifest 25 runs). raw 오탐 증분
  25파일. **silver/gold 는 content_hash 인접중복 dedup 이 대부분 흡수**(history 36,140 vs current
  36,117 — 진짜 버전 이력 23행뿐)했음을 확인·보고 후, 사용자 결정대로 전 계층 삭제(23행 손실 수용).
- **삭제 도구**: [scripts/purge_v2_environment.py](scripts/purge_v2_environment.py) 신설 —
  기본 dry-run(계수)·`--apply` 삭제·`--layers` 선택·멱등. 범위: raw(증분 25·마커 81·diff-target 26)
  + bronze state(워터마크 v2 13 엔트리·receipts 26) + bronze Iceberg(license 65,289·manifest 25)
  + silver(history/current/marker + **sync_state_files 로 R2 스냅샷 동기화** — 스냅샷 복원의 v2
  마커 부활 차단) + gold(detail 13객체·entity 36,117·history 36,140). **보존**: `commerce_entity_key`
  (entity_seq 안정, refactor-guide §4)·gold 마커(재수집분이 워터마크 초과라 증분 자동 포착)·dim(전량
  재생성 자가 치유). 순수 분류 로직 단위테스트 4건(tests/test_purge_v2.py — v1/_RUN/유사이름 무접촉).
- **재수집(수집 라인 재실행, dev)**: DAG 6종 pause → purge --apply → dry-run 재실행 **전 계층 0
  검증** → collect 트리거(당일 completed 제외 규칙으로 **v2 13종만** 수집, mode=first 자가 시드)
  → bronze(36,125행·manifest 13 runs=1/dataset) → silver(**history 36,125 = current 36,125 =
  bronze 36,125**, 업소당 1버전·중복 0·DONE 마커 13) → gold 순 재적재. diff-target 이 새 정렬
  기준으로 재생성되어 **#65 의 `bronze.resort` 재정렬 절차는 불필요해짐**(대체).
- **런북**: [docs/cleanup-v2-environment-data.md](docs/cleanup-v2-environment-data.md) — 환경별
  체크리스트(dev DONE · prod OPEN). CLAUDE.md Pending cleanup tasks 에 등록.
- **후속 검증(다음날)**: 익일 collect 리포트에서 v2 증분이 0(identical) 또는 소량(실변경)인지 확인
  — #65 수정의 최종 실증. 4000건+/일 오탐이 재발하면 즉시 보고.

### 65. v2(환경) 증분 diff 정본화 — 정렬/식별키 alias 해석(전량 오탐 해소) + 신규분 스코프 알림/리포트

request:
- ① raw: **환경(v2) 수집이 매일 4000건+** 신규로 나옴 — 이전 데이터와 중복 여부 확인·조치.
- ② bronze: 수질오염·대기오염물질배출시설 인허가만 4000건+ — 정상 수집인지 검증.
- ③ silver: 알림이 **신규 적재분이 아니라 현재 존재 데이터**로 옴 — 신규 적재분만 대상으로 조치.
- ④ gold: 위 변경 대응 + **정확한 정보 표기 재검증**.

response:
- **근본원인(①② 동일 버그) 규명·실증**: `bronze/incremental.py` 의 `sort_key`/`_ts_num` 이 원본 row 를
  **v1 정본 키 이름으로만**(`row.get("UPDATEDT"/"OPNSFTEAMCODE"/"MGTNO")`) 읽어, v2(환경 13종)는
  해당 키가 전무 → **전 row 가 `(0,0,'','')` 로 붕괴**. 정렬이 무순서(페이지네이션)로 무너져, 내용이
  같아도 매 수집이 위치 어긋남으로 **전량 신규 오탐**. 재현 시뮬레이션(동일 데이터·순서만 상이)에서
  v1=오탐 0 / v2(구)=**전량 오탐** / v2(신)=오탐 0 으로 확정. water_pollution(≈10,602행)의 "4000건+/일"
  이 실데이터가 아니라 이 오탐임을 확인(②의 답=정상 아님, 알고리즘 버그).
- **조치(①②)**: 키 추출을 `commerce_core.schemas.canonical_get` 로 **정본(v1)→v2 별칭 폴백** 해석
  (`_ts_num`·`sort_key`). **내용 동일성/저장(`normalize`·검증키)은 원본 그대로**(§2.2 원천 보존, #63
  해시 계약과 정합) — 키 해석만 정본화. v1 은 무변경(회귀 0). **정렬키 변경 → 배포 후 `python -m
  bronze.resort` 1회로 기존 v2 diff-target 재정렬 필수**(#193 동일 절차, 멱등). 재정렬 전엔 신 정렬
  today ↔ 구 정렬 prev 가 어긋난다.
- **조치(③ silver)**: `quality_tasks.notify_masked_address_dong_skip_summary` 가 `silver_license_current`
  **전량**을 매일 집계해 기적재(현재 존재) 마스킹 주소를 반복 경고하던 것을, **직전 워터마크 이후
  (`collected_at > silver_state._watermark`) 신규 유입분으로 스코프**. notify 는 `mark_silver_done`
  (워터마크 전진) **이전** 단계라 읽는 값이 '직전 run' 워터마크 → 이번 run 신규분만 집계(=current
  affected 판정과 동일 계약). 신규 유입 0 이면 info 로만 남기고 **외부 알림 미발송**(기적재 재경고 종료).
  워터마크 미상이면 전량 폴백(fail-open).
- **조치(④ gold)**: gold 는 이미 silver `collected_at` 워터마크 증분이라 **상류(①) 오탐 해소 시 자동으로
  실변경분만 적재**(코드 변경 불필요 — 자기교정). **표기 정확성 재검증**에서 결함 발견·수정:
  `gold/report.py` 가 **dim 3종(dataset/region/business_status)=매 run 전량 delete+insert 스냅샷** 행수를
  신규 버전 적재와 **합산**해(예: 신규 1건인데 region 1.5만행) 대량 신규 유입처럼 오표기하던 것을,
  **신규 버전 적재(entity/history/detail)** 와 **차원 스냅샷(전량 갱신)** 으로 분리 표기(헤드라인·제목=
  신규 버전 행수, dim 은 별도 '전량 갱신' 섹션).
- **검증**: 신규 회귀테스트 — incremental v2 3건(별칭 정렬·붕괴 금지·동일데이터 재정렬 오탐 0),
  silver 2건(신규 스코프 바인딩·신규 0→알림 미발송), gold report 2건(신규 vs 차원 스냅샷 분리·조기
  스킵). commerce pytest 전건 통과, `python -m security` PASS. **로컬 한계**: DAG 파싱·dbt·resort 실측은
  컨테이너에서(로컬 py 미설치 스택) — 배포 절차는 아래 운영노트.
- **운영 노트(배포 순서)**: (1) 이미지/코드 반영 → (2) `docker compose exec airflow-scheduler python -m
  bronze.resort --dry-run` 로 v2 대상 확인 → (3) `python -m bronze.resort` 재정렬 → (4) 스케줄 재개.
  재정렬 후 첫 v2 수집부터 증분이 정상(동일=identical, 실변경만 증분). **과거 오탐으로 누적된 v2
  이력 정리**(bronze/silver history 의 반복 버전)는 별도 백필 결정(선택) — `commerce_load_gold_refresh`(#64)
  로 gold 재적재 가능. **잔여 한계(기존과 동일)**: UPDATEDT·LASTMODTS 둘 다 무갱신 + 내용만 변한 행은
  조기중단 diff 가 못 잡음(문서화된 전제) — v1/v2 공통이며 content_hash 계약(#63)이 보완.

### 64. commerce_load_gold_refresh — 마커 무관 강제 전량 재적재 DAG(트리거 전용)

request:
- 현재 gold 파일을 바탕으로, 다른 사람도 신규 데이터를 DB 에 받아올 수 있는 전용 DAG 추가.
  마커와 무관하게 DB 에 새로 적재, 스케줄 아님 **트리거로만** 실행. 커밋·PR.

response:
- **신규 DAG `commerce_load_gold_refresh.py`** — `schedule=None`(트리거 전용, catchup 없음).
  build_catalog → load_gold_full → build_code_values → report_gold. 정기 `commerce_load_gold`
  (06:00·마커 증분·조기 스킵)와 병존하는 온디맨드 부트스트랩/강제 새로고침용.
- **loader 확장(`run_load`/`_load_chunked` 에 `force_full` 플래그, 기본 False)**: force_full=True 면
  조기 스킵(no_new_silver)·마커 창을 모두 건너뛰고 청크 경로로 전 객체 삭제→재적재(OOM 바운드).
  완료 후 마커는 최신 hi 로 전진(정기 DAG 와 상태 일관). **`commerce_entity_key` 는 보존**(같은
  업소=같은 entity_seq, refactor-guide §4). **별도 DB 신규 테이블 생성 없음**(기존 gold 객체 재적재).
- 기본값(force_full=False) 경로는 무변경 — 정기 DAG 동작·조기 스킵 회귀 없음(테스트로 고정).
- **검증**: 신규 test_gold_loader.py 3건(force_full 이 조기 스킵 우회·청크에 전파, 기본은 스킵 유지)
  포함 commerce pytest 357 전건 통과, `python -m security` PASS, 컨테이너 DAG parse import_errors 0.

### 63. content_hash 입력 계약 확정 — raw 원본 전체(원천 좌표 포함) · 파생컬럼 구조적 배제

request:
- content_hash 에 사전 협의되지 않은 파생컬럼(행정동/법정동/위도·경도) 값이 들어가면 빼고
  비교할 것 — silver·gold 공통. **단, 브론즈에 원래 있던 원천 좌표 컬럼은 예외(해시 유지) —
  제외 대상은 임의추가/파생 컬럼만**(2026-07-13 사용자 확정 문답). 완료 후 커밋·푸시·이슈.

response:
- **사실 확인**: content_hash 는 bronze 에서 계산(`warehouse.project_records` — raw 레코드
  canonical JSON sha256)되어 silver/gold 로 **승계**된다. **파생컬럼(행정동/법정동/위경도)은
  해시에 원래 들어가지 않는다** — 해시 입력이 raw 레코드뿐이고 파생은 silver 에서 계산되므로
  구조적으로 유입 불가. #62 실측의 동일 원천버전 44건은 **원천 좌표 필드(v1 X/Y·v2 XCRD/YCRD)**
  가 UPDATEDT 무갱신으로 채워진 것 — 위경도(X/Y 의 silver 파생) 변화로 관측됐던 현상의 실체.
- **계약 확정(해시 공식 불변 + 명문화)**: 해시 입력 = **raw 원본 레코드 전체**. 원천 좌표는
  브론즈 원래 컬럼이라 **포함 유지**(좌표 채움도 원천 변경 = 정당한 버전 이력 — 44건은 중복이
  아니라 좌표 채움 버전으로 재해석). 임의추가/파생컬럼의 해시 유입 금지는 "입력이 raw 뿐"인
  구조로 보장하고 warehouse.py·dbt sources.yml·silver_license_history.sql 주석에 계약을 명문화.
  (중간에 좌표를 해시에서 제외하는 구현을 넣었다가 사용자 확인으로 **원복** — 원천 컬럼이므로.)
- **검증**: 계약 고정 테스트 2건(원천 좌표 변경=해시 상이(v1/v2 별칭 포함)·record_json 원본
  보존) 포함 commerce pytest 전건 통과, `python -m security` PASS, dbt parse/compile 정상.
- **마이그레이션 불필요**: 해시 공식이 기존과 동일하게 확정돼 신·구 혼재 없음 — #319(전량
  재빌드)는 사유 소멸로 종결. 기존 132건도 원천 유래(좌표 채움 버전 44 + 같은 배치 원천
  표기차 88)라 정리 대상 아님.

### 62. 기적재 재적재 차단(마커 내구성·핸드셰이크) + 적재 0건 리포트 가시성

request:
- ① 적재 0건이어도 **요약 리포트**는 남길 것(세부 없이 요약 이력만). ② silver/gold 재실행 시
  기적재분이 신규 이력처럼 재적재되는 현상 — 마커 확인·조치: bronze 파일 마커가 없으면 silver
  확인용 마커 생성, silver 적재 성공분은 gold 가 마커로 사용, **R2 파일 외 별도 DB 적재 금지**,
  기적재(마커 보유)만 있으면 적재도 불필요한 검증도 하지 않음, 0건이면 0건임을 알림 표기.

response:
- **진단(8면 병렬 조사 + DB/R2 실측)**: bronze 파일 마커는 정상 실재(`_markers/<short>.completed`
  152/152 + `_watermark.json` + manifest — 생성 불필요). 재적재의 실측 원인 2개 —
  ① `silver_load_run_marker`(Iceberg) **물리 세대 8개**: drop/재생성 반복으로 07-13 이전 DONE 전량
  유실 → 기적재 run 재선별. ② seed_state 가 '신규 run 도착'을 '빌드 미완'으로 오판 → 신규 run 이
  있는 날마다 해당 dataset 의 **기적재 이력 전체 delete+재빌드**(Cosmos 증분 사문화).
  부수 실측: silver/serving 에 동일 원천버전 중복 132건(교차 run 44 — content_hash 에 지오코딩
  파생값(위경도) 포함이 원인, §비고).
- **① 마커 내구성 — R2 파일 스냅샷 이중화**(신규 `commerce_core/silver_state.py`,
  레이어 `commerce_silver_state/` — bronze `commerce_bronze_state` 와 대칭, RDB 아님):
  `_markers.json`(DONE (dataset, bronze_run_id) 전량)·`_watermark.json`(history max(collected_at)).
  `mark_silver_runs_done`/`_unmark_datasets` 직후 동기화(테이블과 한 몸, fail-open),
  `ensure_silver_marker_table` 이 테이블 생성 시 스냅샷에서 **복원**(marker_source=
  'restore_r2_snapshot') — 테이블 유실 사고 재발 시에도 기적재 재적재 차단.
- **② gold 핸드셰이크 — 조기 스킵**(gold/loader.py `no_new_silver`): silver R2 워터마크 이하로
  전 객체 gold 마커가 전진해 있으면(기적재만) **Trino 접속·DDL·적재·검증 전부 생략**,
  리포트에 "⏭ 적재 0건 — 신규 없음(기적재만)" 표기(gold/report.py). 파일 부재/판정 실패는
  fail-open(기존 경로). 실측: 시딩 후 판정 False(실제 신규 존재) — 정확 동작 확인.
- **③ seed 오판 수정**(chunked_run.py `classify_unmarked`): 미마킹 run 을 '빌드 미완'(history 에
  행 있음/DONE 전무 → seed 재빌드)과 '신규 도착'(기존 DONE 존재+미빌드 → **cosmos_pending**,
  재빌드 금지)으로 분류. 기적재 이력 매일 재빌드 중단.
- **④ 0건 리포트**: bronze finalize 의 `if rr:` 가드 제거 — 0건이어도 "적재 대상 없음(0건)" 요약
  전송(세부 없음, 유지보수 섹션 유실도 해소). silver 리포트에 "이번 신규 처리 run 마킹 N건
  (0=기적재만·변경 없음)" 표기(mark_silver_done XCom). gold 는 스킵 시에도 기존 all_done 리포트에
  0건 사유 표기.
- **검증**: commerce pytest 326+188 전건 통과(신규 test_silver_state.py 12건 + 0건 리포트 계약 2건),
  `python -m security` PASS(차단 0), 컨테이너 전 도메인 DAG import 오류 0, 실환경 스냅샷 시딩
  (524건)·gold 판정 실측. dbt 모델/매크로 무변경(마커·증분 체계 유지 — refactor-guide §4 존중).
- **비고(승인 대기)**: content_hash 에 지오코딩 파생값(위경도)·전화번호 표기 등 비결정 요소가
  포함돼 원천 무변경 재수집이 '변경'으로 오판됨(실측 132건 중복의 근본 원인) — 해시 입력에서 파생
  컬럼 제외는 grain 계약 변경이라 별도 승인 후 진행.

### 61. 크로스도메인 리니지 공유 전략 확정 + "Python 때문에 Marquez 불가" 트러블슈팅 체계화

request:
- 타 환경에서 "현재 Python 버전으로 Marquez 실행 불가" 보고 — 되게 수정하고, 버전 변경(상·하향)이
  다른 프로젝트에 영향 없게 검증·수정. 목표는 **도메인 폴더가 달라도 리니지 공유**(vs dbt 폴더 통합).

response:
- **전략 확정 — 폴더 통합·Marquez 상향 둘 다 불필요**: OL/Marquez 는 물리 테이블명으로 stitch
  (실증: 방출자가 다른 Cosmos silver ↔ Airflow Asset gold 가 물리명만으로 자동 연결). 필요한 것은
  **도메인별 OL 방출 배선**뿐 — ① Cosmos 전환(commerce 방식) 또는 ② **`dbt-ol` 래퍼**(경량,
  BashOperator 명령만 교체). 절차·명령: [docs/cosmos.md](docs/cosmos.md) §6.
- **"Python 때문에 Marquez 불가" 실체 규명**: Marquez 3종은 Java/Node 컨테이너(호스트 Python 무관).
  오인 원인 2가지를 체계화 — ① 구 pip `docker-compose`(v1·Python) 파싱 실패 → **Compose v2 필수**
  ② Airflow 이미지 `PYTHON_VERSION` 지원 밖 — **3.2.2 베이스 태그는 3.10~3.13 만 존재**(3.9=404,
  레지스트리 실측). canonical=3.11.
- **수정**: Dockerfile.airflow 에 ① **fail-fast 가드**(지원 밖 Python 이면 명확한 메시지로 즉시 실패)
  ② dbt venv 에 `openlineage-dbt`(dbt-ol 1.51) 동봉 — 타 도메인이 호스트 Python 없이 이미지 안에서
  방출(pip check 충돌 0, dbt-trino 무결 실측). cosmos.md §6 트러블슈팅 + **버전 변경 검증 절차**
  (태그 확인→rebuild→전 도메인 import 0→pytest/security→OL 방출 스모크) 명문화.
- **타 프로젝트 무영향 검증**: rebuild 후 **전 도메인 45개 DAG import 오류 0**(citydata·culture·
  traffic·weather·transit·commerce·common), dbt venv `pip check` clean. 하향(≤3.9)은 태그 부재로
  불가 — 가드가 차단.

### 60. silver seed 재설계 — bronze→silver from-scratch 검증에서 드러난 결함 3건 수정 + 지속가능 구조

request:
- (사용자) bronze 로부터 silver/gold 를 **처음부터 적재**하는 기준으로 검증하라(R2 의 commerce 파생
  데이터는 삭제 가능, bronze 제외). 더 효율적·지속가능한 적재 구조를 한 번 더 전략화하라.
  OOM 은 근본원인부터, 성공할 때까지 반복, 과정을 change-log 에 기록.

response — **from-scratch 검증이 드러낸 결함(각각 수정)**:
1. **`.env.commerce` R2_REGION 손상(#59 작업의 부작용)**: V1 때 임시 env 를 append 할 때 파일 끝 개행이
   없어 `R2_REGION=auto# [임시…]` 로 오염 → boto3 InvalidRegionError → `enrich_admin_dong_ref`(R2 raw
   읽기) 실패. **gold-only 실행은 boto3 를 안 타서 못 잡던 버그** — from-scratch(bronze 읽기)라서 발견.
   `auto` 복구 + 전 라인 오염 스캔(무결).
2. **silver 전량 빌드 OOM(대형 단일 dataset)**: mail_order_sale 934K 가 **단일 스냅샷 run**이라
   include_datasets(dataset 단위)로도, bronze_run_id 로도 못 쪼갬. heap/spill/tc 튜닝(52→64%, tc1)과
   grain-key 버킷(키가 record_json 안 → 전량 파싱 필요)까지 전부 OOM 실측. **근본해법 = 싼 비-json
   컬럼 버킷**: history 는 `content_bucket`(content_hash — 동일 레코드=같은 버킷, adjacent-dedup 보존,
   A→B→A 비연속 재등장 실측 0건), current 는 `key_bucket`(grain 키 컬럼 — 키의 전 버전=같은 버킷,
   latest-1-row 정확). 버킷당 record_json 읽기가 1/K 로 바운드(실측: 133K 버킷 = 피크 41%, 이전 69% OOM).
   dbt: `macros/key_bucket.sql` 신설, history/current 모델에 var-gated 필터(기본 컴파일 SQL 불변),
   pre_hook 는 버킷 중 삭제 스킵(버킷은 disjoint).
3. **seed "행수>0 → skip" 결함(설계 버그, 실측 재현)**: 부분 빌드(93K) 후 재시도가 잘못 skip → Cosmos 에
   **무스코프 증분(=전량 2.9M, OOM 클래스)** 을 넘김. 수정 — 판정을 **마커 커버리지**로:
   `chunked_run.seed_state()` = DONE 없는 publishable run 의 dataset(history 미완) + current 행수≠history
   distinct grain 수(current 미완). 미완 dataset 만 부분행 선삭제 후 재빌드(재개). Cosmos 는 seed 가
   마킹까지 끝낸 뒤에만 의미 있는 증분을 본다.

response — **지속가능 구조(전략 반영)**:
- **공용 메모리 정책 모듈 `commerce_core/trino_mem.py`**: heap 실시간 프로브(/v1/status) ·
  동적 배치 사이징(`dynamic_rows` — 가용 heap 마진 기반, 하드웨어 적응) · **pace()**(쿼리 사이 heap
  회복 대기 — 백투백 garbage 누적 OOM 차단, 실측 46→61% 누적 사망 대응). silver(chunked_run)와
  gold(loader)가 **같은 정책**을 씀(튜닝 지점 단일화, 매직넘버 제거 — env 는 상한/폴백).
  실증: 동적 산정이 128K 를 냄 = 실측 안전값(133K)과 일치.
- 백투백 누적 완화 GC 튜닝: `trino/jvm.config` IHOP=30 + G1PeriodicGCInterval=15s(+MaxRAM 55%,
  task.concurrency=2 — spill 유지). compose 마운트로 영속.
- 운영 원칙: **증분이 기본**(전 레이어 소량·안전), from-scratch 는 복구 경로(마커 기반 재개로 어느
  지점에서 죽어도 미완분만 이어감). 결정트리: 증분 → dataset 배치 → 비-json 버킷 → (그래도 부족하면)
  노드 증설.

**하드웨어 적응 동작(직관 요약)** — "왜 이 박스에선 느리고, 큰 머신에선 빠른가":

| 요소 | 저사양 박스(VM 7.75GB, heap ~4.6GB) | 고사양 예(32GB, heap ~17GB) |
|---|---|---|
| 배치 크기(`free×margin/행당비용`) | silver ~10만행 · gold 21컬럼 detail ~4.2만행 | silver 50~80만(상한) · detail 25만+ — **자동 확대** |
| 버킷 분할 | mail_order_sale ×9 등 잘게 | 대부분 **버킷 자체가 안 생김**(k=1) → dbt 호출 수 급감 |
| pace(GC 회복 대기) | 버킷 사이 수~수십 초 대기 | free 비율 상시 높음 → **즉시 통과(0초)** |
| OS 스왑 | ~1GB 흡수하며 감속(kill 방지) | 미사용 |
| 체감 총시간(from-scratch) | silver ~2h · gold ~1.5-2h | **silver 15-25분 · gold 20-30분** 수준 |

느림의 주범 = 작은 배치 × dbt 프로세스 기동 오버헤드(회당 10-20초) × pace 대기 — 전부 하드웨어
마진에서 **자동 파생**된 것이라 큰 머신에선 소멸한다. 즉 "느리게라도 완주"가 저사양의 설계 목표고,
같은 코드가 고사양에선 빠르게 돈다. **아직 정적인 것 2개(알아둘 것)**:
① [trino/config.properties](../../../../trino/config.properties) `task.concurrency=2` — 이 공유 VM 용
보수 설정(리포지토리 파일). 고사양 노드는 기본(16)이 유리 → compose env 오버라이드로 분리 여지.
② 머신 로컬 `.env.commerce` 의 `COMMERCE_GOLD_BATCH_ROWS`(이 박스 6만) — gitignore 라 다른 머신엔
없음 → 거기선 코드 기본 상한(50만)+동적 산정이 지배.

- **버그 5·6(재개 경로에서 추가 발견·수정)**:
  - **버그5 — 삭제·언마크 분리 소실**: 재개가 dataset 행은 지우고 마커를 남기면, 모델의 증분 술어가
    'DONE run' 을 재처리하지 않아 지운 행이 **영구 소실**(실측: 10 run). 수정 — `_unmark_datasets`
    를 행 삭제와 **한 몸**으로(재빌드 대상 dataset 마커 동반 삭제, test 통과 후 재마킹).
  - **버그6 — 0행 run 영구 미완**: 인접중복 dedup 으로 **행이 하나도 안 남는 run**(diff 전량 재유입)은
    history 존재 기반 마킹이 영원히 못 찍어 seed_state 가 영구 미완 판정 → 매 run 재빌드 낭비.
    수정 — `mark_silver_runs_done(processed_cutoff_run_id=…)`: **cutoff(빌드 시작, KST) 이전의
    publishable run 전부**를 'processed_no_rows' 로 마킹(run_id 가 시각 인코딩 → 문자열 비교
    race-safe; 빌드 중 도착 run 은 다음 증분이 처리).
- **결과(from-scratch 전 체인 완주, 2026-07-13)**: R2 의 silver 파생 3테이블 DROP + gold 전 테이블
  truncate(entity_key 보존) 후 bronze 로부터 재구축 —
  - **silver**: seed_state **complete=True**(마커 커버리지+current 정합), history **2,897,113**
    (구 2,895,880 + 신규 run 유입 − dedup), current **2,892,669 = distinct grain 수와 정확 일치**
    (구 목표값과도 정확 일치). mail_order_sale 934,371 전량(bronze 와 일치). 완전 누락 run 0
    (전량-dedup run 2건은 'later' run 으로 정당성 검증 + cutoff 마킹).
  - **gold**: 빈 상태에서 **단일 run 완주** — entity_history 2,897,113(=silver 정확 일치) ·
    entity 2,892,669(=current 정확 일치) · **detail 78테이블 총 2,897,113(=history 정확 일치,
    전 버전이 정확히 1 detail 에 안착)** · dim 152/452/93 · view 320 · 마커 83/83.
  - **Trino 무사망**: 전 과정(silver 2h + gold 2h) 연속 healthy, OS OOM-kill **0회**(peak RSS 63-67%,
    스왑 ~1GB 흡수). 저사양 박스에서 "느리게라도 완주" 목표 달성 — 동일 코드가 고사양에선 위 표대로
    자동 가속.
  - silver 는 버그 수정 반복 과정에서 재개(pass) 를 거쳐 완성됐고(각 pass 는 마커 기반 재개 경로의
    실전 검증이기도 함), gold 는 최종 코드로 빈 상태 단일 실행 완주. 신규 환경은 수정 완료된 코드로
    cold-start 1회에 동일 결과에 도달한다(전 버그 수정 반영).

## 2026-07-12

### 59. gold 적재 OOM 해결 — cold-start 청크 적재(dataset 배치 + detail content_hash 버킷) + 객체별 재개

request:
- gold 전량 재적재가 이 머신(VM 7.75GB)에서 Trino OOM(exit 137)으로 완주 불가 → silver 처럼 **OOM 을
  피하면서 가장 정확하고 빠르게** 적재되도록 적용. 정상 완료될 때까지 진행하고, **어떤 조치를 했고 어떻게
  실패했는지 change-log 에 기록**할 것.

response — **진단**:
- exit 137 = **cgroup/OS OOM-kill**(Trino 자체 `EXCEEDED_LOCAL_MEMORY_LIMIT` 아님). Trino 힙이
  컨테이너 메모리 상한 없이 `MaxRAMPercentage=80`(≈6.2GB)까지 자라는데, VM 7.75GB 를 컨테이너 10개
  (airflow×4·postgres×2·trino·marquez×3)가 나눠 써 **전체 메모리 초과** → OS 가 최대 프로세스(Trino)를 kill.
- cold-start(마커 없음)는 `run_load` 가 전 객체를 **한 창(wm=None→전량)**에 적재 → entity_history 2.89M +
  entity(현재 2.89M + `min(collected_at) group by grain` 집계) 를 한 번에 → 피크 초과.

response — **조치·시도**(위 실패 → 아래 개선, 순서대로):
1. **[시도1] dataset 배치 청크**(`loader._load_chunked`): entity_history·entity 를 dataset 그룹
   (행수 greedy-pack, budget=`COMMERCE_GOLD_BATCH_ROWS` 기본 50만)으로 스코프 적재. **핵심 정확성**:
   grain=(dataset,opnsfteamcode,mgtno) 이라 `first_collected_at=min(collected_at)` 를 dataset 스코프로
   좁혀도 전역 정확(각 grain 의 전 이력이 한 배치에). 결과: **entity 단계는 완주**(entity_history
   2,895,880=silver 정합, entity 2,891,436 — no OOM). 그러나 **detail 단계에서 OOM 재발**.
   → 실패 원인: `commerce_food_sanitation_business_detail`(21멤버·payload 20열·**1,122,965행**,
   행×열 추출비용 22.4M — 2위의 4배)를 **한 쿼리로** record_json 20개 `json_extract_scalar` 하며
   피크 초과. mail_order_sale(934K)도 budget 초과.
2. **[시도2 = 최종] detail content_hash 서브청크 + 객체별 재개**:
   - `load_detail(part=(bucket,k))`: `mod(from_base(substr(content_hash,1,8),16),k)=bucket` 로 행을 k
     버킷 **균등·배타·완전 분할**(실측 분포 편차 <1%). `_load_detail_chunked` 가 detail 행수>budget 이면
     `k=ceil(rows/budget)` 버킷으로 쪼개 각 쿼리 ≲budget(food_sanitation 3버킷·mail_order_sale 2버킷).
   - **객체별 마커 재개**: 각 객체(entity_history·entity·detail)가 끝나는 즉시 `write_markers_done` 로
     마커 기록 → 재실행 시 `hi 까지 DONE` 객체는 건너뜀(detail 중 OOM 나도 50분짜리 entity 단계 재실행
     안 함). `run_load` 분기도 "core/detail 중 마커 없는 객체가 하나라도 있으면 청크(=재개)"로 변경.
   - 부수: 시도1 이 완주시킨 entity_history(정합 실측: gold=silver 정확 일치)의 마커를 **선주입**해 25분
     재적재 생략(entity 는 안전하게 재적재 — inner-join 특성상 1,233 grain 차이는 로더 고유 동작).
   - **결과**: entity 재적재 완주 + detail 19개 완료(마커 21)했으나 **food_sanitation 에서 또 OOM**.
     → 실패 원인 재규명: content_hash 버킷(374K)도 OOM. 실측으로 진짜 원인 특정 — 단일 dataset 추출
     (general_restaurant 536K×21열)은 **Trino 피크 15%로 여유**인데, `dataset in (21멤버)`는 21개
     dataset 파일을 **동시 스캔**하며 각 split 이 record_json 을 버퍼 → 피크 폭증. 즉 문제는 행수/버킷이
     아니라 **다중 dataset 스캔 팬아웃**이었다(비파티션 테이블이라 dataset 프루닝 불가).
3. **[시도3] detail 을 멤버(dataset)별 1쿼리로**(`_load_detail_chunked` 멤버 루프): cluster 도 멤버마다
   `dataset='m'` 단일 스캔으로 나눠 동시 스캔 폭을 1 dataset 으로 축소. 그래도 food_sanitation OOM 재발
   (budget 50만 → general_restaurant 26.8만행 버킷이 너무 큼).
- **근본원인 규명(실측 프로빙, 이 단계가 핵심)**: 이전까지 "행수/버킷/팬아웃"으로 추정했으나 실측으로
  정확히 특정:
  - `count(*)` 프로빙(피크 15%)은 **오판** — Trino 옵티마이저가 count 에선 `json_extract_scalar` 를
    프루닝해 record_json 을 안 읽는다. 실제 `select`(추출 컬럼 반환)로 측정해야 한다.
  - 실측(general_restaurant, collected_at≤hi): 순수 컬럼 스캔 53.6만행=피크 **30%**, **21개
    `json_extract_scalar` 추출** 53.6만행=**61~71%**(사망 경계), 13.4만행=**40%**, 8.9만행=**36%**.
    task_concurrency 16→1 은 71→61%로 미미(주 원인 아님). parse-once 도 71%로 무효.
  - 즉 **근본원인 = Trino 가 detail 추출에서 record_json 을 행마다 JSON 문서로 materialize → 피크
    메모리가 쿼리 행수에 비례**. scan+project 라 **spill 대상 연산자가 없어**(spill 무의미) 힙이
    커지다가, 컨테이너 메모리 상한 없이 VM 7.75GB 를 컨테이너 10개가 공유하는 상황에서 Trino 가용
    헤드룸(~60%)을 넘기면 **OS OOM-kill(exit 137)**. entity 단계가 멀쩡했던 건 record_json 을 안
    읽고 평문 컬럼만 스캔하기 때문.
4. **[시도4] detail 쿼리 행수를 정적 바운드(budget 10만)**: 개별 쿼리는 ~36% 였으나 **연속 쿼리에서
   메모리가 누적**(26→50→65%)해 결국 OS OOM. 개별 바운드만으론 부족 — **Trino heap 자체가 상한 없이
   커지는 게 CRASH 의 진짜 원인**임이 드러남(json 추출 임시 garbage 가 GC 보다 빨리 쌓이고,
   MaxRAMPercentage=80%(6.2GB) 힙이 VM 여유를 넘겨 libjvmkill/OS 가 컨테이너 kill).
5. **[시도5 = 최종 아키텍처] 2계층 방어 + 하드웨어 적응(사용자 지시 반영)**:
   - **① Trino heap cap** `MaxRAMPercentage 80→52`(≈4GB, [trino/jvm.config](../../../../trino/jvm.config)):
     힙이 VM 여유(others 2.5GB + OS)를 절대 못 넘게 → GC 강제 회수 → **OS OOM-kill 원천 차단**(넘치면
     Trino 자체 clean 에러로 degrade, 컨테이너 사망 아님).
   - **② 파일시스템=스왑(spill)** `spill-enabled=true`([trino/config.properties](../../../../trino/config.properties)):
     집계/조인/정렬/윈도우(entity first_collected_at 집계 등)는 디스크로 spill. ※ scan+project(detail 추출)은
     spill 대상이 아니라 아래 ③ 이 담당(2계층 방어).
   - **③ 동적 배치 사이징(하드웨어 마진 적응)** `loader._dynamic_detail_rows`: detail 쿼리 직전 Trino
     `/v1/status` 로 **가용 heap 실시간 조회** → 배치 행수 = `free×0.35 / (payload_cols×1200B)`. 머신
     스펙·현재 여유가 크면 배치도 커지고(빠름) 작으면 줄어(안전) — **고정 매직넘버 폐기**. 실측(이 박스
     4GB 캡): 21컬럼 detail→42K행, 6컬럼→148K행 자동. 조회 실패 시 env `COMMERCE_GOLD_BATCH_ROWS`(이 박스 6만) 폴백.
   - **④ 적응형 pacing/commit**: `_pace()` 가 detail 쿼리 사이 free heap 이 낮으면 GC 회복 대기.
     `_stream` 은 `COMMERCE_GOLD_COMMIT_ROWS`(기본 5만)마다 커밋해 Postgres txn/WAL 바운드.
   - detail 은 멤버(dataset)별 + content_hash 버킷(시도3)으로 동시 스캔 팬아웃 회피. 객체별 마커 재개
     (시도2)도 유지(운영 중단 복구용).
- **검증(사용자 지시 — 재개 아닌 처음부터)**: gold 전 테이블·마커 **완전 초기화**(entity_key 만 보존) 후
  **from-scratch 단일 실행**으로 OOM 없이 완주해야 진짜 성공(부분/재개 성공은 계승된 불완전 성공으로 간주 안 함).
- **이식성**: 동적 사이징·pacing·commit 은 **번들 코드**(loader)라 어느 환경에서도 동작. heap cap·spill 은
  Trino config([trino/](../../../../trino/))로 리포지토리에 넣음 — compose 가 마운트해야 영속(현재는
  컨테이너 복사 적용: restart 존속, recreate 시 재적용 필요 → 후속 compose 마운트 권고).
- **결과(from-scratch 완전 성공, 2026-07-12)**: gold 완전 초기화 후 **단일 실행으로 OOM 없이 완주**.
  entity_history **2,895,880**(=silver history 정확 일치)·entity 2,891,436·detail 78테이블 총 2,895,880행
  (food_sanitation 1,122,965·mail_order_sale 933,983 — 이전 OOM 주범 포함 전량)·dim 152/452/93·view 320·
  **DONE 마커 83**(전 객체)·총 8,683,939행. **Trino 2시간 내내 healthy, peak 컨테이너 RSS 65%**지만 OS
  OOM-kill 0(heap cap 이 JVM 힙을 4GB 로 묶고 나머지는 회수 가능한 page cache). 동적 사이징 실동작 확인:
  general_restaurant×11·mail_order_sale×16 등 payload 컬럼수 기반 버킷 자동 분할. **부분/재개 아닌
  from-scratch 완주 = 완전 성공.**

### 58. silver-gold-refactor-guide 실행 — C1~C7 + V1(OL 엣지) 구현 + 공유 R2 복구 계약 정립

request:
- [docs/silver-gold-refactor-guide.md](docs/silver-gold-refactor-guide.md) 확정 변경 C1~C7 을 위→아래
  순서로 구현하고 각 수용 기준 통과. V1(OL 엣지)은 런타임 검증 동반.
- (후속 지시) V1 의 OOM 은 **나눠서(청크)/증분 수집으로 해결**할 것. 이 로컬 외 **다른 로컬 환경이
  R2 를 공유**하므로 R2 변경이 상대 환경에 문제로 인식되지 않게 **복구 플로우까지 확인·정립**할 것.

response:
- **C1 gold 식별자 3중 게이트(§20 injection guard)**: `include/gold/measure.py`(유입 — 비식별자 필드
  필터 + `log_event` 품질 이벤트), `include/gold/ddl.py`(생성 — `_assert_detail_safe` 를 `generate_all`/
  `create_detail_sql`/`create_detail_index_sql` 진입에), `include/gold/loader.py`(소비 — `load_detail`
  진입에서 object/payload/members `assert_identifier`, DB 재로드 우회 차단). `tests/test_gold_ddl.py` 에
  주입 케이스 추가. **검증**: pytest 339 통과 · `python -m security` PASS(차단 0).
- **C2 silver history 컬럼 단일화**: `dbt/…/macros/silver_columns.sql`(`silver_history_column_list`) 신설,
  history 모델의 projected_new/prior_tail/최종 select 3중 목록을 매크로로 치환(위치 정합 순서 유지).
- **C3 좌표 변환 매크로**: `dbt/…/macros/geo_transform.sql`(`tm5174_to_wgs84_ctes`) 신설, 인라인 측지
  CTE 10개(geo_mu…geo) + 주석블록 이관. 모델은 `{{ tm5174_to_wgs84_ctes('dong') }}` 한 줄.
- **C4 마스킹 매크로**: `dbt/…/macros/masking.sql`(`null_if_masked_address`) 신설, current 4곳 + history
  dong_token 5중 중복 치환(regexp 백슬래시 원문 보존 확인).
- **C2·3·4 검증**: 컨테이너 `dbt compile` 전/후 **공백 제거 후 바이트 동일**(history·current 양쪽).
  scoped 증분 `dbt run`(resort_complex·yacht_marina·golf_course) 성공. `dbt test`: geo landmark(서울시청)
  · grain unique(history/current) · no_adjacent_duplicates · not_null · masked 컬럼 전부 PASS.
- **C5 gold exposure**: `dbt/…/models/exposures.yml`(`commerce_gold_serving`, culture 형식 준용) 신설.
  `dbt parse` 오류 없음, `dbt ls --resource-type exposure` 에 표시. Cosmos 렌더 무영향(silver DAG 미수정).
- **C6 cleanup 러북 보강**: `docs/cleanup-detail-health.md` §3 에 고아 seed(`commerce_dataset_taxonomy`)
  삭제 + 문서 정리 대상 2항목 추가. 소비자 detail_health 뿐임 재확인(grep).
- **C7 문서 정합 복구**: 파이프라인 5문서(data-model·pipeline/README·gold/README·silver-gold-load-plan·
  beginner-guide)의 "gold 미구현" 진술을 실제(카탈로그 구동 Python→서빙 Postgres, 가동 중)로 정정 +
  정본 포인터. 드리프트 4건(views.md dbt seed→Python · partitioning-indexing-plan 인덱스 6→17+401 정본
  포인터 · tables.md dim_dataset seed 미사용 · timestamps-and-nulls DCBYMD 해소) 정정. 링크 전수 resolve.
- **V1 완료(청크 실행으로 OOM 우회 — 사용자 지시)**: Marquez 기동(`--profile lineage`) → Airflow
  이미지 rebuild(f6c55cc 의 cosmos+OL provider 실체화) → cosmos 1.15 기본 InvocationMode 변경 대응
  (`commerce_load_silver.py` Execution/RenderConfig 에 **SUBPROCESS 명시** — dbt venv 경계 계약 유지) →
  silver 를 **dataset 청크로 스코프**해(신규 운영 노브 `COMMERCE_DBT_VARS`, `.env.commerce` JSON →
  Cosmos operator_args vars) 1회 성공 → **OL 표기 실측**: namespace `trino://trino:8080` · name
  `iceberg_dev.commerce.silver_license_history`(가이드 §3 예상과 일치). `commerce_load_gold.py`
  `load_gold` 에 **Asset inlets/outlets** 부여(env 구동 — `_qualified` 와 동일 dev/prod 계약; trino/postgres
  provider 컨버터가 동일 표기로 변환함을 사전 검증). **Marquez 그래프 실측 확인**: bronze→Cosmos silver
  job→`silver_license_history/current`→`load_gold`→`serving.public.commerce_business_entity(+_history)`
  전 체인 가시화(inEdges/outEdges 등록). 부수 해결: 스케줄러 프로세스의 importlib 메타데이터 캐시가
  세션 중 pip 설치된 trino provider 를 못 봐 inlets 만 탈락하던 문제 — **스케줄러 재시작으로 해소**(원인
  기록). ⚠ 잔여: ① trino provider 는 러닝 컨테이너 임시 설치 — **영속화하려면 Dockerfile.airflow 에
  `apache-airflow-providers-trino` 1줄 필요**(호스트 파일 — 승인 대기; 없으면 컨테이너 재생성 시 inlets
  변환만 소실, fail-open 무해). ② 이 박스는 gold **전량** 재적재(마커 빈 상태 cold start)가 Trino
  OOM(VM 7.75G, 힙 80% 설정)으로 완주 불가 — 3회 실측. 로컬 serving DB 는 부분적재+마커 미기록
  상태로 남았고 **다음 성공 실행이 `_defend` 로 자동 정리**(복구 계약 §7.2, R2 무관).
- **공유 R2 검증·복구 계약 정립(사용자 지시)**: Iceberg 카탈로그·웨어하우스=R2 Data Catalog(REST) —
  bronze/silver/**마커** 전부 환경 간 공유임을 확인. 이 세션의 공유 상태 영향 실측 = **net-zero**
  (무마킹 부분행 0건, history 스냅샷 no-op delete 1개·행수 불변, current 4행 동일값 재계산,
  ref_admin_dong 일일 전량교체는 설계 동작). 문서화: **rebuild-and-ops.md §7**(공유 경계 표 ·
  레이어별 복구 계약(실측 근거+무마킹 검사 쿼리) · 멀티 환경 5수칙 — 동시 실행 금지 / standby paused
  유지 + 검증 실행 change-log 기록 / 저메모리 청크 / 전량교체 무해 / 스냅샷 위생). 이 박스의 DAG
  paused 원상 복원(silver·gold), 임시 env 제거, 컨테이너 전 복원 완료.
- **환경 이슈(코드 무관, 기록)**: 이 박스 `.env.commerce` 에 `JUSO_CONFM_KEY` 부재 →
  `enrich_fill_jibun` 실패(이번 검증에선 해당 run 한정 우회). 정상 운영 환경에는 키가 있어야 한다.

### 57. silver/gold 구조 1차 판정 + 리팩터 가이드(silver-gold-refactor-guide.md) 신설

request:
- 현 프로젝트 silver/gold 레이어의 구조적 정당성 1차 확인.
- 변경해야 할 부분을 Opus 가 문서만 보고 그대로 구현할 수 있는 수정 가이드로 마련.

response:
- **판정**(코드 실측 + 전 도메인 dbt 비교 + 문서 계약 대조): **구조 자체는 정당** — 레이어 분리·
  3계층 증분/마커/중단방어·record_json schema-on-read·gold Python→서빙 Postgres(인덱스/bigserial/뷰는
  dbt-trino 불가) 모두 근거 확인. **Critical 0 · Major 4 · Minor 3**.
  Major: ① gold 식별자 무검증(외부 API 필드명 → DDL/JSONPath f-string, 게이트 부재) ② silver history
  42컬럼 3중 중복(union all 위치 기반 → 무증상 오염 위험) ③ gold lineage 사각(dbt/OL 불가시)
  ④ gold 구현 상태 문서 모순(파이프라인 문서 5곳 "미구현" vs 실제 가동).
- **가이드 신설**: [docs/silver-gold-refactor-guide.md](docs/silver-gold-refactor-guide.md) —
  확정 변경 C1(식별자 3중 게이트)·C2(컬럼 목록 매크로 단일화)·C3(좌표 변환 매크로)·C4(마스킹 매크로)·
  C5(gold exposure 등록)·C6(taxonomy seed 고아 반영)·C7(문서 정합 복구) + 검증 동반 V1(OL inlets/
  outlets → Marquez 엣지). 각 항목 현재/변경 코드/수용 기준/검증 커맨드 명시. **§4 변경 금지 목록**
  (마커 체계·record_json·detail 78 카탈로그·Python gold·entity_key 보존·타 도메인) 명문화.
  §6 전 도메인 관찰(gold 이원화·행정동 축 3원화·명명/재료화/테스트 편차 — 합의용) 수록.
- **CLAUDE.md 체인 연결**(Pending tasks) + docs/README 색인.
- **사실 정정**: [cleanup-detail-health.md](docs/cleanup-detail-health.md) §1 이 gold detail 소스를
  `silver_license_current` 로 잘못 기술 — 실측(`loader.load_detail`)은 **`silver_license_history`**
  (history-form, #80 이력 소실 결함까지 해소). 러북 §1·§2 정정.

## 2026-07-11

### 56. detail_health 레거시 제거 러북(다른 PC 자동 실행용) + CLAUDE.md 체인 연결

request:
- detail_health 정리를 다른 PC 의 Claude Code 가 CLAUDE.md→docs 를 읽고 자동 처리하도록 구성.
- 물리 테이블 삭제는 환경마다 상태가 다르니(다른 PC 에 이미 적용됐을 수 있음) 체크항목으로.

response:
- **러너블 러북 신설**: [docs/cleanup-detail-health.md](docs/cleanup-detail-health.md) — 배경(폐기·gold
  승계) + 사전검증(gold 커버리지) + 코드 정리(dbt/dags 파일·문서) + **§4 환경별 물리 테이블 drop
  체크리스트**(dev `iceberg_dev.commerce` / prod `iceberg.commerce`, SHOW→DROP IF EXISTS, 부재 시 스킵)
  + 검증/완료 처리. 위→아래 순 실행 가능하게 작성.
- **CLAUDE.md 체인 연결**: "Pending cleanup tasks(있으면 읽고 실행)" 섹션 추가 → 다른 PC 에이전트가
  CLAUDE.md 만 읽어도 러북 발견·실행. docs/README.md 색인 추가.
- **실제 삭제는 미수행**(코드/물리 모두) — 러북 상태 OPEN. 물리 테이블은 환경 실측 후에만 drop.

### 55. OpenLineage → Marquez 배선(cross-domain lineage 백엔드, lineage 프로파일)

request:
- Marquez 세워서 cross-domain 통합 lineage 를 볼 수 있게 작업 진행(승인).

response:
- **Airflow 이미지**(`Dockerfile.airflow`): `astronomer-cosmos[openlineage]` +
  `apache-airflow-providers-openlineage>=2,<3`. Cosmos dbt run 태스크가 물리 relation 기반 OL
  이벤트를 방출.
- **방출 배선**(`docker-compose.yml` airflow-common): `AIRFLOW__OPENLINEAGE__TRANSPORT`
  (→ `http://marquez-api:5000`) + `AIRFLOW__OPENLINEAGE__NAMESPACE=commerce-elt`.
- **백엔드**(`docker-compose.yml`): **`lineage` 프로파일** 서비스 3종 — `marquez-db`·`marquez-api`
  (127.0.0.1:5000)·`marquez-web`(UI 127.0.0.1:3000) + `marquez_db_data` 볼륨. 프로파일 격리라
  `docker compose up`(core)엔 안 뜨고, marquez 미기동 시 OL 방출은 fail-open(경고만).
- **원리**: 도메인 간 `source()` 물리 relation 이 생산 도메인 모델 산출물과 동일(확인됨) → Marquez 가
  물리명으로 stitch → 경계 넘는 통합 그래프. dbt 코드/구조 변경 0.
- **기동**: `docker compose --profile lineage up -d` → `commerce_load_silver` 실행 → http://127.0.0.1:3000.
- **검증 한계**: Marquez 이미지(0.50.0)·OL provider 버전은 canonical 고정, 도커 미기동이라 런타임
  미검증 — 최초 기동 시 태그/마이그레이션 조정 가능(core 무영향). 상세: docs/cosmos.md §6.
- **범위**: commerce 파일럿. 전체 6도메인 통합은 나머지 도메인도 OL 방출 시 자동 stitch.

### 54. commerce silver dbt 실행을 Cosmos(DbtTaskGroup)로 전환 + cold-start 시드 가드

request:
- `./dbt ./dags` 의 domains/commerce CLAUDE.md 를 참조해 현행 commerce DBT(silver)에 Cosmos 적용.
- Cosmos 설치(호스트 이미지 변경) 승인. "전체·증분 모두 Cosmos" 가 이상적이나 **OOM 위험은 감수 불가** —
  위험 없는 최선 시나리오로 진행(최종 판단 위임).
- Cosmos 가 기존 구조를 따라가지 못하는(대응 불가) 영역이 있으면 그 발생 이유·대처 불가 이유를
  cosmos doc 에 정리.

response:
- **cosmos doc 신설**: [docs/cosmos.md](docs/cosmos.md) — 채택 형태(DbtTaskGroup·LOCAL·venv), **비대응
  영역과 이유**(청크 전량 빌드/마커·pre_hook 결합/파싱시 dbt/detail_health 선택/notify 순서), cold-start
  시드 계약, cross-domain lineage 방향(OpenLineage→Marquez)까지 단일 소스로 기재. docs/README.md 색인 추가.
- **왜 전체 재빌드는 Cosmos 단독 불가(핵심 발견)**: `silver_license_history` 는 pre_hook
  `delete_unmarked_silver_history_runs` + 마커 기반 증분(`silver_unmarked_publishable_predicate`).
  Cosmos 는 파싱시 정적(모델당 1태스크)이라 `chunked_run` 의 dataset 배치(`--vars include_datasets`)를
  못 한다. 마킹 안 된 전량 빌드 위에 Cosmos 증분을 돌리면 pre_hook(include_datasets 비어 **전역**
  삭제)가 방금 빌드분을 지우고 **비청크 단일 run 재처리 → 예전 OOM(EXCEEDED_LOCAL_MEMORY_LIMIT) 재발**.
- **설계(위험 없는 최선)**: 기존 `commerce_load_silver` DAG 안에 Cosmos `DbtTaskGroup`(dbt_silver) 임베드
  — 모델당 run+test(AFTER_EACH), 프로필은 기존 profiles.yml 재사용, 실행은 별도 dbt venv(LOCAL,
  dbt_executable_path). 기존 단일 BashOperator `dbt test` 대체. 스케줄/DAG 는 1개 유지(분리 안 함).
- **cold-start 시드 가드(`seed_silver_if_empty`)**: `silver_license_history` 가 비었을 때만 청크
  빌드→`dbt test`→(통과 시) DONE 마킹(기존 cold-start 순서를 한 태스크로 캡슐화). 마킹까지 끝내므로
  downstream Cosmos 증분은 no-op(삭제 대상 없음) → OOM 원천 차단. 평상시엔 no-op → Cosmos 가 증분 담당.
  헬퍼 `chunked_run.run_dbt_test` 추가.
- **배선**: `[enrich×3] → seed_silver_if_empty → dbt_silver(Cosmos) → notify_masked_address_summary
  → mark_silver_done → report_silver`. (기존엔 notify 가 run/test 사이였으나 Cosmos 가 run+test 를
  묶으므로 그룹 뒤로 이동 — 기능 동일, 근거 docs/cosmos.md.)
- **호스트 이미지**: `Dockerfile.airflow` airflow env 에 `astronomer-cosmos>=1.8,<2` 추가(승인). dbt 는
  기존 venv 유지(airflow env 엔 dbt 미설치, Cosmos 만). `requirements.txt` 에 호스트 의존 명시.
- **검증(로컬 한계)**: 로컬은 py3.9 + airflow/cosmos 미설치라 DAG 파싱/실측 불가 — **이미지 리빌드 후**
  DAG import·dbt_silver 렌더·`dbt run/test` 확인 필요(절차 docs/cosmos.md §검증). 로컬은 security gate +
  순수 pytest(include/ 로직)만 수행.

---

## 2026-07-10

### 53. 검색·이력 공통축 인덱스 확장(entity/history 11개) + detail 자동 인덱싱(401개)

request:
- 시군구·행정동·법정동 코드, moddt·updatedt·createdt·id·opendate·closedate, 종업원수·평수 등
  검색/조건 단위로 쓰기 좋고 이력과 연결될만한 컬럼은 모두 자동으로 인덱싱할 것.

response:
- **entity/history 확장(6→17개)**: legal_code·updatedt_ts(=updatedt)·lastmodts_ts(=moddt)·
  opened_at(=opendate)·closed_at(=closedate) 를 두 테이블 모두에, natural_id(opnsfteamcode,mgtno,
  =id — entity_seq 몰라도 업소 직접 조회)는 entity 전용(HISTORY_COLUMNS 에 없는 컬럼이라 대상 아님).
- **detail 78개 자동 인덱싱**(`create_detail_index_sql`): 새 API 추가 시 카탈로그 갱신만으로 자동
  적용되는 접미사 규칙 — 날짜(`ymd/dt/date`)·식별번호(`no/num/seqno/asgnno`, =id)·수량규모(`cnt/
  epcnt/area/yarea/scp/tons/flr`, =종업원수·평수). 명칭/구분류(`nm/se/senm/gbn/gbnnm` — 업태명 등)
  는 검색축이 아니라 값 자체가 목적이라 **정규화(commerce_code_value) 영역으로 제외**, 과다인덱싱
  방지. 실측(78테이블/725 payload 컬럼): 401개 매칭(테이블당 평균 5.1개, 대부분 결측 위주 희소 컬럼).
- **버그 수정(구현 중 발견)**: natural_id 인덱스를 처음엔 entity/history 공통으로 넣었다가
  `UndefinedColumn: opnsfteamcode` 로 즉시 실패 — HISTORY_COLUMNS 에 opnsfteamcode/mgtno 가 없음을
  재확인해 entity 전용으로 정정(트랜잭션 전체 롤백이라 데이터 영향 없음, 재적용으로 정상 완료).
- **검증**: 338 테스트 통과 + 보안 게이트 PASS. 라이브 적용(스키마 파괴 없음 — 순수 CREATE INDEX
  IF NOT EXISTS, 데이터 재적재 불필요): 인덱스 94→506(신규 412개, elapsed 29.3s), 행수 무손상
  (entity 2,891,707/history 2,894,754/food_detail 1,122,616 그대로), natural_id 인덱스로 자연키
  직접 조회 Index Scan 확인.

---

## 2026-07-10

### 52. gold entity_seq(bigint 서러게이트) 전환 + 정규화(코드 테이블) + view-구성 인덱스 6개

request:
- 정규화(저카디널리티 detail 컬럼) 진행 시, view 생성에 쓰이는 요소로 인덱스도 적용할 것.
- entity_id 가 해시 문자열인데, 그럴 거면 시퀀스로 숫자를 배정하고 인덱싱하는 게 맞지 않은지 의견을
  묻고, 맞다고 판단되면 적용. 이후 commerce 쪽 초기화하고 gold 레이어 재실행.

response:
- **의견 채택 근거**: entity_id(sha256 text 64자)는 "각 로더가 조율 없이 독립 계산" 이점을 노렸으나
  gold 는 일 1회 배치 적재라 그 이점이 실제로 쓰이지 않고, 85개 테이블 전체 PK/FK·조인이 64byte
  text 를 물어 조인/인덱스 비용만 키우고 있었음 → **bigint 시퀀스가 맞다고 판단해 적용**.
- **entity_seq 전환**: `commerce_entity_key(dataset,opnsfteamcode,mgtno) -> entity_seq bigserial`
  영구 매핑 테이블 신설(**재적재/초기화에도 항상 보존** — 지우면 같은 업소가 다음 적재 때 다른 번호를
  받아 정합성이 깨짐, manifest 사고와 동일 클래스 위험). 자연키는 entity 컬럼으로 그대로 보존(소스
  식별성 유지). `ddl.py`(ENTITY/HISTORY_COLUMNS·DETAIL_KEY_COLUMNS·PK·뷰 조인) + `loader.py`(SQL 측
  sha256 계산 제거 → 배치 단위 Postgres 왕복으로 entity_seq 해석·발급, `resolve_entity_keys`) 전면
  개정. `pg.execute_values` 에 `fetch=True`(RETURNING) 지원 추가.
- **정규화(Option 1, 공유 코드 테이블)**: `normalization-plan.md` §4 Option 1 채택 — detail 스키마는
  불변, `commerce_code_value(domain, value, n_occurrences)` 1개만 신설(테이블 폭증 방지). 후보
  109쌍을 **실제 값 표본으로 전수 재검증**해 72쌍 채택(37쌍은 상수·결측·0/1플래그로 제외, 예:
  `mail_order_sale.uptaenm` 은 동일 필드명이라도 977종 준자유텍스트라 제외). `include/gold/
  code_values.py`(`CANDIDATES`·`build_code_values`) 신설, DAG 신규 task `build_code_values`.
- **인덱스 6개**: `ddl.create_index_sql()` — view SQL 이 **실제로 쓰는 JOIN/WHERE 컬럼만** 인덱싱
  (`commerce_business_entity`/`_history` × dataset·admin_dong_code·(status_code,detail_status_code)).
  구현 중 원안(cluster detail 8개 dataset 인덱스) 은 view 가 detail.dataset 을 predicate 로 쓰지
  않음을 재확인해 **제외**(과반영 회피).
- **검증**: 337 테스트 통과 + 보안 게이트 PASS. **commerce(gold Postgres) 초기화 후 전면 재적재**
  (view 320+table 85 drop → build_catalog → load_gold → build_code_values, 총 1,728s·868만행).
  라이브: `commerce_entity_key` 2,891,707건(entity 와 1:1) · `commerce_code_value` 72도메인·1,049값 ·
  선택도 낮은 API(pharmacy, 0.76%) 조회 **1187ms→18ms**(Bitmap Index Scan 확인) · 고선택도 API
  (general_restaurant, 18.5%) 는 여전히 seq scan — **정상**(플래너가 그 선택도에서 seq scan 이 실제로
  더 빠르다고 올바르게 판단, 인덱스 미적용 문제 아님). marker 83/83 DONE.

### 50. manifest 메타 불일치 원인 규명 + 조치(유지보수 포함 + write_manifest 커밋 배칭)

request:
- manifest 손상 원인을 "가능성"이 아니라 **시나리오 검증 후 확정**하고, 확정되면 조치할 것.

response:
- **원인 확정(초기 가설 기각)**: 일일 Iceberg 유지보수(optimize/expire/orphan)가 발행 게이트 테이블
  메타를 지웠다는 가설은 **검증 결과 기각** — 유지보수 대상이 `bronze_localdata_license` 하나였고,
  R2 Data Catalog 는 테이블별 폴더(`__r2_data_catalog/<uuid>/<table>-<uuid>/`)로 **물리 격리**돼
  bronze 대상 remove_orphan_files 가 manifest 파일을 건드릴 수 없음(location 속성으로 확인).
  **실제 원인**: `bronze_collection_run_manifest` 가 **유지보수에서 빠진 채** write_manifest 가 종·run
  단위 delete-then-insert 로 커밋을 계속 쌓아(재현: 5종 적재 → 스냅샷 2→7, 정리 5삭제 → 12) 스냅샷/
  메타데이터가 무한 축적 → R2 Data Catalog 메타 불일치(ICEBERG_MISSING_METADATA) 유발.
- **조치 ①(유지보수 포함)**: `bronze/maintenance.py` `DEFAULT_TABLES` + `commerce_load_bronze.py`
  `iceberg_maintenance` 대상에 `bronze_collection_run_manifest` 추가 → 매일 optimize/expire(7d)/orphan.
- **조치 ②(커밋 배칭)**: `warehouse.write_manifest` 를 종별 delete+insert(2N 커밋)에서 **단일 DELETE
  (`(source_id,bronze_run_id) IN (VALUES …)`) + 청크 INSERT(100행)** 로 축소(값 전부 `?` 바인딩).
- **검증**: 334 테스트 통과 + 보안 게이트 PASS. 실 Trino 라이브 — 배칭 DELETE `IN (VALUES …)` 구문
  유효(무매칭 no-op), manifest `optimize` 정상 실행(= 이제 유지보수 대상에 실제 포함). 현재 12스냅샷은
  당일 재현분이라 7d retention 이 보호(설계상 정상) — 매일 유지보수로 7일 경과분부터 만료.

---

## 2026-07-08

### 49. silver v1/v2 정규화 + record_json 보존(API별 비공통) + raw→bronze→silver 전구간 검증

request:
- v1/v2 는 backfill 로 해결(bronze 문제 아니면 silver 만 재작업). silver 식별키=(OPNSFTEAMCODE, MGTNO).
- API별 비공통 필드도 key 로 매핑 보존 → gold 에서 API별 table화(무리한 병합/완전정규화 X). history 는
  전 필드 변화 SCD 추적. raw~bronze~silver 전구간 정상동작 확인.

response:
- **silver(dbt)**: `lf(v1,v2)` 매크로로 record_json 에서 정본(v1) 우선 coalesce 정규화 —
  mgtno/opnsfteamcode/status/date/address/coord 모두 v1(MGTNO…)·v2(MNG_NO…) 대응. **record_json 을
  current/history 에 보존** → API별 비공통 필드를 (dataset,opnsfteamcode,mgtno)로 매핑, gold 가 table화.
  history 는 content_hash(전 record) 기반이라 **전 필드 변화 SCD** 유지.
  (ASAC-DBT `feat/silver-v1v2-per-api`: `fe4c7ea`, `7c5db67`.)
- **bronze**: warehouse `iter_increment_rows` 가 v2(MNG_NO) row 를 page 봉투로 오인하던 버그 수정(`e7e1fcc`).
  `project_records` 는 `canonical_get` 으로 v2 mgtno/updatedt 채움(`39b4442`).
- **검증(실 dev Trino/Iceberg)**:
  - silver --full-refresh: history **1,344,778** · current **1,342,691**(v1 무회귀), record_json 전행 채움,
    dental_lab 등 비공통 필드 추출 확인, SCD(한 업소 3버전) 확인.
  - **v2 end-to-end**: env_consulting(093018, v2) raw **208행** → bronze Iceberg **208행**(publishable) →
    silver **208행·distinct 208(붕괴 없음)**, mgtno=MNG_NO·opnsfteamcode=OGDP_INST_CD·상태=SALS_STTS,
    record_json 보존, 비공통 `BPLC_SE_NM`=환경컨설팅회사 추출 확인.
- gold(API별 table화)는 record_json 기반 **후속 레이어**(염두).

### 48. 응답 컬럼 표준 v1/v2 대응 — 환경 13종(신형) 편입 + 양식변경 알림

request:
- (환경) 상권 인허가 13종 수집. datasetView 형식과 대조·상태값 처리기준 확인. key 가 달라도(신형 컬럼)
  기존값과 동일하게 맞추고 **silver 에서 처리**. key 틀어지면 양식변경+error 알림. 알림 호출부에 역할별 설명 추가.

response:
- 라이브 확인: 환경 13종은 **신형(v2) 컬럼 표준** — `MNG_NO`(=MGTNO)·`OGDP_INST_CD`(=OPNSFTEAMCODE)·
  `SALS_STTS_CD/NM`(=TRDSTATE)·`DTL_SALS_STTS_CD/NM`(=DTLSTATE)·`DATA_UPDT_YMD/LAST_MDFCN_YMD`·
  `ROAD_NM_ADDR`·`XCRD/YCRD`. 식별/상태/버전 값 **전부 존재(이름만 다름)**, 13종 전수 확인.
- 별칭 계약: `schemas.COLUMN_ALIASES_V2` + `canonical_get()`/`detect_row_format()`. `Dataset.fmt`(v1|v2) +
  registry `format` 필드. 환경 13종 `category=environment`·`format=v2` 등록(139→152).
- 정규화 위치 = **silver**(grain `(dataset, opnsfteamcode, mgtno)` 붕괴 지점 = 데이터 손실처). bronze 는
  raw 그대로 저장(손실 없음, 증분만 비효율). warehouse 는 `canonical_get` 으로 v2 mgtno/updatedt 채움.
- 알림: `notify_schema_drift`(양식변경→coped=warning / 미인식=error, §19.1 `[commerce][task>level]` 형식) +
  `NOTIFY_ROLES`(역할별 설명 양식) + `role_of()`. bronze 수집 루프에서 등록 fmt≠관측 fmt 시 1회 알림.
- 상태값 처리: 영업상태 `01 영업/03 폐업/04 폐쇄/05 제외사항`(v1 TRDSTATEGBN = v2 SALS_STTS_CD). 상세영업상태
  코드는 **API별 상이**(`11/2/4/BBBB/N…`, datasetView 정의 정본) — NM 동반이라 code→name empirical 확보 가능.
- 테스트: canonical_get/detect_row_format·format·schema_drift·role_of. 전체 pytest 300 통과.
- ⚠️ **silver dbt coalesce(v1↔v2)는 후속**(dbt submodule) — 이게 완료돼야 환경 13종이 silver 에서 정합.

### 47. 분류 2단계화 — 대분류(category) vs 명칭분류(sub_category) 분리

request:
- 명칭에 따른 분류와 대분류(industry)를 구분할 것.

response:
- `Dataset` 에 `sub_category` 필드 추가([schemas.py](include/commerce_core/schemas.py)) + [registry.py](include/commerce_core/registry.py)
  로더 반영. `category`=대분류(유지), `sub_category`=명칭 기반 세분류(대분류 하위, 미지정 허용).
- 산업 32종에 sub_category 부여: meter4·gas3·petroleum2·groundwater3·timber2·sales7·tobacco3·
  job_agency2·education2·funeral1·emission1·animal2.
- **전 139종 sub_category 부여 완료**(산업 32 + culture 56 + 기존 51). food(restaurant/bar/manufacturing/
  sale/meal_service/logistics/vending/bakery)·culture(sports/film_video/game/tourism/amusement/travel/music/
  publishing/performance/culture_arts/camping)·health_medical(institution/care/drug_sale/medical_device) 등.
- `test_registry.py::test_all_have_sub_category`(전 종 sub_category 필수)로 가드. 전체 pytest 296 통과.

### 46. 산업 상권 인허가 32종 추가 (107 → 139) + 라이브 검증

request:
- 산업 분류 상권 인허가(계량기/가스·석유/지하수/목재/각종 판매업/담배/직업소개/장례·요양 교육 등)를
  commerce 로 편입. raw~bronze~silver 이전과 같은 방향으로 적용.

response:
- 소스 34행 중 **중복 2쌍 제거** → 32종 등록(107 → 139), `category: industry`. 중복:
  `LOCALDATA_092701`(목재수입유통업=제재업)·`LOCALDATA_092809`(용기냉동기특정설비=액화석유가스용품제조업체)
  — 동일 코드+OA-id 라 각 1건만 등록(둘째 이름은 코드 재확인 필요, 플래그).
- **라이브 32/32 OK**(fail=0), 식별값 `OPNSFTEAMCODE/MGTNO/UPDATEDT/LASTMODTS` 전 종 존재 →
  bronze/silver 라인 그대로 유효(§45 분석 연장). 신규 최대 통신판매업 total≈932,949·담배소매 95,263.
- 계량기수리업 service_name 은 **비-LOCALDATA `repair092801`**(라이브 total≈113) — 테스트 allowlist 등록.
- `tests/test_registry.py` `EXPECTED_COUNT` 107 → 139 + 비-LOCALDATA allowlist. 전체 pytest 295 통과.
- 레지스트리 헤더 107 → 139 + category 에 `industry` 추가. DAG 는 registry 기반 → 139 job 자동 반영.

### 45. raw 107종 실측 검증 + 응답 필드 커버리지 분석 + bronze 라인 검증

request:
- 현재 기준 모든 raw 호출로 수집 정상 여부 확인. 각 API 응답을 공통/비공통 필드로 분리.
- 기존 bronze/silver 식별값이 107종에 그대로 유효한지 분석. raw·문서 정리 후 bronze/silver 라인 검증.

response:
- 107종 전량 라이브 샘플(1/5) → **수집 107/107 OK, 실패 0**. 응답 row 키 집계: 전 종 공통 14 +
  준공통 5(`DCBYMD`/`TRDSTATENM`/`SITETEL`/`SITEWHLADDR`/`SITEPOSTNO`, 1~2종 결측) + API별 비공통(45 스키마 변형).
- **식별값 `OPNSFTEAMCODE`·`MGTNO`·`UPDATEDT`·`LASTMODTS` = 107/107** → bronze 정렬/식별키·silver 그레인·
  `content_hash`·`TRDSTATEGBN`(상태) 그대로 유효. silver 는 `record_json` schema-on-read(`nullif`)라 비공통/누락 무손실.
- 문서: [docs/pipeline/raw/api-field-coverage.md](docs/pipeline/raw/api-field-coverage.md) 신설 + bronze README 인덱스 추가.
- 검증: 전체 pytest 295 통과 + 신규 culture 1종(`traditional_temple`) bronze raw end-to-end 스모크
  (격리 프리픽스 `_verify207` → status=ok, NDJSON+마커+diff-target 생성, 식별값 4개 보존, 검증 후 삭제).
- 주의: bronze 정렬키의 `OPNSFTEAMCODE`/`LASTMODTS` 포함(#198/#193)은 별도 브랜치 → 107종 완전 적용은 #193·#198
  머지 후. silver(dbt) 그레인은 이미 `(dataset, opnsfteamcode, mgtno)`.

### 44. 문화 상권 인허가 56종 추가 (51 → 107)

request:
- 문화 상권 인허가 56종(골프장·체육시설/영화·비디오/게임/관광·여행/공연/음악·음반/출판·인쇄·광고 등)을
  추가. "문화 카테고리지만 결국 상권"이라 commerce 로 편입. 기존과 같이 수집(중복·명칭 충돌 점검 포함).

response:
- `config/dataset_registry.yaml` 에 56종 추가(51 → 107), `category: culture`. LOCALDATA 코드·oa_id·short
  56종 전부 기존 51종/격리분과 중복 없음(기존 `031103`=숙박업 ≠ 신규 `031105/031107`=야영장). 코드 오름차순 배치.
- `tests/test_registry.py` `EXPECTED_COUNT` 51 → 107. 무결성 6종 통과(개수·short/service_name/oa_id 유니크·
  필수필드·`LOCALDATA_` 접두·daily 전량). 전체 pytest 295 통과.
- 레지스트리 헤더 51 → 107 + category 목록에 `culture` 추가. DAG 는 registry 기반이라 107 job 자동 반영.
- docs 상세 호출량 표(api-call-volume 등)는 107종 실측 재산정 후속.

### 43. raw 수집 대상 12종 추가 (39 → 51) + 레지스트리 무결성 테스트

request:
- 위탁급식영업/집단급식소/식품제조가공업/식품첨가물제조업/식용얼음판매업/단란주점영업/유흥주점영업/
  외국인전용유흥음식점업/의료법인/의료기기수리업/의료기기판매(임대)업/동물용의약품도매상 12종의
  raw 수집 라인이 누락 → 추가.
- 중복 없는지·총 51종 맞는지 확인. 기존 bronze 수집 결과와 명칭(short) 겹침 점검.
- 개발 후 테스트 및 로컬 commit(push 없음). branch: `207-raw-collect-etc`.
- (보건 외 문화 상권 50+종 추가 예정 — 후속 배치.)

response:
- `config/dataset_registry.yaml` 에 12종 추가(39 → 51). LOCALDATA 코드 12개 모두 기존 39종과 중복
  없음, `short`·`oa_id`·`service_name` 전부 유니크. 신규 short 는 기존/격리분과 미충돌 확인
  (group_meal_facility≠group_meal_food_sale, food_mfg≠instant_sale_mfg,
  entertainment_bar≠tour_entertainment_bar, medical_device_sale≠animal_medical_device_sale).
- `tests/test_registry.py` 신설 — 개수 51·short/service_name/oa_id 중복 없음·필수필드·`LOCALDATA_`
  접두·daily 전량 수집대상. 전체 pytest 295 통과.
- 레지스트리 헤더 주석 39 → 51 갱신. DAG 매핑은 registry 기반이라 51 job 으로 자동 반영.
- docs 의 상세 호출량("39종" 산정 표: api-call-volume 등)은 문화 배치까지 합쳐 일괄 재산정 예정.

## 2026-07-07

### 42. silver 마스킹 주소 동단위 매핑 스킵 + 품질 warning 알림 규칙

request:
- 주소 값이 `*` 로 마스킹된 행은 silver 단계에서 동단위 법정동/행정동 매핑을 스킵할 것.
- 해당 데이터가 발생하면 warning 레벨로 로그를 남기고, 전체 카운트와 정산건수, 전체 대비 비율을
  알림 인터페이스로 전달할 것.
- 사전에 인지한 품질 이슈 알림은 `[작업>에러레벨]` 단위로 묶고, 작업이 무엇을 하는지 쉽게
  설명하는 규칙을 `CLAUDE.md` 에 남길 것.

response:
- `silver_license_history` 의 `dong_token` 산출에서 `road_address` 또는 `jibun_address` 에 `*` 가 있으면
  `dong_raw` 를 null 로 만들어 신규/재처리 행의 `legal_dong/legal_code/admin_dong/admin_dong_code`
  매핑을 수행하지 않게 했다.
- `silver_license_current` 에서는 기존 history 행이라도 마스킹 주소이면 법정동/행정동 산출 컬럼을 null 로
  내보내도록 막았다. 기존 history 자체를 소급 정리하려면 `dbt run --full-refresh --select silver_license_history+`
  로 전체 재해석해야 한다.
- `silver.quality_tasks.notify_masked_address_dong_skip_summary()` 를 추가하고 `commerce_load_silver` DAG 에
  `notify_masked_address_summary` 태스크를 `dbt_run_silver -> dbt_test_silver` 사이에 연결했다. 집계는 Trino
  단일 aggregate 쿼리로 수행해 Airflow 메모리에 행 데이터를 싣지 않는다.
- `commerce_core.notify.notify_quality_event()` 를 추가해 `[commerce][<task>><level>]` 제목과
  `affected_rows/settled_rows/affected_ratio_pct` 중심의 품질 알림을 보낼 수 있게 했다.
- `CLAUDE.md` 에 사전 인지 품질 이슈 알림 규칙을 추가하고, 주소/행정구역 문서에 마스킹 주소 예외를
  반영했다.

### 41. silver 행정동 코드 컬럼명 확정 + 행정/법정 코드 시군구 prefix 불일치 error 알림

request:
- silver 산출 컬럼의 행정동 코드는 `admin_dong_code` 로 사용할 것.
- 시군구 코드가 행정동/법정동 코드 앞 5자리를 공유하는 것으로 보이므로, 연산 과정에서
  다른 값이 보이면 error 로그로 남기고 알림 인터페이스로 연결할 것.

response:
- dbt silver history/current 산출 컬럼을 `admin_dong_code` 로 정리했다. 기존 오타성
  `admin_dong_cod` 변경은 즉시 되돌렸고, `admin_code` 산출 참조도 `admin_dong_code` 로 맞췄다.
- `enrich_admin_dong_ref` 단계에 `_sgg_prefix_mismatches()` 검증을 추가했다. `legal_dong_code[:5]`,
  `admin_dong_code[:5]`, 산출 `sgg_code` 가 불일치하면 `admin_dong_sgg_prefix_mismatch` 이벤트를
  **error** 레벨로 기록하고, `notify_completion(..., level="error")` 로 알림 인터페이스에 연결한다.
  불일치가 있어도 적재는 계속 진행해 원천 이상을 관측 가능하게 남긴다.
- 문서(`dbt/docs/address-and-geo.md`)에 `admin_dong_code` 컬럼명과 시군구 prefix 불일치 error/알림
  정책을 반영했다. 회귀 테스트 1개를 추가했다.
- 검증: `compileall` 통과, `pytest test_silver_tasks.py` 5개 통과, `python -m security` 차단 이슈 0.

### 40. silver marker 증분 전환 + DAG 명칭 `commerce_load_silver` 정리 + gold 포장 계획

request:
- `dbt/domains/commerce` 와 commerce docs/changelog 맥락을 확인해 silver 레이어가 매번 전체 데이터를
  재적재하는 문제를 marker 기반 증분으로 바꿀 것. marker 가 없으면 해당 경로 전체 백필을 수행하되,
  대용량 백필에서 RAM/CPU 문제가 없도록 stream I/O 와 최소 병렬 처리를 적용할 것.
- Airflow UI 에 보이는 `commerce_localdata_transform`/`common_admin_dong_bronze` 를
  `commerce_load_silver` 단위로 취합해야 하는지 검토하고, bronze 유지가 맞으면 silver/gold 도달 계획을
  문서로 남길 것.

response:
- dbt silver history 를 `incremental` + `append` 로 전환하고 `silver_load_run_marker` 의
  `(dataset, bronze_run_id, status='DONE')` 를 silver 완료 marker 로 사용. 첫 실행/`--full-refresh`/
  target table 부재 시 `is_incremental()` 이 false 라 publishable bronze 전체를 백필하고, 이후에는
  DONE marker 가 없는 `bronze_run_id` 만 읽는다. dbt test 통과 후 Airflow `mark_silver_done` 태스크가
  DONE 을 기록한다. 증분 첫 행의 인접 중복 방지를 위해
  영향 key `(dataset, opnsfteamcode, mgtno)` 의 기존 최신 1행만 조인해 `content_hash` 를 비교한다.
- DONE marker 가 없는 후보 run 은 dbt pre-hook 으로 history 에서 선삭제 후 재삽입되도록 해
  dbt run/test 실패 후 재시도 중복 위험을 줄였다. 기존 history 가 있는 배포 환경은
  `ensure_silver_marker` 태스크가 marker 를 부트스트랩한다.
- dbt profile `threads: 1` 로 낮춰 동시 warehouse 쿼리를 제한했다. full backfill 도 Trino/Iceberg 쿼리
  안에서 수행하고 Airflow/Python 메모리에 전체 데이터를 올리지 않는 구조로 유지했다.
- DAG 파일/ID 를 `commerce_load_silver.py` / `commerce_load_silver` 로 정리했다. 내부 흐름은
  `[enrich_admin_dong_ref, enrich_fill_jibun] -> dbt_run_silver -> dbt_test_silver`.
- `common_admin_dong_bronze` 는 루트 `dags/` 의 공용 마스터 수집 DAG 이므로 commerce DAG 로 병합하지 않는
  것으로 결정했다. commerce 는 공용 raw `raw/common/admin_dong` 최신본을 읽어 자기 스키마의
  `bronze_ref_admin_dong` 만 갱신한다.
- 운영 문서(`dbt/docs/rebuild-and-ops.md`)와 초보자/주소/README 문서를 marker 증분·full-refresh 백필
  계약으로 갱신하고, [docs/pipeline/silver-gold-load-plan.md](docs/pipeline/silver-gold-load-plan.md) 를
  추가해 silver DAG 경계와 gold current 집계 포장 계획을 남겼다. 공식 근거는 dbt incremental,
  dbt-trino incremental strategy, dbt threads 공식 문서 링크로 표기했다.
- 검증: `compileall` 통과, `pytest test_silver_tasks.py test_load_plan.py` 12개 통과(승인 실행),
  `python -m security` 차단 이슈 0, `pytest test_security.py` 162개 통과. 로컬 `dbt parse` 는
  설치된 dbt 환경에 `dbt-trino` 어댑터가 없어 실행 불가(Airflow dbt venv 계약상 배포 환경에서 확인 필요).

### 39. 버전 정렬 1순위에 LASTMODTS 폴백 — UPDATEDT 결측 시 최종수정시점으로 정렬

request:
- UPDATEDT 가 없으면 "MODDT"(최종수정시점)로도 정렬되게 할 것 + MODDT 전건 존재 여부 확인.

response:
- 컬럼 확인: 원천 날짜/수정 컬럼은 UPDATEDT·LASTMODTS·APVPERMYMD·DCBYMD·APVCANCELYMD·
  CLGSTDT/CLGENDDT·ROPNYMD. "MODDT"에 해당하는 것은 **LASTMODTS**(별도 MODDT 컬럼 없음).
- 커버리지 실측: history 1,344,765행 전부 updatedt_ts·lastmodts_ts **100% 존재**(결측 0) →
  현재 UPDATEDT 결측 0건이라 폴백 필요 행 없음(순수 방어적 개선).
- dbt silver(feat/45): `updatedt_sort` 를 `coalesce(updatedt_ts, lastmodts_ts, epoch)` 로 변경
  (기존 `coalesce(updatedt_ts, epoch)`). UPDATEDT 없는 행이 epoch(최하위)로 밀리지 않고 LASTMODTS 로
  정렬됨. lastmodts_sort(2순위)·grain·dedup 불변. 재빌드 행수·테스트 15/15 **동일**(무영향 확인).
  schema.yml·timestamps-and-nulls.md 갱신.

### 38. silver gu/동 파싱 — 시도 접두 변형(서울시·무공백 결합형) 대응

request:
- 원천 주소 검증 결과 지번 동은 **법정동**(98.3% 매치)으로 확인 — silver 법정동 우선 분류가 정합.
  미매치 원인 중 `서울`(단축 접두)도 함께 대응해 달라는 요청.

response:
- 실측 접두 변형: `서울특별시`(표준·공백) 외 `서울시`(82) + **무공백 결합형**
  `서울특별시마포구`·`서울시노원구공릉1동`(구/동이 시도에 붙음)이 존재.
- dbt silver(feat/45) gu/동 regex 를 관대화: 접두 `서울(?:특별시|시)?` + `\s*`(공백 0개 이상),
  구는 lazy `[가-힣]+?구`(구로구구로동 결합형에서 첫 '구'까지만), 동은 `[가-힣]+\d*(?:동|가)` 로
  **번지 앞 정지**(공덕2동461 → 공덕2동). 비서울·마스킹(`당산동*가`) 주소는 여전히 null(정상).
- 효과(재빌드 실측): gu 98.6→**99.72%**, legal_dong 98.68→**99.06%**(+5,127행). dbt 15/15 통과.
- 남은 null(~0.9%)은 원천 마스킹(56%)·비서울 주소(28%) 등 **원천 한계**(코드 결함 아님).
- 규약 문서 갱신: dbt `docs/address-and-geo.md` §2(gu/동 regex·접두 변형 대응).

request:
- ① Juso 로도 못 찾는 비정형 주소(미해결)의 수집 시 로그 + 처리 결과 값을 **성공/완료 알람
  인터페이스로 연결**할 것. ② MGTNO 충돌 378건 재검토 — 승계·개명이면 같은 MGTNO 에 업장명이
  바뀌는 게 정상 아닌가(MGTNO 가 키, 업장명은 키가 아님)라는 문제 제기.

response:
- ① **알림 연결**: `commerce_core/notify.py` 에 `WebhookNotifier`(discord|generic,
  `COMMERCE_NOTIFY_WEBHOOK_URL/KIND` env 팩토리 — URL 은 register_secret 마스킹 등록,
  전송은 netio.http_post) + `notify_completion()`(성공=info/미해결 있으면 warning,
  처리 결과 값 + 미해결 상위 10건) 추가. `enrich_fill_jibun` 종료 시
  `jibun_fill_unresolved` log_event(미해결 주소·행수·샘플 20건) 기록 후 notify_completion
  호출로 배선. URL 미설정이면 기존과 동일한 no-op(로그만). 테스트 8종 추가(총 286 통과).
- ② **재판정(사용자 지적 타당)**: 같은 MGTNO 의 업장명·상태 변경은 U 증분의 정상 버전
  이력이 맞음(키 충돌 아님). 378건 분해 실측 —
  - 323건(rest_restaurant): 같은 스냅샷 안 **공백 변형 중복**(주소 끝 공백 차이, 이름·시각
    동일) → 같은 업소, current 가 1행 선택으로 이미 무해.
  - **55건(tour_restaurant 54·optical_shop 1): 서로 다른 구청(OPNSFTEAMCODE)이 같은 MGTNO
    발급** — MGTNO 는 자치단체 안에서만 유니크. 별개 업소가 병합되던 실 손실 케이스.
- ② 수정(dbt feat/45): silver 에 `opnsfteamcode` 추출 + 업소 식별키를
  **(dataset, opnsfteamcode, mgtno)** 로 확장(history dedup 파티션·current row_number·
  grain 테스트 3종·schema/README/가이드 갱신). 재빌드 후 current +119행(교차 구청 업소 보존,
  예: CDFI3261042006000001 → 7개 구 7개 업소 각 1행), dbt 테스트 15/15 통과.

request:
- bronze 확인 결과 주소는 전체 문자열(시군구·동 분리 필드 없음), 행안부 법정동코드 미제공 →
  ① lodging 처럼 지번 필드가 비는 경우 도로명으로 지번을 채울 것(Juso API, 결과 안 나오면
  정규식 케이스를 늘려 결과가 나올 때까지 복합 처리). null 이었던 규모와 API 호출 횟수를
  로그로 남길 것. ② district→gu rename + gu_code 추가, 동은 법정동/행정동 판별해
  legal_dong/admin_dong 에 넣고 반대쪽은 R2 `raw/common/admin_dong` 최신본 매핑으로 채울 것
  (legal_code/admin_code 포함). ③ 중부원점 X/Y → 위경도 계산 컬럼 추가(순수 계산이면 그걸로 마무리).

response:
- **enrich 태스크 2종 신설**(`include/silver/enrich_tasks.py`, transform DAG 이
  `[enrich_admin_dong_ref, enrich_fill_jibun] → dbt run → dbt test` 로 배선):
  - `bronze_ref_admin_dong` — R2 raw/common/admin_dong 최신(load_date→ingest_ts 최대) 페이지를
    파싱해 **서울만**(전국 2.1만 행은 Iceberg INSERT 커밋 비용 과다 — `ADMIN_DONG_SIDO_FILTER`)
    전량 교체 적재(769행). 코드값 문자열 고정, `sgg_code`=법정동코드 앞 5자리.
  - `bronze_address_enrichment` — 지번(SITEWHLADDR·LOTNO_ADDR 모두) 결측 도로명(유니크)만
    Juso 조회 후 캐시(키 delete-then-insert, filled 영구/not_found 래더판 스킵/error 재시도,
    500건 주기 플러시). 감사: 행 단위 pattern_id/attempts/api_calls/status +
    `jibun_fill_run` log_event(null_jibun_rows/distinct_addresses/api_calls/filled/...).
- **Juso 클라이언트**(`include/silver/juso.py`) — 정규화 래더 p1 괄호절단 → p2 콤마절단 →
  p3 도로명+번호 접두 → p4 '<구> <로> <번호>' 재조립 → p5 시도 생략. 첫 totalCount≥1 에서
  중단. `LADDER_VERSION` 갱신 시 not_found 재시도. 보안: netio.http_get(타임아웃·응답 캡),
  키는 env `JUSO_CONFM_KEY`(.env.commerce, 자동 마스킹). 단위테스트 13종(`tests/test_juso.py`).
- **dbt silver**(ASAC-DBT feat/45): 지번 채움 coalesce(원천→LOTNO_ADDR→Juso) +
  `jibun_address_source` 계보, `district`→`gu` rename + `gu_code`,
  동 토큰 법정동 우선 판별 → `legal_dong/legal_code/admin_dong/admin_code`(다대다는
  결정적 근사 — 숫자 제거 동명 우선·코드 오름차순), **EPSG:5174→WGS84 순수 계산**
  (`latitude/longitude`, 한반도 bbox 밖 null). 좌표계는 시청·GFC 랜드마크 실측으로
  5174 판별(42~90m vs 2097 230~251m). 규약 문서: dbt `docs/address-and-geo.md`.
- **transform DAG `_dbt_command` 수정**: 호스트 전역 `DBT_PROJECT_DIR`(smoke 프로젝트)이
  cwd 보다 우선해 프로필 오류를 내던 잠재 버그 — 커맨드에 `DBT_PROJECT_DIR` 명시 고정.
- 검증: pytest 281 통과 · security 게이트 PASS · DAG import OK · dbt run(134만 행)/test 13종
  통과 · 랜드마크 좌표 소수 7자리 일치(37.5665851, 126.9782039) · 동 매핑률 98.6%.

### 35. silver 타임존 정책 재확정 — timestamp 전부 KST 일원화(dbt) — #34 뒤집음

request:
- silver 레이어에 저장되는 시각을 **모두 KST 기준**으로 표기(기존 UTC → KST 변환).
  특히 collect time(`collected_at`)은 UTC 로 기록돼 다른 시각과 다르게 보였는데, silver 로 갈 때
  **KST 로 완전히 변환**되어 파일로 저장되어야 함.
- dags 브랜치 `feat/113` → `feat/133-commerce-silver-ingest` 로 rename 후 작업·push.

response:
- 직전 #34(UTC 일원화)를 **뒤집어** silver timestamp 전부 **KST** 로 재확정.
- **dbt(feat/45-silver-dbt-ingest)**: `updatedt_ts`/`lastmodts_ts` 의 `- interval '9' hour` 제거
  (원문이 이미 KST 라 파싱만·무변환), `collected_at` 은 bronze UTC 값을 `+ interval '9' hour` 하여
  KST 로 변환. `schema.yml`·`timestamps-and-nulls.md` §1·`beginner-guide.md` 를 KST 정책으로 개정.
- **bronze 는 UTC 원본 유지**(소스 진실) — 변환은 silver 표기 계층에서만. `warehouse._to_naive_utc()`
  및 source freshness(bronze `collected_at` 기준)는 변경 없음.
- 전 시각 컬럼이 동일 +9h 시프트라 정렬키·인접 dedup·grain 불변(dbt 테스트 4종 영향 없음).
- `medallion-implementation-plan.md` §2.2 타임존 항목을 KST 정책으로 갱신.

### 34. silver 타임존 정책 확정 — timestamp 전부 UTC 일원화(dbt)

request:
- 번거롭더라도 국제표준에 맞게 silver 적재 시 KST 시각을 **UTC 로 변환**해 일원화.
  collected_at 은 9시간 차이가 나는 형태이므로 silver 에서 바로 정합하게 넣을 것.

response:
- (사실관계 정정 후 반영) `collected_at` 은 수집 마커가 처음부터 UTC 로 기록한 값 —
  UTC 표준에서는 **무보정 통과**가 정답(+9h 는 이중 보정). 보정 대상은 KST 원문인
  UPDATEDT/LASTMODTS 쪽으로, 파싱 timestamp 에 `- interval '9' hour` 적용.
- **dbt(feat/45-silver-dbt-ingest)**: `updatedt_ts`/`lastmodts_ts` UTC 변환(-9h, 원문 문자열
  보존), collected_at 무보정 주석 명시, schema.yml 설명 갱신,
  timestamps-and-nulls.md §1 을 "silver timestamp 전부 UTC" 정책으로 개정
  (일별 집계는 KST 날짜 컬럼 기준 — UTC date 절단 금지 등 사용 주의 포함).
- 날짜 컬럼(observed_date/load_date/APVPERMYMD/DCBYMD)은 시간 정보가 없는 KST 달력
  날짜라 변환 비대상(정책 문서에 명시). 정렬·인접 dedup·grain 은 고정 오프셋이라 불변.
- medallion-implementation-plan.md §2.2 타임존 항목을 확정 정책으로 갱신.

## 2026-07-05

### 33. silver 암묵 버저닝 확정(dbt) 반영 + transform DAG 신설 + #109 잔재 import 수정

request:
- silver 는 명시 버전 컬럼(version_seq/valid_from/valid_to/is_current) 없이, 키(MGTNO) 안에서
  **UPDATEDT·LASTMODTS 내림차순 정렬이 곧 버전 순서**(암묵 버저닝)가 되도록 확정.
  current 는 그 정렬의 최신 1행. gold 가 나중에 이 정렬로 현재 상태 갱신만 수행.
- dags 쪽 문서를 변경 내용에 맞게 모두 수정하고, **오케스트레이션(transform DAG)도 설정**.
- 재빌드 시 특정 일자·특정 인허가 API(dataset) 단위 재적재/삭제가 **설정 파일 소폭 수정만으로**
  가능해야 함(dbt 쪽 적재형태·재빌드 정책 + 관리 문서 포함).
- dags 는 dev 기반 feat/113-commerce-silver-ingest, dbt 는 feat/45-silver-dbt-ingest 로 푸시.

response:
- **dbt(ASAC-DBT feat/45-silver-dbt-ingest)**: silver 2종 재작성(SCD2 제거, 정렬키
  UPDATEDT→LASTMODTS→observed_date→collected_at→content_hash, LASTMODTS 파싱 추가,
  '' → null 결측 규약) + 테스트 개정(행 유니크 grain (dataset,mgtno,collected_at,content_hash),
  인접 중복 0) + 단위 제외 vars 4종(exclude_datasets/observed_dates/load_dates/bronze_run_ids,
  macros/exclusions.sql) + 문서 4종(rebuild-and-ops·timestamps-and-nulls·dataset-columns·README).
- **transform DAG 신설**: `commerce_localdata_transform.py` — 05:00 KST(적재 04:00 이후),
  BashOperator 2단(dbt run silver → test silver), common_dbt_smoke 와 동일 dbt venv/env 계약,
  무상태(전량 재빌드 오케스트레이션만). `.env.commerce.example` 에
  COMMERCE_DBT_PROJECT_DIR/COMMERCE_DBT_TARGET 항목 추가.
- **#109 잔재 import 수정(파싱 불능 해소)**: `commerce_load_bronze.py`(common.env/registry/
  settings/storage → commerce_core.*, dags-root sys.path 부트스트랩 추가) ·
  `bronze/load_plan.py`(common→commerce_core paths) · `bronze/warehouse.py`(hashing/settings) ·
  `tests/test_load_plan.py`. 적재 라인 단위테스트 27 통과 재확인.
- **문서 갱신**: medallion-implementation-plan.md(§2 표, §2.2 계약 개정, Step 6·10 상태,
  §6 요약, §7-9 결정 반영), 번들 README(데이터 흐름 3단 DAG 라인·구조도).
- 단위 재적재/삭제 운영 계약: 재적재 = bronze 워터마크 파일(`_watermark.json`) 수정 →
  `commerce_load_bronze` → 자동 반영(전량 재빌드) / 삭제·복원 = dbt vars 수정 → `dbt run`.
  절차 문서: `dbt/domains/commerce/docs/rebuild-and-ops.md`.

## 2026-07-04

### 32. 적재 엔진 기준 = 첫 파일(순서), 크기/legacy 아님 — 07-01 증분 오분류 수정

request:
- (지적) 엔진을 파일 크기로 판단하지 말 것. **첫 파일이라 큰 것**이지 크기가 기준이 아니다.
  첫 파일이니 전체 재적재이고, 이후 파일을 PyIceberg 로 돌리면 안 된다.

response:
- **근거 확인**: 07-01 general_restaurant 파일은 row-NDJSON **280건(293KB)** 짜리 증분인데, 이전
  코드가 마커 `rows_total`(=534,748, API 전체수)을 파일 건수로 오독하고 `increment_mode` 없음을
  legacy 전량으로 취급해 PyIceberg 로 잘못 분류했다(06-30 은 page-NDJSON 534,680건 진짜 full).
- **수정(순서 기준)**: `resolve_load_plan` — 데이터셋 bronze 최초 적재(워터마크 없음)의 **첫 파일 =
  전체 재적재(PyIceberg)**, 그 이후 모든 파일 = **증분(Trino)**. 파일 크기/legacy 로 판단하지 않는다.
  기대 건수는 마커 `increment_count`(신뢰 가능)만 사용, `rows_total` 절대 미사용(없으면 rows>0 판정).
  (앞서 잠깐 넣었던 파일-크기 기준 및 `Storage.size` 추가는 되돌림 — abstract 메서드가 테스트 파손도 유발.)
- **검증(실데이터)**: 워터마크 06-30 상태에서 계획 = 27유닛 전부 Trino 증분·PyIceberg 0.
  general_restaurant 07-01 을 Trino 로 실적재 → **280건**(534K 아님), 534,680 → 534,960(+280).
  단위테스트 **266 통과**, security 차단 0.

### 31. commerce_load_bronze 통합 검증 — 이미지 리빌드 + 동시성/receipt 수정 (전 39종 적재 성공)

request:
- 리빌드까지 진행하고 검증. 실패하면 조치하고 성공할 때까지 반복.

response:
- **이미지**: `feat/7-dockerfile-pyiceburg` 의 pyiceberg[s3fs] 0.11.1 로 이미지 리빌드·재기동 →
  pyiceberg/pyarrow/trino 임포트 확인.
- **통합 검증 중 발견·수정 2건**:
  1. `load_state.write_receipt` 가 `bronze_run_id` 키를 찾다 KeyError(load_unit 결과는 `run_id`) →
     `run_id` 폴백 추가.
  2. **Iceberg 동시 커밋 충돌**: 39 load_one 이 동일 테이블에 병렬 append → `CommitFailedException`
     (낙관적 동시성)으로 28/39 실패. → `load_one` **직렬화**(`max_active_tis_per_dagrun=1`) +
     `load_unit_pyiceberg` 를 **트랜잭션 1커밋**(delete+청크 append 묶음, 스냅샷 폭증/충돌창 제거).
- **재적재 가능 검증**: bronze 테이블 drop + 상태파일 삭제 → 워터마크 없음(전체 재적재 조건) 확인 후
  재실행. (사용자 "iceberg/parquet + 상태 삭제 후 재적재 가능" 설계 실증.)
- **결과(dev, max_dates=1, 06-30 legacy 전량)**: **load_one 39/39 성공·0 실패**, bronze
  **1,342,222행 / 39종 / 1,341,784 유니크 mgtno**, manifest 39 전부 publishable, 워터마크 39·pending 0·
  receipt 39. record_json `json_extract_scalar` 파싱 정상(BPLCNM·TRDSTATENM) → silver 준비 완료.
  단위테스트 **264 통과**, security 차단 0. (직렬 백필 ~17분 — 대부분 general_restaurant 53만건.
  일일 증분은 소량이라 빠름. 07-01 이후는 재실행이 catch-up.)

### 30. commerce_load_bronze 크래시 수정 — page-NDJSON(과거 데이터) 적재 지원

request:
- commerce_load_bronze 첫 실행이 죽음. 원인 분석 요청. (판단) 과거 데이터를 스킵하지 말고
  **현재 코드로 읽어 적재**하도록 변경. pyiceberg 는 최신 stable(0.11.1)로 이미지 반영(별도 host 브랜치).

response:
- **원인**: 워터마크 없는 첫 실행이 raw 를 처음부터 스캔 → `2026-06-30`·`2026-07-01` run 이
  **feat/58 이전 page-NDJSON**(줄=API 페이지 응답, 마커에 increment_mode 없음). 로더가 이를
  스킵하지 않고 **ValueError 로 태스크를 죽임**(계획은 스킵인데 구현이 raise). 게다가 35/39
  데이터셋은 row-NDJSON 증분 파일이 없어(07-02/03 identical) 스킵만으론 bronze 가 빈다.
- **수정(과거 데이터 적재)**: `iter_increment_rows` 를 **두 포맷 모두 지원**으로 변경 —
  row-NDJSON(줄=레코드) + page-NDJSON(줄=페이지 응답 → `parse_page(...).rows`, service_name 필요).
  `resolve_load_plan` 은 워터마크 이후 **완료 run 을 시간순 전부 적재**(legacy 전량=PyIceberg,
  changed=Trino, identical=적재없이 전진). diff_target 우회/legacy 스킵 제거.
- **검증**: 실제 데이터 드라이런 — 66 유닛(legacy 62 PyIceberg + changed 4 Trino), general_restaurant
  06-30 legacy 파일에서 실제 레코드 파싱 확인(534,680건, 전체 인허가 컬럼). 단위테스트 **263 통과**,
  `python -m security` 차단 0.
- **pyiceberg**: 이미지에 미설치 확인(trino·pyarrow 는 있음) → host repo `feat/7-dockerfile-pyiceburg`
  브랜치에서 `Dockerfile.airflow` 에 `pyiceberg[s3fs]>=0.11.1,<0.12` 추가(별도 작업). requirements.txt 동기화.

### 29. bronze 적재 — 수집과 분리된 commerce_load_bronze DAG(PyIceberg/Trino) + 파일 상태 (Step 2~3)

request:
- medallion(#28) Step 2~6 구현. dags=ASAC-DAG `feat/113-commerce-bronze`, dbt=ASAC-DBT
  `feat/38-commerce-Bronze-Layer-ingest`.
- (재설계 지시) 수집 DAG 에 적재를 직결하지 말 것 — 대용량에서 Trino OOM. raw 와 bronze 를 분리,
  적재는 어느 raw 를 읽었는지 이력관리(재적재 가능·raw 와 격리된 공간). complete 없어 못 읽은
  일자·대상은 기록 후 다음 실행 재시도(현재-2일 재감시, 3일 폐기). 어느 날짜까지 적재했는지 파일로
  관리(없으면 처음부터 전체 재적재). **전체 재적재는 PyIceberg, 그 외는 Trino 증분**(한 번에 모든
  날짜 X). **RDB 사용 금지.** 로더 content_hash dedup 은 증분 경로에서 무의미 → 미적용.

response:
- **수집·적재 분리**: `commerce_raw.py` 는 raw-only 로 복원(이전 결합 되돌림 — Trino 무의존).
  신규 **[commerce_load_bronze.py](commerce_load_bronze.py)** DAG 가 적재 전담
  (`resolve_plan → plan_units → ensure_warehouse → load_one.expand → finalize`).
- **적재 엔진 [include/bronze/warehouse.py](include/bronze/warehouse.py)**: mode=first(전체 스냅샷)
  → **PyIceberg**(R2 Data Catalog REST + R2 S3 FileIO, `delete`+Arrow `append`, 커밋 1회 — Trino
  코디네이터 우회로 OOM 회피), mode=changed(소량) → **Trino** 증분(`?` 파라미터 바인딩). 단일 테이블
  `commerce.bronze_localdata_license`(39종 dataset 컬럼 구분) + `bronze_collection_run_manifest`
  (데이터셋별 발행 게이트). row-NDJSON 라인 스트리밍, canonical sha256 content_hash(컬럼 보존, 로더
  dedup 없음). 식별자만 assert_identifier 보간(allow-sql).
- **파일 상태(RDB 없음) [include/bronze/load_state.py](include/bronze/load_state.py)**: raw 와 격리된
  `{prefix}/commerce_bronze_state/`(commerce_ prefix) 에 워터마크(데이터셋별 마지막 적재 run) +
  pending(complete 없는 (date, short), 현재-2일 재감시·3일 폐기) + receipt(적재 감사 로그).
  **[load_plan.py](include/bronze/load_plan.py)**: `resolve_load_plan`(무손실 skip — diff-target 은
  완료 run 에서만 전진하므로 incomplete 건너뛰어도 무손실), `commit_watermark`(적재 성공분까지만
  전진, 실패 run 직전 정지 → 다음 실행 재시도). 실행당 `COMMERCE_LOAD_MAX_DATES`(기본 3) 바운드.
- **격리·명명**: Iceberg 물리 저장은 R2 Data Catalog 관리(폴더=UUID, 클라이언트 지정 불가) —
  구분 핸들은 논리 스키마 `commerce`. 상태/이력 파일만 `commerce_` prefix 로 직접 관리.
- **env/deps**: `.env.commerce.example`(COMMERCE_SCHEMA·COMMERCE_BRONZE_STATE_LAYER·
  COMMERCE_LOAD_MAX_DATES·TRINO_*·R2 Data Catalog), `requirements.txt` 에 `trino`·`pyiceberg[s3fs]`
  추가(**이미지 추가 필요**).
- **테스트**: `test_warehouse.py`·`test_load_state.py`·`test_load_plan.py` 신규 — 전체 **260 통과**
  (239→+21), `python -m security` 차단 0. 두 DAG(collect raw-only / load) 파싱·배선 검증.
  PyIceberg/Trino 실제 왕복은 이미지 통합(Step 4).
- **silver/gold(dbt)** 는 ASAC-DBT `feat/38` 에 별도 구현(Step 5~6): `dbt/domains/commerce/`
  (sources·silver_license_history SCD2·silver_license_current·테스트). Step 4/7 백필,
  Step 8 좌표 보정, transform DAG(Step 10)는 후속.

### 28. medallion 구현 계획 문서 — raw→bronze(Iceberg)→silver→gold + 좌표 보정 설계 (제안)

request:
- python/airflow/iceberg/dbt/trino/R2 환경에서 bronze·silver·gold 역할 분담과 "raw 적재를
  bronze 레이어로 통합할지"를 검토하고, 단계별 to-do 의 **구현 방법까지 정의한 docs** 요청.
- 추가 요구: 업장 좌표(중부원점 TM 추정)와 주소 기반 **재보정 위경도**를 silver 에서 조인하도록
  설계 포함(어떤 로직/API 로 보정할지 확인 필요). 제약: `dbt/` 하위 타 도메인 폴더는 타인
  작업물이라 의존·병합하지 않음, dbt commerce 프로젝트는 미구현 상태에서 신규 설계.

response:
- **[docs/pipeline/medallion-implementation-plan.md](docs/pipeline/medallion-implementation-plan.md) 신설**(제안·승인 대기 항목 포함):
  raw=R2 랜딩(현행 유지, #75/#21 결정 준수) / bronze=Iceberg `commerce.bronze_localdata_license`
  단일 변경로그 테이블(record_json 통짜 + 계보 컬럼, `load_date` 파티션, delete-then-insert 멱등,
  dataset 단위 manifest 발행 게이트) / silver=dbt SCD2(`silver_license_history`·
  `silver_license_current`, 전순서 버전 정렬 + 연속 중복 제거로 A→B→A 원복 보존) /
  gold=`gold_commerce_license_status_current`(현황 스냅샷, 일별 추이는 phase-2). **raw→bronze 통합 안 함**
  (Trino 는 iceberg 커넥터뿐이라 JSONL 직질의 불가 — 부족한 것은 적재 단계).
- 백필·분기 정합성 복구는 `_diff_target` 전체본 기반 `mode=full_reconcile` 단일 코드 경로.
- **좌표 보정 설계**: 원천엔 X/Y(좌표계 미표기)뿐, 위경도 없음 확인 → 2트랙(좌표 변환 pyproj
  ·주소 지오코딩 API)+EPSG(2097 vs 5174) 실측 판별, `commerce_geocode_collect` 신규 수집 라인
  (raw 보존 → `bronze_geocode_address`, address_key 멱등·증분), silver 에서 LEFT JOIN 으로
  `lon/lat_corrected`·`location_source`·`location_quality` 산출. API 후보(VWorld/Kakao/Naver/juso)
  비교·약관/쿼터 체크리스트는 확정 전 조사 항목으로 명시.
- Step 1~10 순차 to-do(각 구현 방법·완료 기준) + 결정 대기 7건(§7) 정리.
  인덱스 갱신: docs/pipeline/README.md · docs/README.md 문서 맵.

### 30. HTTP 호출 경계를 공통 클라이언트로 전환 (#78)

request:
- 소스 API 호출을 `dags/common/http`(#78 HttpCore)로 통합. 단, commerce 는 §20 보안
  게이트가 `netio.http_request` 사용을 명시하므로 그 커버리지(SSRF 가드·응답 상한·TLS
  강제·예외 마스킹)를 잃지 않을 것.

response:
- `include/bronze/clients.py` 의 `SeoulOpenApiClient` 수동 재시도 루프를 HttpCore 로 대체.
  **netio 를 HttpCore 의 Transport(`_NetioTransport`)로 감싸** §20 커버리지를 그대로 유지하고,
  그 위에 통합 재시도(429/5xx+연결오류)·redaction 로깅·rate limit·typed 예외(HttpProblemError)를
  얹음(합성 — HttpCore Transport 계약의 의도된 확장점).
- **업무 오류 분류(INFO-000/100/200 등, `parse_page`)는 도메인에 그대로 유지** — HTTP 200
  응답 본문에서 판정, HttpCore 는 전송/HTTP 상태만 담당. 재시도 소진/HTTP 오류는
  `SeoulApiError("ERROR-NETWORK", redact(...))` 로 변환해 bronze 마커 계약 보존.
- `rate_limit=None` — 기존 `SEOUL_REQUEST_DELAY_SECONDS` 간격 유지(이중 지연 방지).
- (#78 코드리뷰 반영) `security/redaction.py` structural 패턴에 `serviceKey=` 쿼리 키 추가
  (공공데이터포털/KMA 형식 — dags/common/security 로 재복사), `parse_page` 의 비정수
  `list_total_count` 를 `SeoulApiError("ERROR-PARSE")` 로 래핑(원시 ValueError 누출 차단),
  `bronze/resolve.py` CLI 에 dags 루트 부트스트랩 추가(`python -m bronze.resolve` 복구).
- 검증: commerce 241 테스트 통과, 오프라인 스모크(성공·재시도·소진·업무오류 4경로) 통과,
  URL 경로 키가 HttpCore 로그에서 마스킹 확인.

### 29. 서울 base URL env 이름 통일 — SEOUL_OPEN_API_BASE_URL (#78)

request:
- 루트 `.env` 통합 원칙(#72)에 따라 base URL env 이름도 루트 이름
  `SEOUL_OPEN_API_BASE_URL`로 통일할 것 — 사용자 결정.

response:
- `settings.py` 읽기 훅 `SEOUL_OPENAPI_BASE_URL` → `SEOUL_OPEN_API_BASE_URL` 개명,
  configuration.md·test_security.py 예시 동반 개명. 실환경 값 이관 불필요 — #72 때
  `.env.commerce` 항목은 이미 삭제(코드 기본값과 동일)돼 코드 훅만 남아 있었음.
- 공용 서울 어댑터(`dags/common/http/seoul.py`)도 같은 이름 하나만 읽는다.

### 28. include/common → include/commerce_core 개명 + storage 승격 (#109)

request:
- dags/common(공통 상위 패키지, #77 에러 모듈)과 commerce top-level `common` 이 단일 프로세스
  DagBag 로드(`airflow dags test` 등)에서 충돌(sys.modules 캐시가 `import common.errors` 를 가림).
- 방침(Q&A 합의): **dags/common 을 상위 개념으로** 보고 **겹치는 기능만 먼저 합침**,
  나머지 모듈은 **무삭제 보존**(notify 포함), 잔류 패키지 이름은 `commerce_core`.

response:
- **storage 승격**: `storage.py` 의 범용 부분(Storage/LocalStorage/R2Storage)을 `dags/common/storage.py`
  로 이동(+`build_storage` 팩토리, Settings 결합 제거). commerce 쪽은 `commerce_core/storage.py`
  얇은 어댑터로 대체 — 클래스 재수출 + `get_storage()` 가 기존 Settings/env 계약 그대로 유지(동작 불변).
- **개명**: `include/common/` → `include/commerce_core/` (env/hashing/notify/paths/registry/schemas/
  settings 전 모듈 보존). 소비처 17파일 import 전환(`from common.*` → `from commerce_core.*`),
  DAG·scripts·conftest 부트스트랩에 dags 루트 추가(`common.storage` 해석용).
- **금지 규약 추가**: 번들 안에 top-level `common` 패키지 재도입 금지(CLAUDE.md §19 명시).
- 검증: commerce 전 테스트 239 통과, `python -m security` 차단 0, 단일 프로세스 전체 DagBag
  파싱 재현 테스트 통과(충돌 해소 확인).

### 27. 취약점 코퍼스 — 정적 detector 20종 발화 증명 + 격리 (feat/96)

request:
- 보안 플러그인 검증/증명 목적으로 **취약점 코드(코퍼스)** 작성. 자기 프로젝트라 방어적 용도의
  취약 샘플 생성은 문제 없음. (Q&A) 산출물 형태 = **취약점 코퍼스**(detector 별 취약 샘플 +
  self-audit 격리 + "스캔하면 N건 검출" e2e 테스트).

response:
- **격리 설계(무침습)**: 샘플을 `tests/vuln_corpus/*.pysample` 로 저장. self-audit(`_iter_files`)는
  `.py/.md/...` 확장자만 스캔하므로 `.pysample` 은 **불가시** → 취약 코드가 트리에 있어도 게이트
  청결 유지(audit.py 수정 0). e2e 테스트가 각 샘플을 tmp 에 실제 파일명(`.py`/`requirements.txt`/
  `.env.commerce.example`/`.gitignore`)으로 복사해 매핑 detector 발화를 잠근다.
- **다중 에이전트 워크플로**(map→author→적대적 검증): audit.py 의 `STATIC_CHECKS` 20종을 매핑,
  detector 별 **읽기 쉬운 취약 샘플**을 병렬 저작하고 각 샘플을 실제 정규식과 대조해 발화 예측 검증.
  22개 샘플 산출(credential_material 은 PEM·벤더토큰·URL userinfo 3종) → **정적 detector 20종 전수 커버**.
- **push-safety**: 벤더 토큰(`ghp_…`)·PEM 개인키·bidi 제어문자는 커밋 시 GitHub push protection 을
  건드리므로 커밋 금지 — 샘플엔 `__ASSEMBLE__` placeholder, 테스트가 조각 결합으로 런타임 조립
  (pem/ghp_token/bidi). 나머지는 provider 포맷 아닌 **합성 값**이라 커밋 안전(전 파일 push-safe 스캔 통과).
- **Ground-truth**: 22 샘플을 조립·materialize 해 매핑 detector 실행 → **전수 발화 확인**(fires + detail 일치).
- **검증**: 전 테스트 **239 통과**(213→+26: 파라미터화 발화 22 + 완전성/격리/게이트청결/통합스캔 4),
  `python -m security` 차단 0(코퍼스 무누출 확인). `test_every_static_detector_has_a_corpus_sample` 로
  신규 detector 추가 시 대응 샘플 누락을 자동 강제. 문서: [tests/vuln_corpus/README.md](tests/vuln_corpus/README.md).

### 26. 보안 플러그인 정밀 리뷰 — opus 구현부 6개 결함 수정 + 회귀 잠금 (feat/96)

request:
- 어제(fable) 진행 중 **opus 로 구현된 영역**에 미흡한 부분이 있으니, 해당 영역을 확인해
  **전체 조치 후 테스트**까지 진행할 것.

response:
- **적대적 정밀 리뷰**로 opus-era 모듈에서 실동작 결함 6종 확인·수정(전부 commerce 번들 안):
  - **P1 (HIGH) TLS 가드 우회** — `netio.http_request` 가 `verify is False` 만 차단해
    `verify=0`/`""` 로 인증서 검증 비활성이 통과. → falsy 전부 차단(None=기본 위임·truthy=CA 경로만 허용).
  - **P2 (HIGH) IPv6 SSRF 우회** — `::7f00:1`(IPv4-compatible `::/96`)·6to4/teredo 내장 IPv4 가
    미검사로 루프백/IMDS 도달 가능. → `::/96` 차단 + `sixtofour`/`teredo` 내장 IPv4 를 v4 정책으로 재검사.
  - **P3 (MED) dict '키' 미마스킹** — `Redactor.redact`·`events._json_safe` 가 값만 가리고 키는
    통과 → 시크릿이 키로 오면 누출. → 두 경로 모두 문자열 키 마스킹(등록 시크릿 포함 키만 치환, 일반 필드명 보존).
  - **P4 (MED) 비밀번호 검증 크래시** — `verify_password(None)`/`needs_rehash(None)` 가
    AttributeError(DB NULL → 로그인 DoS). 문서화된 "깨끗한 거부"와 모순. → 비문자열 저장값을 크래시 없이 거부(False)/재해시(True).
  - **P5 (MED) 이식성 회귀** — `audit.py` 의 `(?i:...)` 스코프 인라인 플래그가 **Python 3.11+ 전용**
    → <3.11 프로젝트에서 audit import 시 `re.error`(플러그인 이식성 목표와 모순). → 문자클래스로 재작성(3.9+ 동작).
  - **P6 (MED) 게이트 CLI 크래시** — 문서화된 `python -m security` 가 Windows **cp949** 콘솔에서
    `—`(em-dash) 로 `UnicodeEncodeError`. → 비UTF-8 콘솔에서 UTF-8 buffer 폴백(출력 보장).
- **회귀 테스트 6종(20 파라미터 케이스) 추가** — 각 수정을 잠금(자기감사 회피 위해 위반 샘플은 조각 결합/tmp).
- **검증**: 전 테스트 **213 통과**(190→213), `python -m security` 차단 0(경고 3건은 CLI 단독 실행 런타임
  점검 = 정상). cp949 콘솔에서 게이트 CLI 정상 출력 확인.

### 25. 보안 플러그인 커버리지 확장 — SSRF·아카이브·암호·정적점검 11종 + 적대적 검증 (feat/96)

request:
- 추가 개발된 보안 모듈의 커버 영역 **외의 다른 취약점**도 커버할 수 있게, **최신 보안
  가이드라인**을 이용해 추가 대응 모듈 구성.
- 사용법 문서와 **적용 기술 목록·해설 문서**를 security 문서로 남길 것. 커밋·push.

response:
- **가이드라인 리서치**(다중 에이전트 웹 조사): OWASP Top 10:2025(A03 공급망·A10 예외처리
  신설, SSRF→A01), CWE Top 25 2025(SQLi #2·경로탐색 #6·자원무제한 #25 신규), ASVS 5.0.0,
  PEP 706·Trojan Source(CVE-2021-42574)·SSRF/비밀번호 저장 치트시트 확보 → SEC-01~20 계획.
- **신규 모듈 2종**: `crypto.py`(CSPRNG 토큰·상수시간 비교·PBKDF2-HMAC-SHA256 600k 비밀번호
  해시, NFKC 정규화·자기서술 인코딩·needs_rehash), `archive.py`(zip-slip·압축폭탄·심링크 차단
  안전 추출, PEP 706 filter, 스트리밍 바이트 재검증).
- **기존 모듈 확장**: `netio`(SSRF 가드 `assert_url_allowed` — 명시 CIDR 차단 IPv4/6·IMDS,
  호스트명 DNS 해석; 응답 크기 상한 `max_response_bytes`), `redaction`(URL userinfo 마스킹·
  `sanitize_log_value` 로그 인젝션 무력화), `log_filter`(`neutralize_controls` opt-in).
- **정적 점검 7→18종**: credential_material(PEM/벤더 토큰/URL 비번, CRITICAL)·trojan_source·
  sql_injection·unsafe_extract·insecure_file_ops·weak_hash·insecure_random·web_misconfig·
  xml_parsing·cleartext_http·requirements_hygiene 신설 + tls/yaml/dangerous 강화. 런타임 점검 4종.
- **적대적 검증**(다중 에이전트, 차원별 리뷰→독립 검증): 확인된 **16개 결함 전부 수정** —
  SSRF 호스트명 우회(resolve_dns 기본 True), 로그 포맷문자열 %-지정자 훼손(렌더 후 마스킹),
  tar 압축폭탄(next() 스트리밍+압축입력 상한), 정적 점검 우회(shell=True 멀티라인·SQL 내부
  따옴표·yaml 위치 로더·자격증명 라인공유·filter 부분일치·requirements 환경마커·weak_hash
  대문자·userinfo 토큰단독·verify_password 크래시·NFKC 누락 등).
- **검증**: 회귀 테스트 20건 추가 → **전 테스트 174 통과**, `python -m security` 차단 0
  (자기매칭 방지: 패턴은 조각 결합/chr()/이스케이프로 작성).
- **문서**: [docs/security/usage.md](docs/security/usage.md)(사용법) ·
  [docs/security/techniques.md](docs/security/techniques.md)(적용 기술 목록+해설·가이드라인 매핑)
  신규, security.md 위협 모델 확장, README·CLAUDE §20·Share.md 갱신.
- **커버리지 3차 확장(commerce 밖 조사 반영)**: sample 의 auth 백엔드(FastAPI)·notifications·
  dbt·DB IO 를 조사(auth 는 이미 파라미터화 ORM·CSRF·세션·레이트리밋으로 견고) → 플러그인이
  아직 못 잡던 **백엔드/DB IO 취약점 클래스**를 이식 대비로 추가: 신규 `dbio.py`
  (`assert_identifier` 동적 식별자 검증 · `mask_dsn` URL+libpq DSN 비밀번호 마스킹), 정적 점검
  2종(`no_sql_text_injection` SQLAlchemy `text()` 주입 HIGH · `open_redirect_advisory` CWE-601
  MEDIUM). 정적 점검 18→20종. 조치는 전부 **commerce 번들 안**에서 수행(플러그인이 이식원). 검증:
  전 테스트 **190 통과**, `python -m security` 차단 0.

### 24. 통합 보안 플러그인化 — install_security() 원샷 + net/file/API/이벤트 가드 (feat/96)

request:
- 1차: `dags/domains/commerce` 안에서 대응 가능한 **모든 보안이 적용**되게 처리.
- 2차: 전체 프로그램(모든 IT 프로젝트)에서 쓸 수 있는 **통합 보안처리 플러그인**으로 —
  현 폴더 구조를 유지한 채, network IO·file IO·log·stdout·예외처리·API receipt/response 를
  security 코드 **하나의 적용**으로 커버하도록 사전 대응(ready). 추후 common 폴더로 제공 시
  다른 프로젝트는 **받아쓰기만 하면 되는 수준**으로.
- 단, 로그 분석은 가능해야 함 — 취약점을 만들지 않는 경계에서 처리/에러 로그의
  기록·해석·전송이 되게 구조화. 이식 방법과 제공 기능을 정리한 보안 문서 작성.
- 브랜치: `feat/96-security-plugin`.

response:
- **플러그인 신규 모듈 6종** ([include/security/](include/security/), 전부 stdlib only·번들 비종속):
  `bootstrap.py`(**`install_security()` 원샷** — env 시크릿 적재 + 로그/stdout·stderr/
  sys·threading 예외훅 마스킹, idempotent·기동 비차단), `stdio_guard.py`(print·미처리
  트레이스백 마스킹), `netio.py`(`http_request/get/post` — timeout 자동 주입·TLS 검증 비활성
  차단·예외 args 스크럽 후 같은 타입 재전파), `fileio.py`(`safe_key`/`safe_join` 경로 주입
  차단 + `write_json_redacted` at-rest 마스킹 저장), `api_guard.py`(`api_receipt`/
  `response_summary`/`scrub_url·headers·params` — 저장 가능한 무시크릿 요청 영수증·응답 요약),
  `events.py`(`log_event`/`log_exception` — **마스킹된 단일 라인 JSON** 처리/에러 로그, 반환
  dict 는 알림 전송에도 안전 → 분석 가능성 유지). `redaction.py` 에 `scrub_exception`(예외
  체인 args 마스킹) 추가, audit 에 런타임 점검 3종(log/stdout/excepthook 설치 여부) 등록.
- **1차 wiring**: DAG(`commerce_raw.py`)·scripts 2종 → `install_security()`;
  bronze `clients.py` HTTP 호출 → `netio.http_request`(정책 단일점);
  `silver_tasks.build_silver` 경계에 `assert_safe_segment(short)`/`assert_iso_date` 추가.
  기존 redact 지점(마커 error·notify·재시도 로그)은 유지(이중 방어).
- **검증**: 신규 테스트 24케이스 포함 전 테스트 **106 통과**, `python -m security` 차단 0
  (런타임 설치 3종은 CLI 단독 실행에서 warn = 정상, 문서화).
- **문서**: [docs/security/security.md](docs/security/security.md)(위협 모델 11경로·모듈 표·
  로그 분석 경계·검증), [docs/security/adoption.md](docs/security/adoption.md)(받아쓰기 이식
  가이드 + common 승격 계약 + 에이전트 프롬프트), docs/security/README·CLAUDE.md §20·Share.md 갱신.

## 2026-07-02

### 23. DAG 명칭 변경 — commerce_localdata_{elt,recollect} → commerce_{collect,recollect}_raw

request:
- `commerce_localdata_elt` → `commerce_collect_raw`, `commerce_localdata_recollect` →
  `commerce_recollect_raw` 로 DAG 명칭 변경.

response:
- **DAG 파일**: dag_id·TaskFlow 함수명·하단 호출·docstring 의 이름 갱신. 파일명도 정합화 —
  `commerce_localdata.py` → **`commerce_raw.py`**(git mv; 두 raw 수집 DAG 공용, feat/73 선례
  "파일=dag 네이밍" 준수).
- **문서/코드**: 번들 문서 전반의 dag_id 참조 일괄 치환 + 기존 stale 파일 참조
  (`seoul_commerce_dag.py`)를 `commerce_raw.py` 로 정정, `markers.py` docstring 반영.
  change-log 과거 이력은 불변 유지.
- **검증**: 전 테스트 **82 통과**, 보안 게이트 PASS. 컨테이너 `dags reserialize` 후 신규 dag_id 2개가
  `commerce_raw.py` 에서 **import 오류 없이 등록** 확인. 옛 `commerce_localdata_*` 는 파일 삭제로
  메타DB 고아 등록으로 남음(실행 이력 보존, UI 에서 removed 표시 — `airflow dags delete` 는 선택).

### 22. 증분 저장 흐름 확정 — 랜딩→비교(조기중단)→증분→diff 이동(수집일 태깅)

request:
- R2 배치 점검 결과 `run_id=07-01` 이 full 을 그대로 들고 있는 어긋남 지적("위치가 정 반대").
  확정 흐름: 최초 run(06-30)은 비교 대상이 없으니 full 적재 + 그 내용이 diff 폴더에 복사(정렬본+해시).
  이후 run 은 ① API 결과를 run 경로에 **먼저 저장** → ② 정렬 → ③ diff 폴더 파일과 비교하며
  **다른 내용만 별도 위치에 저장**(정렬돼 있으므로 **같은 정보가 위치하면 비교 중단**) →
  ④ 구 diff 삭제 후 **오늘본을 run 경로에서 diff 로 이동**, **완료(이동됨)/중단(안 옮겨짐)을
  파일명의 수집일(연/월/일)로 구분**.
- (합의 Q&A) 증분 위치 = run 폴더 안(`run_id=X/<short>.jsonl`), full 랜딩 = `run_id=X/_full/`.
  이력 재정리 = run_id=07-01 full 을 06-30 대비 증분으로 교체(권장안 채택).

response:
- **paths.py**: `bronze_full_landing_key`(`_full/` 랜딩), diff-target 키를 **수집일 태깅**
  (`_diff_target/<short>.<YYYY-MM-DD>.jsonl` + 같은 이름 `.key`)으로 변경, `run_collect_date`·
  `diff_target_prefix`(발견용 접두) 추가.
- **incremental.py**: `diff_new_rows(stop_on_aligned_match=)` — 정렬 프런티어에서 같은 정보(키+내용)
  첫 일치 시 **비교 중단**(전제: 내용 변경 시 UPDATEDT 갱신). `find_diff_target` — `<short>.` 접두
  나열로 최신 날짜 diff 발견(구형 무날짜 인식). `incremental_store` 재작성: ①landing 업로드(수집분
  보존 우선) → ②비교 → ③증분만 run 폴더 저장(첫 수집=full) → ④새 날짜 diff copy→구 날짜 삭제→
  landing 삭제(**이동**; 새 파일 먼저 만들어 중단 시 자가 복구, 동일자 재실행 자기삭제 가드).
  identical 이어도 ④ 수행(diff 파일명 날짜=최신 완료 수집일).
- **bronze_tasks.py**: 수집일(run_id 파생, 비형식이면 observed_date 폴백)·prev 발견 배선, 마커에
  `diff_target_key` 추가. **storage.py**: `copy()` 재추가(ABC read+write, R2 `copy_object` 서버사이드).
- **이력 재정리**: `scripts/retrofit_run_increment.py` — run_id=07-01 full 을 06-30 대비 증분으로
  교체 + 구형 무날짜 diff 파일을 `<short>.2026-07-01.*` 로 리네임(dry-run 기본·`--apply`).
  `scripts/seed_diff_target.py` 도 수집일 태깅으로 갱신.
- **검증**: 단위테스트 확장(조기중단 소비량·발견 최신날짜/구형·랜딩 이동·동일자 재실행 가드·시드→
  identical) 포함 **전 82 통과**. 설계 문서 §1/3/4 갱신. 보안 게이트 PASS.
- **R2 재정리 실행 완료(실측)**: run_id=07-01 데이터 **1,248MB(39 full) → 0.48MB(증분 23파일)**
  — 무변경 16종은 파일 삭제(identical=마커만), 변경 23종만 증분(예: general_restaurant 534,748행→
  280행/293KB, bakery 12행 — 사전 실증치와 일치). `_diff_target` **78/78 전부 날짜 태깅**
  (`<short>.2026-07-01.*`, 무날짜 잔존 0). run_id=06-30 full(최초)·`_backup` 무손상.
  총 용량 5,195MB → **3,947MB**(약 1.25GB 회수).

### 21. 원천 레이어 리네임 bronze/commerce → raw/commerce + 레이어 접두 .env 관리

request:
- R2 데이터가 이미 `bronze/commerce/` → `raw/commerce/` 로 이동된 상태(`move_bronze_to_raw` 실행 —
  실측: bronze/commerce=0객체, raw/commerce=314객체[run 스냅샷·`_diff_target`·`_backup`])인데 **코드는
  아직 `bronze/commerce`** 를 써서 DAG 이 이동 데이터·시드 diff-target 을 못 읽는 어긋남 발생.
- (결정) **raw/commerce 로 통일**하고, 레이어 접두 값을 **.env 로 관리**하도록 변경.
- (팀 결정 #75) R2 오브젝트 원본 경로는 `raw/` 채택, Iceberg 테이블명·dag_id 의 `bronze` 명칭은
  유지 (용어: **raw = R2 랜딩 원본, bronze = Iceberg 웨어하우스 원본층**).

response:
- **paths.py**: `BRONZE_LAYER="bronze/commerce"` → `RAW_LAYER=os.getenv("COMMERCE_RAW_LAYER","raw/commerce")`,
  `SILVER_LAYER=os.getenv("COMMERCE_SILVER_LAYER","silver/commerce")`. 모든 경로 함수가 `RAW_LAYER` 사용.
  기본값 = 목표 경로라 env 미설정이어도 raw/commerce. (함수명 `bronze_*` 는 대규모 리팩터 회피 위해
  유지 — 경로 문자열만 raw 로 전환.)
- **.env.commerce(.example)**: `COMMERCE_RAW_LAYER=raw/commerce` · `COMMERCE_SILVER_LAYER=silver/commerce`
  추가(관련 값 .env 관리), 경로 주석 raw/commerce 로 갱신.
- **문서 7종**: `bronze/commerce` 경로 리터럴 → `raw/commerce`(README·storage·common_info·configuration·
  recollect·incremental-sort-diff·deploy-prod). change-log 과거 이력은 불변 유지.
- **scripts 정리**: 1회성 다 쓴 것 삭제(`prune_duplicate_runs`·`backup_diff_target`·`move_bronze_to_raw`
  — 뒤 둘은 없어진 `storage.copy` 의존), 재사용 가치 있는 `seed_diff_target.py`(step0)만 존치.
  `.airflowignore` 에 `scripts/**` 추가(파서 제외).
- **검증**: 전 테스트 **79 통과**(경로 단정 raw/commerce 로 갱신), 보안 게이트 PASS. 기능 확인 —
  `paths.bronze_diff_target_key`=`raw/commerce/_diff_target/…` 가 R2 이동본과 정합(실존 확인),
  `COMMERCE_RAW_LAYER` override 반영 확인 → **DAG 이 이동 데이터·시드 diff-target 을 그대로 사용.**
- **⚠ 배포 순서(#75 계약)**: 마커 조회(`markers.py`)·diff-target 경로가 `RAW_LAYER` 에서 파생 —
  **데이터 이관 완료 후 배포 필수**. 이관 전 배포 시 과거 run 미인식 → 전량 재수집·동일자 제외 계약 공백.
- **후속(선택)**: 함수/변수명·docstring·문서의 conceptual "bronze" 용어를 raw 로 통일(대규모 리네임은 별도).

### 20. DAG 네이밍 통합 — seoul_commerce_daily/recollect → commerce_localdata_elt/recollect (feat/73-dag-naming)
request:
- 팀 공통 DAG 네이밍 규칙 `<domain>_<dataset>_<stage>` 확정(#73): stage 역할형(elt/recollect 등),
  commerce dataset=localdata, 파일명=dag_id(밀접한 DAG 쌍은 공통 접두 파일명).
response:
- dag_id: `seoul_commerce_daily` → `commerce_localdata_elt`, `seoul_commerce_recollect` →
  `commerce_localdata_recollect`. 파일 `seoul_commerce_dag.py` → `commerce_localdata.py`(두 DAG 공존).
- 번들 docs/README의 dag_id 참조 일괄 갱신. 스토리지 경로·마커 계약은 dag_id와 무관하므로 변경 없음.
- 옛 dag_id 실행 이력은 Airflow 메타DB에 보존(삭제 안 함), 신규 id로 새로 시작.

### 19. 인증키 env-var 계약 변경 — SEOUL_OPENAPI_KEY → SEOUL_API_KEY_COMM, 루트 .env 로 이관 (feat/70-env-key-unification)
request:
- 도메인별 서울 API 키 환경변수를 `SEOUL_API_KEY_<도메인약어>` 규칙으로 통합(#70). commerce 는
  `SEOUL_OPENAPI_KEY` → `SEOUL_API_KEY_COMM`.
- commerce 인증키는 `.env.commerce` 가 아니라 **호스트 루트 `.env` 로 이관**한다(번들 자립 의도의
  부분 폐기 — 사용자 승인). `SEOUL_OPENAPI_BASE_URL` 은 `settings.py` 기본값과 동일하므로
  `.env.commerce` 에서 삭제.
- 배경: 루트 `.env` 의 culture 키가 같은 이름(`SEOUL_OPENAPI_KEY`)이라 setdefault 로더 특성상
  commerce 가 culture 키로 호출하던 충돌 해소.
response:
- `settings.py` 가 `SEOUL_API_KEY_COMM` 을 읽도록 변경(내부 필드명 `seoul_openapi_key` 유지).
  `clients.py`/`resolve.py` 오류 메시지, `test_security.py`, 번들 docs/README/deploy 문서 일괄 반영.
- `.env.commerce`/`.env.commerce.example` 에서 인증키·BASE_URL 제거 + 이관 안내 주석.
  키 입력 위치 안내를 "루트 `.env`" 로 수정(configuration.md gap 표 포함).
- 신규 이름은 `KEY` 포함 → security 자동 마스킹(`_SECRET_NAME_RE`) 유지 확인.

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
- **docs**: [docs/pipeline/raw/incremental-sort-diff.md](docs/pipeline/raw/incremental-sort-diff.md)
  (모델·정렬·검증키·diff·수집흐름·step0·검증). 단위테스트 **14 통과**.
- **라이브 end-to-end 검증 완료(실 Seoul API, 격리 프리픽스 `_verify58`)**: run1=first(row-NDJSON 확인:
  MGTNO 있음/LOCALDATA 봉투 아님) + diff-target 생성 → run2 동일 데이터=identical(증분 파일 미생성) →
  변경분=changed(변경 업장1 + 신규행만 증분, diff-target 3행으로 롤링). **이력 보존 확인**: 이전 run 증분
  유지 + 같은 업장(mgtno)의 **원본('태평')·변경('태평_CHG') 두 버전 공존 → 이력 추적 가능(True)**.
  검증 후 `_verify58` 6객체 전량 삭제(실 bronze 무오염). **조치 필요 없음**(정상 동작).
- **사이드 이펙트 분석/대응**: 기존 bronze=page-NDJSON, 신규=row-NDJSON → **형식 혼재**(이력 손실 아님 —
  구 run 보존 + 신규 run 은 증분). 실운영 첫 수집은 `_diff_target` 미존재라 mode=first 로 전체 저장
  (자가 시드; step0 로 사전 시드하면 첫 수집부터 diff). **다운스트림(dbt 로더) row-NDJSON 대응은 feat/58 밖**.
- 커밋·푸시(feat/58). (부수) CLAUDE.md 영어 통일 + Change Log Rule 에 request:/response: 규격 명시(별도 커밋).

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
