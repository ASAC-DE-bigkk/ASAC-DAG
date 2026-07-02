# commerce — 서울 인허가(LOCALDATA) 수집·가공 번들

서울 열린데이터광장 **인허가(LOCALDATA) 39종**을 `commerce` 도메인으로 수집·가공하는
Airflow 카테고리 번들이다. **서빙 DB·외부 매니페스트 없이** run_id 폴더의 마커로 수집 결과·이력을
관리한다. 코드·설정·테스트·문서·규약·런타임 인자(`.env.commerce`)가 **이 폴더 안에 자립**한다.

- **위치**: `dags/domains/commerce/` (`dags/` 는 ASAC-DAG git 서브모듈)
- **Orchestration**: 호스트의 Apache Airflow(`elt-infra` compose, **LocalExecutor**) + Postgres(메타DB)
- **Storage**: 컨테이너 로컬 볼륨 ↔ Cloudflare R2 — `STORAGE_BACKEND` 로 전환
- **No serving DB·No 외부 매니페스트**: 상태/이력은 **run_id 폴더의 마커**(completed/incomplete)

> 작업 경계: commerce 변경은 이 폴더 안에서만. 호스트의 루트 `.env`·`docker-compose.yml`·
> `Dockerfile.airflow` 는 번들 밖이므로 건드리지 않는다. 필요한 환경변수는 루트 `.env` 가
> 아니라 **이 번들의 `.env.commerce`** 로 공급한다 → [docs/configuration.md](docs/configuration/configuration.md).

## 데이터 흐름

```text
bronze  {prefix}/raw/commerce/<YYYY>/<MM>/<DD>/run_id=<YYYY-MM-DD_HHMMSS_mmm>/<short>.jsonl   # API당 1파일(원본 페이지 NDJSON)
state   .../run_id=<...>/_markers/<short>.completed|.incomplete + _RUN.*                        # 수집 결과 마커(DB·매니페스트 대체)
silver  {prefix}/silver/commerce/<short>/observed_date=YYYY-MM-DD/part-000.parquet  # 공통 19컬럼 정규화 (로직만 보존·DAG 미와이어링)
```

- **현 DAG 라인은 bronze(원본 수집) 전용**이다. silver 가공 로직은 [include/silver/](include/silver/)
  에 보존되어 있으나 DAG 오케스트레이션에서 분리되어 있다(별도 silver DAG 없음).
- **전체 순회 + 완전성 점검**: 1회 ≤1000건(`SEOUL_PAGE_SIZE`)씩 마지막 페이지까지 순회하고
  수집건수 == `list_total_count` 를 확인해야 `completed` 마커. 불완전은 `incomplete` 마커.
- **DAG 실행 1회 = run_id 폴더 1개**, bronze 는 그 폴더 안에서만 파일 생성.
- **중복/재수집**: 매 실행 전체 수집(스킵 없음) → **중복 제거는 silver 가 `MGTNO` 로**(silver 로직 기준).

## 환경변수 (정상 동작 조건)

commerce 가 읽는 모든 인자는 [docs/configuration.md](docs/configuration/configuration.md) 에 정리돼 있다.
인증키 `SEOUL_API_KEY_COMM` 은 **호스트 루트 `.env`** 에 있다(ASAC-DAG#70 에서 도메인 공통
`SEOUL_API_KEY_<도메인>` 규칙으로 이관). 나머지 commerce 전용 값은 이 번들의 `.env.commerce` 가 채운다:

```bash
cd dags/domains/commerce
cp .env.commerce.example .env.commerce      # PowerShell: Copy-Item
# 인증키는 루트 .env 의 SEOUL_API_KEY_COMM(필수). R2 쓰면 STORAGE_BACKEND=r2 + R2_* 확인.
```

DAG 임포트 시 [include/common/env.py](include/common/env.py) 의 `load_commerce_env()` 가
`.env.commerce` 를 `os.environ` 에 채운다(프로세스/compose env 가 우선, 빈 값만 setdefault).
값에는 `${VAR}` 참조를 쓸 수 있어 **루트 `.env` 와 겹치는 R2 값은 중복 없이 불러온다**.

| 변수 | 기본 | 비고 |
|---|---|---|
| `SEOUL_API_KEY_COMM` | (없음) | **필수** — 루트 `.env` 에서 주입(#70). 없으면 수집 불가 |
| `STORAGE_BACKEND` | `local` | `local` \| `r2` |
| `R2_ENDPOINT` / `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` | `${R2_DEV_*}` | r2 일 때 — 루트 `.env` 값을 참조 |
| `R2_BUCKET` | `${R2_DEV_BUCKET_NAME}` | 루트는 `R2_DEV_BUCKET_NAME`/`R2_BUCKET_NAME`, commerce 는 `R2_BUCKET` — 참조로 매핑 |
| `SEOUL_PAGE_SIZE` / `SEOUL_MAX_PAGES` | `1000` / (없음) | 페이지 크기 / 비우면 무제한(끝까지 순회) |

> **보안**: 시크릿(인증키·R2 자격증명)이 로그·예외·마커(at-rest)·알림으로 새지 않게 마스킹하고,
> 흔한 취약 패턴을 정적 점검한다. 단일 종합검증: `PYTHONPATH=…/include python -m security`.
> 위협 모델·처리 로직: [docs/security/security.md](docs/security/security.md), 코드: [include/security/](include/security/).

## 빠른 시작

호스트 Airflow 스택(`elt-infra`)을 띄운다(루트에서, 루트 `.env` 사용):

```bash
docker compose up -d                # 루트의 docker-compose.yml (postgres/trino/airflow×4)
# UI: http://localhost:30585
# DAG: commerce_collect_raw(전체 수집) · commerce_recollect_raw(미완료만 6h 재수집) — UI 에서 토글 ON
# Grid/Graph 에서 ingest_one[<API>] 매핑으로 API별 성공/실패/대기 확인(map_index 라벨)
```

> 컴포즈는 `./dags` 를 마운트하므로 이 번들의 `.env.commerce` 도 컨테이너에서 보이고,
> DAG 가 임포트될 때 자동 적재된다. silver(parquet)·R2 백엔드(`boto3`)에 필요한
> `boto3`/`pandas`/`pyarrow` 는 호스트 이미지에 **이미 포함**되어 추가 설치 없이 동작한다
> ([requirements.txt](requirements.txt) 는 명세용). R2 적재는 `STORAGE_BACKEND=r2` 면 끝.

### backfill / 재수집

```bash
docker compose exec airflow-scheduler airflow dags trigger commerce_collect_raw   # 매 실행이 전체 수집
docker compose exec airflow-scheduler airflow dags trigger commerce_collect_raw -c '{"observed_date":"2026-06-01"}'
docker compose exec airflow-scheduler airflow dags backfill commerce_collect_raw -s 2026-06-01 -e 2026-06-07
```

## 프로젝트 구조

`dags/domains/<category>/` 가 자립 단위 — DAG·코드(include)·설정·테스트·**문서·규약·런타임 인자**가
한 폴더 안에 있다. DAG 가 자기 `include` 를 sys.path 에 부트스트랩하고 `.env.commerce` 를 적재하므로
**`dags/` 만 다른 Airflow 프로젝트로 옮겨도 바로 실행**된다. 진입점: [Share.md](Share.md) ·
규약: [docs/project_setting.md](docs/architecture/project_setting.md).

```text
dags/
└─ domains/
   └─ commerce/                  # ★ 카테고리 자립 단위
      ├─ commerce_raw.py     # DAG: commerce_collect_raw · commerce_recollect_raw (sys.path+env 부트스트랩)
      ├─ include/                 # PYTHONPATH 루트 (common/bronze/silver 가 top-level)
      │  ├─ common/               # settings · env · storage · paths · schemas · hashing · registry · notify(알림 IF)
      │  ├─ bronze/               # clients · validators · bronze_tasks(NDJSON+마커) · markers(재수집) · resolve
      │  └─ silver/               # validators · silver_tasks
      ├─ config/                  # dataset_registry.yaml(인허가 39종) · non_license_datasets.yaml(격리, 로드 X)
      ├─ tests/                   # test_clients · test_bronze_tasks · test_silver_tasks · test_markers · test_notify
      ├─ docs/                    # 주제별 폴더: architecture/ · configuration/ · operations/ · pipeline/(+bronze/)
      ├─ requirements.txt         # 번들 런타임 의존성 명세(boto3/pandas/pyarrow 등 — 이미지 기본 포함)
      ├─ .env.commerce(.example)  # 런타임 환경변수(실파일은 gitignore)
      ├─ README.md                # ← 이 파일(번들 개요)
      ├─ CLAUDE.md                # 작업 경계 + 데이터 엔지니어링 원칙 + §19 구조 규약
      ├─ Share.md                 # 공유 자료 인덱스(컨텍스트 분리 진입점)
      └─ .airflowignore           # include/ config/ tests/ docs/ 파싱 제외
```

> 호스트 프로젝트 루트(`dags/` 밖, 별도 리포)에 `docker-compose.yml`·`Dockerfile.airflow`·
> 루트 `.env` 가 있다. 이 번들은 그 위에서 동작하되, 자기 인자는 `.env.commerce` 로 자립한다.

- 공통 컬럼·`UPDATEDT` 검증·전체 카탈로그·서비스명 채우는 법: [docs/common_info.md](docs/pipeline/common_info.md)
- 미해석 service_name 채우기: `docker compose exec airflow-scheduler python -m bronze.resolve verify`

## 테스트

```bash
PYTHONPATH=dags/domains/commerce/include python -m pytest dags/domains/commerce/tests -q   # clients / bronze / silver
```
