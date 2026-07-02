# culture 도메인 — bronze 원본 적재

문화(culture) 도메인의 원본 소스 데이터(**KOPIS** + **서울 열린데이터광장**)를
Cloudflare R2의 `bronze/culture/` 경로에 그대로 적재하는 Airflow 파이프라인입니다.
이 도메인에 필요한 모든 코드는 레포의 **도메인별 디렉토리 규칙**에 맞춰
`domains/culture/` 한 트리 안에 자기완결로 모여 있습니다.

## 폴더 구조와 역할

```
domains/culture/
├─ culture_bronze.py        # ⭐ Airflow DAG 엔트리 (여기서 유일하게 스캔되는 파일)
├─ culture_ingest/                 # 임포트 전용 패키지 (DAG 스캔 제외, import만)
│  ├─ common/                      #   도메인 무관 공통 프레임워크
│  │  ├─ config.py                 #     R2 접속정보 · 실행 컨텍스트(RunContext) · 파티션 경로 규칙
│  │  ├─ http.py                   #     재시도 붙은 HTTP 세션 · 페이지(Page) 자료구조
│  │  ├─ landing.py                #     적재 싱크(R2/로컬) · 페이지/매니페스트 기록 · 결과(DatasetResult)
│  │  └─ checks.py                 #     수집 검증(완전성·드리프트·freshness) — 계약 v0
│  └─ source/                      #   culture 소스 계층 (이 도메인 전용)
│     ├─ config.py                 #     적재 루트(LANDING_ROOT) · 소스 API 키 로딩
│     ├─ clients.py                #     KOPIS(XML) · 서울 열린데이터(JSON) HTTP 클라이언트
│     ├─ datasets.py               #     12개 데이터셋 레지스트리 (단일 진실 원천)
│     └─ ingest.py                 #     적재 오케스트레이션 (run_batch / ingest_one)
├─ scripts/
│  └─ run_culture_ingest.py        # 로컬 실행용 CLI (dry-run / R2 적재)
├─ .airflowignore                  # DAG 파일만 스캔, 패키지는 import만 되게 제외
└─ README.md                       # (이 문서)
```

### 각 파일 역할 한눈에

| 파일 | 역할 |
|------|------|
| `culture_bronze.py` | 일배치 DAG. `plan → ingest_dataset(데이터셋별 동적 매핑) → report` 흐름. 데이터셋 하나가 실패해도 격리·재시도. |
| `common/config.py` | R2 접속정보 해석(dev→`R2_DEV_*`, prod→`R2_*`), `.env` 파싱, 실행 1회를 식별하는 `RunContext`, 객체 키 prefix 생성. |
| `common/http.py` | 429/5xx에 자동 재시도하는 `requests` 세션, 소스 무관 페이지 단위 응답 묶음 `Page`. |
| `common/landing.py` | 받아온 페이지를 R2(또는 로컬)에 쓰는 싱크, 실행마다 `_manifest.json` 기록, 적재 결과 집계 `DatasetResult`. |
| `source/config.py` | 적재 루트 `bronze/culture`, KOPIS/서울 API 키 환경변수 로딩·검증. |
| `source/clients.py` | KOPIS(XML, 페이징/상세/예매상황판)와 서울 열린데이터(JSON, 1000행 윈도우) 호출. **원본 bytes만 받아오고 파싱은 안 함**(파싱은 후속 dbt 몫). |
| `source/datasets.py` | 채택한 12개 데이터셋을 한 줄씩 정의한 레지스트리. 데이터셋 추가 = 여기 한 줄 추가, 코드 변경 없음. |
| `common/checks.py` | 적재 직후 **수집 계약 v0** 검증: 완전성(min_rows)·드리프트(key_fields)·freshness. 결과를 매니페스트 `checks` 블록에 동봉. |
| `source/ingest.py` | 데이터셋 1개 → 원본 객체 + 매니페스트 적재 + 검증. 정량 **run 리포트** 빌드/적재. CLI용 `run_batch`·DAG용 `ingest_one` 진입점. |
| `scripts/run_culture_ingest.py` | Airflow 없이 로컬에서 dry-run 또는 실제 R2 적재를 돌리는 CLI. |

Airflow는 dags 폴더를 재귀적으로 스캔하므로 이 DAG는
`domains/culture/culture_bronze.py`에서 자동 인식됩니다.
`culture_ingest/`와 `scripts/`는 `.airflowignore`로 **DAG 스캔에서는 제외**되지만
import은 됩니다 — DAG가 자기 디렉토리(`domains/culture/`)를 `sys.path`에 넣고
`culture_ingest.*`를 불러옵니다.

## 시크릿(인증키)

DAG는 키를 **환경변수에서만** 읽습니다. `docker-compose`가 `sample/.env`를
(`env_file:`로) 모든 Airflow 컨테이너에 주입하므로 `KOPIS_SERVICE_KEY`,
`SEOUL_API_KEY_CULT`, `R2_DEV_*`가 `os.environ`으로 들어옵니다.
**키는 절대 커밋하지 않습니다** — `.env`는 `sample` 상위 레포에서 gitignore 처리됩니다.

## R2 적재 경로 (bronze 원본)

```
bronze/culture/<소스>/<데이터셋>/load_date=<KST날짜>/ingest_ts=<UTC시각>/page-NNNN.<xml|json>
                                                                       /_manifest.json
```
`ingest_ts`가 실행 1회를 격리하므로, 재시도나 중단된 부분 실행이
이전 데이터를 덮어쓰거나 오염시키지 않습니다.

추가로 run마다 정량 **신뢰성 리포트**를 남깁니다:
```
bronze/culture/_reports/load_date=<KST>/ingest_ts=<UTC>/run_report.json
```

## 수집 신뢰성 (계약 v0 · 검증 · 정량 리포트)

계획안의 "조용히 깨진다"(잘못된 값도 정상처럼 보인다)를 방어하기 위해, bronze
단계에서 dbt/Trino 없이 다음을 강제합니다.

- **수집 계약 v0 (코드로)** — `datasets.py` 레지스트리의 데이터셋마다
  `min_rows`(완전성 하한)·`freshness_sla_hours`(신선도 목표)·`key_fields`(필수 필드)를 선언.
- **적재 직후 검증** (`common/checks.py`) — 완전성(행 수 ≥ min_rows)·드리프트(원본
  레코드에 `key_fields` 존재)·freshness(적재 시각이 SLA 이내). 결과는 각 데이터셋
  `_manifest.json`의 `checks` 블록과 관측 필드(`observed_fields`)로 기록.
- **정량 run 리포트** — 커버리지(landed/expected %)·총 행수·위반 목록·freshness·`slo_passed`를
  `run_report.json`으로 적재 + 로그로 surface. "깨지면 빨리, 무엇이 영향인지" 숫자로 답함.
- **런타임 게이트(opt-in)** — DAG 파라미터 `fail_on_violation=True`면 계약 위반 시 run을
  실패시킴(기본 off — 계약 v0 안정화 전 거짓 경보 회피). 수집 자체 실패는 항상 빨갛게 실패.

## 로컬 실행

```bash
# domains/culture/ 에서 실행 — 로컬 디렉토리로 dry-run (R2 안 씀)
python scripts/run_culture_ingest.py --dry-run --local-dir ./_dryrun \
    --env-file ../../../sample/.env --date-from 20260601 --date-to 20260628

# seoul-dev 버킷에 실제 적재
python scripts/run_culture_ingest.py --target dev --env-file ../../../sample/.env \
    --date-from 20260101 --date-to 20261231 --include-detail --max-detail 200
```
