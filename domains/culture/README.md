# culture 도메인 — bronze 원본 적재

문화(culture) 도메인의 원본 소스(**KOPIS** + **서울 열린데이터광장**, 12개 데이터셋)를
Cloudflare R2에 그대로 적재하고, 조회 가능한 bronze Iceberg 테이블까지 만드는 Airflow
파이프라인입니다. 모든 코드·문서가 `domains/culture/` 한 트리에 자기완결로 모여 있습니다.

## 파이프라인 한눈에

```text
   KOPIS API (XML)          서울 열린데이터 (JSON)
        │                         │
        └───────────┬─────────────┘
                    ▼
     culture_bronze DAG                   (Airflow · @daily)
     plan → ingest_dataset(×12 동적매핑) → report
                    │
        ┌───────────┴────────────┐
        ▼                        ▼
   R2 raw (원본 보존)        bronze Iceberg (Trino · 선택)
   raw/culture/…             iceberg[_dev].culture.bronze_*
                    │
                    ▼
        ASAC-DBT: silver → gold     (다른 레포 · dbt)
```

## 문서 (docs/)

| 문서 | 내용 | 이런 걸 찾으면 |
|---|---|---|
| [docs/README.md](docs/README.md) | 문서 인덱스 · 목적별 내비 | 어디부터 볼지 |
| [docs/architecture.md](docs/architecture.md) | 오케스트레이션 전략 + 코드 지도 | DAG가 어떻게 도는지 / 코드 어디에 뭐가 |
| [docs/sources.md](docs/sources.md) | 12데이터셋 카탈로그 · 소스 API · 좌표/CRS | 어떤 데이터를 어디서 |
| [docs/storage.md](docs/storage.md) | R2 파티션 · bronze 스키마(다운스트림 계약) | dbt로 소비하려면 |
| [docs/reliability.md](docs/reliability.md) | 수집 계약 v0 · run_report SLO | 무엇이 깨지고 어떻게 아나 |
| [docs/operations.md](docs/operations.md) | 트리거·재수집·디버깅·env | 돌리거나 고장났을 때 |
| [change-log.md](change-log.md) | 설계영향 변경 로그 | 왜 이렇게 바뀌었나 |

## 폴더 구조

```
domains/culture/
├─ culture_bronze.py               # ⭐ Airflow DAG 엔트리 (유일하게 스캔되는 파일)
├─ culture_ingest/                 # 임포트 전용 패키지 (DAG 스캔 제외, import만)
│  ├─ common/                      #   도메인 무관 공통 프레임워크
│  │  └─ config.py · http.py · landing.py · checks.py · warehouse.py
│  └─ source/                      #   culture 소스 계층 (이 도메인 전용)
│     └─ config.py · clients.py · datasets.py · ingest.py
├─ scripts/run_culture_ingest.py   # 로컬 실행 CLI
├─ docs/                           # 상세 설계 문서 (위 표)
├─ change-log.md
├─ README.md                       # (이 문서)
└─ .airflowignore
```
> 각 파일의 역할은 [docs/architecture.md](docs/architecture.md) 참조.

## 빠른 시작

```bash
# domains/culture/ 에서 — 로컬 dry-run (R2 안 씀)
python scripts/run_culture_ingest.py --dry-run --local-dir ./_dryrun \
    --env-file ../../../sample/.env --date-from 20260601 --date-to 20260628
```
> 전체 실행·트리거·재수집·env는 [docs/operations.md](docs/operations.md).

## 경계 (이 도메인이 하는 / 안 하는 것)

- ✅ **한다**: 원본 수집 → R2 raw 보존 → bronze Iceberg 적재 → 수집 계약 v0 검증.
- ❌ **안 한다**: silver/gold 변환(파싱·타입화·dedup·집계)은 **ASAC-DBT(dbt)** 의 몫. 이 레포는 bronze까지.
- 🔒 **쓰기 범위**: `raw/culture/`(R2) · `iceberg[_dev].culture.bronze_*`(Trino)만. 공유 인프라·타 도메인 스키마는 건드리지 않음.
