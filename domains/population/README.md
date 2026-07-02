# population 도메인 — bronze 원본 적재

서울시 **실시간 도시데이터 인구혼잡도**(`citydata_ppltn`, 121개 장소)를 5분마다 수집해
Cloudflare R2의 `raw/population/` 경로에 원본으로 적재하고, Iceberg bronze 테이블에
**원본 payload + 추적 메타데이터**로 넣는 Airflow 파이프라인입니다. 이 도메인 코드는
레포의 도메인별 디렉토리 규칙(이슈 #16)에 맞춰 `domains/population/` 한 트리에
자기완결로 모여 있습니다.

## 폴더 구조와 역할

```
domains/population/
├─ seoul_ppltn_collect.py         # ⭐ Airflow DAG 엔트리 (여기서 유일하게 스캔되는 파일)
├─ ppltn_ingest/                  # import 전용 패키지 (DAG 스캔 제외, import만)
│  ├─ common/                     #   도메인 무관 얇은 helper (이슈 #16)
│  │  ├─ config.py                #     R2/env(dev·prod)·RunContext·redact_secret·raw 경로 규칙
│  │  ├─ http.py                  #     낮은 수준 HTTP GET (urllib) + 결과 dataclass
│  │  ├─ trino.py                 #     Trino 연결·카탈로그/스키마·SQL 식별자 검증
│  │  ├─ bronze.py                #     ★ payload+메타데이터 bronze DDL/INSERT (멱등)
│  │  └─ landing.py               #     R2/로컬 raw 적재 싱크
│  └─ source/                     #   서울 citydata_ppltn 소스 전용
│     ├─ config.py                #     적재 루트(raw/population)·source_id·API 키
│     ├─ client.py                #     citydata_ppltn 호출 (원본 bytes만, 파싱 X)
│     ├─ areas.py                 #     121개 장소(AREA_NM) 레지스트리
│     └─ ingest.py                #     오케스트레이션 fetch→R2 raw→bronze insert + 리포트
├─ scripts/
│  └─ run_ppltn_ingest.py         # 로컬 실행용 CLI (dry-run / 실제 적재)
├─ docs/
│  └─ bronze-metadata.md          # bronze 메타데이터 최소기준 & 이유 (이슈 #16 근거)
├─ .airflowignore                 # DAG 파일만 스캔, 패키지는 import만 되게 제외
└─ README.md                      # (이 문서)
```

Airflow는 dags 폴더를 재귀적으로 스캔하므로 이 DAG는
`domains/population/seoul_ppltn_collect.py`에서 자동 인식됩니다. `ppltn_ingest/`와
`scripts/`는 `.airflowignore`로 **DAG 스캔에서는 제외**되지만 import은 됩니다 — DAG가
자기 디렉토리를 `sys.path`에 넣고 `ppltn_ingest.*`를 불러옵니다.

## bronze 설계: schema-on-read (원본 payload)

DAG는 응답을 필드로 **분해하지 않습니다.** 원본 레코드 JSON을 `payload` 컬럼에 통째로
넣고 추적 메타데이터만 남깁니다. 개별 필드(area_nm, congest_lvl 등) 분해는 후속
**silver(dbt)** 가 `json_extract`로 합니다. 자세한 이유는
[docs/bronze-metadata.md](docs/bronze-metadata.md) 참고.

- 이점: API 필드가 바뀌어도 bronze가 안 깨지고(COLUMN_NOT_FOUND 방지), 파싱 규칙을
  SQL(dbt)로 버전 관리하며, 원본이 payload로 보존돼 재처리(replay)가 가능.

## 시크릿(인증키)

DAG는 키를 **환경변수에서만** 읽습니다. `docker-compose`가 `sample/.env`를 (`env_file:`)
모든 Airflow 컨테이너에 주입하므로 `SEOUL_API_KEY_PPLT`, `R2_DEV_*`가 `os.environ`으로 들어옵니다.
**키는 절대 커밋하지 않습니다** — `.env`는 `sample` 상위 레포에서 gitignore 처리됩니다.
API key가 요청 URL 경로에 포함되므로, 예외 로깅 시 `redact_secret`으로 마스킹합니다.

## R2 적재 경로 (bronze 원본, 이슈 #16 규칙)

```
raw/population/seoul_ppltn/load_date=<KST날짜>/<ingest_ts>_<request_id>.json
```
`ingest_ts`가 실행 1회를 격리하고, bronze 테이블 적재는 같은 `ingest_ts` 파티션을
delete-then-insert로 멱등 처리하므로 재시도가 중복을 만들지 않습니다.

run마다 정량 리포트도 남깁니다(일배치 리포트/알림 DAG의 소스):
```
raw/population/_reports/load_date=<KST>/ingest_ts=<UTC>/run_report.json
```

## 로컬 실행

```bash
# domains/population/ 에서 — 로컬 디렉토리로 dry-run (R2/Trino 안 씀, 5개 장소만)
python scripts/run_ppltn_ingest.py --dry-run --local-dir ./_dryrun \
    --env-file ../../../sample/.env --max-areas 5

# seoul-dev 버킷 + iceberg_dev bronze 실제 적재
python scripts/run_ppltn_ingest.py --target dev --env-file ../../../sample/.env
```
