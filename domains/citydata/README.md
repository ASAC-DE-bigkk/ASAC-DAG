# citydata 도메인 — 서울 실시간 도시데이터 통합 수집·변환

서울시 **실시간 도시데이터 통합 API**(`citydata`, 121개 장소)를 **5분마다 병렬로** 수집하는
**단일 수집원** 도메인입니다. 한 응답에 인구·상권(신한카드)·지하철/버스 승하차·따릉이·
날씨/대기질 등 ~15개 블록이 담기며, 이를 raw(R2)·bronze(Iceberg)로 적재한 뒤 dbt 로
도메인별 silver/gold 마트로 파생합니다. **인구(seoul_ppltn) 마트도 이제 이 citydata
번들의 `LIVE_PPLTN_STTS` 블록에서 파생**합니다(별도 인구 전용 수집 파이프라인 은퇴).

이 도메인 코드는 레포의 도메인별 디렉토리 규칙(이슈 #16)에 맞춰 `domains/citydata/`
한 트리에 자기완결로 모여 있습니다.

## 폴더 구조와 역할

```
domains/citydata/
├─ citydata_bronze.py          # ⭐ 수집 DAG (5분 병렬) — 통합 API → R2 raw(gzip) + 블록 bronze
├─ citydata_transform.py       # ⭐ 변환 DAG (5분) — dbt 로 인구+citydata silver/gold 빌드·테스트
├─ citydata_maintenance.py     # ⭐ 유지보수 DAG (주간) — Iceberg optimize/expire + R2 metadata/고아 정리
├─ citydata_ingest/            # import 전용 패키지 (DAG 스캔 제외, import만)
│  ├─ common/                  #   도메인 무관 얇은 helper (이슈 #16)
│  │  ├─ config.py             #     R2/env(dev·prod)·RunContext·redact_secret·raw 경로 규칙
│  │  ├─ http.py               #     낮은 수준 HTTP GET (공통 HttpCore #78 위임)
│  │  ├─ trino.py              #     Trino 연결·카탈로그/스키마·SQL 식별자 검증
│  │  ├─ citydata_bronze.py    #     ★ (장소×블록) 행 bronze DDL/INSERT (멱등, seoul_citydata)
│  │  └─ landing.py            #     R2/로컬 raw 적재 싱크
│  └─ source/                  #   서울 citydata 통합 API 소스 전용
│     ├─ config.py             #     적재 루트·source_id·API 키
│     ├─ citydata.py           #     통합 API 호출(원본 bytes) + 블록 분해 파서 + allowlist
│     ├─ areas.py              #     121개 장소(AREA_NM) 레지스트리
│     ├─ citydata_ingest.py    #     오케스트레이션 (1)fetch+R2 raw (2)raw→블록 bronze + 리포트
│     └─ maintenance.py        #     Iceberg 유지보수 + boto3 R2 정리 (두 스키마)
├─ docs/
│  └─ bronze-metadata.md       # bronze 메타데이터 최소기준 & 이유 (이슈 #16 근거)
├─ .airflowignore              # DAG 파일만 스캔, 패키지는 import만 되게 제외
└─ README.md                   # (이 문서)
```

Airflow는 dags 폴더를 재귀적으로 스캔하므로 세 DAG(`citydata_*.py`)가 자동 인식됩니다.
`citydata_ingest/`는 `.airflowignore`로 **DAG 스캔에서는 제외**되지만 import은 됩니다 —
DAG가 자기 디렉토리를 `sys.path`에 넣고 `citydata_ingest.*`를 불러옵니다.

## 수집 태스크: fetch_raw >> load_bronze >> report

태스크 경계는 **"다시 만들 수 없는 것"과 "다시 만들 수 있는 것" 사이**입니다:

- `fetch_raw` — 121장소 통합 API를 **병렬 호출**하고 **수집 즉시 gzip**해 R2 raw에 박제
  (~177KB→~25KB, 전 블록 원본 보존). 실시간 응답은 재현 불가이므로 받는 즉시 박제까지
  한 태스크. XCom으로는 payload가 아니라 **raw 객체 키 + 메타데이터**만 넘깁니다.
- `load_bronze` — XCom의 키로 R2 raw를 다시 읽어 **allowlist 블록만** (장소×블록) 행으로
  Iceberg bronze(`bronze_seoul_citydata`)에 멱등 적재. 겹침 블록(도로/주차/도착정보/
  문화행사)은 raw 에만 남깁니다 — 각 도메인 전용 원천이 canonical(#192).
- `report` — run 리포트를 R2에 기록(`all_done`이라 실패한 run도 리포트가 남음).

## 변환·유지보수

- **`citydata_transform`** (5분) — dbt 로 **인구(seoul_ppltn) + citydata(seoul_citydata)
  전 모델**을 빌드/테스트. 인구·상권·승하차·따릉이·대기질 silver + 크로스 신호 gold.
  전 모델 incremental — 재생성이 없어 스냅샷/파일 누적을 최소화.
- **`citydata_maintenance`** (주간) — 두 스키마의 Iceberg 테이블에 optimize·expire_snapshots·
  remove_orphan_files + boto3로 R2가 못 잡는 옛 metadata.json·버려진 디렉터리를 정리.

## 스키마 분리 (seoul_ppltn + seoul_citydata, #69)

인구 마트는 `seoul_ppltn` 스키마, 그 외 citydata 신호(상권/승하차/따릉이/대기질)는
`seoul_citydata` 스키마로 분리합니다. bronze(`bronze_seoul_citydata`)는 seoul_citydata에
있고, 인구 silver 는 이 bronze의 `LIVE_PPLTN_STTS` 블록을 파싱해 seoul_ppltn에 씁니다.

## bronze 설계: schema-on-read (블록 원본 payload)

DAG는 응답을 필드로 **분해하지 않습니다.** 통합 응답을 블록 단위로 쪼개 각 블록 원본 JSON을
`payload` 컬럼에 통째로 넣고 추적 메타데이터만 남깁니다((area, block_name, payload) 구조).
개별 필드 분해는 후속 **silver(dbt)** 가 블록별로 `json_extract`로 합니다. 자세한 이유는
[docs/bronze-metadata.md](docs/bronze-metadata.md) 참고.

- 이점: API 필드가 바뀌어도 bronze가 안 깨지고(COLUMN_NOT_FOUND 방지), 파싱 규칙을
  SQL(dbt)로 버전 관리하며, 원본이 payload로 보존돼 재처리(replay)가 가능.

## 시크릿(인증키)

DAG는 키를 **환경변수에서만** 읽습니다. `docker-compose`가 `sample/.env`를 (`env_file:`)
모든 Airflow 컨테이너에 주입하므로 `SEOUL_API_KEY_PPLT`, `R2_DEV_*`가 `os.environ`으로 들어옵니다.
**키는 절대 커밋하지 않습니다** — `.env`는 `sample` 상위 레포에서 gitignore 처리됩니다.
API key가 요청 URL 경로에 포함되므로, 예외 로깅 시 `redact_secret`으로 마스킹합니다.

## R2 적재 경로

```
raw/population/seoul_citydata/load_date=<KST날짜>/<ingest_ts>_<request_id>.json.gz
```
`ingest_ts`가 실행 1회를 격리하고, bronze 테이블 적재는 같은 `ingest_ts` 파티션을
delete-then-insert로 멱등 처리하므로 재시도가 중복을 만들지 않습니다. (raw prefix 기본값은
기존 아카이브 연속성을 위해 `raw/population`을 유지 — `SEOUL_PPLTN_LANDING_ROOT`로 덮어쓰기 가능.)

run마다 정량 리포트도 남깁니다:
```
raw/population/_reports/seoul_citydata/load_date=<KST>/ingest_ts=<UTC>/run_report.json
```
