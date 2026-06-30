# Storage & Layout

CLAUDE.md §2의 비협상 데이터 규칙을 구현. 동일한 `key`가 백엔드(local/R2)에 무관하게
사용되며, 백엔드는 자신의 root/bucket만 앞에 붙인다. 구현:
[../include/common/storage.py](../../include/common/storage.py) ·
[../include/common/paths.py](../../include/common/paths.py).

## 백엔드 전환

`STORAGE_BACKEND` 환경변수로 결정([../include/common/storage.py](../../include/common/storage.py)의 `get_storage()`):

| 값 | 백엔드 | 위치 | 용도 |
|---|---|---|---|
| `local` | `LocalStorage` | `LOCAL_DATA_ROOT`(=`/opt/airflow/data`) | 개발/스모크(자격증명 불필요) |
| `r2` | `R2Storage`(boto3) | `s3://<R2_BUCKET>/...` | dev/prod |

코드는 `get_storage()` 하나만 호출 — 단계 로직은 백엔드를 모른다. 환경변수 전체:
[configuration.md](../configuration/configuration.md).

## 경로 규칙 (결정적)

bronze 는 **DAG 실행 1회 = `run_id` 폴더 1개**(스냅샷)을 **연/월/일 디렉터리 아래**에 둔다
(연/월/일은 run_id 날짜에서 파생). silver 는 **논리일** 파티션. `{prefix}`(=`COMMERCE_STORAGE_PREFIX`,
비우면 없음)·bucket 접두는 스토리지 백엔드가 붙인다([paths.py](../../include/common/paths.py)):

```text
{prefix}/bronze/commerce/<YYYY>/<MM>/<DD>/run_id=<YYYY-MM-DD_HHMMSS_mmm>/<short>.jsonl       # API당 1파일(원본 페이지 NDJSON)
{prefix}/bronze/commerce/<YYYY>/<MM>/<DD>/run_id=<...>/_markers/<short>.completed | .incomplete  # API별 수집 결과 마커(JSON, 리니지 포함)
{prefix}/bronze/commerce/<YYYY>/<MM>/<DD>/run_id=<...>/_markers/_RUN.completed | .incomplete      # 실행 전체 마커
{prefix}/silver/commerce/<short>/observed_date=YYYY-MM-DD/part-000.parquet                       # 공통 19컬럼 정규화
```

예시(2026-06-30 14:30:25.123 KST 실행):

```text
bronze/commerce/2026/06/30/run_id=2026-06-30_143025_123/general_restaurant.jsonl
bronze/commerce/2026/06/30/run_id=2026-06-30_143025_123/_markers/general_restaurant.completed
bronze/commerce/2026/06/30/run_id=2026-06-30_143025_123/_markers/_RUN.completed
silver/commerce/general_restaurant/observed_date=2026-06-30/part-000.parquet
```

### bronze: API당 1파일(NDJSON) + 마커

- `<short>.jsonl` = 그 API 의 **모든 페이지를 줄단위 NDJSON**(줄 1개 = 원본 응답 1페이지, 가공 없음).
- **bronze 는 이 `run_id` 폴더 안에서만** 파일을 만든다 — 외부 경로(예전 `commerce/_manifest/`)에
  상태 파일을 두지 않는다.
- 중복 제어: bronze 는 매 실행 전체 수집(스킵 없음), **중복 제거는 silver 가 `MGTNO` 로**.

### 마커 (수집 상태 = 외부 매니페스트 대체)

상태/이력은 `run_id` 폴더의 마커가 전부(DB·외부 매니페스트 없음). **API당 마커 1개**(상호배타):

| 마커 | 의미 | 다음 실행 |
|---|---|---|
| `_markers/<short>.completed` | cap 없이 끝까지 + 건수 일치(status=ok) | — |
| `_markers/<short>.incomplete` | 건수 불일치/부분(cap)/오류(status=partial\|failed) | 재수집 |
| (마커 없음) | 이번 실행 미시도 | — |
| `_markers/_RUN.completed\|.incomplete` | 실행 전체 요약(metrics) | — |

> '완료'와 '미완료'를 **동시에** 두면 중복·불일치 위험이라, API당 1개만 둔다(타입이 곧 상태).

## 메타데이터 / 리니지 (소스 식별자 보존, CLAUDE.md §2.1)

페이지별 `.meta.json` 사이드카 대신, **마커 JSON 이 리니지를 담는다**
([bronze_tasks.py](../../include/bronze/bronze_tasks.py)의 `_write_bronze`):

```json
{
  "marker": "completed",
  "status": "ok",
  "source_system": "seoul_open_data_plaza",
  "source_name": "LOCALDATA_072404",
  "source_uri": "http://openapi.seoul.go.kr:8088/***/json/LOCALDATA_072404/<start>/<end>/",
  "domain": "commerce", "short": "general_restaurant", "oa_id": "OA-16094",
  "observed_date": "2026-06-30", "collected_at": "2026-06-30T05:30:25+00:00",
  "run_id": "<airflow_run_id>", "bronze_run_id": "2026-06-30_143025_123",
  "schema_version": "v1",
  "pages_written": 535, "rows_total": 534680, "list_total_count": 534680, "complete": true,
  "bronze_key": "bronze/commerce/2026/06/30/run_id=2026-06-30_143025_123/general_restaurant.jsonl",
  "pages": [{"page": 1, "start": 1, "end": 1000, "rows": 1000, "content_hash": "9f2a...c4"}]
}
```

- 페이지별 `content_hash` 는 마커의 `pages[]` 에 보존(원본 NDJSON 줄과 1:1).
- 소스 네이티브 ID(`MGTNO` 등)는 정규화 레코드에 그대로 보존한다(내부 ID로 대체 금지).
- 인증키(`SEOUL_OPENAPI_KEY`)는 `source_uri`/로그에서 `***` 로 마스킹된다(CLAUDE.md §2.5).

## Cloudflare R2 설정 (`STORAGE_BACKEND=r2`)

R2 는 S3 호환 — **boto3** S3 클라이언트에 커스텀 엔드포인트(path-style·SigV4·region `auto`)를 준다
([storage.py](../../include/common/storage.py)의 `R2Storage`). s3fs 가 아니라 boto3 를 쓰는 이유는
호스트 이미지에 boto3 만 있고 s3fs 는 없기 때문(번들 안에서 자립 해결).
값은 `.env.commerce` 로 공급([configuration.md](../configuration/configuration.md) §2.3):

```bash
STORAGE_BACKEND=r2
R2_ENDPOINT=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
R2_BUCKET=seoul-dev          # prod 는 seoul-prod  (※ 루트 .env 의 R2_BUCKET_NAME 과 다른 키)
R2_ACCESS_KEY_ID=<R2 API 토큰 Access Key ID>
R2_SECRET_ACCESS_KEY=<R2 API 토큰 Secret>
R2_REGION=auto
```

- R2 대시보드 → **R2 → Manage R2 API Tokens**에서 Access Key/Secret 발급, 버킷 최소 권한.
- 버킷은 dev/prod 분리(`seoul-dev`/`seoul-prod`), 토큰도 환경별 분리.
- 자격증명은 bronze 페이로드/로그/경로/커밋에 **절대 저장 금지**(CLAUDE.md §2.5).
- R2 백엔드는 `boto3` 만 필요하며 호스트 이미지에 **이미 포함**(추가 설치 불필요) — [requirements.txt](../../requirements.txt).

### 연결 점검

```bash
docker compose exec airflow-scheduler python - <<'PY'
import sys; sys.path.insert(0, "/opt/airflow/dags/domains/commerce/include")
from common.env import load_commerce_env; load_commerce_env()
from common.storage import get_storage
s = get_storage()
s.write_text("healthcheck/ping.txt", "ok")
print("exists:", s.exists("healthcheck/ping.txt"))
PY
```

## 재처리 가능성 (CLAUDE.md §2.4)

- bronze는 원본 그대로 보존 → 정규화 로직 개선 후 옛 원본으로 silver 재생성 가능.
- 요청 메타·소스 타임스탬프·식별자 보존 → 과거 데이터 식별 가능.
- silver는 `observed_date` 파티션 재생성, bronze는 절대 덮어쓰지/삭제하지 않는다.
