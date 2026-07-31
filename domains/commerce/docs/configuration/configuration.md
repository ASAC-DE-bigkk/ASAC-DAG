# Configuration — commerce 실행 인자 / 환경변수

commerce 가 **현재 프로젝트에서 정상 동작하기 위해 필요한 모든 인자**를 한곳에 정리한다.

> **2026-07-28 개편**: commerce 전용 값의 **단일 소스는 루트 `.env` 의 `commerce 전용값` 블록**이다.
> 이 번들의 `.env.commerce` 는 그 값을 코드가 읽는 이름으로 **매핑(상속)** 만 하는 얇은 레이어가 되었다.
> generic 이름(`SCHEMA_VERSION`/`STORAGE_BACKEND`/`SEOUL_*`/`R2_REGION`)은 타 도메인과 공유되는
> 프로세스 env 오염을 막으려 루트에서 **`COMMERCE_` 접두로 네임스페이스**한다. **값을 바꾸려면
> `.env.commerce` 가 아니라 루트 `.env` 를 고친다.**

> 작업 경계: commerce **코드/설정/문서** 변경은 `dags/domains/commerce/`(= `dags/` 서브모듈) 안에서 한다.
> `docker-compose.yml` · `Dockerfile.airflow` 등 호스트 인프라 파일은 여전히 임의로 바꾸지 않는다(합의 후).
> 단 **env 값은 위 개편에 따라 루트 `.env` 의 `commerce 전용값` 블록에서 관리**한다.

---

## 1. 어떻게 주입되는가 (루트 `.env` `commerce 전용값` 블록 + `.env.commerce` 매핑)

commerce 전용 값의 **단일 소스는 루트 `.env` 의 `commerce 전용값` 블록**이다(`env_file: .env` 로
전 컨테이너 프로세스 env 에 주입). DAG 임포트 시
[include/commerce_core/env.py](../../include/commerce_core/env.py) 의 `load_commerce_env()` 가
이 폴더의 `.env.commerce` 를 읽어, 그 안의 `${...}` 참조를 프로세스 env(=루트 값)로 치환한 뒤
`os.environ` 에 채운다. 즉 `.env.commerce` 는 **루트 값을 코드가 읽는 이름으로 매핑**할 뿐이다.

```text
우선순위(높음 → 낮음)
  1) 프로세스 env   ← 루트 .env(commerce 전용값 블록) / docker-compose environment:
  2) .env.commerce  ← 루트 값을 코드 이름으로 매핑(${...}) / 단독 실행용 :- 기본값
  3) settings.py 기본값 ← 둘 다 없을 때
```

- `setdefault` 의미라 **프로세스 env(루트 `.env`)가 항상 우선**한다. `.env.commerce` 의
  `${COMMERCE_…:-기본}` 은 루트 값이 있으면 그 값을, 루트 `.env` 없이 단독 실행할 때만 `기본`을 쓴다.
- generic 이름은 루트에서 **`COMMERCE_` 접두 네임스페이스**(예: `COMMERCE_STORAGE_BACKEND`)로 두고
  `.env.commerce` 가 코드 이름(`STORAGE_BACKEND`)으로 되돌린다. 이미 안전한 이름(`COMMERCE_*`/`JUSO_*`/
  `SEOUL_API_KEY_COMM`)은 루트에서 **동일 이름**이라 `.env.commerce` 매핑 없이 직접 상속된다.
- 파일이 없어도 조용히 통과(배포가 env 를 직접 주입하는 경우를 막지 않음).
- 경로 override: `COMMERCE_ENV_FILE=/path/to/file`.
- 값(시크릿)은 로그에 남기지 않는다 — 적용 **개수만** 기록(CLAUDE.md §2.5).

### `${VAR}` 참조 — 루트 `.env` 와 겹치는 값은 불러온다

`.env.commerce` 값에는 `${VAR}` / `${VAR:-default}` 를 쓸 수 있고, 로더가 **현재 프로세스
env(= 루트 `.env`/compose 가 주입한 값)** 로 치환한다. 그래서 루트 `.env` 와 겹치는 R2
자격증명·엔드포인트·버킷은 **중복 저장하지 않고** 루트 키에서 불러온다. 이름이 다른 경우도
참조로 잇는다:

```bash
# .env.commerce — 루트 값을 코드가 읽는 이름으로 매핑
# generic 이름: 루트 COMMERCE_* → 코드 이름 (단독 실행용 :- 기본값)
STORAGE_BACKEND=${COMMERCE_STORAGE_BACKEND:-local}
SCHEMA_VERSION=${COMMERCE_SCHEMA_VERSION:-v1}
SEOUL_PAGE_SIZE=${COMMERCE_SEOUL_PAGE_SIZE:-1000}
# R2(공유 인프라): 루트 R2_DEV_* → 코드 이름
R2_ENDPOINT=${R2_DEV_ENDPOINT}
R2_BUCKET=${R2_BUCKET_NAME:-seoul}          # 루트는 R2_BUCKET_NAME(프로드), commerce 는 R2_BUCKET
R2_ACCESS_KEY_ID=${R2_DEV_ACCESS_KEY_ID}
R2_SECRET_ACCESS_KEY=${R2_DEV_SECRET_ACCESS_KEY}
```

| 형식 | 동작 |
|---|---|
| `${NAME}` | `NAME` 값으로 치환(없으면 빈 문자열) |
| `${NAME:-기본}` | `NAME` 이 없거나 비었으면 `기본` |
| `${NAME-기본}` | `NAME` 이 없을 때만 `기본` |

> 치환은 **프로세스 env** 기준이다. 컨테이너 런타임엔 루트 `.env` 가 compose 로 주입돼 있어
> 동작하지만, 루트 `.env` 없이 호스트에서 단독 실행하면 참조는 빈 값이 된다(이 경우 r2 는
> 쓰지 않으므로 무방). 같은 이름의 키는 이미 프로세스 env 에 있으면 그게 우선(setdefault)이라
> 참조는 사실상 "루트 값을 쓴다"는 문서 역할을 한다.

### 셋업

```bash
# 1) 실값은 루트 .env 의 'commerce 전용값' 블록에 채운다(SEOUL_API_KEY_COMM·JUSO_CONFM_KEY·
#    COMMERCE_STORAGE_BACKEND 등). 루트 .env.example 의 동명 블록이 템플릿.
# 2) 번들 매핑 파일을 그대로 복사(수정 불필요 — 이제 시크릿 없음)
cd dags/domains/commerce
cp .env.commerce.example .env.commerce     # PowerShell: Copy-Item
```

`.env.commerce` 는 이제 매핑만 담아 **시크릿이 없다**(루트 `.env` 로 이관). 여전히 번들
[.gitignore](../../.gitignore) 로 제외되며, 템플릿 [.env.commerce.example](../../.env.commerce.example) 만 추적된다.

---

## 2. 환경변수 전체 목록

읽는 코드: [include/commerce_core/settings.py](../../include/commerce_core/settings.py) ·
[include/commerce_core/registry.py](../../include/commerce_core/registry.py) ·
[include/commerce_core/env.py](../../include/commerce_core/env.py).

> **소스**: 아래 generic 키(`SEOUL_*`/`STORAGE_BACKEND`/`LOCAL_DATA_ROOT`/`SCHEMA_VERSION`/`R2_REGION`)는
> 루트 `.env` 의 `COMMERCE_<KEY>` 로 관리되고 `.env.commerce` 가 코드 이름으로 매핑한다.
> `COMMERCE_*`/`JUSO_*`/`SEOUL_API_KEY_COMM` 은 루트에서 **동일 이름**으로 직접 상속된다. 전체 대응표는 §3.

### 2.1 서울 OpenAPI

| 변수 | 기본값 | 필수 | 설명 |
|---|---|---|---|
| `SEOUL_API_KEY_COMM` | (없음) | **예** | 인증키. **루트 `.env` 에서 주입**(ASAC-DAG#70 이관, `SEOUL_API_KEY_<도메인>` 규칙) — 반드시 채워야 bronze 수집 가능. 로그/경로/메타에 노출 금지 |
| `SEOUL_OPEN_API_BASE_URL` | `http://openapi.seoul.go.kr:8088` | 아니오 | API 베이스 URL |
| `SEOUL_PAGE_SIZE` | `1000` | 아니오 | 1회 조회 건수(서울 상한 1000으로 캡) |
| `SEOUL_MAX_PAGES` | (없음)=무제한 | 아니오 | **비우면/미설정=무제한**(끝까지 순회). 일반 API 는 호출 횟수 제한 없음. `>0`=부분 수집(개발용), `0`·음수도 무제한 |
| `SEOUL_REQUEST_DELAY_SECONDS` | `0.2` | 아니오 | 페이지 간 지연(초) |

### 2.2 스토리지

| 변수 | 기본값 | 필수 | 설명 |
|---|---|---|---|
| `STORAGE_BACKEND` | `local` | 아니오 | `local`(컨테이너 볼륨) \| `r2`(Cloudflare R2) |
| `LOCAL_DATA_ROOT` | `/opt/airflow/data` | local 시 | 로컬 백엔드 루트 |
| `COMMERCE_STORAGE_PREFIX` | (없음) | 아니오 | bucket 아래 공통 접두(예: `dev/<id>`) → `{prefix}/raw/commerce/…`. 비우면 접두 없음 |
| `SCHEMA_VERSION` | `v1` | 아니오 | bronze 마커(리니지) JSON 의 schema_version |

### 2.3 Cloudflare R2 (`STORAGE_BACKEND=r2` 일 때만)

아래 값은 `.env.commerce` 에서 **루트 `.env` 키를 `${...}` 로 참조**한다(중복 저장 안 함).

| 변수 | `.env.commerce` 의 소스 | 설명 |
|---|---|---|
| `R2_ENDPOINT` | `${R2_DEV_ENDPOINT}` | `https://<account-id>.r2.cloudflarestorage.com` |
| `R2_BUCKET` | `${R2_BUCKET_NAME:-seoul}` | 버킷명(프로드 `seoul`). **루트는 `R2_BUCKET_NAME`, commerce 는 `R2_BUCKET`** — 참조로 이름 매핑 |
| `R2_ACCESS_KEY_ID` | `${R2_DEV_ACCESS_KEY_ID}` | R2 API 토큰 Access Key ID |
| `R2_SECRET_ACCESS_KEY` | `${R2_DEV_SECRET_ACCESS_KEY}` | R2 API 토큰 Secret |
| `R2_REGION` | `${COMMERCE_R2_REGION:-auto}` | boto3 region_name — 루트 `COMMERCE_R2_REGION`(기본 `auto`) |

### 2.4 레지스트리

| 변수 | 기본값 | 설명 |
|---|---|---|
| `COMMERCE_REGISTRY_PATH` | `config/dataset_registry.yaml`(이 번들) | 수집 대상 YAML 경로 override |
| `COMMERCE_ENV_FILE` | `.env.commerce`(이 번들) | env 파일 경로 override |

---

## 3. 루트 `.env` ↔ commerce 매핑 대응표

commerce 가 코드에서 읽는 키와, 그 값이 루트 `.env` 어디서 오는지:

| commerce 가 읽는 키(코드) | 루트 `.env` 소스 | `.env.commerce` 매핑 |
|---|---|---|
| `SEOUL_API_KEY_COMM` | `SEOUL_API_KEY_COMM`(#70) | — (동일 이름, 직접 상속) |
| `STORAGE_BACKEND` | `COMMERCE_STORAGE_BACKEND` | `${COMMERCE_STORAGE_BACKEND:-local}` |
| `SCHEMA_VERSION` · `LOCAL_DATA_ROOT` · `SEOUL_PAGE_SIZE` · `SEOUL_MAX_PAGES` · `SEOUL_REQUEST_DELAY_SECONDS` · `R2_REGION` | `COMMERCE_<KEY>`(네임스페이스) | `${COMMERCE_<KEY>:-기본}` |
| `COMMERCE_STORAGE_PREFIX` · `COMMERCE_RAW_LAYER` · `COMMERCE_SILVER_LAYER` · `COMMERCE_SCHEMA` · `COMMERCE_DBT_TARGET` · `COMMERCE_LOAD_LOOKBACK_DAYS` · `COMMERCE_ICEBERG_EXPIRE_DAYS` | 동일 이름 | — (직접 상속) |
| `COMMERCE_MARKERS_LAYER` · `COMMERCE_BRONZE_STATE_LAYER` · `COMMERCE_SILVER_STATE_LAYER` · `COMMERCE_SERVE_STATE_LAYER` · `COMMERCE_DIFF_TARGET_LAYER` · `COMMERCE_WATCHDOG_STATE_LAYER` · `COMMERCE_LOGS_LAYER` | 동일 이름 — 값 = `ops/control/state/commerce/…`(#60 존 정리: 마커=지시 파일 포함, 오너 해석) | — (직접 상속) |
| `JUSO_CONFM_KEY` · `JUSO_REQUEST_DELAY_SECONDS` · `JUSO_MAX_ADDRESSES` | 동일 이름 | — (직접 상속) |
| `R2_BUCKET` | `R2_BUCKET_NAME`(프로드 `seoul`, 2026-07-28 전환) | `${R2_BUCKET_NAME:-seoul}`(이름 매핑) — dev 복귀는 `${R2_DEV_BUCKET_NAME:-seoul-dev}` |
| `R2_ENDPOINT` · `R2_ACCESS_KEY_ID` · `R2_SECRET_ACCESS_KEY` | 동일 이름 | — (직접 상속) |

> generic 이름을 루트에서 `COMMERCE_` 접두로 두는 이유: 루트 `.env` 는 `env_file` 로 **모든 도메인**
> 컨테이너에 주입되므로, `SCHEMA_VERSION`·`STORAGE_BACKEND`·`R2_REGION` 같은 generic 이름을 그대로
> 올리면 타 도메인 프로세스 env 를 오염시킨다. 네임스페이스로 격리하고 `.env.commerce` 가 되돌린다.

---

## 4. 파이썬 의존성

[../requirements.txt](../../requirements.txt) 참조. 현재 호스트 이미지(루트 `Dockerfile.airflow`,
이 번들 밖)에는 `boto3`/`pandas`/`pyarrow`(+`trino`/`dbt`)가 **이미 포함**되어 R2·silver 모두
추가 설치 없이 동작한다. s3fs 는 미설치이며 사용하지 않는다.

| 기능 | 필요 패키지 | 이미지 상태 |
|---|---|---|
| bronze(local) 수집 | `requests`,`PyYAML`(Airflow 동봉) | ✅ |
| silver(parquet) | `pandas`,`pyarrow` | ✅ (pandas 2.3 / pyarrow 24) |
| R2 백엔드 | `boto3` | ✅ (boto3 1.43) |

> 셋 다 이미지에 이미 있으므로 추가 설치 불필요. 향후 새 패키지가 필요하면(번들 밖 작업이므로
> 별도 합의 필요): `pip install -r dags/domains/commerce/requirements.txt`.

---

## 5. 동작 확인

```bash
# 1) env 적재/우선순위 확인(시크릿 미출력)
PYTHONPATH=dags/domains/commerce/include \
  python -c "from commerce_core.env import load_commerce_env; print(load_commerce_env())"

# 2) 인증키/서비스명 검증(컨테이너)
docker compose exec airflow-scheduler \
  python -m bronze.resolve verify        # SEOUL_API_KEY_COMM 적재 후 152종 점검

# 3) 단위 테스트(Docker 불필요)
PYTHONPATH=dags/domains/commerce/include python -m pytest dags/domains/commerce/tests -q
```
