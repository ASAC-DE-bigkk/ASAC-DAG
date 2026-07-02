# Configuration — commerce 실행 인자 / 환경변수

commerce 가 **현재 프로젝트에서 정상 동작하기 위해 필요한 모든 인자**를 한곳에 정리한다.
환경이 바뀌며(루트 `.env` 가 R2/Iceberg 중심으로 재구성) commerce 전용 변수가 빠졌기 때문에,
이 번들은 **자체 환경파일 `.env.commerce`** 로 필요한 값을 자립적으로 주입한다.

> 작업 경계: commerce 관련 변경은 **`dags/domains/commerce/` 안에서만** 한다. 루트 `.env` ·
> `docker-compose.yml` · `Dockerfile.airflow` 는 이 번들(`dags/` 서브모듈) 밖이므로 임의로
> 바꾸지 않는다. 필요한 값은 `.env.commerce` 로 공급한다.

---

## 1. 어떻게 주입되는가 (`.env.commerce` + 로더)

DAG([../commerce_raw.py](../../commerce_raw.py))가 임포트될 때
[include/common/env.py](../../include/common/env.py) 의 `load_commerce_env()` 가
이 폴더의 `.env.commerce` 를 읽어 `os.environ` 에 채운다.

```text
우선순위(높음 → 낮음)
  1) 프로세스 env   ← 루트 .env / docker-compose environment: 가 주입한 값
  2) .env.commerce  ← 이 번들이 채우는 commerce 전용 값(없는 키만 setdefault)
  3) settings.py 기본값 ← 둘 다 없을 때
```

- `setdefault` 의미라 **배포 환경이 직접 준 값이 항상 우선**한다. commerce 전용으로 빠진
  값(`STORAGE_BACKEND` 등)만 `.env.commerce` 가 메운다. 인증키 `SEOUL_API_KEY_COMM` 은
  **루트 `.env` 소관**(ASAC-DAG#70 이관) — `.env.commerce` 에 두지 않는다.
- 파일이 없어도 조용히 통과(배포가 env 를 직접 주입하는 경우를 막지 않음).
- 경로 override: `COMMERCE_ENV_FILE=/path/to/file`.
- 값(시크릿)은 로그에 남기지 않는다 — 적용 **개수만** 기록(CLAUDE.md §2.5).

### `${VAR}` 참조 — 루트 `.env` 와 겹치는 값은 불러온다

`.env.commerce` 값에는 `${VAR}` / `${VAR:-default}` 를 쓸 수 있고, 로더가 **현재 프로세스
env(= 루트 `.env`/compose 가 주입한 값)** 로 치환한다. 그래서 루트 `.env` 와 겹치는 R2
자격증명·엔드포인트·버킷은 **중복 저장하지 않고** 루트 키에서 불러온다. 이름이 다른 경우도
참조로 잇는다:

```bash
# .env.commerce 의 R2 블록 — 루트 .env 값을 그대로 사용
R2_ENDPOINT=${R2_DEV_ENDPOINT}
R2_BUCKET=${R2_DEV_BUCKET_NAME}            # 루트는 R2_DEV_BUCKET_NAME, commerce 는 R2_BUCKET
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
cd dags/domains/commerce
cp .env.commerce.example .env.commerce     # PowerShell: Copy-Item
# 인증키는 루트 .env 에 SEOUL_API_KEY_COMM 으로 채운다(필수, #70). R2 쓰면 R2_* 확인.
```

`.env.commerce` 는 이 번들의 [.gitignore](../../.gitignore) 로 커밋 제외(시크릿),
템플릿 [.env.commerce.example](../../.env.commerce.example) 만 추적된다.

---

## 2. 환경변수 전체 목록

읽는 코드: [include/common/settings.py](../../include/common/settings.py) ·
[include/common/registry.py](../../include/common/registry.py) ·
[include/common/env.py](../../include/common/env.py).

### 2.1 서울 OpenAPI

| 변수 | 기본값 | 필수 | 설명 |
|---|---|---|---|
| `SEOUL_API_KEY_COMM` | (없음) | **예** | 인증키. **루트 `.env` 에서 주입**(ASAC-DAG#70 이관, `SEOUL_API_KEY_<도메인>` 규칙) — 반드시 채워야 bronze 수집 가능. 로그/경로/메타에 노출 금지 |
| `SEOUL_OPENAPI_BASE_URL` | `http://openapi.seoul.go.kr:8088` | 아니오 | API 베이스 URL |
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
| `R2_BUCKET` | `${R2_DEV_BUCKET_NAME}` | 버킷명. **루트는 `R2_DEV_BUCKET_NAME`/`R2_BUCKET_NAME`, commerce 는 `R2_BUCKET`** — 참조로 이름 매핑 |
| `R2_ACCESS_KEY_ID` | `${R2_DEV_ACCESS_KEY_ID}` | R2 API 토큰 Access Key ID |
| `R2_SECRET_ACCESS_KEY` | `${R2_DEV_SECRET_ACCESS_KEY}` | R2 API 토큰 Secret |
| `R2_REGION` | `auto`(리터럴) | boto3 region_name — R2 는 `auto`(루트에 없음) |

### 2.4 레지스트리

| 변수 | 기본값 | 설명 |
|---|---|---|
| `COMMERCE_REGISTRY_PATH` | `config/dataset_registry.yaml`(이 번들) | 수집 대상 YAML 경로 override |
| `COMMERCE_ENV_FILE` | `.env.commerce`(이 번들) | env 파일 경로 override |

---

## 3. 환경이 바뀌며 빠진 값 (gap)

루트 `.env`(R2/Iceberg 중심) ↔ commerce 요구 변수 비교:

| commerce 가 읽는 키 | 루트 `.env` 상태 | `.env.commerce` 가 메움 |
|---|---|---|
| `SEOUL_API_KEY_COMM` | **있음**(#70 에서 이관, 값 직접 입력) | — (루트 소관) |
| `STORAGE_BACKEND` | 없음 | ✅ `local` |
| `R2_BUCKET` | `R2_DEV_BUCKET_NAME`/`R2_BUCKET_NAME` 으로 이름 다름 | ✅ `${R2_DEV_BUCKET_NAME}` 참조로 이름 매핑 |
| `SCHEMA_VERSION`/`LOCAL_DATA_ROOT`/`SEOUL_*`/`R2_REGION` | 없음 | ✅ 리터럴 |
| `R2_ENDPOINT`/`R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY` | 있음(dev/prod 세트) | ✅ `${R2_DEV_*}` 로 **불러옴**(중복 저장 안 함, 프로세스 env 가 있으면 그게 우선) |

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
  python -c "from common.env import load_commerce_env; print(load_commerce_env())"

# 2) 인증키/서비스명 검증(컨테이너)
docker compose exec airflow-scheduler \
  python -m bronze.resolve verify        # SEOUL_API_KEY_COMM 적재 후 39종 점검

# 3) 단위 테스트(Docker 불필요)
PYTHONPATH=dags/domains/commerce/include python -m pytest dags/domains/commerce/tests -q
```
