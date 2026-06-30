# Environments

commerce 의 "환경"은 **스토리지 백엔드** 한 축으로 정해진다(서빙 DB 없음, 코드는 동일).
호스트 Airflow 스택은 하나의 `docker-compose.yml`(`elt-infra`, LocalExecutor)이고,
commerce 전용 인자는 이 번들의 `.env.commerce` 가 공급한다 → [configuration.md](configuration.md).

> 과거 문서가 설명하던 `.env.local`/`.env.dev`/`.env.prod` + override/server 컴포즈 +
> CeleryExecutor + serving DB 구성은 **현재 프로젝트에 존재하지 않는다**. 실제 구성에 맞춰
> 갱신했다.

## 스토리지 백엔드 (유일한 환경 축)

`STORAGE_BACKEND` 으로 결정([../include/common/storage.py](../../include/common/storage.py)):

| 값 | 백엔드 | 위치 | 자격증명 | 추가 패키지 |
|---|---|---|---|---|
| `local`(기본) | `LocalStorage` | 컨테이너 `LOCAL_DATA_ROOT`(=`/opt/airflow/data`) | 불필요 | 없음(bronze) |
| `r2` | `R2Storage`(boto3) | `s3://<R2_BUCKET>/...` (예: `seoul-dev`) | `R2_*` | `boto3`(이미지 기본 포함) |

- 코드는 `get_storage()` 하나만 호출 — 단계 로직은 백엔드를 모른다. **백엔드 전환은
  `.env.commerce` 의 `STORAGE_BACKEND` 값만 바꾸면 끝**(코드 변경 없음).
- silver(parquet)는 백엔드와 무관하게 `pandas`/`pyarrow` 가 필요하다([requirements.txt](../../requirements.txt)).

> ⚠️ **local 백엔드 영속성**: 현재 호스트 컴포즈는 `/opt/airflow/data` 를 호스트 볼륨으로
> 마운트하지 않는다 → local 산출물은 컨테이너 수명과 함께 사라진다(개발/스모크용). 영속이
> 필요하면 (a) `STORAGE_BACKEND=r2` 를 쓰거나, (b) 호스트 컴포즈에 데이터 볼륨을 추가한다
> (호스트 변경 = 번들 밖, 합의 후).

## 환경변수

값은 두 경로에서 온다:

1. **호스트 프로세스 env** — 루트 `.env`(`env_file: .env`)와 compose `environment:` 가 주입
   (Airflow/Postgres/Trino/dbt/R2-Iceberg 등). **commerce 전용 값은 여기 추가하지 않는다.**
2. **`.env.commerce`** — DAG 임포트 시 `load_commerce_env()` 가 채움(프로세스 env 가 우선).

commerce 가 읽는 키 전체와 기본값: [configuration.md](configuration.md).

| 환경 | `STORAGE_BACKEND` | `R2_BUCKET` | 자격증명 |
|---|---|---|---|
| local (기본) | `local` | – | 없음 |
| dev (R2) | `r2` | `seoul-dev` | dev R2 토큰 |
| prod (R2) | `r2` | `seoul-prod` | prod R2 토큰(분리) |

> dev/prod 분리는 **버킷·토큰**으로 보장한다. 별도 컴포즈 파일이 아니라 `.env.commerce`
> (또는 호스트 env)의 `R2_BUCKET`/`R2_*` 값으로 가른다.

## 코드 변경 없이 전환

```bash
# .env.commerce 에서 한 줄만 바꾼다
STORAGE_BACKEND=r2
R2_BUCKET=seoul-dev      # prod 는 seoul-prod
# (R2 는 boto3 사용 — 호스트 이미지에 이미 포함, 추가 설치 불필요)
```

`local` 은 R2 자격증명 없이 동작하므로 신규 기여자가 클라우드 셋업 없이 바로 개발 가능,
`r2` 는 prod 와 동일한 R2 기반 경로를 공유한다.

배포 절차: [deploy-local.md](../operations/deploy-local.md) · [deploy-dev.md](../operations/deploy-dev.md) ·
[deploy-prod.md](../operations/deploy-prod.md).
