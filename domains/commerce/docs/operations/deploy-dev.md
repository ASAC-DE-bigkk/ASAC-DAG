# Deploy — dev (스토리지 = Cloudflare R2 dev 버킷)

dev 는 **Cloudflare R2 dev 버킷**(`seoul-dev`)을 쓴다. prod 와 동일한 R2 기반 경로를
공유하되 버킷/자격증명은 prod 와 분리된다([deploy-prod.md](deploy-prod.md)). local 과의
차이는 **스토리지 백엔드뿐** — 호스트 컴포즈/명령은 동일하다.

> 클라우드 없이 순수 로컬은 [deploy-local.md](deploy-local.md). 환경 축: [environments.md](../configuration/environments.md).

## 1. commerce 환경파일

```bash
cd dags/domains/commerce
cp .env.commerce.example .env.commerce
```

채울 항목 — commerce 전용 값만 적고, R2 자격증명/엔드포인트/버킷은 **루트 `.env` 를 참조**한다
(템플릿 기본값이 이미 `${R2_DEV_*}` 참조라 보통 그대로 두면 된다):

```bash
SEOUL_OPENAPI_KEY=<발급키>
STORAGE_BACKEND=r2

# R2 블록은 루트 .env 값을 불러옴(중복 입력 불필요). 템플릿 기본:
#   R2_ENDPOINT=${R2_DEV_ENDPOINT}
#   R2_BUCKET=${R2_DEV_BUCKET_NAME}            # 루트는 R2_DEV_BUCKET_NAME → commerce 는 R2_BUCKET
#   R2_ACCESS_KEY_ID=${R2_DEV_ACCESS_KEY_ID}
#   R2_SECRET_ACCESS_KEY=${R2_DEV_SECRET_ACCESS_KEY}
#   R2_REGION=auto
```

루트 `.env` 에 해당 키가 없으면 `${VAR:-기본값}` 또는 실제 값으로 바꿔 넣는다. R2 토큰
발급/권한은 [storage.md](../architecture/storage.md)의 "Cloudflare R2 설정"(버킷명 dev). 전체 변수·참조
규칙: [configuration.md](../configuration/configuration.md).

> 호스트 루트 `.env`(번들 밖)가 같은 이름의 R2 키를 이미 프로세스 env 로 주입하면 그 값이
> 우선한다(setdefault). 이름이 다른 `R2_BUCKET` 은 `${R2_DEV_BUCKET_NAME}` 참조로 매핑된다.

## 2. 의존성

R2 백엔드는 `boto3`, silver 는 `pandas`/`pyarrow` 가 필요하다. 현재 호스트 이미지에 셋 다
**이미 포함**(boto3 1.43 / pandas 2.3 / pyarrow 24)되어 있어 **추가 설치 없이 동작**한다
([requirements.txt](../../requirements.txt) 는 명세용). s3fs 는 미설치이며 사용하지 않는다.

## 3. 기동 & 실행

```bash
docker compose up -d                       # 호스트 루트에서 (UI :30585)
docker compose exec airflow-scheduler airflow dags trigger seoul_commerce_daily
```

R2 적재 확인:

```bash
docker compose exec airflow-scheduler python - <<'PY'
import sys; sys.path.insert(0, "/opt/airflow/dags/domains/commerce/include")
from common.env import load_commerce_env; load_commerce_env()
from common.storage import get_storage
s = get_storage()
s.write_text("healthcheck/ping.txt", "ok")
print("R2 ok:", s.exists("healthcheck/ping.txt"))
PY
```

## 4. 코드/인자 수정 반영

- `./dags` 바인드 마운트라 코드·`.env.commerce` 수정은 스케줄러 재파싱으로 반영(재빌드 불필요).
- 단, 새 패키지가 필요해질 경우(현재 boto3/pandas/pyarrow 는 이미지에 이미 있음) 이미지/환경
  변경이라 재빌드·재기동이 필요할 수 있다.

## 트러블슈팅

| 증상 | 원인/조치 |
|---|---|
| `R2 backend requires ...` | `R2_BUCKET/ENDPOINT/ACCESS_KEY_ID/SECRET_ACCESS_KEY` 중 빈 값 |
| 데이터가 R2 에 안 보임 | `STORAGE_BACKEND=r2` 인지 + `R2_BUCKET` 채워졌는지 확인(빈값이면 local 로 적재) |
| 데이터가 prod 와 섞임 | `R2_BUCKET=seoul-dev` 인지 확인(prod 와 버킷 분리) |
| 키 이름 불일치 | 루트 `.env` 는 `R2_BUCKET_NAME` — commerce 는 `R2_BUCKET` 필요 |
