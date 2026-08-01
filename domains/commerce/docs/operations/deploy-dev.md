# Deploy — dev (레거시, 현재 사용하지 않음)

> ## ⚠️ 이 문서는 폐지된 구성을 설명한다
>
> **2026-07-28 전환(change-log §79) 이후 dev 버킷(`seoul-dev`)·dev 카탈로그(`iceberg_dev`)로
> 가는 신규 쓰기가 없다.** 루트 `.env` 에 `R2_DEV_*`·`TRINO_DEV_ICEBERG_CATALOG` 활성 설정도
> 없다. `seoul-dev` 는 전환 전 구조가 그대로 남은 **보존본**이며 현행과 저장 구조가 다르다
> (구 `YYYY/MM/DD` raw 레이아웃, ops 존 밖 루트 `runs/`·`errors/` 등).
>
> - 현행 배포는 [deploy-prod.md](deploy-prod.md), 환경 현황은
>   [environments.md](../configuration/environments.md) §3.
> - 자격증명 없이 도는 스모크는 [deploy-local.md](deploy-local.md).
> - `seoul-dev` 정리는 "쓰는 쪽 되돌리기"가 아니라 **참조 0 확인 후 삭제**(ASK-Seoul#78 `Y-2`)
>   대상이고 별도 이슈에서 다룬다.
>
> 아래 절차는 **이력 참고용**이다. 그대로 실행하면 dev 우선 규칙(`common.storage.r2_env`)이
> 되살아나 운영 기록이 두 버킷으로 갈린다(`Z-7`).

## (이력) dev 버킷 배포 절차

dev 는 **Cloudflare R2 dev 버킷**(`seoul-dev`)을 썼다. prod 와 동일한 R2 기반 경로를
공유하되 버킷/자격증명은 prod 와 분리했다. local 과의 차이는 **스토리지 백엔드뿐**이었다.

## 1. 환경 설정 (루트 `.env` 의 `commerce 전용값` 블록)

commerce 실값은 **루트 `.env`** 에서 관리한다(2026-07-28 개편). 번들의 `.env.commerce` 는 루트 값을
코드 이름으로 매핑만 하므로 복사만 하고 그대로 둔다:

```bash
cd dags/domains/commerce
cp .env.commerce.example .env.commerce      # 매핑만 담김 — 수정 불필요
```

dev 용으로 **루트 `.env` 의 `commerce 전용값` 블록**에서 설정:

```bash
# 루트 .env (호스트 프로젝트)
SEOUL_API_KEY_COMM=<발급키>          # #70 이관, 필수
COMMERCE_STORAGE_BACKEND=r2          # dev = R2 dev 버킷 사용
# JUSO_CONFM_KEY=<도로명주소 승인키>  # silver 지번 보강 쓰면
# R2 dev 자격증명/버킷은 루트 .env 의 R2_DEV_* 세트를 그대로 사용
#   (.env.commerce 가 R2_BUCKET=${R2_DEV_BUCKET_NAME} 등으로 코드 이름에 매핑)
```

R2 토큰 발급/권한은 [storage.md](../architecture/storage.md)의 "Cloudflare R2 설정"(버킷명 dev).
전체 변수·매핑 대응표: [configuration.md](../configuration/configuration.md).

> generic 이름(`STORAGE_BACKEND` 등)은 루트에서 `COMMERCE_` 접두로 두고 `.env.commerce` 가 코드
> 이름으로 되돌린다. 이름이 다른 `R2_BUCKET` 은 루트 `R2_DEV_BUCKET_NAME` 참조로 매핑된다.

## 2. 의존성

R2 백엔드는 `boto3`, silver 는 `pandas`/`pyarrow` 가 필요하다. 현재 호스트 이미지에 셋 다
**이미 포함**(boto3 1.43 / pandas 2.3 / pyarrow 24)되어 있어 **추가 설치 없이 동작**한다
([requirements.txt](../../requirements.txt) 는 명세용). s3fs 는 미설치이며 사용하지 않는다.

## 3. 기동 & 실행

```bash
docker compose up -d                       # 호스트 루트에서 (UI :30585)
docker compose exec airflow-scheduler airflow dags trigger commerce_collect_raw
```

R2 적재 확인:

```bash
docker compose exec airflow-scheduler python - <<'PY'
import sys; sys.path.insert(0, "/opt/airflow/dags/domains/commerce/include")
from commerce_core.env import load_commerce_env; load_commerce_env()
from commerce_core.storage import get_storage
s = get_storage()
s.write_text("healthcheck/ping.txt", "ok")
print("R2 ok:", s.exists("healthcheck/ping.txt"))
PY
```

## 4. 코드/인자 수정 반영

- `./dags` 바인드 마운트라 코드·`.env.commerce`(매핑) 수정은 스케줄러 재파싱으로 반영(재빌드 불필요).
- **루트 `.env` 값 변경은 `env_file` 재주입이 필요** → `docker compose up -d`(컨테이너 재생성)로 반영.
- 단, 새 패키지가 필요해질 경우(현재 boto3/pandas/pyarrow 는 이미지에 이미 있음) 이미지/환경
  변경이라 재빌드·재기동이 필요할 수 있다.

## 트러블슈팅

| 증상 | 원인/조치 |
|---|---|
| `R2 backend requires ...` | `R2_BUCKET/ENDPOINT/ACCESS_KEY_ID/SECRET_ACCESS_KEY` 중 빈 값 |
| 데이터가 R2 에 안 보임 | 루트 `COMMERCE_STORAGE_BACKEND=r2` + `R2_DEV_BUCKET_NAME` 채워졌는지 확인(빈값이면 local 로 적재) |
| 데이터가 prod 와 섞임 | 루트 `R2_DEV_BUCKET_NAME=seoul-dev` 인지 확인(prod 와 버킷 분리) |
| 키 이름 불일치 | 루트 `.env` 는 `R2_BUCKET_NAME` — commerce 는 `R2_BUCKET` 필요 |
