# Deploy — local (스토리지 = 로컬 볼륨)

`STORAGE_BACKEND=local` 로 컨테이너 볼륨에 수집한다. **R2 등 클라우드 자격증명이 필요 없다**
— 신규 기여자가 바로 시작하는 모드. R2 를 쓰는 dev 는 [deploy-dev.md](deploy-dev.md).

> 호스트 스택은 단일 `docker-compose.yml`(`elt-infra`, LocalExecutor) + 루트 `.env`(번들 밖).
> commerce 인자는 이 번들의 `.env.commerce`. 두 축 개념은 [environments.md](../configuration/environments.md).

## 사전 요건

- Docker Desktop(Compose v2). Windows 는 WSL2 백엔드 권장.
- 호스트 루트 `.env` 가 채워져 있어야 한다(Airflow/Postgres 등 — 번들 밖).

## 1. commerce 환경파일

```powershell
cd dags/domains/commerce
Copy-Item .env.commerce.example .env.commerce
```

- `SEOUL_OPENAPI_KEY` 입력(**필수**) — 없으면 `check_api_key` 게이트에서 전체 실패.
- `STORAGE_BACKEND=local`(기본), `LOCAL_DATA_ROOT=/opt/airflow/data` 확인.
- 전체 변수: [configuration.md](../configuration/configuration.md).

## 2. 기동 (호스트 루트에서)

```bash
docker compose up -d
docker compose ps
```

- 최초 기동 시 `airflow-init` 가 DB 마이그레이션 + admin 계정 생성 후 종료.
- 이미지가 없으면 자동 빌드.
- UI: http://localhost:30585

> 컴포즈가 `./dags` 를 마운트하므로 `.env.commerce` 가 컨테이너에서 보이고, DAG 임포트 시
> `load_commerce_env()` 가 자동 적재한다. 코드/`.env.commerce` 수정은 스케줄러 재파싱으로 반영.

## 3. 파이프라인 실행

UI 에서 `seoul_commerce_daily` 토글 ON → ▶. 또는:

```bash
docker compose exec airflow-scheduler airflow dags trigger seoul_commerce_daily
docker compose exec airflow-scheduler \
  airflow dags trigger seoul_commerce_daily -c '{"observed_date":"2026-06-01"}'
```

## 4. 산출물 확인

```bash
# 컨테이너 내부 로컬 볼륨 산출물(LOCAL_DATA_ROOT)
docker compose exec airflow-scheduler sh -lc 'find /opt/airflow/data -maxdepth 4 -type f | head'
```

> ⚠️ **영속성**: 현재 호스트 컴포즈는 `/opt/airflow/data` 를 호스트 볼륨으로 마운트하지
> 않으므로 local 산출물은 컨테이너 수명과 함께 사라진다(개발/스모크용). 영속이 필요하면
> `STORAGE_BACKEND=r2`([deploy-dev.md](deploy-dev.md)) 또는 호스트 컴포즈에 데이터 볼륨 추가
> (번들 밖 변경 → 합의 후).

## 5. 의존성 주의 (silver)

silver(parquet) 태스크는 `pandas`/`pyarrow` 가 필요하다. 호스트 이미지에 없으면
`build_silver_one` 이 실패한다 → [requirements.txt](../../requirements.txt) 설치(호스트 변경, 합의 후).
bronze 만 검증하려면 silver 실패는 무시 가능(bronze 결과는 run_id 폴더의 마커에 남는다).

## 6. 정리

```bash
docker compose down        # 컨테이너 삭제(볼륨 유지)
docker compose down -v     # 볼륨까지 삭제(Postgres 초기화)
```

## 테스트 (Docker 불필요)

```bash
PYTHONPATH=dags/domains/commerce/include python -m pytest dags/domains/commerce/tests -q
```

## 트러블슈팅

| 증상 | 원인/조치 |
|---|---|
| `check_api_key` 실패 | `.env.commerce` 의 `SEOUL_OPENAPI_KEY` 미설정/오타 |
| DAG 안 보임 | `docker compose logs airflow-dag-processor` 에서 import 에러 확인 |
| `build_silver_one` 실패 | 이미지에 `pandas`/`pyarrow` 없음 → requirements 설치 |
| 산출물이 사라짐 | local 볼륨 미마운트(위 §4 영속성) → r2 사용 |
