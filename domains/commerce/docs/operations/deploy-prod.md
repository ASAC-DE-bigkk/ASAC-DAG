# Deploy — prod (현행 운영 구성)

**현재 동작 중인 유일한 구성이다.** 오브젝트 버킷은 **`seoul`**, Iceberg 카탈로그는
**`iceberg`**, 실행 타깃은 **`prod`**(`DBT_TARGET`·`ASK_SEOUL_TARGET`·`COMMERCE_DBT_TARGET`
셋 다). 호스트 스택은 단일 컴포즈다.

> 환경 현황·실측: [environments.md](../configuration/environments.md).
> R2 발급: [storage.md](../architecture/storage.md). 전체 변수: [configuration.md](../configuration/configuration.md).

## 구성 요약

- 스토리지: R2 버킷 **`seoul`** (루트 `COMMERCE_STORAGE_BACKEND=r2` · `R2_BUCKET_NAME=seoul`)
- Iceberg: `TRINO_ICEBERG_CATALOG=iceberg`
- 자격증명: 루트 `R2_ENDPOINT`/`R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY` 를 **이름 그대로 상속**
  (`.env.commerce` 에 해당 줄이 없다)
- 시크릿: **루트 `.env`**(commerce 값·`JUSO_CONFM_KEY` 이관처)는 `600` 권한·시크릿 매니저 주입 권장, 커밋 금지(gitignore)

> **`seoul-dev` 는 레거시다.** 2026-07-28 전환 이후 신규 쓰기가 없고 저장 구조가 현행과 다르다.
> 버킷을 되돌리는 운영 시나리오는 없다 — [environments.md](../configuration/environments.md) §3.

## 1. 환경 설정

번들 매핑 파일은 복사만(수정 불필요), 실값은 **루트 `.env`**(호스트)에서 관리한다:

```bash
cd dags/domains/commerce
cp .env.commerce.example .env.commerce      # 매핑만 담김
```

**루트 `.env` 의 `commerce 전용값` 블록**에서 설정:

```bash
# 루트 .env (호스트 — 600 권한 권장)
SEOUL_API_KEY_COMM=<발급키>          # #70 이관, 필수
COMMERCE_STORAGE_BACKEND=r2
COMMERCE_DBT_TARGET=prod
R2_BUCKET_NAME=seoul
TRINO_ICEBERG_CATALOG=iceberg
DBT_TARGET=prod
ASK_SEOUL_TARGET=prod
# JUSO_CONFM_KEY=<도로명주소 승인키>  # silver 지번 보강 쓰면
```

`R2_DEV_*` 는 **채우지 않는다.** 채우면 `common.storage.r2_env` 의 dev 우선 규칙 때문에 일부
기록기(실패 상세·처리량)가 다른 버킷으로 새어 나가고, 같은 날짜 기록이 두 버킷에 동시에
들어간다(ASK-Seoul#78 `Z-7`). 매핑·우선순위 규칙: [configuration.md](../configuration/configuration.md).

## 2. 의존성

`boto3`(R2) + `pandas`/`pyarrow`(silver) 가 호스트 이미지에 **이미 포함**되어 추가 설치 없이
동작한다([requirements.txt](../../requirements.txt) 는 명세용). 새 패키지가 필요해지면 이미지
변경은 번들 밖 — 운영 합의 후 반영. (s3fs 는 미설치이며 사용하지 않음)

## 3. 기동 & 검증

```bash
docker compose up -d
docker compose exec airflow-scheduler airflow dags trigger commerce_collect_raw   # 매 실행이 전체 수집
```

R2 적재 확인은 [deploy-dev.md](deploy-dev.md) §3 과 동일(버킷만 prod).

## 4. 보안 체크리스트

- [ ] `SEOUL_API_KEY_COMM`·`R2_*` 는 로그/경로/커밋에 노출 금지(CLAUDE.md §2.5)
- [ ] **루트 `.env`**(commerce 시크릿 이관처)는 `600`, 가능하면 시크릿 매니저 주입
- [ ] R2 토큰은 prod 버킷 한정·최소 권한
- [ ] webserver(UI) 직접 노출 금지 — 앞단 TLS 리버스 프록시(호스트 정책)

## 5. 백업 & 복구

- **metadata DB(Postgres)**: Airflow 상태/이력. 정기 `pg_dump` 또는 관리형 백업.
- **bronze(R2)**: 소스 truth. R2 버전닝/수명주기 정책 검토. bronze 가 살아있으면 silver 는
  **언제든 재처리로 복구** 가능([operations.md](operations.md)).
- 마커/run_id 폴더도 같은 R2(prod 버킷)의 `raw/commerce/` 아래에 있어 bronze 와 함께 보존된다.

> serving DB 가 없으므로 별도 서빙 백업 대상은 없다. 상태는 run_id 폴더의 마커.
