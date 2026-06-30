# Deploy — prod (스토리지 = Cloudflare R2 prod 버킷)

prod 는 dev 와 **버킷·자격증명을 분리**한다(`seoul-prod`, 전용 토큰). 호스트 스택 자체는
dev 와 동일한 단일 컴포즈이므로, 분리는 **`.env.commerce` 의 `R2_BUCKET`/`R2_*` 값**과
운영 정책(시크릿 관리·백업)으로 보장한다.

> 환경 축: [environments.md](../configuration/environments.md). R2 발급: [storage.md](../architecture/storage.md).
> 전체 변수: [configuration.md](../configuration/configuration.md).

## 분리 원칙 요약

- 스토리지: R2 **prod 버킷**(`STORAGE_BACKEND=r2`, `R2_BUCKET=seoul-prod`, dev `seoul-dev`와 분리)
- 자격증명: prod 전용 R2 토큰(권한 최소화), dev 와 분리
- 시크릿: `.env.commerce` 는 `600` 권한·시크릿 매니저 주입 권장, 커밋 금지(gitignore)

## 1. 환경파일

```bash
cd dags/domains/commerce
cp .env.commerce.example .env.commerce
chmod 600 .env.commerce
```

```bash
SEOUL_OPENAPI_KEY=<발급키>
STORAGE_BACKEND=r2

# R2 블록: 루트 .env 의 prod 키를 참조(권장) — 템플릿의 ${R2_DEV_*} 를 prod 키로 바꾼다:
R2_ENDPOINT=${R2_ENDPOINT}
R2_BUCKET=${R2_BUCKET_NAME}                 # 루트 prod 버킷(예: seoul). prod 전용이면 명시값으로
R2_ACCESS_KEY_ID=${R2_ACCESS_KEY_ID}
R2_SECRET_ACCESS_KEY=${R2_SECRET_ACCESS_KEY}
R2_REGION=auto
```

> 루트 `.env` 의 prod 자격증명을 쓰지 않고 prod 전용 토큰/버킷을 분리하려면 위 참조 대신
> 실제 값을 직접 적는다(`R2_BUCKET=seoul-prod` 등). 참조 규칙: [configuration.md](../configuration/configuration.md).

## 2. 의존성

`boto3`(R2) + `pandas`/`pyarrow`(silver) 가 호스트 이미지에 **이미 포함**되어 추가 설치 없이
동작한다([requirements.txt](../../requirements.txt) 는 명세용). 새 패키지가 필요해지면 이미지
변경은 번들 밖 — 운영 합의 후 반영. (s3fs 는 미설치이며 사용하지 않음)

## 3. 기동 & 검증

```bash
docker compose up -d
docker compose exec airflow-scheduler airflow dags trigger seoul_commerce_daily   # 매 실행이 전체 수집
```

R2 적재 확인은 [deploy-dev.md](deploy-dev.md) §3 과 동일(버킷만 prod).

## 4. 보안 체크리스트

- [ ] `SEOUL_OPENAPI_KEY`·`R2_*` 는 로그/경로/커밋에 노출 금지(CLAUDE.md §2.5)
- [ ] `.env.commerce` 는 `600`, 가능하면 시크릿 매니저 주입
- [ ] R2 토큰은 prod 버킷 한정·최소 권한
- [ ] webserver(UI) 직접 노출 금지 — 앞단 TLS 리버스 프록시(호스트 정책)

## 5. 백업 & 복구

- **metadata DB(Postgres)**: Airflow 상태/이력. 정기 `pg_dump` 또는 관리형 백업.
- **bronze(R2)**: 소스 truth. R2 버전닝/수명주기 정책 검토. bronze 가 살아있으면 silver 는
  **언제든 재처리로 복구** 가능([operations.md](operations.md)).
- 마커/run_id 폴더도 같은 R2(prod 버킷)의 `bronze/commerce/` 아래에 있어 bronze 와 함께 보존된다.

> serving DB 가 없으므로 별도 서빙 백업 대상은 없다. 상태는 run_id 폴더의 마커.
