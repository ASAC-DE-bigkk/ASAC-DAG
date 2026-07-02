# 환경변수 관리 Infisical 전환 (.env → app.infisical.com)

- 상태: 진행 중 — B안(과도기) 가동 완료 2026-07-02, A안 정착 대기
- 작성일: 2026-07-02
- 이슈: [#76](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/76) (논의) / 브랜치: `feat/76-infisical-secrets` (합의 후)
- 선행: [#70 env 키 통합](2026-07-02-feat-env-key-unification.md) (PR #72) — 통일된 키 이름 체계 위에 도입

## 배경 · 목표

현재 시크릿(소스 API 키, R2 자격증명, Airflow/Postgres 비밀번호)은 **각자 로컬 `.env` 파일**로
관리한다. 문제:

- 키 공유가 사이드채널(메신저 등)로 이루어짐 — 온보딩·키 회전 시마다 전원 수동 반영 (#70에서 실제 발생)
- 값 불일치 감지 불가 (누가 어떤 값을 쓰는지 모름)
- 감사(audit)·버전 이력 없음, 유출 시 회전 절차 부재

→ **Infisical Cloud(app.infisical.com)** 를 단일 소스로 삼아 시크릿을 중앙 관리한다.
`.env`는 생성물(캐시)로 격하되거나 제거된다.

## 현황 (전환 대상 인벤토리)

루트 `.env` 기준 — #70 통합 후:

| 그룹 | 키 | 비고 |
|---|---|---|
| 소스 API | `SEOUL_API_KEY_{COMM,TRAN,PPLT,CULT,TRIC}`, `PUBLIC_DATA_API_KEY_BUS`, `KOPIS_SERVICE_KEY`, `KMA_SERVICE_KEY` | 도메인별 발급 — 담당자 소유 |
| R2/Iceberg | `R2_*`, `R2_DEV_*`, `CLOUDFLARE_*`, `WRANGLER_R2_SQL_AUTH_TOKEN` | prod/dev 세트 분리돼 있음 |
| Airflow 런타임 | `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY`, `AIRFLOW_ADMIN_*` | 인프라 시크릿 |
| Postgres | `POSTGRES_*` | 〃 |
| 비밀 아님(설정) | `TRINO_ICEBERG_CATALOG`, `SMOKE_SCHEMA`, `ASK_SEOUL_*`, `KMA_BASE_URL`, `SEOUL_ACC_INFO_*`, `DBT_TARGET` 등 | 시크릿 아님 — Infisical에 같이 둘지 결정 필요(열린 질문 3) |
| commerce 번들 | `.env.commerce` (STORAGE_BACKEND, `${R2_DEV_*}` 참조 등) | 시크릿 아님(인증키는 #70에서 루트로 이관됨) — 전환 대상 제외 후보 |

## 접근 방식 후보

compose가 `env_file: .env`로 전 컨테이너에 주입하는 현 구조를 어디서 대체할지에 따라 3안:

### A안 — `infisical run` 래핑 (권장)
```bash
infisical run --projectId <id> --env dev -- docker compose up -d
```
- Infisical CLI가 시크릿을 프로세스 env로 주입 → compose `environment:`/`env_file` 대신 변수 참조(`${VAR}`)로 전달
- `.env` 파일이 디스크에 남지 않음(최소 노출). compose 쪽은 `env_file` → 변수 참조로 수정 필요
- 로컬 인증: `infisical login`(개인), CI/서버: Machine Identity(Universal Auth)

### B안 — `infisical export` 로 .env 생성 (전환 부담 최소)
```bash
infisical export --projectId <id> --env dev --format dotenv > .env
```
- compose·코드 수정 **0건** — `.env`를 손으로 만들던 것을 명령 한 줄로 대체
- 디스크에 평문 `.env`가 여전히 남음(현행과 동일 수준). 과도기 단계로 적합

### C안 — Airflow Secrets Backend / Infisical Agent
- Airflow connections/variables 수준 통합 또는 사이드카 에이전트가 주입
- 현 파이프라인은 `os.environ` 직접 참조라 적용 범위가 안 맞고 운영 부담 큼 — **비추천(현 단계)**

**권장 경로: B안(과도기, 즉시) → A안(정착)**. 두 안 모두 코드의 `os.environ` 참조는 무변경.

## 진행 기록 (2026-07-02)

- [x] Infisical Cloud 셋업: Organization·Project 생성, dev 환경에 시크릿 51키 업로드 (사용자)
- [x] CLI 설치 + `infisical login` + `.infisical.json` 생성·커밋 (sample#6)
- [x] **B안 가동**: `infisical export --env dev --format dotenv > .env` → 키 51개 백업과 완전 일치 확인
  → 컨테이너 재기동 → 컨테이너 env 정상(따옴표 오염 0) → `traffic_incident_bronze` 트리거 success
- ⚠️ 운영 주의: PowerShell `>` 리다이렉트는 UTF-16 저장 — export 는 **Git Bash에서** 실행할 것
- [ ] 팀원 전파 (login → export → 재기동), 개인값은 Personal Override 사용
- [ ] A안 정착 (compose env_file 제거 + infisical run 래퍼) — 별도 브랜치
- [ ] CI/서버용 Machine Identity 발급

## 단계별 계획 (합의 후 이슈화)

1. Infisical 조직/프로젝트 셋업 — 프로젝트 1개(`seoul-elt` 등), 환경 `dev`/`prod`, 폴더로 그룹 분리(`/source-api`, `/r2`, `/airflow`, `/postgres`)
2. 현행 `.env` 값 업로드 + 팀원 초대(권한: 도메인 담당자는 자기 소스 키 write, 나머지 read)
3. B안 적용: README/온보딩 문서를 "`infisical export > .env`"로 갱신, `.env.example`은 키 목록 문서로 유지
4. A안 전환: compose `env_file` 제거 → `environment: ${VAR}` 참조로 수정, 실행 스크립트(`scripts/up.ps1` 등) 제공
5. CI/원격 배포용 Machine Identity 발급·문서화
6. 검증: 신규 클론 → infisical 인증 → 기동 → DAG 트리거 e2e

## 리스크

- **오프라인/장애 시 기동 불가**(A안): Infisical Cloud 의존 — export 캐시(fallback) 절차 병기 필요
- CLI 설치가 전원 필수 (Windows: scoop/winget, 컨테이너엔 불필요 — 호스트에서 주입)
- 무료 플랜 제한(멤버 수·환경 수) 확인 필요
- `.env.commerce` 등 번들 파일과의 우선순위 규칙은 현행 유지(프로세스 env 우선이라 Infisical 주입값이 자연히 이김)

## 열린 질문 (합의 필요)

1. 프로젝트/환경 구조 — 단일 프로젝트 + dev/prod 환경 2개로 충분한가? (도메인별 폴더 분리안 포함)
2. 전환 경로 — B안(과도기)→A안(정착) 2단계로 갈지, 바로 A안으로 갈지
3. 시크릿 아닌 설정값(`TRINO_ICEBERG_CATALOG` 등)도 Infisical로 옮길지, `.env.config`(비밀 아님, 커밋 가능)로 분리할지
4. 결제/플랜 — 무료 플랜으로 시작? 조직 계정 소유자는 누구?
