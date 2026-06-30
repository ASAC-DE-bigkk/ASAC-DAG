# Share.md — 공유 자료 인덱스

컨텍스트가 분리(새 세션/다른 프로젝트/다른 에이전트)될 때, **이 파일 하나만** 공유하면
핵심 규약·계약·자료에 접근할 수 있도록 모아 둔 진입점이다. (경로는 이 파일 위치
`dags/domains/commerce/` 기준. 새 자료가 생기면 여기에 링크를 추가한다.)

> **작업 경계**: commerce 변경은 **`dags/domains/commerce/` 안에서만**. `dags/` 는 git
> 서브모듈(ASAC-DAG)이라 루트 `.env`·`docker-compose.yml`·`Dockerfile.airflow` 는 번들
> 밖이다. 필요한 환경변수는 루트 `.env` 가 아니라 **이 번들의 `.env.commerce`** 로 공급한다.

## 1. 구조 규약 (heritage — 다른 카테고리도 따라갈 것)

| 자료 | 내용 |
|---|---|
| [docs/project_setting.md](docs/architecture/project_setting.md) | **dags 폴더 구성 규약** — `dags/domains/<category>/` 자립 단위(include/config/tests/docs), DAG 자기-부트스트랩 import + env 적재, `dags/` 만 옮겨도 실행되는 이식성 |
| [CLAUDE.md](CLAUDE.md) (Working Scope·§19) | 작업 경계 + 위 규약의 에이전트(Claude/Codex)용 요약 — 작업 시 준수 |

## 2. 실행 인자 / 환경변수 (정상 동작 조건)

| 자료 | 내용 |
|---|---|
| [docs/configuration.md](docs/configuration/configuration.md) | **필요한 환경변수 전체 + `.env.commerce` 주입 방식** — 환경이 바뀌며 빠진 값(SEOUL_OPENAPI_KEY 등) 정리 |
| [.env.commerce.example](.env.commerce.example) | 환경변수 템플릿(실파일 `.env.commerce` 는 gitignore) |
| [include/common/env.py](include/common/env.py) · [include/common/settings.py](include/common/settings.py) | env 로더 · 설정 dataclass |

## 3. 파이프라인 계약 (commerce 카테고리)

| 자료 | 내용 |
|---|---|
| [docs/common_info.md](docs/pipeline/common_info.md) | 공통 19컬럼·`UPDATEDT` 검증·저장/마커 계약·39종 카탈로그·재수집(backfill)·서비스명 채우기 |
| [docs/bronze/](docs/pipeline/bronze/) | 원천 수집 실호출 분석 — 페이지네이션 정렬(컬럼 정렬 없음) · API 호출량(1,361회/수집, 39종) · 영업상태 추적 모델(1행 in-place) · 수집 불가 원인·해소(39종 전 종 해소) |
| [docs/README.md](docs/README.md) | 카테고리 코드 위치·문서 인덱스·빠른 실행 |
| [config/dataset_registry.yaml](config/dataset_registry.yaml) | 수집 대상 단일 진실 공급원 |

## 4. 프로젝트 운영

| 자료 | 내용 |
|---|---|
| [README.md](README.md) | 개요·환경 모델·빠른 시작·프로젝트 구조 |
| [docs/environments.md](docs/configuration/environments.md) · [docs/storage.md](docs/architecture/storage.md) | 환경(local/r2) 분리 · 저장 경로/R2 |
| [docs/deploy-local.md](docs/operations/deploy-local.md) · [docs/deploy-dev.md](docs/operations/deploy-dev.md) · [docs/deploy-prod.md](docs/operations/deploy-prod.md) | 배포 |
| [docs/architecture.md](docs/architecture/architecture.md) · [docs/operations.md](docs/operations/operations.md) | 아키텍처 · 운영 런북 |
| [docs/recollect-and-alerts.md](docs/operations/recollect-and-alerts.md) | 재수집 DAG · 알림 인터페이스(비활성) · API별 진행 가시성 |
| [docs/security/security.md](docs/security/security.md) | **보안 대응** — 시크릿 마스킹·입력검증·정적점검 + **단일 포인트 종합검증**(`python -m security`). 코드: [include/security/](include/security/) (stdlib·이식 가능) |
| [change-log.md](change-log.md) | 변경 이력(작성일·순서 내림차순) |

## 핵심 한 줄 요약

- **DB·외부 매니페스트 없음**: 수집 상태·이력은 run_id 폴더의 마커(`_markers/<short>.completed|.incomplete`).
- **이식성**: DAG 가 자기 `include` 를 sys.path 에 부트스트랩 + `.env.commerce` 를 적재 →
  `dags/` 만 옮기면 코드도 인자도 함께 따라온다.
- **카테고리 자립**: `dags/domains/commerce/` 안에 코드·설정·테스트·문서·규약(CLAUDE.md·Share.md)·
  런타임 인자(`.env.commerce`)가 전부.
