# 공통 통합 로드맵 (Common Integration Roadmap)

> 도메인별로 개별 작업한 결과물을 `dags/common` 공통 패키지로 통합하기 위한 로드맵.
> 이 문서는 전체 방향·현황·결정 기록만 담는 **인덱스**이며,
> **개별 작업 계획은 [plans/](plans/README.md)에 `YYYY-MM-DD-<slug>.md`로 1건씩 작성한다.**

- 상태: 초안 (Draft)
- 작성일: 2026-07-02
- 관련 이슈: (생성 후 링크)

---

## 1. 배경

6개 도메인(commerce, culture, population, traffic, transit, weather)이 각자
독립적으로 개발되어, HTTP 클라이언트 / R2 랜딩 / 설정 로딩 / Trino 접근 등
동일한 관심사의 코드가 도메인마다 중복 구현되어 있다.

## 2. 현황 — 도메인별 공통성 모듈

| 도메인 | 위치 | 모듈 |
|---|---|---|
| commerce | `domains/commerce/include/common/` | env, hashing, notify, paths, registry, schemas, settings, storage |
| culture | `domains/culture/culture_ingest/common/` | checks, config, http, landing, records, warehouse |
| population | `domains/population/ppltn_ingest/common/` | bronze, config, http, landing, trino |
| traffic | `domains/traffic/traffic_ingest/common/` | runtime |
| transit | `domains/transit/seoul_transit/` | api, config, r2_landing, records |
| weather | `domains/weather/weather_ingest/common/` | runtime |

## 3. 통합 후보 (관심사별)

| 관심사 | 중복 구현 | 통합 목표 |
|---|---|---|
| HTTP / API 클라이언트 | culture `http`, population `http`, transit `api`, commerce `clients` | `common/http.py` (TBD) |
| R2 / 오브젝트 스토리지 랜딩 | culture `landing`, population `landing`, transit `r2_landing`, commerce `storage` | `common/landing.py` (TBD) |
| 설정 / 환경변수 로딩 | culture·population·transit `config`, commerce `env`·`settings`, traffic·weather `runtime` | `common/config.py` (TBD) |
| Trino / 웨어하우스 접근 | culture `warehouse`, population `trino` | `common/warehouse.py` (TBD) |
| 레코드 유틸 | culture `records`, transit `records` | (TBD) |
| 알림 (notify) | commerce `notify` | (TBD) |

## 4. 작업 원칙

1. **Plan-first**: 코드 착수 전 [plans/](plans/README.md)에 작업별 계획 문서를 작성·합의한다.
   (1 작업 = 1 계획 문서, 파일명은 브랜치명과 대응)
2. **Issue-first**: 1 이슈 = 1 브랜치. 조직 이슈 템플릿 사용.
3. 도메인 DAG 동작을 깨지 않는다 — 통합은 관심사 단위로 점진적으로 진행하고,
   각 단계에서 기존 도메인 코드는 공통 모듈 위임(re-export) 후 제거한다.
4. 각 도메인의 기존 설계 의도(git 이력)를 확인한 뒤 통합안을 낸다.

## 5. 단계별 계획

각 단계 착수 시 [plans/](plans/README.md)에 상세 계획 문서를 만들고 여기서 링크한다.

- [x] Step 0 — 스캐폴딩: `dags/common/`, `dags/docs/`, 본 계획 문서 생성
- [ ] Step 0.5 — 계약(contract) 통일 (코드 통합 전 선행):
  - [x] 서울 API 키 환경변수 이름 통합 → [plans/2026-07-02-feat-env-key-unification.md](plans/2026-07-02-feat-env-key-unification.md) (PR #72 리뷰 중)
  - [x] DAG 네이밍 규칙 정의·적용 → [plans/2026-07-02-feat-dag-naming-convention.md](plans/2026-07-02-feat-dag-naming-convention.md) (PR #74 리뷰 중)
  - [ ] 저장 계약 — R2 원본 `raw/` 전환 → [plans/2026-07-02-feat-r2-raw-prefix.md](plans/2026-07-02-feat-r2-raw-prefix.md) (#75 합의, commerce 마이그레이션 방식 대기)
  - [ ] 환경변수 관리 Infisical 전환 → [plans/2026-07-02-feat-infisical-secrets.md](plans/2026-07-02-feat-infisical-secrets.md) (기획 초안)
- [ ] Step 1 — 관심사별 상세 비교 분석 (도메인별 구현 차이·의도 파악)
- [ ] Step 2 — 통합 우선순위 및 인터페이스 합의:
  - [ ] 공통 HTTP 클라이언트 기획 → [plans/2026-07-02-feat-common-http-client.md](plans/2026-07-02-feat-common-http-client.md)
  - [ ] 공통 에러 모듈 기획 (RFC 9457 + R2) → [plans/2026-07-02-feat-common-error-module.md](plans/2026-07-02-feat-common-error-module.md)
- [ ] Step 3 — 관심사 단위 통합 + 도메인별 전환 (TBD)
- [ ] Step 4 — 중복 코드 제거 및 문서화 (TBD)

## 6. 결정 기록 (Decision Log)

| 날짜 | 결정 | 근거 |
|---|---|---|
| 2026-07-02 | 공통 패키지 위치를 `dags/common/`으로 확정 | 도메인(`dags/domains/`)과 대칭 구조, Airflow dags 경로에서 직접 import 가능 |
| 2026-07-02 | 계획 문서를 단일 파일에 누적하지 않고 `docs/plans/YYYY-MM-DD-<slug>.md`로 작업별 분리 | 계획이 계속 쌓이면 단일 문서는 비대해짐. 날짜로 시간순 정렬 + slug로 feat 식별, 브랜치명과 대응 |
| 2026-07-02 | 서울 API 키는 `SEOUL_API_KEY_<도메인약어>` 규칙으로 루트 `.env`에서 일원 관리, commerce 키도 `.env.commerce`에서 루트로 이관 | 이름 충돌(commerce/culture 동명 키)로 commerce가 culture 키로 호출하던 버그 해소. 번들 자립 의도(#70, commerce change-log #19)는 인증키에 한해 부분 폐기 — 사용자 승인 |
| 2026-07-02 | DAG 네이밍 규칙 `<domain>_<dataset>_<stage>` 확정 (#73) — stage는 역할형(bronze/transform/elt/recollect/smoke), dataset 생략은 옵션(확장 가능성 있으면 명시 권장), 공통 DAG는 `common` 접두, 파일명=dag_id | 접두(`seoul_`/도메인/소스)·단계 표기 혼재 해소. 옛 dag_id 실행 이력은 메타DB에 보존 |
| 2026-07-02 | (재검토) transit `*_elt` → `*_bronze` 재변경 — elt는 DAG 안 일괄 변환에만 사용. 저장 계약(테이블·경로) 네이밍은 별도 이슈로 분리 | transit 실동작은 bronze 한정(변환은 ASAC-DBT 몫, docstring에 의도 명시) — 같은 접미사의 이중 의미(EL만 vs 일괄) 차단 |
| 2026-07-02 | (#75 합의) R2 오브젝트 원본 경로는 `raw/` 채택, dag_id·Iceberg 테이블명의 `bronze`는 유지 | 용어 구분 확정: raw=R2 랜딩 원본, bronze=Iceberg 웨어하우스 원본층. PR #74 리뷰(@yooseongjin527) 제안 수용 |
