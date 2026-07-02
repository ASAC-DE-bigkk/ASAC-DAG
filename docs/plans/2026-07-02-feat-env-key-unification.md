# 서울 API 키 환경변수 이름 통합

- 상태: 진행 중 (구현 완료 — PR/머지 대기)
- 작성일: 2026-07-02
- 이슈: [#70](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/70) / 브랜치: `feat/70-env-key-unification`
- 담당 항목: 사용자 이슈 초안 1번 + 2번

## 배경 · 목표

도메인별로 각자 만든 서울 열린데이터 API 키 환경변수 이름이 제각각이고
(`SEOUL_API`, `SEOUL_API_KEY`, `SEOUL_OPENAPI_KEY`×2, `SEOUL_OPEN_API_KEY`),
`SEOUL_API_KEY_<도메인약어>` 규칙으로 통합한다.

**조사에서 확인된 실버그**: commerce(`.env.commerce`)와 culture(루트 `.env`)가
**같은 이름 `SEOUL_OPENAPI_KEY`를 서로 다른 키 값으로** 사용 중. compose가 루트
`.env`를 컨테이너에 주입하고 commerce 로더(`include/common/env.py`)는 setdefault
(프로세스 env 우선)이므로, **commerce DAG가 culture 키로 호출하는 충돌 상태** —
이번 rename이 이를 해소한다.

## 1. Rename 매핑 및 영향 범위

| 담당 | 도메인 | 기존 → 신규 | 위치 | 코드 수정 지점 |
|---|---|---|---|---|
| 상용 | commerce | `SEOUL_OPENAPI_KEY` → `SEOUL_API_KEY_COMM` | `.env.commerce` | `include/common/settings.py:72`, `include/bronze/clients.py:85`, `include/bronze/resolve.py`, `tests/test_security.py`, `.env.commerce.example`, 번들 docs 다수, `change-log.md` 기록 |
| 정현 | transit | `SEOUL_API` → `SEOUL_API_KEY_TRAN` | 루트 `.env` | `seoul_transit/config.py:41` (`load_key` 기본값), `docs/subway_source.md`, `docs/parking_source.md` |
| 경민 | population | `SEOUL_API_KEY` → `SEOUL_API_KEY_PPLT` | 루트 `.env` | `ppltn_ingest/source/config.py:21`, `seoul_ppltn_collect.py` docstring, `README.md` |
| 성진 | culture | `SEOUL_OPENAPI_KEY` → `SEOUL_API_KEY_CULT` | 루트 `.env` | `culture_ingest/source/config.py:17`, `culture_bronze_ingest.py` docstring, `README.md` |
| 성헌 | traffic | `SEOUL_OPEN_API_KEY` → `SEOUL_API_KEY_TRIC` | 루트 `.env` | `traffic_ingest/acc_info.py:49`, `docs/source.md` |
| — | transit(bus) | `PUBLIC_DATA_API_DE` → `PUBLIC_DATA_API_KEY_BUS` | 루트 `.env` | `seoul_transit/config.py:49` (`load_bus_key` 기본값), `seoul_bus_elt.py` docstring, `docs/bus_source.md` |
| — | — | `TRAIN_API` **삭제** (미사용) | 루트 `.env` | 코드 참조 없음 확인 완료 — `.env`에서 제거만 하면 됨 |

확인된 사항:
- `docker-compose.yml`은 `env_file`로 `.env` 전체를 주입하고 개별 변수 매핑이 없음 → compose 수정 불필요.
- 신규 이름 전부 `KEY`를 포함 → commerce 보안 모듈 자동 마스킹 정규식(`redaction.py`) 유지됨.
- 루트 `.env.example`에는 해당 키들이 없음 → (선택) 이번에 신규 이름으로 추가해 팀 온보딩 개선 가능.
- commerce는 env-var 계약 변경이므로 번들 규칙(CLAUDE.md §19)에 따라 `change-log.md`에 기록 필요.

## 2. `.env.commerce` → 루트 `.env` 통합 가능 여부 (조사 결과)

**기술적으로는 가능** — 로더가 "파일 없으면 조용히 스킵"하도록 설계돼 있어 루트로 옮겨도 동작한다.
단, `.env.commerce` 분리는 **의도적 설계**로 확인됨:

- 출처: 커밋 `472e485` (commerce 번들 도입) + `domains/commerce/CLAUDE.md` §19
- 의도: **번들 자립(portability)** — "dags/를 다른 Airflow 프로젝트에 옮겨도 번들 안에서 완결"
  이 명시 규칙이고, "**commerce 변수를 호스트 루트 `.env`에 추가하지 말 것**"이 문서화된 금지사항.
- `${R2_DEV_*}` 참조 치환 등 루트 `.env`와의 연결 장치도 이 의도에 맞춰 구현돼 있음.

→ **결정(2026-07-02, 사용자 승인)**: 서울 인증키는 루트 `.env`로 **이관**(의도 부분 폐기),
`SEOUL_OPENAPI_BASE_URL`은 `settings.py` 기본값과 동일하므로 삭제. 스토리지 등 나머지
commerce 전용 값은 `.env.commerce`에 유지(번들 자립 구조 자체는 존속). commerce
`change-log.md` #19에 계약 변경 기록.

## 3. 작업 절차 및 결과

1. [x] 루트 `.env` 키 rename + `TRAIN_API` 삭제 + `.env.example`에 신규 이름 추가
2. [x] 도메인별 코드 기본값/상수 rename + commerce 키 루트 `.env` 이관 (34개 파일)
3. [x] 도메인별 docstring·docs rename, commerce `change-log.md` #19 기록
4. 검증:
   - [x] commerce 보안 게이트 PASS (blocking 0건) + `test_security.py` 30개 통과
   - [x] 수정된 전 파이썬 파일 `py_compile` 통과, 옛 이름 잔존 0건 (change-log 이력 제외)
   - [x] traffic/weather 테스트 통과. commerce `test_markers`/`test_bronze_tasks` 8건 실패는
     **선행 feat/59에서 이미 깨져 있던 것**(테스트 더블 `delete` 미구현) — 본 작업과 무관, dev에서 재현 확인
   - [x] 컨테이너 재기동 후 실동작 확인 (2026-07-02): DAG import 에러 0건, 키 영향
     DAG 6개(population/traffic/subway/parking/bus/culture) 수동 트리거 **전부 success**,
     commerce 는 `bronze.resolve verify` **39/39 인증 성공** — 충돌 해소로 자기 키 사용 확인.
     테스트 후 전 DAG paused 원복

## 리스크 · 열어둔 질문

- `.env`는 gitignore 대상(로컬 파일) — **팀원 각자 자기 `.env`를 수동으로 rename해야 함**. 공지 필요.
- 컨테이너 재기동 필요 (env_file은 기동 시점 주입).
- population의 기존 이름 `SEOUL_API_KEY`가 신규 이름들의 접두사와 겹치므로, 전환 기간에
  옛 이름 fallback을 두지 말고 **일괄 전환**할 것 (fallback이 있으면 혼선 가중).
- (열림) `.env.commerce` 통합 여부 — §2 사용자 결정 대기.
