# 공통 에러 모듈 — RFC 9457 Problem Details + R2 적재

- 상태: 초안 (기획 — 팀 의견 수렴 중)
- 작성일: 2026-07-02
- 이슈: [#77](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/77) (논의) / 브랜치: `feat/77-common-error-module` (합의 후)
- 관련: [공통 HTTP 클라이언트](2026-07-02-feat-common-http-client.md) — HTTP 오류가 이 모듈의 주 생산자

## 배경 · 목표

도메인별로 에러 처리·기록 방식이 제각각(로그만 남김 / 태스크 실패만 / commerce는 마커+알림).
에러를 **표준 포맷으로 직렬화해 저장**하는 공통 모듈을 `dags/common/`에 만든다.

- 포맷: **RFC 9457 (구 RFC 7807) Problem Details** 준수
- 저장: **1차 R2** (경로 규약 아래). DB(Postgres) 저장 및 이중 저장 여부는 **추후 결정**(열린 질문 1)

## 에러 문서 포맷 (RFC 9457 + 확장 멤버)

```jsonc
{
  // ── RFC 9457 표준 멤버 ──
  "type": "https://github.com/ASAC-DE-bigkk/ASAC-DAG/errors/seoul-api-auth",  // 에러 유형 URI(안정 슬러그)
  "title": "Seoul OpenAPI authentication failed",       // 유형의 사람용 요약(유형당 고정)
  "status": 401,                                        // HTTP 유래 오류면 상태코드 (아니면 생략)
  "detail": "INFO-100: 인증키가 유효하지 않습니다",        // 이 발생 건의 구체 설명 (시크릿 redaction 필수)
  "instance": "urn:asac:run:commerce_localdata_elt:manual__2026-07-02T.../ingest_one:3",  // 발생 지점 URI
  // ── 확장 멤버 (RFC 9457 §3.2 허용) ──
  "domain": "commerce",
  "dag_id": "commerce_localdata_elt",
  "task_id": "ingest_one",
  "run_id": "manual__2026-07-02T...",
  "try_number": 3,
  "source_system": "seoul_openapi",                     // 외부 소스 유래면
  "request": {"method": "GET", "url_redacted": "http://openapi.seoul.go.kr:8088/***/json/..."},
  "occurred_at": "2026-07-02T03:12:45.123+00:00",
  "schema_version": "v1"
}
```

- `type` 레지스트리: `common/errors/types.py`에 슬러그·title을 상수로 등록(오타 방지, 유형 카탈로그 겸용).
  미등록 예외는 `.../errors/unhandled`로 수렴.
- **redaction 필수**: `detail`/`request`에 키·토큰이 절대 들어가지 않게 저장 직전 공통 redact 적용
  (commerce `include/security/redaction.py`를 `dags/common/security/`로 승격해 재사용 — [adoption 가이드](../../domains/commerce/docs/security/adoption.md) 있음).

## R2 저장 경로 규약 (안)

```
errors/<domain>/<dag_id>/observed_date=YYYY-MM-DD/<run_id>/<occurred_at>_<type-slug>.json
```

- run 단위로 모여 재수집·감사에 쓰기 좋고, 날짜 파티션으로 수명주기(보존기간) 관리 용이
- 추후 DB/Iceberg 적재 시 이 JSON이 원본(bronze 관점) 역할 — 이중 저장 결정과 무관하게 재처리 가능

## 모듈 구성 (안) — `dags/common/errors/`

| 파일 | 역할 |
|---|---|
| `problem.py` | `Problem` dataclass + `to_dict()/from_exception()` 변환 |
| `types.py` | type 슬러그 레지스트리 (도메인 공통 + 소스별) |
| `sink.py` | R2 writer (경로 규약, redact 적용). 추후 `DbSink` 추가 자리 |
| `airflow.py` | `on_failure_callback` 팩토리 — DAG에 한 줄로 연결 |

수집 경로 2가지:
1. **자동**: 태스크 실패 시 `on_failure_callback`이 예외 → Problem 변환·적재
2. **명시적**: HTTP 클라이언트 등이 재시도 끝에 던지는 typed 예외(`ProblemError`)가 자기 Problem을 보유

## 단계별 계획 (합의 후)

1. `common/errors/` 구현 + 단위 테스트 (redaction·경로·직렬화)
2. 파일럿 1개 도메인 적용(추천: traffic — DAG 1개, 구조 단순) → 실패 유도 e2e 확인
3. 전 도메인 DAG에 callback 연결 (도메인별 커밋)
4. (추후) DB 저장/이중 저장 결정 → `DbSink` 추가

## 열어둔 질문 (합의 필요)

1. DB 저장·이중 저장 여부 — **추후 결정으로 합의됨** (R2 선행)
2. 기록 범위: 태스크 실패 전부 + HTTP typed 예외? 계약 위반(검증 실패) 같은 비예외 이벤트도 포함할지
3. `type` URI 형식: GitHub URL 기반(위 예시) vs `urn:asac:error:<slug>` — 실접속 가능 URL이면 문서 링크 겸용 가능
4. 보존 기간(수명주기 규칙) 및 dev/prod 버킷 분리
5. 알림(notify)과의 연동 — commerce notify 승격과 묶을지, 별도 기획으로 뺄지
