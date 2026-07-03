# 공통 에러 모듈 — RFC 9457 Problem Details + R2 적재

- 상태: 진행 중 (합의 완료 — 이슈 #77 논의 반영)
- 작성일: 2026-07-02 (개정: 2026-07-03 — 팀 합의 반영)
- 이슈: [#77](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/77) / 브랜치: `feat/77-common-error-module`
- 관련: [공통 HTTP 클라이언트](2026-07-02-feat-common-http-client.md) — HTTP 오류가 이 모듈의 주 생산자

## 배경 · 목표

도메인별로 에러 처리·기록 방식이 제각각(로그만 남김 / 태스크 실패만 / commerce는 마커+알림).
에러를 **표준 포맷으로 직렬화해 저장**하는 공통 모듈을 `dags/common/errors/`에 만든다.

## 범위 (in / out) — #77 합의

**In**: 재시도 소진 후 최종 실패(태스크 실패 + HTTP typed 예외)의 기록.

**Out** (별도 기획으로 분리):
- network I/O(request/response 세트)·file I/O 이력 — "실행 이력(observability)" 기획으로 분리, 계약 위반 등 비예외 이벤트도 그때 재론
- 알림(notify) 연동 — commerce notify 승격 건과 함께 별도 기획
- DB(Postgres) 저장·이중 저장 — 추후 결정 (`DbSink` 자리만 남김)
- common 전반의 테스트 방법론 — 이 이슈 범위 밖

## 에러 문서 포맷 (RFC 9457 + 확장 멤버)

```jsonc
{
  // ── RFC 9457 표준 멤버 ──
  "type": "urn:asac:error:api-timeout",       // 영구 불변 식별자(검색 키) — #77 합의
  "title": "External API request timed out",  // 유형의 사람용 요약(유형당 고정)
  "status": 504,                              // HTTP 유래 오류면 상태코드 (아니면 생략)
  "detail": "TimeoutError: request timeout",  // 이 발생 건의 구체 설명 (redaction 필수)
  "instance": "urn:asac:run:traffic_incident_bronze:scheduled__.../land_seoul_traffic_raw:3",
  // ── 확장 멤버 (RFC 9457 §3.2 허용) ──
  "domain": "traffic",
  "dag_id": "traffic_incident_bronze",
  "task_id": "land_seoul_traffic_raw",
  "run_id": "scheduled__2026-07-03T...",
  "try_number": 3,
  "source_system": "seoul_topis",             // 외부 소스 유래면
  "request": {"method": "GET", "url": "...(redacted)"},
  "occurred_at": "2026-07-03T03:12:45.123456+00:00",
  "docs_url": "https://github.com/.../docs/errors/api-timeout.md",  // 문서 위치(있으면)
  "schema_version": "v1"
}
```

- **`type` = `urn:asac:error:<slug>`** (#77 합의): GitHub URL은 repo 이름·문서 경로 변경 시
  과거 기록의 URI가 달라져 같은 에러의 검색이 깨진다. 식별(type)과 문서 위치(docs_url)를
  한 칸에 섞지 않는다 — 문서 링크는 별도 확장 멤버 `docs_url`에 저장 시점 스냅샷으로 기록
  (옛 기록의 docs_url이 dead link가 되어도 식별·검색에는 무해).
- `type` 레지스트리: `common/errors/types.py`에 슬러그·title·docs_url을 상수로 등록
  (오타 방지, 유형 카탈로그 겸용). 미등록 예외는 `urn:asac:error:unhandled`로 수렴.
- **redaction 필수**: `detail`/`request`에 키·토큰이 절대 들어가지 않게 저장 직전 공통 redact 적용
  — commerce `include/security/redaction.py`를 `dags/common/security/`로 승격 재사용
  (내용 무수정 사본 — 원본 수정 후 재복사로 동기화, [adoption 가이드](../../domains/commerce/docs/security/adoption.md)).

## R2 저장 경로 규약 — #77 합의 (날짜-우선 파티션)

```
errors/observed_date=YYYY-MM-DD/domain=<domain>/dag_id=<dag_id>/<run_id>__<HHMMSSffffff>_<type-slug>.json
```

- **날짜-우선**: "오늘 전 도메인에서 뭐 실패했나" 스윕과 추후 Trino/Iceberg 날짜 파티션 프루닝에 유리.
- **per-error JSON** (rolling JSONL 불채택): R2는 append 불가라 rolling은 컴팩션 계층이 추가로 필요하고,
  에러는 재시도 소진 후에만 기록되어 발생량이 적어 작은 파일 문제가 미미. 대량화되면 재검토.
- `run_id`의 `:` `+` 등 예약문자는 `-`로 정규화(원본 run_id는 문서 본문에 보존).
- 보존 3개월(수명주기 규칙) + dev/prod 버킷 분리(`R2_DEV_*` 환경변수 규약).
- 추후 DB/Iceberg 적재 시 이 JSON이 원본(bronze 관점) 역할 — 재처리 가능.

## 모듈 구성 — `dags/common/errors/`

| 파일 | 역할 |
|---|---|
| `problem.py` | `Problem` dataclass + `to_dict()/from_exception()` 변환, `ProblemError` typed 예외 |
| `types.py` | type 슬러그 레지스트리 (slug·title·docs_url, 예외 클래스 → 유형 분류) |
| `sink.py` | R2 writer (경로 규약, 저장 직전 redact). 추후 `DbSink` 추가 자리 |
| `airflow.py` | `on_failure_callback` 팩토리 — DAG에 한 줄로 연결 |

수집 경로 2가지:
1. **자동**: 태스크 실패 시 `on_failure_callback`이 예외 → Problem 변환·적재
2. **명시적**: HTTP 클라이언트 등이 재시도 끝에 던지는 typed 예외(`ProblemError`)가 자체 Problem을 보유

## 단계별 계획

1. [x] `common/errors/` 구현 + `common/security/` 승격 + 단위 테스트
   (redaction — commerce `test_bronze_marker_error_is_redacted` 본뜸 · 경로 · 직렬화 · 콜백)
2. [x] 파일럿 traffic 적용 — 기존 콜백과 리스트로 병행. 실패 유도 e2e 완료(dev R2 적재·redaction·
   재시도 중 미기록 확인). 관찰: dag.test 경로에서는 콜백 컨텍스트에 exception 이 없어 `detail`
   생략됨 — 실스케줄 경로에서 재확인 필요(열린 질문 3)
3. [x] 전 도메인 DAG에 callback 연결 (도메인별 커밋) — transit(3)·culture(1)·population(3)·
   weather(3)·traffic 보완(2)·commerce(2, #109 개명 선행 필요). 외부 소스 없는 transform/report
   는 source_system 생략, 혼합 소스(culture)도 생략(추후 ProblemError 로 소스별 지정)
4. [ ] (추후) DB 저장/이중 저장 결정 → `DbSink` 추가

## 열어둔 질문 (구현 중 발견)

3. Airflow 3 콜백 컨텍스트의 `exception` 전달 여부 — dag.test 경로에서는 미전달로 `detail` 이
   비었음. 실스케줄러 경로 검증 후, 미전달이면 TI 로그 tail 등 보강 검토

## 결정 기록 (#77 논의)

| 결정 | 근거 | 제안 |
|---|---|---|
| 에러 전용 범위 | I/O 이력은 모듈 정체가 달라짐 → 별도 기획 | Exisign 제안 분리 |
| 날짜-우선 파티션 | 일 단위 스윕·파티션 프루닝 유리 | Exisign 수용 |
| per-error JSON 유지 | R2 append 불가 + 발생량 적음 | 원안 유지 |
| `urn` type + `docs_url` 분리 | URL 변경이 검색을 깨뜨림, 식별≠문서위치 | kang-gyeongmin 수용 |
| 보존 3개월·버킷 분리 | — | Exisign 수용 |
| redaction 단위 테스트 포함 | at-rest 누출 차단 실증 | kang-gyeongmin 수용 |
