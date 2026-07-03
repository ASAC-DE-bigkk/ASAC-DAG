# 공통 HTTP 클라이언트 — 소스 API 호출 통합

- 상태: 진행 중 (합의 완료 — 이슈 #78 논의 반영)
- 작성일: 2026-07-02 (개정: 2026-07-03 — 팀 합의 반영)
- 이슈: [#78](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/78) / 브랜치: `feat/78-common-http-client`
- 관련: [공통 에러 모듈](2026-07-02-feat-common-error-module.md) — 오류를 RFC 9457 Problem으로 변환·적재
- 로드맵 연결: [plan.md](../plan.md) §3 통합 후보 "HTTP/API 클라이언트" 관심사의 선행 기획

## 배경 — 현황 (조사 결과)

| 도메인 | 구현 | 재시도/타임아웃 |
|---|---|---|
| commerce | `requests` + 자체 클래스(`SeoulClient`) | 수동 재시도(backoff), timeout O, 오류 분류(INFO-100 등) |
| culture | `requests` + urllib3 `Retry` 세션 | 세션 레벨 재시도 |
| population | `urllib.request` 직접 | timeout O, 재시도 자체 구현 |
| transit | `urllib.request` 직접 | 수동 지수 백오프 |
| traffic | `urllib` (acc_info) | 단발 호출 |
| weather | `urllib` (kma) | — (KMA 429 백오프는 #106에서 보강) |

같은 관심사(타임아웃·재시도·키 주입·오류 분류·로그 마스킹)를 6번 다르게 구현 중.
redaction 편차: commerce 완비, population 예외 redact 호출 중, traffic/weather 정의만
존재(적용 범위 재확인 필요), transit·culture 없음(@kang-gyeongmin 보정) —
**core 로 끌어올려 편차를 한 번에 제거**한다.

## 설계 — 합성(composition) 중심 + 얇은 계약

```
dags/common/http/
├── core.py        # HttpCore — 구체 클래스 (인터페이스 아님)
│                  #   timeout 강제/재시도(backoff+jitter, Retry-After)/rate limit/redaction 로깅
├── auth.py        # 키 주입 전략: QueryKey("serviceKey"), PathKey(서울식 {api_key}), HeaderKey
├── errors.py      # HttpProblemError(typed) — 공통 에러 모듈(#77) ProblemError 상속
├── limits.py      # rate limit 3단 계층(코드 기본값 < config < 호출측 override)
├── contract.py    # typing.Protocol — 테스트 대체용 얇은 계약 (Transport)
└── seoul.py       # SeoulOpenApiClient — 서울 열린데이터광장 공용 어댑터 (Q3 합의)
```

- **소스별 클라이언트는 HttpCore 를 상속하지 않고 주입받아 사용(has-a)**. 상속은 한 단계만
  허용(깊은 상속 트리 금지).
- 응답 포맷(JSON/XML) 파싱은 core 밖(어댑터 소관) — core 는 bytes/text 까지만.
- async 는 현재 불필요(과설계 금지) — 필요 시 같은 계약으로 `AsyncHttpCore` 병렬 제공.

### 보안 요구 (HttpCore 에서 강제 — 우회 불가)

1. `timeout` 없는 호출 불가(기본값 존재, None 즉시 ValueError)
2. `verify=False` 금지 (파라미터 자체를 노출하지 않음)
3. 예외·로그의 URL 은 **항상 redact 후 노출** — `dags/common/security/`(#77 승격분) 사용
4. 키는 env 에서만 로드(#70 통일 이름), 코드·로그·저장물에 평문 금지
5. 재시도는 멱등 GET 에만 기본 적용, 429/5xx 지수 백오프+jitter, `Retry-After` 존중
6. 소스별 rate limit — 아래 3단 계층

## 합의 결과 (열린 질문 → 확정, 2026-07-03 코멘트)

| 질문 | 결정 | 근거/제안 |
|---|---|---|
| 1. HTTP 라이브러리 | **requests 로 통일** | 전원 동의 (이미지 포함·다수 사용) |
| 2. 파일럿 도메인 | **population — @kang-gyeongmin 담당** | 전원 동의. urllib 직접·구조 단순 |
| 3. 서울 어댑터 위치 | **common (`common/http/seoul.py`)** + 서비스별 key 를 생성/호출 시 지정 가능 | kang·mason 찬성, Exisign 보완(key 분리) 수용 |
| 4. rate limit | **3단 계층: 코드 안전 기본값 < config 파일(`common/config/http_limits.yaml`, env 로 교체) < 호출측 override** | kang 제안 — 순수 상수는 튜닝에 재배포 필요, 순수 yaml 은 값 없으면 깨짐. 운영이 배포 없이 조절 |

## 단계별 계획

1. [x] `common/http/` 구현 + 단위 테스트 20건(timeout 강제·verify 미노출·재시도/Retry-After·
   typed 예외 redaction·rate limit 계층·auth 전략·서울 어댑터·base URL env). commerce
   `SeoulClient` 의 오류분류/redaction 경험은 core 설계에 선반영(@kang-gyeongmin 제안)
2. [x] 전환 완료(호출 경계만 HttpCore 로 교체 — 성공 기준·파싱·스키마 판단은 도메인 유지):
   - **population** — `common/http.py` shim(fetch 시그니처 보존), 단발 호출 유지
   - **traffic** — `runtime.fetch_url` 내부만 위임, 단발(재시도=Airflow task retries)
   - **transit** — `api._read` 의 수동 지수 백오프를 HttpCore 로(#29 정책 2·4·8s 등가 보존)
3. [ ] **소유자 위임** — 아래 두 도메인은 방금 머지된 도메인 특화 로직(회귀 테스트 포함)이
   있어, 그 의도를 가장 잘 아는 소유자가 직접 전환한다(무중단·회귀 방지):
   - **weather** — KMA 429 백오프(#106, `Retry-After` 무제한 존중 + 300s 상한)는 HttpCore
     기본(30s 캡)과 튜닝이 달라, 소유자가 `backoff_max` 등을 맞춰 전환 + 전용 테스트 이관
   - **culture** — KOPIS 오버슛 400=목록 끝(#84, CLOSED) 판별이 `requests.HTTPError` 에
     묶여 있고 회귀 테스트가 이를 단언 → 소유자가 `HttpProblemError.status==400` 으로
     판별 이관 + 회귀 테스트 재작성
4. [ ] **commerce** — top-level `common` 충돌 해소(#109) 머지 후 마지막 전환(오류 분류 로직 큼)
5. [ ] 각 도메인 기존 http 모듈은 위임(re-export shim) 후 제거 — 무중단(@kang-gyeongmin) ·
   plan.md 통합 원칙 3 준수

## 열어둔 질문

- (없음) 서울 base URL env 이름은 **루트 .env 이름 `SEOUL_OPEN_API_BASE_URL`로 통일 완료**
  (#78 — #72 키 통일과 같은 원칙, 사용자 결정). commerce 코드 훅·문서·테스트 예시의
  구 이름 `SEOUL_OPENAPI_BASE_URL` 은 전부 개명. 실환경 값 이관 불필요 — #72 때
  `.env.commerce` 항목은 이미 삭제(기본값과 동일)돼 코드 훅만 남아 있었음
