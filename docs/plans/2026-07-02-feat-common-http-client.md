# 공통 HTTP 클라이언트 — 소스 API 호출 통합

- 상태: 초안 (기획 — 팀 의견 수렴 중)
- 작성일: 2026-07-02
- 이슈: [#78](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/78) (논의) / 브랜치: `feat/78-common-http-client` (합의 후)
- 관련: [공통 에러 모듈](2026-07-02-feat-common-error-module.md) — 오류를 RFC 9457 Problem으로 변환·적재
- 로드맵 연결: [plan.md](../plan.md) §3 통합 후보 "HTTP/API 클라이언트" 관심사의 실행 기획

## 배경 — 현황 (조사 결과)

| 도메인 | 구현 | 재시도/타임아웃 |
|---|---|---|
| commerce | `requests` + 자체 클래스(`SeoulClient`) | 수동 재시도(backoff), timeout O, 오류 분류(INFO-100 등) |
| culture | `requests` + urllib3 `Retry` 세션 | 세션 레벨 재시도 |
| population | `urllib.request` 직접 | timeout O, 재시도 자체 구현 |
| transit | `urllib.request` 직접 | 수동 지수 백오프 |
| traffic | `urllib` (acc_info) | 단발 호출 |
| weather | `urllib` (kma) | 〃 |

같은 관심사(타임아웃·재시도·키 주입·오류 분류·로그 마스킹)를 6번 다르게 구현 중.
보안 수준도 제각각 — 키가 URL 경로에 들어가는 서울 API 특성상 예외 메시지·로그로 키가
샐 수 있는데, redaction이 있는 곳은 commerce뿐.

## 설계 방향 (권장안)

질문받은 "인터페이스화 vs 부모 클래스"에 대한 결론: **합성(composition) 중심 + 얇은 계약**.

```
dags/common/http/
├── core.py        # HttpCore — 구체 클래스 (인터페이스 아님)
│                  #   세션/timeout 강제/재시도(backoff+jitter)/rate limit/redaction 로깅
├── auth.py        # 키 주입 전략: QueryKey("serviceKey"), PathKey(서울식 /KEY/), HeaderKey
├── errors.py      # HttpProblemError(typed) → 공통 에러 모듈 Problem 변환
└── contract.py    # typing.Protocol — 테스트 대체용 얇은 계약 (Transport)
```

- **소스별 클라이언트는 HttpCore를 상속하지 않고 주입받아 사용(has-a)**:

```python
class SeoulOpenApiClient:                    # 도메인 소유, dags/common 의존
    def __init__(self, core: HttpCore, key: str):
        self._core = core                    # 합성 — 부모 아님
        self._auth = PathKey(key)            # 서울식: URL 경로에 키
    def fetch(self, service: str, start: int, end: int) -> dict: ...
```

### 왜 상속(부모 클래스)보다 합성인가

- Python에는 자바식 인터페이스가 없고 그 역할은 `abc.ABC`(추상 부모) 또는
  `typing.Protocol`(구조적 계약)이 담당 — "인터페이스화"라는 표현 자체는 맞음
- 그러나 **부모 클래스에 공통 로직을 쌓는 방식은 시간이 갈수록 부모가 비대해지고**(각 소스의
  특수 요구가 부모 옵션으로 역류), 부모 수정이 6개 도메인에 동시 파급됨
- 합성이면: 새 소스 추가 = 어댑터 클래스 1개, HttpCore 변경은 계약(시그니처)만 지키면 안전,
  테스트는 `contract.py`의 Protocol로 가짜 Transport 주입
- 상속은 **한 단계만** 허용(예: 서울 OpenAPI 계열 5개 도메인이 공유하는 `SeoulOpenApiClient`를
  도메인이 감싸는 정도) — "깊은 상속 트리 금지"를 규칙으로 명시

### 보안 요구 (HttpCore에서 강제 — 우회 불가)

1. `timeout` 없는 호출 불가(기본값 존재, None 금지)
2. `verify=False` 금지 (파라미터 자체를 노출하지 않음)
3. 예외·로그의 URL은 **항상 redact 후 노출** (키가 경로에 들어가는 서울 API 대응 —
   commerce `security/redaction.py`를 `dags/common/security/`로 승격해 사용)
4. 키는 env에서만 로드(#70 통일 이름), 코드·로그·저장물에 평문 금지 — Infisical 전환 기획과 정합
5. 재시도는 멱등 GET에만 기본 적용, 429/5xx 지수 백오프+jitter, `Retry-After` 존중
6. 소스별 rate limit(쿼터) 설정 가능 — 호출량 문서(commerce api-call-volume 등) 기반

### 확장성 고려

- 새 소스(신규 도메인·API) = auth 전략 선택 + 어댑터 1개 — core 무변경
- 응답 포맷(JSON/XML) 파싱은 core 밖(어댑터 소관) — core는 bytes/text까지만 책임
- 추후 async 필요 시 `AsyncHttpCore`를 같은 계약으로 병렬 제공 가능(현재는 불필요 — 과설계 금지)

## 단계별 계획 (합의 후)

1. `common/http/` + `common/security/`(redaction 승격) 구현, 단위 테스트
2. 파일럿: population(urllib 직접·구조 단순) 전환 → 실DAG 검증
3. 도메인 순차 전환(도메인별 커밋·검증): transit → traffic → weather → culture → commerce
   (commerce는 오류 분류 로직이 커서 마지막)
4. 각 도메인 기존 http 모듈은 위임(re-export) 후 제거 — plan.md 통합 원칙 3 준수

## 열어둔 질문 (합의 필요)

1. HTTP 라이브러리 통일: `requests`(이미 이미지 포함, 다수 사용) 기준으로 통일해도 되는지
2. 파일럿 도메인: population 추천 — 다른 선호 있는지 (각 도메인 담당자 일정 고려)
3. rate limit 기본값을 소스별로 어디에 둘지 (코드 상수 vs config yaml)
4. `SeoulOpenApiClient`(5개 도메인 공용 서울 API 어댑터)를 common에 둘지, 얇게 core만 두고
   서울 어댑터는 도메인별 유지할지 — **common에 두는 것을 권장**(가장 큰 중복이 서울 API 호출)
