# 보안 대응 — 시크릿 마스킹 · 입력검증 · 종합검증

commerce 번들의 **보안 대응 전용 기능**과 그 처리 로직, 그리고 **단일 포인트 종합검증** 구조를
정리한다. 코드는 이식 가능한 독립 패키지 [../../include/security/](../../include/security/) 에 모여 있고,
stdlib 만 쓰므로 어느 Airflow 번들에도 `include/security/` 를 그대로 떨어뜨려 적용할 수 있다.

> 한 줄 요약: **시크릿(서울 OpenAPI 키·R2 자격증명)이 로그·예외·마커(at-rest)·알림으로 새지
> 않게 마스킹**하고, **사용자 입력의 경로 주입을 막고**, **흔한 취약 패턴(하드코딩 키·`.env`
> 추적·`yaml.load`·`eval`·`verify=False`·timeout 누락)을 정적 점검**한 뒤, 이 모두를
> `run_security_verification()` **한 곳**에서 검증한다.

---

## 1. 위협 모델 — 상정한 공격/누출 경로와 대응

| # | 경로 | 위협 | 대응 | 심각도 |
|---|---|---|---|---|
| 1 | **로그**(stdout/Task log) | `requests` 예외·URL 에 인증키가 박혀 평문 로그로 노출 | `install_log_redaction()` 가 전 핸들러에 마스킹 필터 + 호출측 `redact()` | High |
| 2 | **bronze 마커 JSON(at-rest)** | 네트워크 실패 시 `error` 필드에 키 박힌 URL 이 **영구 저장** | clients 에서 예외 메시지 `redact()`, bronze 에서 마커 저장 전 `redact()`(이중) | **Critical** |
| 3 | **예외 트레이스백** | 예외 메시지/체인에 시크릿 포함 | 로그 필터가 `exc_text` 를 포맷·마스킹 | High |
| 4 | **알림 채널**(webhook/email 등) | 외부 채널로 메시지/컨텍스트 전송 시 시크릿 유출 | `notify_exception` 이 message·context 를 전송 전 `redact()` | High |
| 5 | **커밋된 파일** | `.env.commerce` 추적·소스 하드코딩 키·예시에 실제 값 | audit: `env_gitignored` · `no_hardcoded_secrets` · `env_example_clean` | Critical/High |
| 6 | **경로 주입**(path traversal) | `observed_date` 파라미터(`../`, 절대경로)가 파티션 경로 조작 | `assert_iso_date()` 로 입력 경계 차단(`is_safe_segment`) | High |
| 7 | **역직렬화/코드주입** | `yaml.load`(SafeLoader 없이)·`eval`·`exec`·`pickle`·`os.system`·`shell=True` | audit: `safe_yaml_load` · `no_dangerous_calls` | High |
| 8 | **전송 보안** | `verify=False` 로 TLS 인증서 검증 비활성 | audit: `tls_verify` | High |
| 9 | **자원 고갈(DoS)** | HTTP 호출에 `timeout` 누락 → 무한 대기 | audit: `http_timeouts`(clients 는 30s 지정) | Medium |
| 10 | **자격증명 취급** | R2 액세스/시크릿 키가 로그/경로/페이로드로 흘러감 | settings/env 에만 보관·로그 미기록 + redactor 가 R2 키를 literal 로 마스킹 | High |

근거 원칙: CLAUDE.md §2.5(자격증명을 bronze·로그·경로·config·vector 메타에 저장 금지).

---

## 2. 구성요소 ([include/security/](../../include/security/))

| 모듈 | 역할 |
|---|---|
| [redaction.py](../../include/security/redaction.py) | 마스킹 엔진 — `Redactor`, literal/structural 패턴, 기본 redactor, `redact()`/`register_secret()`/`refresh_env_secrets()` |
| [log_filter.py](../../include/security/log_filter.py) | `SecretRedactingFilter`(logging.Filter) + `install_log_redaction()`/`is_log_redaction_installed()` |
| [inputs.py](../../include/security/inputs.py) | 입력 검증 — `assert_iso_date`/`assert_safe_segment`/`is_*` (경로 주입 차단) |
| [audit.py](../../include/security/audit.py) | 정적 점검 — `Finding` + 파일/패턴 기반 점검 7종 + 런타임 자기검증 |
| [verify.py](../../include/security/verify.py) | **단일 포인트** — `run_security_verification()`/`assert_secure()`/`SecurityReport` |
| [\_\_main\_\_.py](../../include/security/__main__.py) | CLI — `python -m security`(exit code = 차단 이슈 유무) |

---

## 3. 처리 로직

### 3.1 마스킹(redaction) — 2중 방어

literal 과 structural 을 **둘 다** 적용한다(서로의 빈틈을 메움).

1. **literal redaction** — `os.environ` 에서 *이름이 시크릿 패턴*(`KEY|SECRET|TOKEN|PASSWORD|
   CREDENTIAL|ACCESS_KEY|…`, 단 `*_URL/_ENDPOINT/_PATH` 는 제외)인 변수의 **실제 값**을 모아
   텍스트 어디에 나오든 치환한다. 가장 정확 — 키가 URL 경로에 박혀도 잡는다. 짧은 값(<6자)·
   미해석 참조(`${...}`)·placeholder 는 등록하지 않아 오탐을 막는다.
2. **structural redaction** — 실제 값을 몰라도 형태로 잡는다:
   - 서울 OpenAPI URL 경로 키: `http://openapi.seoul.go.kr:8088/<KEY>/json/…`
   - host 없는 경로 형태(requests 예외): `… url: /<KEY>/json/…`
   - `Authorization: Bearer …`, AWS 액세스 키(`AKIA…`)
   - 이름있는 시크릿 할당/쿼리: `secret=…`·`token=…`·`api_key=…`·`access_key_id=…`·`password=…`
     (이름 직후 `=`/`:` 앵커로 `secretary=` 같은 부분일치 오탐 차단)

치환 결과는 `***REDACTED***`. `redact()` 는 문자열뿐 아니라 **dict/list/예외**를 재귀 마스킹한다.

```python
from security import redact, refresh_env_secrets
refresh_env_secrets()                 # env 의 시크릿을 기본 redactor 에 등록(install 시 자동)
redact("url: /<KEY>/json/SVC/1/1/")   # → "url: /***REDACTED***/json/SVC/1/1/"
```

### 3.2 로그 필터

`install_log_redaction()` 은 (1) env 시크릿을 기본 redactor 에 적재하고 (2) 마스킹 필터를
**루트·airflow·airflow.task 로거와 그 핸들러들**에 단다(idempotent). 필터는 레코드의
`msg`·`args`·`exc_info`(트레이스백)를 출력 직전 마스킹한다.

> logging 주의점: 필터를 *로거* 에 달면 그 로거로 직접 들어온 레코드만 거른다(전파분은 안 거름).
> 그래서 **핸들러**에도 단다. 한계: install 이후 새로 추가되는 핸들러에는 자동 적용되지 않는다 →
> 진짜 위험한 곳(예외→마커 저장)은 호출측 `redact()` 로 한 번 더 가린다(§3.1, 위협 #2).

DAG 는 임포트 시 `load_commerce_env()` 직후 `install_log_redaction()` 을 1회 호출한다
([../../commerce_raw.py](../../commerce_raw.py)).

### 3.3 입력 검증(경로 주입 차단)

`observed_date` 파라미터는 silver 파티션 경로(`observed_date=<...>`)로 흘러간다. 사용자 입력이
`../`·절대경로·구분자·제어문자를 담으면 의도치 않은 위치에 쓰기/덮어쓰기가 가능하다. DAG 의
`resolve_observed_date` 가 입력 경계에서 `assert_iso_date()`(YYYY-MM-DD 강제)로 막는다.

### 3.4 정적 점검(audit)

git 호출 없이 번들 파일을 훑어 후보를 찾고 `Finding(check, severity, ok, detail)` 로 보고한다.
점검: `no_hardcoded_secrets`(Critical) · `env_example_clean`(Critical) · `env_gitignored` ·
`safe_yaml_load` · `no_dangerous_calls` · `tls_verify`(High) · `http_timeouts`(Medium) +
런타임 `redactor_selftest`(가짜 키 마스킹 실증) · `log_redaction_installed`.

---

## 4. 단일 포인트 종합검증

세 경로 모두 **같은 함수**를 호출한다. 차단 기준 = CRITICAL/HIGH 미통과.

```bash
# 1) CLI (운영/수동) — exit 0=차단없음, 1=차단
PYTHONPATH=dags/domains/commerce/include python -m security
PYTHONPATH=dags/domains/commerce/include python -m security --no-runtime   # 정적만

# 2) 테스트(CI 게이트)
PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_security.py -q
```

```python
# 3) 코드(배포 전 게이트 등)
from security import run_security_verification, assert_secure
report = run_security_verification()      # SecurityReport(findings=[...])
print(report.render()); report.ok; report.blocking
assert_secure()                           # 차단 이슈 있으면 SecurityError
```

---

## 5. 번들 적용 지점(wiring)

| 위치 | 적용 |
|---|---|
| [commerce_raw.py](../../commerce_raw.py) | env 적재 직후 `install_log_redaction()`; `resolve_observed_date` 에 `assert_iso_date()` |
| [include/bronze/clients.py](../../include/bronze/clients.py) | 네트워크 예외 메시지·재시도 경고 로그를 `redact()`(마커 저장 메시지의 키 차단) |
| [include/bronze/bronze_tasks.py](../../include/bronze/bronze_tasks.py) | 마커 `error` 필드·실패 로그를 저장/출력 전 `redact()`(이중 방어) |
| [include/common/notify.py](../../include/common/notify.py) | 알림 message·context 를 전송 전 `redact()` |

---

## 6. 이식 / 프로젝트 전체 일괄적용

`include/security/` 는 **외부 의존성 0(stdlib)**·**번들 비종속**이라 그대로 이식된다.

1. `include/security/` 디렉터리를 대상 번들의 `include/` 로 복사.
2. 대상 DAG 부트스트랩(`sys.path.insert(... "include")`)·env 적재 직후 한 줄 추가:
   ```python
   from security import install_log_redaction
   install_log_redaction()
   ```
3. 누출 위험 지점(외부 API 클라이언트의 예외/URL 로그, 상태 파일에 저장되는 error 메시지,
   알림 전송부)에 `from security import redact` 를 적용.
4. CI 에 `python -m security` 또는 `pytest .../test_security.py` 를 게이트로 추가.

시크릿 식별은 **환경변수 이름 규칙**(KEY/SECRET/TOKEN/…)으로 자동이라 새 프로젝트의 키도
대부분 추가 설정 없이 잡힌다. 규칙 밖 시크릿은 `register_secret(value)` 로 등록.

---

## 7. 확장 포인트

- **새 시크릿 값**: `register_secret("…")` 또는 env 이름을 시크릿 규칙에 맞춰 두면 자동.
- **새 마스킹 패턴**: `Redactor(patterns=[...])` 로 주입하거나 `redaction._STRUCTURAL_PATTERNS` 확장.
- **새 점검**: `audit.py` 에 `check_*(root)->Finding` 추가 후 `STATIC_CHECKS` 에 등록 → 종합검증에 자동 포함.

---

## 8. 한계 / 운영 주의

- 로그 필터는 install 시점의 핸들러를 기준으로 부착된다 → 이후 동적 추가 핸들러는 미적용
  (그래서 위험 지점은 호출측 `redact()` 로 이중 방어). Airflow Task 핸들러는 프로세스 init 시
  존재하므로 DAG 임포트 시 install 로 커버된다.
- 정적 점검은 휴리스틱이다 — 하드코딩 점검은 16자+ 고엔트로피·`AKIA…`·시크릿 이름 할당을 본다.
  통과가 "절대 안전"을 보증하진 않는다(정기적 비밀 점검/시크릿 매니저 사용 권장).
- 실제 시크릿이 들어있는 `.env.commerce` 는 gitignore 대상이며 하드코딩 스캔에서 제외한다
  (커밋되지 않음을 `env_gitignored` 로 점검). 절대 커밋 금지(CLAUDE.md §2.5).
- 마스킹은 **출력/저장 시점** 방어다 — 시크릿을 변수로 다루는 것 자체는 정상. 키 자체를
  파일명/경로/파티션에 쓰지 않는 설계 원칙은 그대로 유지한다.
