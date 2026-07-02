# 통합 보안 플러그인 — 마스킹 · IO 가드 · 입력검증 · 종합검증

commerce 번들의 **보안 대응 전용 서브시스템** 문서. 코드는 이식 가능한 독립 패키지
[../../include/security/](../../include/security/) 에 모여 있고 **stdlib 만** 쓰므로 어느 프로젝트
(Airflow 번들·서버·배치 스크립트)에도 디렉터리째 떨어뜨려 쓸 수 있다. 이식 절차는
[adoption.md](adoption.md) — 받는 쪽은 **복사 + `install_security()` 한 줄**이면 된다.

> 한 줄 요약: **엔트리포인트에서 `install_security()` 한 번**으로 로그·stdout/stderr·미처리
> 예외훅의 시크릿이 자동 마스킹되고, network IO/file IO/API 기록/저장(at-rest)/알림용
> **가드 함수**가 준비(ready)되며, 이 모두를 `run_security_verification()` **한 곳**에서 점검한다.
> 로그는 계속 **분석 가능**하다 — 값은 남기고 시크릿만 가린다(§5).

---

## 1. 위협 모델 — 상정한 누출/공격 경로와 대응

| # | 경로 | 위협 | 대응 | 심각도 |
|---|---|---|---|---|
| 1 | **로그**(logging) | `requests` 예외·URL 에 인증키가 박혀 평문 로그로 노출 | `install_security()` → 로그 필터(전 핸들러) + 호출측 `redact()` | High |
| 2 | **stdout/stderr** | `print()`/서드파티 직접 출력 → Airflow 가 task 로그로 흡수 | `install_security()` → stdout/stderr 마스킹 프록시 | High |
| 3 | **미처리 예외** | uncaught traceback 이 stderr 로 평문 출력 | `install_security()` → sys/threading excepthook 마스킹 | High |
| 4 | **저장(at-rest)** — 마커/상태 JSON | 네트워크 실패 시 `error` 필드에 키 박힌 URL 이 **영구 저장** | 저장 전 `redact()` (clients 예외 스크럽과 이중) + `write_json_redacted()` | **Critical** |
| 5 | **network IO** | timeout 누락(자원고갈)·TLS 검증 비활성·예외 메시지 누출 | `netio.http_request()` — timeout 주입·verify 비활성 차단·예외 args 스크럽 | High |
| 6 | **file IO** — 경로 주입 | 입력이 `../`·절대경로로 파티션/키 경로 조작 | `assert_iso_date()`/`assert_safe_segment()` + `safe_key()`/`safe_join()` | High |
| 7 | **API 기록** — 요청/응답 | 호출 URL·헤더·파라미터·응답 원문에 인증정보 포함 | `api_receipt()`/`response_summary()`/`scrub_url·headers·params()` | High |
| 8 | **알림 채널**(webhook/email) | 외부 전송 메시지/컨텍스트에 시크릿 | `redact(message)`/`redact(context)`; `log_exception()` 반환 dict 는 전송-안전 | High |
| 9 | **커밋된 파일** | `.env` 추적·소스 하드코딩 키·예시에 실제 값 | audit: `env_gitignored`·`no_hardcoded_secrets`·`env_example_clean` | Critical/High |
| 10 | **역직렬화/코드주입** | `yaml.load`(Loader 없음)·`eval`·`exec`·`pickle`·`shell=True` | audit: `safe_yaml_load`·`no_dangerous_calls` | High |
| 11 | **자격증명 취급** | R2/API 키가 로그·경로·페이로드로 흘러감 | env 이름 규칙 자동 수집(literal 마스킹) + structural 패턴 | High |

근거 원칙: CLAUDE.md §2.5(자격증명을 bronze·로그·경로·config·vector 메타에 저장 금지), §20.

---

## 2. 구성요소 ([include/security/](../../include/security/))

| 모듈 | 역할 |
|---|---|
| [bootstrap.py](../../include/security/bootstrap.py) | **원샷 설치** — `install_security()`(로그+stdout+예외훅+env 시크릿), `security_status()`, `is_security_installed()` |
| [redaction.py](../../include/security/redaction.py) | 마스킹 엔진 — `Redactor`(literal+structural), `redact()`, `scrub_exception()`(예외 체인 args 마스킹), `register_secret()`, `refresh_env_secrets()` |
| [log_filter.py](../../include/security/log_filter.py) | logging 마스킹 — `SecretRedactingFilter`, `install_log_redaction()` |
| [stdio_guard.py](../../include/security/stdio_guard.py) | stdout/stderr 마스킹 프록시 + sys/threading **excepthook** 마스킹 |
| [netio.py](../../include/security/netio.py) | **network IO 가드** — `http_request/get/post`(timeout 주입·TLS 검증 강제·예외 스크럽), `safe_url()` |
| [fileio.py](../../include/security/fileio.py) | **file IO 가드** — `safe_key()`/`safe_join()`(경로 주입 차단), `write_json_redacted()`/`write_text_redacted()`(at-rest 마스킹) |
| [api_guard.py](../../include/security/api_guard.py) | **API 요청/응답 가드** — `api_receipt()`, `response_summary()`, `scrub_url/headers/params()` |
| [events.py](../../include/security/events.py) | **분석 가능 구조화 로깅** — `log_event()`/`log_exception()`(마스킹된 단일 라인 JSON, §5) |
| [inputs.py](../../include/security/inputs.py) | 입력 검증 — `assert_iso_date`/`assert_safe_segment`/`is_*` |
| [audit.py](../../include/security/audit.py) | 정적 점검 7종 + 런타임 점검 4종(`Finding`) |
| [verify.py](../../include/security/verify.py) | **단일 포인트** — `run_security_verification()`/`assert_secure()`/`SecurityReport` |
| [\_\_main\_\_.py](../../include/security/__main__.py) | CLI — `python -m security`(exit code = 차단 이슈 유무) |

전 모듈 stdlib only(외부 의존성 0) · 번들 비종속(`requests` 는 쓸 때만 지연 임포트).

---

## 3. 런타임 가드 — `install_security()` 원샷

```python
from security import install_security
install_security()      # env 적재 직후, 엔트리포인트(DAG/스크립트/서버)당 1회
```

설치 내용(모두 idempotent, 실패해도 예외를 던지지 않아 기동을 막지 않음):

1. **env 시크릿 적재** — 이름이 `KEY|SECRET|TOKEN|PASSWORD|CREDENTIAL|ACCESS_KEY|AUTH|…` 규칙
   (단 `*_URL/_ENDPOINT/_PATH` 등 제외)인 환경변수의 **값**을 기본 redactor 에 literal 등록.
   짧은 값(<6자)·`${...}` 참조·placeholder 는 제외(오탐 방지). 규칙 밖 시크릿은
   `install_security(extra_secrets=[...])` 또는 `register_secret(value)`.
2. **로그 마스킹** — 루트·airflow 로거와 그 핸들러들에 `SecretRedactingFilter`
   (msg/args/traceback 마스킹).
3. **stdout/stderr 마스킹** — `print()`·서드파티 직접 출력을 write 시점에 마스킹.
4. **미처리 예외훅 마스킹** — `sys.excepthook`/`threading.excepthook` 이 찍는 트레이스백 마스킹.

마스킹은 literal(등록된 실제 값)과 structural(형태 기반: 서울 OpenAPI URL 경로 키,
`Authorization/Bearer`, AWS 액세스 키, `secret=`·`token=`·`api_key=` 류 할당/쿼리)을
**둘 다** 적용한다(defense in depth). 치환 결과는 `***REDACTED***`.

---

## 4. 가드 함수(ready) — 코드가 취약점을 만들 수 없게 하는 헬퍼

런타임 가드(§3)가 못 막는 지점(저장 내용·경로 구성·전송 페이로드)은 **가드 함수를 통과**시킨다.
아직 전 코드에 강제 적용된 것은 아니고, **미리 갖춰진(ready) 표준 통로**다 — 새 코드는 이 통로를
쓰고, 기존 코드는 발견 시 교체한다(CLAUDE.md §20 트리거).

```python
from security import (http_get, safe_key, safe_join, write_json_redacted,
                      api_receipt, response_summary, redact, scrub_exception)

# network IO — timeout 자동 주입 · TLS 검증 비활성 차단 · 예외 메시지 마스킹(같은 타입 재전파)
resp = http_get(url)                          # 또는 http_request("GET", url, session=s, timeout=10)

# file IO — 경로 주입 차단 조립 + at-rest 마스킹 저장
key = safe_key("raw/commerce", date_dir, f"run_id={run_id}", f"{short}.jsonl")
path = safe_join(local_root, "reports", name)             # root 탈출 시 ValueError
write_json_redacted(path, marker)                          # 로컬 파일 마스킹 저장
storage.write_json(key, redact(marker))                    # 오브젝트 스토리지는 redact 후 저장

# API 기록 — 저장/로그/알림에 넣어도 안전한 요청 영수증·응답 요약
receipt = api_receipt(method="GET", url=full_url, service=svc, status=resp.status_code,
                      elapsed_ms=dt, params=params, headers=headers)
summary = response_summary(status=resp.status_code, body=resp.content)   # 길이+sha256+마스킹 샘플
```

원문 보존(bronze)과의 관계: **원본 데이터는 그대로 저장**한다(§2.2 원본 보존 원칙).
가드는 *관측 메타*(로그·마커·알림·영수증)에서 시크릿을 지우는 것이지 데이터를 훼손하지 않는다.

---

## 5. 로그 분석 경계 — 기록·해석·전송은 그대로, 시크릿만 없이

보안 때문에 로그를 못 쓰게 되면 실패다. 경계는 "**값은 남기고 시크릿만 가린다**":

- 처리로그/에러로그의 **기록**: `log_event()`/`log_exception()` 이 고정 스키마의
  **단일 라인 JSON** 으로 남긴다(표준 logging 경유 → 파일/수집기 어디로든).
  ```json
  {"ts": "2026-07-03T00:00:00+00:00", "level": "error", "event": "exception",
   "where": "ingest_one:general_restaurant", "short": "general_restaurant",
   "error_type": "SeoulApiError", "error": "[SVC] ERROR-NETWORK: ... ***REDACTED*** ...",
   "traceback": ["...마스킹된 줄들..."]}
  ```
- **해석**: 모든 필드가 JSON 파싱 가능·스키마 고정(`ts/level/event/where` + 자유 필드) →
  grep/jq/Loki/ELK/CloudWatch 어디서든 집계·검색 가능. 페이지 수·행 수·소요·상태 등
  **운영 지표는 전부 남는다**.
- **전송**: `log_event()`/`log_exception()`/`api_receipt()` 의 **반환 dict 는 이미 마스킹**돼
  있어 webhook/email/slack 컨텍스트로 그대로 실어 보내도 안전하다
  (commerce 는 [include/common/notify.py](../../include/common/notify.py) 가 전송 전 `redact()` 를 한 번 더 적용 — 이중 방어).

---

## 6. 단일 포인트 종합검증

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
assert_secure()                           # 차단 이슈 있으면 SecurityError
```

점검 목록: 정적 7종(`no_hardcoded_secrets`·`env_example_clean` Critical / `env_gitignored`·
`safe_yaml_load`·`no_dangerous_calls`·`tls_verify` High / `http_timeouts` Medium) +
런타임 4종(`redactor_selftest` High / `log_redaction_installed`·`stdout_redaction_installed`·
`excepthook_redaction_installed` Medium). 런타임 설치 3종은 **CLI 단독 실행에서 warn 이 정상**
(엔트리포인트에서 install 되므로) — 프로세스 안 점검은 `security_status()`.

---

## 7. 번들 적용 지점(wiring — 1차 적용 완료)

| 위치 | 적용 |
|---|---|
| [commerce_raw.py](../../commerce_raw.py) | env 적재 직후 **`install_security()`**(로그+stdout+예외훅); `resolve_observed_date` 에 `assert_iso_date()` |
| [include/bronze/clients.py](../../include/bronze/clients.py) | HTTP 호출을 **`netio.http_request()`** 로(정책 단일점) + 예외/재시도 로그 `redact()` |
| [include/bronze/bronze_tasks.py](../../include/bronze/bronze_tasks.py) | 마커 `error` 필드·실패 로그를 저장/출력 전 `redact()`(이중 방어) |
| [include/silver/silver_tasks.py](../../include/silver/silver_tasks.py) | `build_silver` 경계에서 `assert_safe_segment(short)`/`assert_iso_date(observed_date)` |
| [include/common/notify.py](../../include/common/notify.py) | 알림 message·context 를 전송 전 `redact()` |
| [scripts/*.py](../../scripts/) | env 적재 직후 `install_security()`(스크립트도 엔트리포인트) |

---

## 8. 이식(2차 — 통합 플러그인으로 제공)

`include/security/` 는 외부 의존성 0·번들 비종속이라 **디렉터리 복사만으로 이식**된다.
절차·트리거·복사-붙여넣기 프롬프트: **[adoption.md](adoption.md)**.

- 지금: 각 번들의 `include/security/` 로 복사(현 폴더 구조 유지). import 이름은 항상 `security`.
- 추후 `dags/common/` 등 **공용 폴더에서 단일 제공**으로 승격 시: 패키지를 그 폴더로 옮기고
  각 소비자가 그 경로를 `sys.path` 에 추가하면 끝 — **import 이름(`security`)과 API 가 동일**해
  소비 코드는 한 줄도 바뀌지 않는다(중복 복사본 제거만 하면 됨).

---

## 9. 확장 포인트

- **새 시크릿 값**: env 이름을 시크릿 규칙에 맞추면 자동. 규칙 밖은 `register_secret(value)`
  또는 `install_security(extra_secrets=[...])`.
- **새 마스킹 패턴**: `Redactor(patterns=[...])` 주입 또는 `redaction._STRUCTURAL_PATTERNS` 확장.
- **새 민감 헤더/파라미터**: `api_guard._SENSITIVE_HEADERS` 확장(이름 기반 마스킹).
- **새 점검**: `audit.py` 에 `check_*(root)->Finding` 추가 후 `STATIC_CHECKS` 등록 → 종합검증 자동 포함.
- **HTTP 기본 timeout**: env `SECURITY_HTTP_TIMEOUT`(기본 30초).

---

## 10. 한계 / 운영 주의

- 로그/stdout 가드는 **출력 시점** 방어이고 write 호출 1회 단위다 — 시크릿이 여러 write 로
  쪼개지는 극단 케이스는 못 잡는다(일반 print/traceback 은 한 write 에 온전히 옴).
  진짜 위험 지점(저장·전송)은 가드 함수/호출측 `redact()` 로 이중 방어한다.
- 로그 필터는 install 시점의 핸들러 기준 — 이후 동적 추가 핸들러 미적용(Airflow task 핸들러는
  프로세스 init 시 존재해 커버됨). stdout 프록시도 install 이후 스트림 교체 시 재설치 필요.
- 정적 점검은 휴리스틱 — 통과가 "절대 안전"을 보증하지 않는다(시크릿 매니저·정기 점검 병행 권장).
- 실제 시크릿이 담긴 `.env.commerce` 는 gitignore 대상·스캔 제외(추적 여부는 `env_gitignored` 로 점검).
- 마스킹은 관측 경로 방어다 — 시크릿을 변수로 다루는 것 자체는 정상이며, 키를
  파일명/경로/파티션에 쓰지 않는 설계 원칙은 그대로 유지한다.
