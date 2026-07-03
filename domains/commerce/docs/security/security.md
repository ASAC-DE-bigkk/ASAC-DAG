# 통합 보안 플러그인 — 마스킹 · IO 가드 · 입력검증 · 종합검증

commerce 번들의 **보안 대응 전용 서브시스템** 문서. 코드는 이식 가능한 독립 패키지
[../../include/security/](../../include/security/) 에 모여 있고 **stdlib 만** 쓰므로 어느 프로젝트
(Airflow 번들·서버·배치 스크립트)에도 디렉터리째 떨어뜨려 쓸 수 있다. 이식 절차는
[adoption.md](adoption.md) — 받는 쪽은 **복사 + `install_security()` 한 줄**이면 된다.

> 한 줄 요약: **엔트리포인트에서 `install_security()` 한 번**으로 로그·stdout/stderr·미처리
> 예외훅의 시크릿이 자동 마스킹되고, network IO(+SSRF)/file IO/아카이브/암호/API 기록/
> 저장(at-rest)/알림용 **가드 함수**가 준비(ready)되며, 이 모두를 `run_security_verification()`
> **한 곳**에서 점검한다. 로그는 계속 **분석 가능**하다 — 값은 남기고 시크릿만 가린다(§5).
>
> 최신 가이드라인 반영(2026-07, 2차 확장): **OWASP Top 10:2025**(A03 공급망·A10 예외처리
> 신설, SSRF→A01 흡수), **CWE Top 25 2025**(SQLi #2·경로탐색 #6·자원무제한 #25 신규),
> **ASVS 5.0.0**, **PEP 706/Trojan Source(CVE-2021-42574)/SSRF·비밀번호 저장 치트시트**.

---

## 1. 위협 모델 — 상정한 누출/공격 경로와 대응

시크릿 누출(#1–11)은 1차, 코드/입력/전송/공급망 취약점(#12–22)은 2차 확장(feat/96)이다.

| # | 경로 | 위협 | 대응 | 가이드라인 |
|---|---|---|---|---|
| 1 | **로그**(logging) | 예외·URL 에 인증키가 박혀 평문 로그로 노출 | `install_security()` → 로그 필터(전 핸들러) + `redact()` | CWE-532 |
| 2 | **stdout/stderr** | `print()`/서드파티 직접 출력 → task 로그로 흡수 | `install_security()` → stdout/stderr 마스킹 프록시 | CWE-532 |
| 3 | **미처리 예외** | uncaught traceback 이 stderr 로 평문 출력 | `install_security()` → sys/threading excepthook 마스킹 | CWE-209 |
| 4 | **저장(at-rest)** | 실패 `error` 필드에 키 박힌 URL 이 **영구 저장** | 저장 전 `redact()` + `write_json_redacted()` | **CWE-312** |
| 5 | **network IO** | timeout 누락·TLS 검증 비활성·예외 메시지 누출 | `netio.http_request()`(timeout·verify 강제·예외 스크럽) | CWE-295/400 |
| 6 | **file IO** — 경로 주입 | 입력이 `../`·절대경로로 파티션/키 경로 조작 | `assert_*`/`safe_key()`/`safe_join()` | CWE-22 (#6) |
| 7 | **API 기록** | 호출 URL·헤더·파라미터·응답 원문에 인증정보 | `api_receipt()`/`response_summary()`/`scrub_*()` | CWE-201 |
| 8 | **알림 채널** | 외부 전송 메시지/컨텍스트에 시크릿 | `redact()` · `log_exception()` 반환 dict | CWE-201 |
| 9 | **커밋된 파일** | `.env` 추적·하드코딩 키·예시에 실제 값 | audit: `env_gitignored`·`no_hardcoded_secrets`·`env_example_clean` | CWE-798 |
| 10 | **역직렬화/코드주입** | `yaml.load`/`eval`/`exec`/`pickle`/`marshal`/`os.popen`/`shell=True` | audit: `safe_yaml_load`·`no_dangerous_calls`(확장) | CWE-502/94/78 |
| 11 | **자격증명 취급** | R2/API 키가 로그·경로·페이로드로 흘러감 | env 이름 규칙 자동 수집 + structural 패턴 | CWE-522 |
| 12 | **SSRF** | 사용자 URL 로 사설/메타데이터(IMDS) 요청 유도 | `assert_url_allowed()`(명시 CIDR 차단, IPv4/6·IMDS) + `http_request(url_check=True)` | CWE-918 · A01:2025 |
| 13 | **자원 고갈** | 거대 응답·압축폭탄으로 메모리/디스크 고갈 | `http_request(max_response_bytes=)` · `safe_extract_*`(엔트리/총량/압축비 상한) | CWE-770(#25)/409 |
| 14 | **경로 탈출(아카이브)** | zip-slip/심링크로 dest 밖에 쓰기 | `safe_extract_zip/tar`(멤버 검증 + realpath 봉쇄 + PEP 706 filter) | CWE-22 · CVE-2007-4559 |
| 15 | **SQL 주입** | 문자열 조립 SQL 을 `execute()`·SQLAlchemy `text()` 에 전달 | audit: `no_sql_injection`·`no_sql_text_injection` + `assert_identifier()`(동적 식별자) | CWE-89(#2) |
| 15b | **오픈 리다이렉트** | 입력 파생 값으로 리다이렉트 대상 지정 | audit: `open_redirect_advisory`(리터럴 아닌 대상 경고) | CWE-601 · A01 |
| 15c | **DB 자격증명 로그** | 연결 문자열(DSN)이 예외/로그로 통째 노출 | `mask_dsn()`(URL userinfo + libpq `password=` 마스킹) | CWE-532 |
| 16 | **커밋 자격증명 원문** | PEM 개인키·벤더 토큰(ghp_/xox…)·URL 비밀번호 | audit: `no_credential_material`(CRITICAL) | CWE-798/321 |
| 17 | **Trojan Source** | bidi/zero-width 문자로 코드 로직 위장 | audit: `no_trojan_source`(chr 조립 스캔) | CVE-2021-42574 |
| 18 | **약한 암호** | 보안 용도 md5/sha1·`random` 으로 토큰 생성 | audit: `no_weak_hash`·`no_insecure_random` + `crypto`(secrets/PBKDF2) | CWE-327/330 |
| 19 | **비밀번호 저장** | 평문/약한 해시로 비밀번호 저장 | `crypto.hash_password()`(PBKDF2-HMAC-SHA256 600k, 자기서술) | ASVS V6/V11 |
| 20 | **로그 인젝션** | 개행/ANSI 로 위조 로그라인·터미널 공격 | `sanitize_log_value()` + 필터 `neutralize_controls`(opt-in) | CWE-117 |
| 21 | **취약 파일 조작** | `tempfile.mktemp`(레이스)·world-writable chmod | audit: `no_insecure_file_ops` | CWE-377/732 |
| 22 | **공급망/전송/설정** | 버전 무제한 deps·평문 http·XXE·debug/CORS 오설정 | audit: `requirements_hygiene`·`cleartext_http`·`xml_parsing`·`web_misconfig`(advisory) | A03:2025/CWE-319/611 |

근거 원칙: CLAUDE.md §2.5, §20. 상세 매핑은 §11(가이드라인 대응표).

---

## 2. 구성요소 ([include/security/](../../include/security/))

| 모듈 | 역할 |
|---|---|
| [bootstrap.py](../../include/security/bootstrap.py) | **원샷 설치** — `install_security()`(로그+stdout+예외훅+env 시크릿), `security_status()`, `is_security_installed()` |
| [redaction.py](../../include/security/redaction.py) | 마스킹 엔진 — `Redactor`(literal+structural, **URL userinfo 포함**), `redact()`, `scrub_exception()`, **`sanitize_log_value()`(로그 인젝션 무력화)**, `register_secret()`, `refresh_env_secrets()` |
| [log_filter.py](../../include/security/log_filter.py) | logging 마스킹 — `SecretRedactingFilter`(+`neutralize_controls` opt-in), `install_log_redaction()` |
| [stdio_guard.py](../../include/security/stdio_guard.py) | stdout/stderr 마스킹 프록시 + sys/threading **excepthook** 마스킹 |
| [netio.py](../../include/security/netio.py) | **network IO 가드** — `http_request/get/post`(timeout·TLS·예외 스크럽), **`assert_url_allowed()`(SSRF)**, **`max_response_bytes`(응답 상한)**, `safe_url()` |
| [fileio.py](../../include/security/fileio.py) | **file IO 가드** — `safe_key()`/`safe_join()`(경로 주입 차단), `write_json_redacted()`/`write_text_redacted()`(at-rest 마스킹) |
| [archive.py](../../include/security/archive.py) | **아카이브 안전 추출** — `safe_extract_zip()`/`safe_extract_tar()`(zip-slip·압축폭탄·심링크 차단, PEP 706) |
| [crypto.py](../../include/security/crypto.py) | **암호 유틸** — `generate_token()`(CSPRNG), `constant_time_equals()`, `hash_password()`/`verify_password()`/`needs_rehash()`(PBKDF2 600k) |
| [dbio.py](../../include/security/dbio.py) | **DB IO 가드** — `assert_identifier()`(동적 테이블/컬럼명 검증), `mask_dsn()`(연결 문자열 비밀번호 마스킹, URL+libpq 형) |
| [api_guard.py](../../include/security/api_guard.py) | **API 요청/응답 가드** — `api_receipt()`, `response_summary()`, `scrub_url/headers/params()` |
| [events.py](../../include/security/events.py) | **분석 가능 구조화 로깅** — `log_event()`/`log_exception()`(마스킹된 단일 라인 JSON, §5) |
| [inputs.py](../../include/security/inputs.py) | 입력 검증 — `assert_iso_date`/`assert_safe_segment`/`is_*` |
| [audit.py](../../include/security/audit.py) | 정적 점검 **18종** + 런타임 점검 4종(`Finding`) |
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
  (commerce 는 [include/commerce_core/notify.py](../../include/commerce_core/notify.py) 가 전송 전 `redact()` 를 한 번 더 적용 — 이중 방어).

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

점검 목록: **정적 20종** — Critical: `no_hardcoded_secrets`·`no_credential_material`·
`env_example_clean` / High: `env_gitignored`·`safe_yaml_load`·`no_dangerous_calls`·`tls_verify`·
`no_trojan_source`·`no_sql_injection`·`no_sql_text_injection`·`no_unsafe_extract`·
`no_insecure_file_ops`·`no_insecure_random` / Medium: `http_timeouts`·`no_weak_hash`·
`no_web_misconfig`·`cleartext_http_advisory`·`requirements_hygiene`·`open_redirect_advisory` /
Low: `xml_parsing_advisory` — 및 **런타임 4종**
(`redactor_selftest` High / `log_redaction_installed`·`stdout_redaction_installed`·
`excepthook_redaction_installed` Medium). 런타임 설치 3종은 **CLI 단독 실행에서 warn 이 정상**
(엔트리포인트에서 install 되므로) — 프로세스 안 점검은 `security_status()`. 의도적 예외 표식:
`# security: allow-sql`·`# security: allow-bidi`·`usedforsecurity=False`.

---

## 7. 번들 적용 지점(wiring — 1차 적용 완료)

| 위치 | 적용 |
|---|---|
| [commerce_raw.py](../../commerce_raw.py) | env 적재 직후 **`install_security()`**(로그+stdout+예외훅); `resolve_observed_date` 에 `assert_iso_date()` |
| [include/bronze/clients.py](../../include/bronze/clients.py) | HTTP 호출을 **`netio.http_request()`** 로(정책 단일점) + 예외/재시도 로그 `redact()` |
| [include/bronze/bronze_tasks.py](../../include/bronze/bronze_tasks.py) | 마커 `error` 필드·실패 로그를 저장/출력 전 `redact()`(이중 방어) |
| [include/silver/silver_tasks.py](../../include/silver/silver_tasks.py) | `build_silver` 경계에서 `assert_safe_segment(short)`/`assert_iso_date(observed_date)` |
| [include/commerce_core/notify.py](../../include/commerce_core/notify.py) | 알림 message·context 를 전송 전 `redact()` |
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
