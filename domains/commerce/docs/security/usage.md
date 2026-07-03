# 보안 플러그인 사용법 (usage)

`include/security/` 의 **모든 공개 기능**을 상황별로 정리한 실전 사용 가이드. 위협 모델·설계
근거는 [security.md](security.md), 적용 기술 해설은 [techniques.md](techniques.md), 이식 절차는
[adoption.md](adoption.md). 여기서는 **무엇을 언제 어떻게 호출하는가**만 다룬다.

전 기능 stdlib only · import 이름은 항상 `security`. Python 3.9+.

---

## 0. 30초 요약

```python
from security import install_security
install_security()          # 엔트리포인트당 1회(env 적재 직후) — 이후 로그/stdout/예외 자동 마스킹
```
그 다음, 데이터를 만들 때는 아래 가드 함수를 **표준 통로**로 쓴다:

| 하려는 일 | 함수 |
|---|---|
| HTTP 호출 | `http_get`/`http_post`/`http_request` |
| 외부 URL(사용자 입력) 호출 | `http_request(..., url_check=True)` / `assert_url_allowed` |
| 경로/키 만들기 | `safe_key` / `safe_join` / `assert_iso_date` / `assert_safe_segment` |
| 저장(at-rest) | `redact(obj)` / `write_json_redacted` |
| 아카이브 풀기 | `safe_extract_zip` / `safe_extract_tar` |
| 토큰/비밀번호 | `generate_token` / `hash_password` / `verify_password` / `constant_time_equals` |
| API 호출 기록 | `api_receipt` / `response_summary` |
| 처리/에러 로그(분석용) | `log_event` / `log_exception` |
| 점검 | `python -m security` |

---

## 1. 설치(런타임 가드) — `install_security()`

**언제**: DAG/스크립트/서버 등 프로세스 진입점에서, env 로드 직후 **딱 1회**.

```python
from security import install_security, security_status

install_security()                                   # 기본: 로그+stdout/stderr+예외훅+env 시크릿
install_security(neutralize_log_controls=True)       # 추가로 로그 제어문자 무력화(CWE-117)
install_security(extra_secrets=[read_token()])       # env 이름 규칙 밖 시크릿 수동 등록
security_status()   # {'log_redaction': True, 'stdout_redaction': True, 'excepthook_redaction': True}
```

- idempotent(여러 번 호출해도 안전) · 실패해도 예외를 던지지 않음(기동 비차단).
- 이후 `logging`·`print()`·미처리 트레이스백에서 등록된 시크릿이 자동 마스킹된다.

---

## 2. 네트워크 IO — `http_request` / SSRF 가드

```python
from security import http_get, http_request, assert_url_allowed

# 신뢰된 고정 URL — timeout 자동 주입 · TLS 검증 비활성 차단 · 예외 메시지 마스킹
resp = http_get(url, session=my_session)
resp = http_request("GET", url, session=s, timeout=10, params={...})

# 사용자/외부가 준 URL — SSRF 가드 켜기(호스트명 DNS 해석 후 사설·메타데이터 IP 차단)
resp = http_request("GET", user_url, url_check=True)                 # 기본 resolve_dns=True
resp = http_request("GET", user_url, url_check=True,
                    allowed_hosts={"api.partner.com"})               # 화이트리스트도 가능

# 요청 없이 URL 검증만
assert_url_allowed(user_url)                    # 통과 시 url 반환, 아니면 UnsafeURLBlocked
if is_url_allowed(user_url): ...

# 응답 크기 상한(거대 응답 자원 고갈 차단) — 초과 시 ResponseTooLarge
resp = http_request("GET", url, session=s, max_response_bytes=5 * 2**20)
```

주의: `verify=False` 전달은 `InsecureRequestBlocked` 로 **차단**된다(정상 동작). SSRF 완전
방어(DNS 리바인딩)는 stdlib 래퍼 한계 밖 — `url_check=True` 는 `allow_redirects=False` 를 기본
적용하고 검증 시점 IP 를 검사한다(상세: [security.md](security.md) §10).

---

## 3. 파일 IO — 경로 주입 차단 · 마스킹 저장

```python
from security import safe_key, safe_join, assert_iso_date, assert_safe_segment
from security import write_json_redacted, redact

# 오브젝트 스토리지 키(외부 입력 컴포넌트 포함) 조립 — ../ · 절대경로 · 구분자 거부
key = safe_key("raw/commerce", date_dir, f"run_id={run_id}", f"{short}.jsonl")

# 로컬 경로 — root 밖으로 벗어나면 ValueError
path = safe_join(local_root, "reports", user_name)

# 개별 세그먼트 경계 검증
assert_iso_date(observed_date)          # YYYY-MM-DD 강제(파티션 키)
assert_safe_segment(short)              # 단일 세그먼트 안전성

# 저장(at-rest) 전 마스킹
write_json_redacted(path, marker)                       # 로컬 파일
storage.write_json(key, redact(marker))                 # 오브젝트 스토리지(호출측 redact)
```

---

## 4. 아카이브 안전 추출

```python
from security import safe_extract_zip, safe_extract_tar, UnsafeArchiveError

try:
    written = safe_extract_zip(src_zip, dest_dir,
                               max_entries=1000, max_total_bytes=100*2**20, max_ratio=100.0)
    written = safe_extract_tar(src_tar, dest_dir, max_total_bytes=100*2**20)
except UnsafeArchiveError as e:
    ...   # zip-slip · 압축폭탄 · 심링크/특수엔트리 · 상한 초과
```
- 멤버명 검증(절대경로/`..`/드라이브) + realpath 봉쇄 + 엔트리수/총량/압축비 상한.
- 헤더 선언 크기를 신뢰하지 않고 **실제 스트리밍 바이트**로 재검증(폭탄 중도 절단).
- 반환값(기록된 경로 목록)은 `log_event` 로 영수증 남기기 좋다.

---

## 5. 암호 유틸 — 토큰 · 비밀번호 · 상수시간 비교

```python
from security import (generate_token, generate_hex_token, constant_time_equals,
                      hash_password, verify_password, needs_rehash)

token = generate_token()                        # URL-safe 256비트 CSPRNG(secrets)
csrf  = generate_hex_token(16)

if constant_time_equals(provided, expected):    # 타이밍 부채널 없는 비교(CWE-208)
    ...

encoded = hash_password(pw)                      # PBKDF2-HMAC-SHA256 600k, 자기서술 인코딩
if verify_password(pw, encoded):                # 손상값도 예외 없이 False
    if needs_rehash(encoded):                   # 정책 상향 시 로그인 시점 재해시
        encoded = hash_password(pw)
```
Argon2id 를 쓸 수 있는 환경이면 그쪽이 1순위 — 이 모듈은 **의존성 0 stdlib 폴백**이다.

---

## 5b. DB IO — 동적 식별자 검증 · 연결 문자열 마스킹

값은 항상 드라이버의 **파라미터 바인딩**을 쓴다(sqlalchemy/psycopg/sqlite3). 이 가드는
바인딩으로 못 막는 **식별자**(동적 테이블/컬럼명)와 **DSN 자격증명**만 담당한다.

```python
from security import assert_identifier, mask_dsn

# 값 → 파라미터 바인딩(플러그인 밖, 드라이버 기능)
session.execute(text("SELECT * FROM t WHERE id = :id"), {"id": user_id})

# 식별자(동적 테이블/컬럼명)만 어쩔 수 없이 끼울 때 → 허용 문자만 통과
col = assert_identifier(sort_col)                  # 영숫자·밑줄·schema.table 만; 아니면 ValueError
session.execute(text(f"SELECT * FROM logs ORDER BY {col}"))   # col 은 검증됨

# 연결 문자열을 로그/예외에 남기기 전 비밀번호 마스킹(URL 형 + libpq key=value 형)
log.error("db connect failed: %s", mask_dsn(settings.database_url))
```
정적 점검 `no_sql_text_injection` 이 `text(f"…")`·`text("…" + x)` 등 조립 SQL 을, `no_sql_injection`
이 `cursor.execute(f"…")` 를 잡는다(파라미터 바인딩·`:name` 은 통과).

---

## 6. API 요청/응답 기록(리니지, 무시크릿)

```python
from security import api_receipt, response_summary

receipt = api_receipt(method="GET", url=full_url, service=svc,
                      params=params, headers=headers,
                      status=resp.status_code, ok=resp.ok, elapsed_ms=dt)
summary = response_summary(status=resp.status_code, body=resp.content)   # 길이+sha256+마스킹 샘플
storage.write_json(marker_key, {"receipt": receipt, "response": summary})   # 그대로 저장 안전
```
URL 경로/쿼리 키·`Authorization`/`Cookie`/`X-Api-Key` 헤더·시크릿 이름 파라미터가 마스킹된다.

---

## 7. 분석 가능한 구조화 로그

```python
from security import log_event, log_exception

log_event("bronze.page_fetched", where="ingest_one:general_restaurant",
          page=3, rows=1000, url=user_url)      # 마스킹된 단일 라인 JSON

try:
    ...
except Exception as exc:
    rec = log_exception(exc, where="ingest_one:general_restaurant", short=short)
    notifier.send(subject="...", context=rec)   # 반환 dict 는 이미 마스킹 → 전송 안전
```
`ts/level/event/where` 고정 스키마 + 자유 필드. jq/Loki/ELK 로 바로 집계 가능(값은 남고
시크릿만 가려짐).

---

## 8. 직접 마스킹이 필요할 때

```python
from security import redact, scrub_exception, sanitize_log_value, register_secret

log.warning("api failed: %s", redact(str(exc)))          # 로그/문자열/dict/list/예외 재귀 마스킹
raise scrub_exception(exc)                                # 예외 타입 보존 + 메시지 체인 마스킹
log.info("user input=%s", sanitize_log_value(user_text))  # 제어문자 무력화(위조 로그라인 차단)
register_secret(dynamic_secret)                           # 런타임 시크릿 추가 등록
```

---

## 9. 점검(단일 포인트)

```bash
PYTHONPATH=dags/domains/commerce/include python -m security             # exit 0=차단없음, 1=차단
PYTHONPATH=dags/domains/commerce/include python -m security --no-runtime  # 정적만
PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_security.py -q
```
```python
from security import run_security_verification, assert_secure
report = run_security_verification(); print(report.render())
assert_secure()                                          # 차단 이슈 있으면 SecurityError
```

정적 점검(18종)이 하드코딩 시크릿·PEM/토큰·경로탐색·SQLi·zip-slip·약한 암호·Trojan Source·
TLS 비활성·공급망 위생 등을 훑는다. 의도적 예외는 라인 끝 표식으로 통과시킨다:
`# security: allow-sql`, `# security: allow-bidi`, `usedforsecurity=False`(약한 해시 비보안 용도).

---

## 10. 자주 쓰는 조합

```python
# (a) 사용자 입력 → 안전한 외부 호출 + 마스킹 저장
url = assert_url_allowed(user_url)
resp = http_request("GET", url, session=s, url_check=True, max_response_bytes=10*2**20)
storage.write_json(safe_key("raw", assert_iso_date(ds), f"{assert_safe_segment(name)}.json"),
                   redact({"receipt": api_receipt(method="GET", url=url, status=resp.status_code)}))

# (b) 로그인(암호) — 저장은 해시만, 비교는 상수시간, 정책 상향은 자동
enc = hash_password(pw)
...
if verify_password(pw, enc) and needs_rehash(enc):
    enc = hash_password(pw)
```
