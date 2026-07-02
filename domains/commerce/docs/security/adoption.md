# 보안 서브시스템 적용·이식 가이드 (Claude / Codex 용)

[../../include/security/](../../include/security/) 를 **다른 번들/프로젝트로 가져가 적용**하는 절차.
에이전트(Claude/Codex)가 그대로 따라 실행할 수 있도록 단계·트리거·검증을 명시한다.
처리 로직·위협 모델은 [security.md](security.md) 참고.

> 전제: 이 패키지는 **외부 의존성 0(stdlib only)**·**번들 비종속**이다. 디렉터리만 복사하면 된다.

---

## 3단계 적용

### 1) 복사
`include/security/` 디렉터리 전체를 대상 프로젝트의 **import 루트**(예: `<project>/include/`)로 복사.
- 대상에 `.airflowignore` 가 있으면 `include/**` 가 DAG 파싱 제외인지 확인(이미 그렇게 쓰는 게 표준).
- import 루트가 `sys.path` 에 올라가 있어야 `from security import …` 가 동작
  (이 번들은 DAG 가 `sys.path.insert(0, ".../include")` 로 자기 부트스트랩).

### 2) 와이어링
(a) **엔트리포인트(DAG 등) env 적재 직후 1회** — 로그 마스킹 설치:
```python
from security import install_log_redaction
install_log_redaction()          # 이후 모든 로그/예외에서 시크릿 자동 마스킹
```
(b) **누출 지점에 `redact()`** (아래 §적용 트리거):
```python
from security import redact
log.warning("api failed: %s", redact(str(exc)))   # 로그
marker["error"] = redact(error)                    # 저장(at-rest) 전 — 필수
notifier.send(message=redact(msg), context=redact(ctx))  # 외부 전송 전
```
(c) **사용자 입력을 경로/식별자로 쓰는 곳**에 검증:
```python
from security import assert_iso_date, assert_safe_segment
assert_iso_date(date_param)       # YYYY-MM-DD 강제(파티션 키 주입 차단)
assert_safe_segment(name_param)   # ../ · 구분자 · 제어문자 거부
```

### 3) 점검 연결(CI / 로컬) — **단일 포인트**
```bash
PYTHONPATH=<project>/include python -m security            # exit 0=차단없음, 1=차단
PYTHONPATH=<project>/include pytest <tests>/test_security.py -q
```
CI 파이프라인 게이트로 위 명령을 추가한다(차단=CRITICAL/HIGH 발생 시 빌드 실패).

---

## 적용 트리거 — 언제 redact/검증을 넣나

| 상황(코드 추가/수정) | 조치 |
|---|---|
| 외부 API/네트워크 예외·URL 을 **로그**에 남김 | `redact()` (예외 메시지가 저장물로 가면 **필수**) |
| error/메타데이터를 **스토리지/마커/DB 에 저장** | 저장 전 `redact()` (at-rest 누출 차단) |
| 외부 채널(**webhook/email/slack**)로 메시지 전송 | `redact(message)` · `redact(context)` |
| **사용자 입력**(params)을 경로/식별자로 사용 | `assert_iso_date()` / `assert_safe_segment()` |
| **새 시크릿 env** 추가 | 이름을 `KEY/SECRET/TOKEN/CREDENTIAL/ACCESS_KEY/…` 규칙에 맞춰 둠(자동 마스킹) 또는 `register_secret(value)` |
| **새 DAG/엔트리포인트** | env 적재 직후 `install_log_redaction()` 1회 |
| HTTP 호출 | `timeout=` 지정 / yaml 은 `safe_load` / `eval·exec·pickle·shell=True·verify=False` 금지 |

---

## 검증 리포트 읽는 법

`python -m security` 출력은 점검별 `[PASS]` / `[warn]`(non-blocking) / `[FAIL!]`(blocking).
**차단 = CRITICAL/HIGH 미통과** → exit code 1. MEDIUM 이하는 경고(빌드는 통과).
`log_redaction_installed` 가 CLI 단독 실행에서 `warn` 인 것은 정상(런타임에 install 됨).

---

## 확장

- **새 마스킹 패턴**: `Redactor(patterns=[...])` 주입 또는 `redaction._STRUCTURAL_PATTERNS` 확장.
- **새 정적 점검**: `audit.py` 에 `check_*(root) -> Finding` 추가 후 `STATIC_CHECKS` 에 등록 →
  `run_security_verification()`(단일 포인트)에 자동 포함.
- **시크릿 식별 규칙 변경**: `redaction._SECRET_NAME_RE` / `_SECRET_NAME_DENY` 조정.

---

## 에이전트용 복사-붙여넣기 프롬프트

아래를 Claude/Codex 에 그대로 전달하면 이식을 수행한다(`<…>` 만 대상에 맞게 치환):

```text
<source>/include/security/ 를 <target>/include/ 로 복사해 보안 서브시스템을 이식해줘.
1) <target> 엔트리포인트(DAG 등) env 적재 직후 `from security import install_log_redaction;
   install_log_redaction()` 한 줄 추가.
2) 외부 API 예외/URL 을 로그하거나, error·메타데이터를 스토리지/마커/DB 에 저장하거나,
   외부 채널로 전송하는 지점을 찾아 `from security import redact` 로 감싸줘(저장 전은 필수).
3) 사용자 입력을 경로/식별자로 쓰는 곳에 assert_iso_date()/assert_safe_segment() 적용.
4) tests 에 test_security.py 패턴으로 검증 추가.
5) 끝으로 `PYTHONPATH=<target>/include python -m security` 가 차단 이슈 0 인지 확인하고,
   결과를 보고해줘. 차단이 있으면 0 이 될 때까지 보완.
작업 경계: <target> 외부 파일은 건드리지 말 것.
```
