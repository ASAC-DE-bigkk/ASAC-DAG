# 통합 보안 플러그인 — 적용·이식 가이드 (사람/Claude/Codex 공용)

[../../include/security/](../../include/security/) 를 **다른 번들/프로젝트로 가져가 적용**하는 절차.
받는 쪽 기준 "받아쓰기" 수준을 목표로 한다: **① 폴더 복사 → ② `install_security()` 한 줄 →
③ (선택) 가드 함수 채택 → ④ 게이트 연결**. 처리 로직·위협 모델은 [security.md](security.md).

> 전제: 이 패키지는 **외부 의존성 0(stdlib only)**·**번들 비종속**이다. 어떤 프레임워크와도
> 무관하게 동작한다(Airflow·FastAPI·순수 스크립트). `requests` 조차 쓸 때만 지연 임포트한다.

---

## 최소 적용(받아쓰기 2단계)

### 1) 복사
`include/security/` 디렉터리 전체를 대상 프로젝트의 **import 루트**로 복사.
- import 루트가 `sys.path` 에 있어야 `from security import …` 가 동작
  (Airflow 번들이면 DAG 의 `sys.path.insert(0, ".../include")` 부트스트랩이 이미 해준다).
- `.airflowignore` 를 쓰는 프로젝트면 `include/**` 가 DAG 파싱 제외인지 확인.

### 2) 원샷 설치 — 엔트리포인트(DAG/스크립트/서버 기동부)당 1회, env 적재 직후
```python
from security import install_security
install_security()
```
이것만으로 적용되는 것: **로그**·**stdout/stderr(print)**·**미처리 예외 트레이스백**의 시크릿
자동 마스킹 + env 시크릿 자동 수집(이름 규칙 `KEY/SECRET/TOKEN/PASSWORD/CREDENTIAL/…`).
규칙 밖 시크릿은 `install_security(extra_secrets=[...])`.

여기까지가 **런타임 가드**다. 이후는 코드가 데이터를 만들 때 쓰는 **가드 함수**(아래 3)
— 새 코드부터 적용하고, 기존 코드는 트리거 표를 만나면 교체한다.

### 3) (선택·권장) 가드 함수 채택 — 표준 통로
| 하려는 일 | 쓰는 것 |
|---|---|
| HTTP 호출 | `http_request/http_get/http_post` — timeout 자동 주입 · TLS 검증 비활성 차단 · 예외 메시지 마스킹(같은 타입 재전파) |
| 경로/키 조립(입력 포함) | `safe_key(*parts)`(스토리지 키) / `safe_join(root, *parts)`(로컬, root 탈출 차단) |
| 사용자 입력 검증 | `assert_iso_date(v)` / `assert_safe_segment(v)` |
| error/메타 **저장**(at-rest) | 저장 직전 `redact(obj)` / 로컬 파일이면 `write_json_redacted(path, obj)` |
| API 호출 기록 | `api_receipt(...)` → 저장/로그/알림에 그대로 사용 가능한 영수증 |
| 응답 관측 메타 | `response_summary(status=…, body=…)` — 길이+sha256+마스킹 샘플 |
| 처리/에러 로그(분석용) | `log_event("이벤트", where=…, **필드)` / `log_exception(exc, where=…)` — 마스킹된 단일 라인 JSON, 반환 dict 는 외부 전송에도 안전 |
| 외부 채널 전송 | `redact(message)` · `redact(context)` (또는 위 반환 dict 사용) |
| 예외 재포장/전파 | `scrub_exception(exc)` — 예외 args 를 체인까지 마스킹(타입 보존) |

### 4) 게이트 연결(CI/로컬) — 단일 포인트
```bash
PYTHONPATH=<project>/include python -m security            # exit 0=차단없음, 1=차단
PYTHONPATH=<project>/include pytest <tests>/test_security.py -q
```
`tests/test_security.py` 를 함께 복사하면 패키지 자체 검증(마스킹/가드/주입 차단)이 따라간다.
번들 특화 테스트(bronze 마커 등)는 대상에 맞게 삭제/교체.

---

## 적용 트리거 — 언제 무엇을 넣나 (기존 코드 점검용)

| 상황(코드 추가/수정) | 조치 |
|---|---|
| 외부 API/네트워크 호출 | `netio.http_request()` 사용(또는 최소한 `timeout=` 지정) |
| 네트워크 예외·URL 을 **로그** | `redact()` (예외 메시지가 저장물로 가면 **필수**) |
| error/메타데이터를 **스토리지/마커/DB 에 저장** | 저장 전 `redact()` / `write_json_redacted()` |
| 외부 채널(webhook/email/slack) 전송 | `redact(message)`·`redact(context)` 또는 `log_exception()` 반환 dict |
| **사용자 입력**을 경로/식별자로 사용 | `assert_iso_date()`/`assert_safe_segment()`/`safe_key()`/`safe_join()` |
| API 호출 이력을 남김 | `api_receipt()`/`response_summary()` (URL·헤더·파라미터 원문 저장 금지) |
| **새 시크릿 env** | 이름을 `KEY/SECRET/TOKEN/CREDENTIAL/ACCESS_KEY/…` 규칙에 맞춤(자동) 또는 `register_secret()` |
| **새 엔트리포인트**(DAG/스크립트/서버) | env 적재 직후 `install_security()` 1회 |
| 그 외 항상 | yaml 은 `safe_load` / `eval·exec·pickle·shell=True`·TLS 검증 비활성 금지(audit 이 잡는다) |

---

## 검증 리포트 읽는 법

`python -m security` 출력은 점검별 `[PASS]` / `[warn]`(non-blocking) / `[FAIL!]`(blocking).
**차단 = CRITICAL/HIGH 미통과** → exit 1. MEDIUM 이하는 경고(빌드 통과).
런타임 설치 3종(`log/stdout/excepthook_redaction_installed`)이 **CLI 단독 실행에서 warn 인 것은
정상** — 엔트리포인트 프로세스 안에서는 `install_security()` 후 `security_status()` 로 확인.

---

## 공용(common) 폴더 단일 제공으로 승격할 때

지금은 번들마다 `include/security/` 복사본(현 폴더 구조 유지)이지만, 추후 `dags/common/` 같은
**공용 폴더에서 한 벌만 제공**하는 구조로 바꿀 때의 계약:

1. 패키지 디렉터리(`security/`)를 공용 폴더로 이동(내용 무수정 — 번들 비종속이므로 그대로 동작).
2. 각 소비자(엔트리포인트)는 그 폴더를 `sys.path` 에 추가:
   `sys.path.insert(0, "<공용 폴더 경로>")` (Airflow 라면 이미 dags 루트가 올라와 있으면 끝).
3. **import 이름(`security`)과 공개 API 는 동일** → 소비 코드는 한 줄도 안 바뀐다.
   각 번들의 로컬 복사본만 지우면 됨(동일 이름 이중 존재 금지 — sys.path 앞쪽이 이긴다).
4. 버전 이력은 공용 폴더의 change-log 로 일원화하고, 각 번들 CI 게이트(`python -m security`)는
   그대로 둔다(`--root <번들>` 로 번들별 정적 점검 가능).

---

## 에이전트용 복사-붙여넣기 프롬프트

아래를 Claude/Codex 에 그대로 전달하면 이식을 수행한다(`<…>` 만 대상에 맞게 치환):

```text
<source>/include/security/ 를 <target>/<import 루트>/ 로 복사해 통합 보안 플러그인을 이식해줘.
1) <target> 의 모든 엔트리포인트(DAG/스크립트/서버 기동부)에서 env 적재 직후
   `from security import install_security; install_security()` 를 1회 호출하게 해줘.
2) HTTP 호출부를 `from security import http_request` 래퍼로 바꾸거나 timeout= 을 보장하고,
   error·메타데이터를 스토리지/마커/DB 에 저장하는 지점과 외부 채널(webhook 등) 전송 지점에
   `redact()`(저장 전 필수)를 적용해줘.
3) 사용자 입력을 경로/식별자로 쓰는 곳에 assert_iso_date()/assert_safe_segment()/safe_key() 적용.
4) 처리/에러 로그가 필요한 곳은 log_event()/log_exception() 으로 구조화(분석 가능 + 마스킹).
5) <source> 의 tests/test_security.py 를 <target> 테스트로 복사(번들 특화 테스트는 대상에 맞게 조정).
6) 끝으로 `PYTHONPATH=<target>/<import 루트> python -m security` 가 차단 이슈 0 인지 확인하고
   결과를 보고해줘. 차단이 있으면 0 이 될 때까지 보완.
작업 경계: <target> 외부 파일은 건드리지 말 것.
```
