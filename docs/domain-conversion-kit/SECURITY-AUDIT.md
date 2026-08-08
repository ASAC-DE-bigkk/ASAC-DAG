# 전환 킷 자체 보안 감사 (사전 준비물 대상)

이 킷이 **추가로 도입한 코드/도구**를 스스로 감사한 결과 + 발견 취약점의 방어코드. (기존 커머스
코드는 별도 감사됨 — ASAC-DAG#743·docs/security. 여기선 **이번에 준비한 부분만** 본다.)

## 위협 모델

킷 도구는 (a) 실 D1 에 SQL 을 실행하고(read), (b) 에이전트/게시본이 저작한 SQL 을 다루며,
(c) yml 을 파싱한다. 핵심 위협: **내 도구가 악성/드리프트 패턴을 프로덕션 D1 에 실행하는
벡터가 되어** 내부표(`_keys` 등)를 읽는 것. 값 인젝션은 게이트웨이와 무관(내 도구는 예시값을
이스케이프해 리터럴 치환).

## 감사 결과 (도구별)

| 도구 | 실 D1 실행? | 실행 전 가드 | 판정 | 조치 |
|---|---|---|---|---|
| `verify_authored.py` | ✅ | `audit_pattern_sql`(감사 후 실행) | **안전** | — |
| `precheck_patterns.py` | ✅ | **없었음** | 🔴 취약 | **방어코드 추가**(아래) |
| `drift_recheck.py` | ✅ | **없었음** | 🔴 취약 | **방어코드 추가**(아래) |
| `dump_and_replica.py` | ✅(PRAGMA·SELECT *) | 테이블명=통제된 목록 | 저위험 | 목록 고정(입력 아님) |
| `local_run.py` | ❌(로컬 레플리카) | SELECT/WITH 체크 | 안전 | — |
| `merge_to_draft.py` | ❌ | — | 안전 | 값은 `json.dumps` 인용, 실행 안 함 |
| `generate_pattern_catalog.py`·`lint_usage_patterns.py` | ❌ | — | 안전 | 파싱·렌더만 |

## 발견 취약점 + 방어코드

### V1. precheck·drift 가 감사 없이 실 D1 실행 (🔴 → 수정)

- **문제**: `precheck_patterns.py`·`drift_recheck.py` 는 yml 의 패턴 SQL 을 예시값 치환 후
  **정적 감사 없이** `se._d1()` 로 실 D1 에 실행했다. 게시/초안 패턴이 `_keys` 를 콤마 조인으로
  참조하면(레드팀이 뚫었던 형), 이 도구가 그걸 프로덕션 D1 에 실행해 유출 벡터가 된다.
- **방어**: 두 도구에 **실행 전 정적 감사 게이트** 추가. 스캔한 yml 의 게시 테이블(모델명 ∪
  패턴 d1_table)을 allowlist 로, `audit_pattern_sql` 로 SELECT/WITH·금지토큰·**테이블 스코프**를
  검사해 위반이면 **실행하지 않고 `blocked` 로 기록**. exit code 도 blocked>0 이면 1.
- **검증**: 악성 yml(`_keys` 콤마조인 + 정상 패턴)로 실측 — 악성은 `blocked`(실 D1 미실행),
  정상은 통과(오탐 0). 즉 도구가 유출 벡터가 되지 않는다.

```
# 방어 후 동작 (evil.yml: legit_read + evil_keys_leak)
{"total": 2, ..., "blocked": 1}   # evil_keys_leak 은 실행 전 차단
```

### V2. 저작 230건 자체의 감사 (통과 — 조치 불요)

- 저작 패턴은 `verify_authored.py` 가 **감사 후에만** 실 D1 검증했다. 230건 전부 감사 위반 0
  (자기 도메인 gold_ 테이블만 참조, 내부표 0). 초안에 위험 패턴이 섞여 있지 않음을 실측 확인.

### V3. 시크릿 취급 (통과)

- 모든 실 D1 도구는 `install_security()` 를 호출(로그·stdout·예외훅 마스킹). 토큰은 env 에서만
  읽고 출력 경로 없음. D1 에러 본문은 `se._d1` 이 `redact` 후 절단.

## 남은 권고 (킷 승격 시)

- precheck/drift 의 방어 게이트는 킷 `pattern_audit.py` 를 재사용한다 — `common/serving/` 승격 시
  임포트 경로만 바꾼다(로직 동일).
- 게이트웨이 P0(§p0-auditor)이 서면 이 도구들의 가드는 **이중 방어**가 된다(게시자 측 + 실행 측).
- dump_and_replica 의 테이블 목록은 코드 상수(`domain_facts.json` 파생)로 유지 — 외부 입력으로
  바꾸지 말 것(임의 테이블 덤프 방지).
