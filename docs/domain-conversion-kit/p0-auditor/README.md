# P0 테이블 스코프 감사기 — 참고 구현 위치와 이식 안내

ASK-Seoul-Serving#192 의 **P0**(게이트웨이가 패턴 SQL 이 자기 제품 테이블만 읽는지 실행 전
검사)를 위한 참고 구현 모음. 게이트웨이(JS)가 이식할 때 여기를 참조한다. #217 [DAG]
"P0 감사기 참고 구현 제공(JS 이식 지원)" 사전작업.

## 왜 P0 인가 (한 줄)

게이트웨이는 저장 패턴 SQL 을 공유 D1 전체에 verbatim 실행하며 **어느 테이블을 읽는지 검사하지
않는다**(값 bind 만). 같은 DB 에 `_keys`(API 키 해시+이메일)·`_usage`·타 도메인 표가 있어,
`SELECT key_hash, email FROM <제품표>, _keys` 같은 콤마 조인이 그대로 실행된다. P0 는 실행 직전
참조 테이블이 그 제품 선언 테이블(± 공용 축) 안인지 검사해 차단한다.

## 참고 구현 위치 (정확히)

| 무엇 | 프로젝트 · 경로 | 비고 |
|---|---|---|
| **정본 감사 코어(Python)** | `ASAC-DAG` · `domains/commerce/include/gold/pattern_audit.py` | 커머스 게시 감사. 토크나이저 기반. 레드팀 검증(ASAC-DAG#743) |
| **도메인 무관 일반화판** | `ASAC-DAG` · `docs/domain-conversion-kit/pattern_audit.py` | allowlist 를 인자로. `build_allowlist()` 포함 |
| **게시 게이트 연결 예** | `ASAC-DAG` · `domains/commerce/include/gold/serving_export.py` (`_handoff_rows` 안 `audit_patterns` 호출) | 게시 직전 위반분 제외 + 경보 |
| **CLI self-test(적대 23케이스)** | `ASAC-DAG` · `domains/commerce/scripts/audit_pattern_sql.py --self-test` | 커머스 판 회귀 |
| **JS 이식 스케치** | `ASAC-DAG` · `docs/domain-conversion-kit/p0-auditor/p0_port_sketch.js` | 이 폴더. 게이트웨이가 옮길 대상 |
| **레드팀 회귀 데이터(18케이스)** | `ASAC-DAG` · `docs/domain-conversion-kit/p0-auditor/redteam_payloads.json` | 이식판 검증용 |
| **JS self-test** | `ASAC-DAG` · `docs/domain-conversion-kit/p0-auditor/_selftest.mjs` | `node _selftest.mjs` |

> 커밋 고정 링크(안 사라짐): 이 킷은 `ASAC-DAG` 브랜치 `prep/domain-conversion-kit` 에 있다.
> PR 병합 시 `common/serving/` 로 승격 예정(ASAC-DAG#747).

## 검증 상태 (실측)

- **Python 정본**: 18/18 레드팀 케이스 일치(레드팀이 뚫었던 콤마 조인·파생 뒤 콤마·스칼라
  서브쿼리·스키마 한정·pragma TVF 전부 차단).
- **JS 이식 스케치**: 같은 18/18 일치(`node _selftest.mjs` 통과). 즉 **이식판이 정본과 동일 판정**.

## 게이트웨이 이식 지점

`handleRunPattern`(`marketplace/src/index.js`)에서 `env.DB.prepare(converted).bind()`(647줄)
**직전**:

```js
import { auditPatternSql } from "./pattern-plus/p0-audit.js"; // 이식본
// 그 제품이 선언한 테이블(d1_table ∪ 공용 축)을 소문자 Set 으로
const allowed = new Set(productDeclaredTables.map((s) => s.toLowerCase()));
const violations = auditPatternSql(pattern.sql, allowed);
if (violations.length) return problem(400, "pattern out of scope", violations[0]);
```

- `productDeclaredTables` 는 카탈로그/발행 메타에서 그 product 의 테이블(+ `JOIN_AXES` 등 공용
  축)로 구성. 커머스는 `d1_*`, 타 도메인은 `gold_<domain>_*`.
- **정규식으로 만들지 말 것**(FROM 뒤 첫 식별자만 잡아 콤마 조인 2번째 테이블 누락 → 레드팀이
  뚫음). 반드시 토크나이저로 FROM 절 **모든** 테이블을 열거한다(스케치가 그렇게 돼 있다).

## 한계 (정직하게)

- 이건 **게이트웨이 자체 방어**로 승격하는 것이다(지금은 도메인 게시 게이트에만 있음). 그래야
  게시자가 누구든 닫힌다.
- 정적 파서라 SQLite 문법 엣지가 있으면 **과다 차단(fail-closed)** 쪽으로 동작(안전). 신규
  SQLite 문법을 열 때는 레드팀 회귀에 케이스를 추가해 검증한다.
