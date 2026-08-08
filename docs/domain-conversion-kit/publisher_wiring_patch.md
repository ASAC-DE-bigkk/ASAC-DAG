# 공유 게시기 배선 패치 — 전 도메인 패턴 게시 감사

`common/serving/publisher.py` 에 `pattern_audit` 를 걸어, **모든 도메인**이 패턴 게시 시점에
테이블 스코프 감사를 받게 한다. 지금은 커머스만 자체 게시기(`serving_export._handoff_rows`)에서
감사를 받고, 공유 게시기를 쓰는 나머지 도메인은 **무감사**로 게시된다.

## 적용 위치

`_product_meta_rows`(패턴 rows 생성, 현재 193~211줄)에 감사를 넣고, allowlist 는 `publish()`
(404줄)에서 도메인 전체 계약으로 만들어 넘긴다.

## 패치 (개념 diff — 실제 줄번호는 적용 시 대조)

### 1) 임포트 + allowlist 빌드 (publish 함수 진입부)

```python
from common.serving.pattern_audit import audit_pattern_sql, build_allowlist
from common.serving.contract import log_event  # 또는 기존 로거

def publish(contracts, ...):
    # 도메인 게시 제품 테이블 전체 = allowlist (model_name = D1 물리 테이블명).
    # 크로스도메인 소스는 계약이 명시 선언한 것만(현재 전 도메인 0건 — 미래 대비 인자).
    allowlist = build_allowlist(
        (c.model_name for c in contracts),
        cross_domain_sources=(
            s for c in contracts for s in getattr(c, "cross_domain_sources", ())
        ),
    )
    ...
    columns_rows, ext_rows, pattern_rows, display_rows = _product_meta_rows(
        contract, record, allowlist)   # ← allowlist 추가
```

### 2) 감사 후 위반 패턴 제외 (_product_meta_rows 안, pattern_rows 생성 직후)

```python
def _product_meta_rows(contract, record, allowlist):   # ← allowlist 파라미터 추가
    ...
    pattern_rows = [ ... 기존 그대로 ... ]

    # ── 게시 전 정적 보안 감사 (신규) ──────────────────────────────
    # 게이트웨이는 패턴 SQL 을 공유 D1 전체에 verbatim 실행하며 테이블 스코프를 검사하지
    # 않는다(값 bind 만). "이 도메인 패턴은 이 도메인 게시 제품 테이블만 읽는다"를 여기서
    # 강제한다. 위반분은 게시에서 제외하고 경보한다(통과분 게시는 막지 않는다).
    kept = []
    for row in pattern_rows:
        violations = audit_pattern_sql(row["sql"], allowlist)
        if violations:
            log_event(
                "serve.pattern_audit_reject", level="error",
                product_id=contract.product_id, pattern_id=row["pattern_id"],
                violations=violations[:2],
                hint="lint_usage_patterns.py 로 사전 검사 후 SQL 수정",
            )
            continue
        kept.append(row)
    pattern_rows = kept
    # ──────────────────────────────────────────────────────────────

    return columns_rows, ext_rows, pattern_rows, display_rows
```

## 왜 게시기(공유)인가

- 커머스 자체 게시기에만 감사가 있으면 **커머스 경로만** 지켜진다. 공유 게시기를 쓰는 4개
  도메인은 무감사다. 여기 한 번 걸면 **전 도메인 공통**으로 닫힌다.
- 이것은 여전히 **게시자 측** 방어다(게이트웨이 자체 방어 P0 는 ASK-Seoul-Serving#192 로 별개).
  게시기 감사 + 게이트웨이 P0 이 이중으로 겹쳐야 완전하다.

## 안전성 (적용 전 확인 — 이미 실측)

- 현행 4개 도메인 156패턴 전부 이 감사를 **위반 0** 으로 통과한다(스테이징 실측). 즉 배선을
  켜도 **기존 게시가 깨지지 않는다**.
- allowlist 는 deny-by-default 라 실 D1 의 내부표(`_keys`·`_usage`·`_ops_*`·`_service_keys`)·
  카탈로그/핸드오프 표·타 도메인 표가 자동 거부된다(실측 확인).

## 커머스와의 정합 (중복 제거 — 함께 처리 권장)

배선을 켜면 커머스도 이 공유 감사를 받게 하는 게 맞다(지금은 커머스 전용 사본이 따로 있다).
`domains/commerce/include/gold/pattern_audit.py` 를 공유 모듈의 **얇은 래퍼**로 바꾼다:

```python
# commerce/include/gold/pattern_audit.py (전환 후)
from common.serving.pattern_audit import audit_pattern_sql, audit_patterns, build_allowlist

def commerce_allowlist():
    from gold.serving_export import SERVING_SPEC
    return build_allowlist(s.d1_table for s in SERVING_SPEC)
```

커머스 테스트(`test_pattern_audit.py`)·self-test 는 `audit_pattern_sql(sql, allowlist)` 시그니처를
그대로 쓰므로 통과한다(레드팀 페이로드 8종 포함). 두 사본이 갈라지는 위험이 사라진다.
