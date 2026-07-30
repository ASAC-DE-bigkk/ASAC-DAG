# Weather·Traffic 제품 관측 및 D1 전달 설계

## 목적

Weather·Traffic 제품의 rich `public_gold` 의미 계약을 D1까지 보존하고, raw·Bronze·Gold·D1 단계와 실제 제품 헬스를 운영 콘솔에서 추적한다. Dashboard는 폐기 예정이므로 변경하지 않는다.

## D1 정적 계약과 런타임 발행 정보

`ServingContract`는 dbt manifest의 `config.meta.public_gold`를 선택적으로 읽고, Publisher는 이를 `_catalog.public_gold` UTF-8 JSON으로 기록한다. 기존 공개 계약의 time role, timezone, null meaning, quality state, coverage, semantic caveat, do-not-use-for를 description으로 축소하지 않는다.

10개 named operation은 `config.meta.serving.mcp_projection`에 있으므로 `_catalog.mcp_projection` sibling JSON으로 기록한다. `_catalog.publication_id`, row count, freshness, serving status는 Publisher가 매 실행 쓰는 런타임 정보로 유지한다. 두 정적 JSON column은 기존 `_ensure_catalog_schema`의 additive migration으로 추가하며, 선언이 없는 도메인은 `null`을 유지한다.

## 제품 이벤트

공통 `common.ops.product_observability`는 target-aware R2 writer를 재사용해 아래 경로에 append-only 이벤트를 기록한다.

```
ops/product-events/observed_date=YYYY-MM-DD/domain=<weather|traffic>/layer=<raw|bronze|gold|d1>/...
```

저장 실패는 fail-open이다. raw·Bronze·Gold는 기존 성공 callback 뒤에 기록해 task graph와 asset 순서를 바꾸지 않는다. D1은 Publisher `ProductRecord`마다 success, degraded, skipped_retained, failed를 기록해 `publication_id`와 source freshness 기반 `publication_delay`를 함께 남긴다.

## 제품 헬스

일일 reliability data-plane은 별도 task를 늘리지 않고 `product_health`를 XCom에 추가하고 `ops/product-health/...`에 snapshot을 기록한다. 값이 실제 집계되지 않거나 Gold/D1 조회가 실패한 경우 0으로 대체하지 않고 `quality_state: unknown`과 `null_meaning`을 기록한다.

Weather는 expected/observed issue count, 핵심 4개 카테고리 coverage, grid place coverage, forecast horizon, issue/collection delay, publication delay를 제공한다. Traffic은 latest-link Gold에서 observed link count, available value ratio, link age p50/p95, source observation·collection delay, stale link ratio를 집계하고 publication delay는 D1 event에서 제공한다.

Reliability report 자체의 PASS/FAIL 판정은 새 제품 헬스 조회 실패로 바뀌지 않는다. 또한 report task 실행 메트릭은 데이터 레이어가 아닌 `ops` 레이어로 기록한다.

## 검증 범위

- D1 catalog migration과 rich static JSON/runtime `publication_id` 동시 보존
- D1 failure 포함 ProductRecord 이벤트화
- Weather·Traffic profile query와 unknown 품질 상태
- 기존 receipt acknowledge, Gold marker, asset callback 순서 보존
- 실제 R2, D1, Trino prod write와 Dashboard 변경은 이 PR에서 실행하지 않음
