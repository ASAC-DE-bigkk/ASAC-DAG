# Weather·Traffic 제품 관측과 D1 메타데이터 전달 설계

## 목적

ASAC-DAG #623은 Weather·Traffic 데이터 제품이 raw, bronze, gold, D1 단계에서 어떤 상태로 도달했는지와 실제 데이터 신뢰성 지표를 기록한다. D1 `_catalog`에는 정적 public-Gold projection과 실제 `publication_id`를 함께 보관해 API 소비자가 설명만 보지 않도록 한다. Dashboard와 Worker API는 이 범위에서 변경하지 않는다.

## 보존할 불변식

- 공통 D1 Publisher의 `Contract Load → Gate → Write → row-count verify → _catalog upsert → API smoke` 순서, snapshot last-known-good 보장, rollback은 유지한다.
- `_catalog` upsert가 실패하면 기존 snapshot과 catalog를 복원한다. public-Gold metadata도 같은 upsert에 포함되므로 metadata만 갱신된 반쪽 게시가 생기지 않는다.
- 기존 일별 reliability 알림은 `max_active_runs=1`, Discord 전달 후 fingerprint 기록, 상태 저장 실패 fail-open 규칙을 유지한다.
- 신규 ops artifact 기록 실패는 기존 `runmetrics`와 같은 비차단 관측 실패로 로그에 드러낸다. 데이터 적재·D1 게시의 성공 의미를 observability sink 장애 때문에 바꾸지 않는다.

## D1 catalog 전달

`ServingContract`가 dbt manifest의 `config.meta.public_gold`를 선택적으로 읽고, Publisher가 `_catalog.public_gold` TEXT column에 안정적인 UTF-8 JSON으로 기록한다.

- JSON에는 DBT validator가 산출하는 rich public-Gold 정보와 `mcp_projection`이 들어간다. 시간 역할, timezone, null meaning, quality state, coverage, caveats, do-not-use-for가 그대로 남는다.
- `_catalog.publication_id`는 각 성공 또는 보존된 게시의 런타임 UUID이며 정적 JSON 안에 복제하지 않는다.
- 새 column은 기존 `_ensure_catalog_schema`의 additive migration으로 만든다. 기존 API consumer는 새 column을 무시해도 되고, 새 MCP consumer는 `public_gold`와 sibling `publication_id`를 함께 읽는다.
- Weather·Traffic의 10개 제품에서는 `public_gold` 누락을 contract-load test에서 실패시킨다. 다른 도메인의 기존 Publisher 호환성을 위해 선언이 없는 계약은 `null`로 유지한다.

## 제품 이벤트

새 공통 sink는 append-only JSON을 `ops/product-events/<domain>/observed_date=YYYY-MM-DD/` 아래에 기록한다. 이벤트는 다음 필드를 가진다.

```text
schema_version, event_id, domain, layer, status, product_ids,
dag_id, task_id, source_run_id, try_number, observed_at,
row_count, quality, publication_id, event_reason
```

- `layer`는 `raw`, `bronze`, `gold`, `d1`만 사용한다. 각 object key는 domain/layer/DAG/task/run/retry 식별자를 포함해 재시도 사실을 보존한다.
- raw·bronze 이벤트는 각 도메인의 적재 완료 경계에서, gold 이벤트는 해당 Weather/Traffic Gold transform의 성공·실패 결과에서 기록한다.
- 공통 serving DAG factory는 Publisher 성공 record마다 d1 이벤트를 기록한다. `PublicationError`도 내부 record의 publication id, stage, rollback status, 이유를 기록한 뒤 원래 오류를 다시 올린다.
- 이벤트는 제품별 행 수와 quality/status만 보관하며 원본 payload, request credential, 비식별화되지 않은 API parameter를 보관하지 않는다.

## 제품 헬스 스냅샷

기존 daily reliability data plane에 별도의 `ops/product-health/<domain>/observed_date=YYYY-MM-DD/` snapshot을 추가한다. health snapshot은 `observed_at`, metric 값, 측정 상태, 최신 stage event/D1 catalog evidence를 함께 가진다. R2/D1 읽기 실패는 지표를 `null`과 `unknown`으로 표현하며 데이터 제품을 거짓 PASS로 만들지 않는다.

### Weather

| 지표 | 정의 |
| --- | --- |
| `expected_issue_count` | KMA base interval과 reliability lookback에서 계산한 기대 발표 수 |
| `observed_issue_count` | lookback 안의 관측된 `base_date + base_time` 수 |
| `core_category_coverage` | 현재 장소 예보의 TMP, REH, WSD, POP, SKY, PTY 핵심 카테고리 값 충족 비율 |
| `mapped_place_coverage` | 현재 예보가 있는 `place_id` 수 / weather place mapping 전체 수 |
| `forecast_horizon_hours` | 현재 시점부터 가장 먼 `forecast_at`까지의 시간 |
| `issue_delay_minutes` | 관측 시각 - 최신 `forecast_issued_at_max` |
| `collection_delay_minutes` | 관측 시각 - 최신 `forecast_collected_at_max` |
| `publication_delay_minutes` | 관측 시각 - 4개 Weather D1 제품 중 가장 오래된 `_catalog.exported_at` |

### Traffic

| 지표 | 정의 |
| --- | --- |
| `observed_link_count` | `gold_traffic_flow_link_latest`의 link 행 수 |
| `available_value_ratio` | `flow_value_quality = available` link 비율 |
| `link_age_p50_minutes` / `link_age_p95_minutes` | 관측 시각 - `observed_at_kst`의 link별 p50/p95 |
| `source_observation_delay_minutes` | 관측 시각 - 최신 `observed_at_kst` |
| `collection_delay_minutes` | 관측 시각 - 최신 `collected_at_kst` |
| `stale_link_ratio` | age가 serving freshness SLO(90분)를 넘은 link 비율 |
| `publication_delay_minutes` | 관측 시각 - 6개 Traffic D1 제품 중 가장 오래된 `_catalog.exported_at` |

`publication_delay`는 게시 시각 이후 제품이 얼마나 오래됐는지이며, event-time과 publication-time의 부적절한 차이를 추정하지 않는다. 값의 정의와 단위는 snapshot payload에 포함한다.

## Reliability layer 정정

Weather와 Traffic의 `collect_and_notify`, `collect_pipeline_data_plane` decorator를 `layer="ops"`로 바꾼다. `runmetrics`는 layer 문자열을 제한하지 않으므로 raw/bronze 데이터 스키마를 변경하지 않으며, 문서와 regression test가 reliability report가 ops로 기록됨을 보장한다.

## 검증

- D1 catalog schema migration, rich metadata serialization, rollback 및 다른 도메인 호환성 unit test
- product-event key, retry/failed event, secret-free payload unit test
- Weather·Traffic health query와 null/unknown 상태 unit test
- reliability decorator layer regression test
- serving wrapper와 focused DAG import test
- 실제 외부 API, prod D1, Dashboard는 실행하지 않는다. dev smoke는 자격과 안전 trigger가 있는 경우에만 후속으로 수행한다.
