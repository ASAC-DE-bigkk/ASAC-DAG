# Serving P0 Publication Safety 설계

## 목적

Weather·Traffic 공통 D1 Publisher가 적재 뒤의 검증·catalog·API smoke에서 실패해도 마지막 정상 snapshot과 catalog를 보존하고, 대용량 snapshot의 D1 HTTP 왕복을 안전하게 줄인다.

## 설계

- Snapshot은 `__staging`에만 동적 batch를 적재한다. 활성 테이블을 `__previous`으로 보관한 뒤 staging을 활성화하고, read-back·catalog·smoke 실패 시 `__previous`으로 복구한다.
- `INSERT ... VALUES` statement는 실제 UTF-8 byte 기준 `80_000` bytes 이하로 만든다. 단일 row가 이를 넘으면 D1 요청 전에 실패한다.
- HTTP request는 동적 statement를 최대 4개, 총 `256_000` bytes 이하의 `batch` body로 보낸다. D1 batch의 statement별 100,000-byte 및 전체 30-second 제한을 넘지 않기 위한 guardrail이다.
- 제품별 append-only `_publication_ledger`에 publication ID, source run, stage, row count, statement/batch count, 최대 statement bytes, D1 metrics, outcome, recovery outcome을 기록한다.
- catalog는 product별 smoke가 성공한 뒤에만 갱신한다. external API URL 미설정은 `not_evaluated`로 기록하지만 API-ready 성공으로 해석하지 않는다.
- append/upsert는 현재 active-table 변경 방식이므로 snapshot처럼 완전 rollback하지 않는다. gate·PK read-back·ledger만 P0에서 보강한다.

## 비범위

- prod D1, Gateway/Worker, MCP, Weather export 자동 trigger는 범위 밖이다.
- 단순 cron 기반 Traffic export는 추가하지 않는다.

## 검증

1. SQL statement와 HTTP batch의 byte/개수 제한을 pure unit test로 검증한다.
2. staging write, read-back, catalog, smoke 실패마다 snapshot/catalog 복구와 ledger 기록을 검증한다.
3. 실제 dev D1에 risk window를 publish해 HTTP batch 수, row count, catalog, ledger를 확인한다.
