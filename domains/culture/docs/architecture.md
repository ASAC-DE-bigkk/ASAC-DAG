# 아키텍처 — 오케스트레이션 전략 + 코드 지도

culture bronze 수집이 **어떻게(오케스트레이션)** 돌고 **어디에(코드)** 로직이 있는지.

## 1. 오케스트레이션 전략 (Airflow 레벨)

DAG [`culture_bronze_ingest`](../culture_bronze_ingest.py) (스케줄 `@daily`). 태스크 흐름:

```text
plan ──▶ ingest_dataset (12개 동적 매핑 · 병렬) ──▶ report (all_done)
```

- **plan** — 적재할 데이터셋 목록과 공유 `ingest_ts`를 계산. 여기서 `target`을 검증(fail-fast).
- **ingest_dataset** — 데이터셋마다 매핑 태스크 1개. 하나 실패해도 격리·재시도.
- **report** — 모든 결과를 모아 정량 run 리포트 적재(`all_done`로 항상 실행).

### 설계 결정 (왜)

> 🚧 TODO(후속 PR): 각 항목 서술
- **데이터셋당 매핑 태스크 1개** — 실패 격리·재시도·그리드 가시성.
- **멱등성(`ingest_ts` 파티션)** — 재시도/부분 실행이 이전 데이터를 안 덮어씀(delete-then-insert). → [storage.md](storage.md)
- **target fail-closed** — 오타가 prod로 새지 않게 `_plan`에서 즉시 실패. → [change-log #41](../change-log.md)
- **coverage 분모 = plan** — 실패가 리포트에서 사라지지 않게. → [reliability.md](reliability.md) · [change-log #39](../change-log.md)
- **롤링 날짜창 / lookback** — …
- **계약 위반 게이트 opt-in(`fail_on_violation`)** — …

### 데이터 흐름 (오케스트레이션 관점)

> 🚧 TODO(후속 PR): source API → landing(R2 raw) → (선택)bronze Iceberg 순서 다이어그램/서술

## 2. 코드 지도 (패키지·모듈)

`culture_bronze_ingest.py`(DAG 엔트리)는 얇게 흐름만 잡고, 로직은 `culture_ingest/` 패키지에
위임한다. **common**(도메인 무관 프레임워크) vs **source**(culture 전용)로 나뉜다.

| 파일 | 역할 |
|------|------|
| [`culture_bronze_ingest.py`](../culture_bronze_ingest.py) | 일배치 DAG. `plan → ingest_dataset → report`. |
| [`culture_ingest/common/config.py`](../culture_ingest/common/config.py) | R2 접속정보(dev/prod), `.env` 파싱, `RunContext`, 키 prefix, `normalize_target`. |
| [`culture_ingest/common/http.py`](../culture_ingest/common/http.py) | 429/5xx 재시도 `requests` 세션, 페이지 묶음 `Page`. |
| [`culture_ingest/common/landing.py`](../culture_ingest/common/landing.py) | R2/로컬 싱크, 페이지·`_manifest.json` 기록, 결과 `DatasetResult`. |
| [`culture_ingest/common/checks.py`](../culture_ingest/common/checks.py) | 수집 계약 v0 검증(완전성·드리프트·freshness). → [reliability.md](reliability.md) |
| [`culture_ingest/common/warehouse.py`](../culture_ingest/common/warehouse.py) | bronze Iceberg 테이블 생성/적재(`BronzeWarehouse`, Trino HTTP). → [storage.md](storage.md) |
| [`culture_ingest/source/config.py`](../culture_ingest/source/config.py) | 적재 루트 `bronze/culture`, 소스 API 키 로딩. |
| [`culture_ingest/source/clients.py`](../culture_ingest/source/clients.py) | KOPIS(XML)·서울(JSON) HTTP 클라이언트. 원본 bytes만 받음. |
| [`culture_ingest/source/datasets.py`](../culture_ingest/source/datasets.py) | 12데이터셋 레지스트리(단일 진실 원천). → [sources.md](sources.md) |
| [`culture_ingest/source/ingest.py`](../culture_ingest/source/ingest.py) | 적재 오케스트레이션(`run_batch`/`ingest_one`), run 리포트 빌드. |
| [`scripts/run_culture_ingest.py`](../scripts/run_culture_ingest.py) | Airflow 없이 로컬 실행 CLI. → [operations.md](operations.md) |

### 진입점 & 콜그래프

> 🚧 TODO(후속 PR): `ingest_one`(DAG) vs `run_batch`(CLI) → `ingest_dataset` → `landing`/`warehouse` 콜그래프

### `.airflowignore` — DAG 스캔 vs import

`culture_ingest/`·`scripts/`는 DAG 스캔에서 제외(import만). DAG가 자기 디렉토리를 `sys.path`에
넣고 `culture_ingest.*`를 불러온다.
