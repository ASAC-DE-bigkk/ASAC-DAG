# Architecture

CLAUDE.md 원칙을 코드로 옮긴 배치 아키텍처. **메달리온 레이어**(raw→bronze→silver→gold)를 DAG 로
오케스트레이션한다: 수집(`commerce_collect_raw`/`recollect_raw`) → Iceberg 적재
(`commerce_load_bronze`) → dbt 정규화(`commerce_load_silver`). gold 는 미구현. 서빙 DB·외부
매니페스트 없이 **run_id 폴더 마커 + Iceberg 발행 manifest + silver DONE 마커**로 상태를 관리한다.
전체 계보·테이블 정의: [../pipeline/data-model.md](../pipeline/data-model.md).

## 실행 모드

**Maintainability First**(CLAUDE.md §1 Mode B). 근거: 반복 배치, 이력 누적, 완전성/재수집
요건이 전제. 단, 과도한 설계는 배제(§14) — Kafka/Spark/K8s/serving DB 없이 Python + Airflow +
오브젝트 스토리지(local/R2)만 사용.

## 데이터 흐름

```text
discover(registry 152종) ─> fetch(끝까지 순회) ─> [raw] run_id 폴더: <short>.jsonl + _markers
                                                        │  commerce_load_bronze(최근 N일 증분)
                                                        ▼
                                                   [bronze] Iceberg bronze_localdata_license
                                                     record_json 통짜(schema-on-read) + 발행 manifest
                                                        │  commerce_load_silver(dbt/Trino)
                                                        ▼
                                                   [silver] history/current/detail (v1/v2 통합 = lf 매크로)
                                                        ┊┄> [gold] (미구현)
```
*(수집=raw(R2 NDJSON), 적재=bronze(Iceberg), 정규화=silver(dbt). v1/v2 필드는 silver 에서 하나로 합쳐진다 — [../pipeline/data-model.md §2](../pipeline/data-model.md).)*

각 계층의 책임(CLAUDE.md §9):

| 계층 | 포맷 | 책임 | 변경성 |
|---|---|---|---|
| **raw** | R2 `<short>.jsonl`(row-NDJSON) + `_markers/` | 소스 truth 보존(끝까지 순회)·수집 상태 | run_id 스냅샷, 불변 |
| **bronze** | Iceberg `bronze_localdata_license` | `record_json` 통짜(schema-on-read) 변경로그 + 발행 `manifest` | append-only, 멱등 `(dataset, bronze_run_id)` |
| **silver** | dbt(Trino/Iceberg) | **152종 공통(v1/v2 통합, `lf()` 매크로)** 정규화 — history/current/detail | incremental(marker 기반) |
| **gold** | — | (미구현) silver current 집계 예정 | — |

> 파싱(parsed)·serving DB·벡터 계층은 미포함. 필요 시 gold 이후 파생물로 추가(CLAUDE.md §10).

## DAG 구조 (`commerce_collect_raw` · `commerce_recollect_raw`)

[../commerce_raw.py](../../commerce_raw.py). 공통 태스크를 공유하는 **DAG 2개**:
**daily**(전체 수집) · **recollect**(미완료만 재수집, 6h). 흐름은 동일, target 선정만 다르다.

```text
resolve_observed_date ─┐
make_bronze_run_id ────┤
check_api_key (gate) ──┤
(plan_all|find_incomplete)─┴─> ingest_one.expand ──> finalize_run ─> (_RUN 마커/metrics)
```

> **이 절은 수집(raw) DAG 만 다룬다.** 이후 레이어는 별도 DAG 다: **`commerce_load_bronze`**(04:00 KST,
> raw→Iceberg 적재 + Iceberg 유지보수) · **`commerce_load_silver`**(05:00 KST, dbt 정규화·보강·테스트·리포트) ·
> **`commerce_collect_watchdog`**(수집 누락 감시 알림). 상세: [../pipeline/bronze/](../pipeline/bronze/README.md) ·
> [../pipeline/silver/](../pipeline/silver/README.md).

- `catchup=False`, `max_active_runs=1`, `retries=2`, `retry_delay=3m`.
- **`make_bronze_run_id`**: 실행시각(KST·ms) 폴더명을 1회 계산 → 모든 ingest 가 공유(같은 run_id 폴더).
- **target 선정**: daily=`plan_all_targets`(전체) · recollect=`find_incomplete_targets`(최근 run 의
  미완료 API만; 대상 0개면 수집 안 함 → 빈 매핑, run 폴더 미생성). → [../operations/recollect-and-alerts.md](../operations/recollect-and-alerts.md) §1.
- **Dynamic Task Mapping + API별 라벨**: `ingest_one.expand(short=…)` — 데이터셋마다 태스크 1개,
  `map_index_template="{{ short }}"` 로 **Grid/Graph 에 API 이름으로 표시**(성공/실패/대기 가시화).
  실패 격리(한 데이터셋 오류가 전체를 막지 않음). 각 ingest 는 자기 API 파일 1개 + 마커 1개만 쓴다.
- **gate**: `check_api_key` 가 인증키를 선검증 → 키 오류면 전체 빠른 실패.
- 태스크 간에는 **저장 키/요약(작은 dict)** 만 XCom 으로 전달, 페이로드는 스토리지 재조회.
- `params`: `observed_date`(silver 논리일 override). force 없음 — 매 실행이 전체 수집.
- **알림**: DAG 완료 리포트(#218)·품질 경고·`commerce_collect_watchdog`(수집 누락)로 **Discord 통지가 동작**한다
  (webhook URL 을 env 로 설정한 경우) — [../operations/recollect-and-alerts.md](../operations/recollect-and-alerts.md) §2.

태스크는 모두(CLAUDE.md §11): **재시도 안전**(같은 run_id 폴더에 덮어씀) · **관찰 가능**
(마커 + finalize metrics) · **작게 분리**. 중복 제거는 silver 가 `(OPNSFTEAMCODE, MGTNO)` 로.

## 멱등성 & 백필

- 매 실행이 전체 수집 → 별도 스킵/force 없음. 같은 업장 중복은 **silver 가 `(OPNSFTEAMCODE, MGTNO)` 로 제거**.
- `incomplete` API 는 다음 실행에서 자연히 재수집. 특정 실행 무효화는 그 `run_id` 폴더 삭제.
- 특정일: `observed_date` 파라미터(silver 파티션) / 범위: `airflow dags backfill`.
- 자세한 절차는 [operations.md](../operations/operations.md), 완전성 점검은 [common_info.md](../pipeline/common_info.md) §4-1.

## 컴포넌트 (호스트 스택 — `elt-infra`, LocalExecutor)

```text
┌─────────────┐  ┌───────────┐  ┌──────────────┐  ┌────────────┐
│ apiserver   │  │ scheduler │  │ dag-processor│  │ triggerer  │
│ :30585→8080 │  │ (+ 태스크 │  │ (DAG 파싱)   │  │            │
└──────┬──────┘  │  실행)    │  └──────────────┘  └────────────┘
       │         └─────┬─────┘            │
       │               │ storage(local/R2)│
       └───────┬───────┴──────────────────┘
         ┌─────▼──────┐        ┌──────────┐
         │  postgres  │        │  trino   │ (dbt/iceberg — bronze 적재·silver dbt)
         │ (metadata) │        └──────────┘
         └────────────┘
```

- **LocalExecutor** — 별도 워커/브로커(Celery/Redis) 없음. 태스크는 scheduler 프로세스가 실행.
- Postgres 는 **Airflow 메타데이터 전용**(serving DB 없음).
- Trino/Iceberg/dbt(R2 Data Catalog) 는 commerce 의 **bronze 적재**(PyIceberg/Trino)와 **silver 정규화**(dbt)에 사용된다.
- 호스트 컴포즈/이미지는 이 번들 밖(별도 리포). 환경/인자: [environments.md](../configuration/environments.md) ·
  [configuration.md](../configuration/configuration.md).
