# Architecture

CLAUDE.md 원칙을 코드로 옮긴 배치 아키텍처. 현재 **DAG 라인은 bronze(원본 수집) 전용**이며,
silver 가공 로직은 보존하되 오케스트레이션에서 분리되어 있다(§DAG 구조 참고). 서빙 DB·외부
매니페스트 없이 **run_id 폴더의 마커**로 상태를 관리한다(파싱/serving 계층 없음).

## 실행 모드

**Maintainability First**(CLAUDE.md §1 Mode B). 근거: 반복 배치, 이력 누적, 완전성/재수집
요건이 전제. 단, 과도한 설계는 배제(§14) — Kafka/Spark/K8s/serving DB 없이 Python + Airflow +
오브젝트 스토리지(local/R2)만 사용.

## 데이터 흐름

```text
discover(registry) ──> fetch(끝까지 순회) ──> [BRONZE]            ┊┄> normalize ┄> [SILVER]
 (수집 대상)            (원본 페이지)         run_id 폴더:          (로직만 보존)  parquet
                                            <short>.jsonl + 마커   (DAG 미와이어링)
                                                  │
                                                  └──> [_markers] (completed|incomplete — DB·매니페스트 대체)
```
*(DAG 오케스트레이션은 BRONZE 까지. `┊┄>` 구간은 [include/silver/](../../include/silver/) 에 로직만 보존되고 DAG 결선 없음.)*

각 계층의 책임(CLAUDE.md §9):

| 계층 | 포맷 | 책임 | 변경성 |
|---|---|---|---|
| **bronze** | `<short>.jsonl`(원본 페이지 NDJSON) | 소스 truth 보존(끝까지 순회) | run_id 스냅샷, 불변 |
| **마커** | `_markers/<short>.completed\|.incomplete`(JSON) | 수집 결과·리니지·이력 | ingest 가 API당 1개 |
| **silver** | Parquet | 공통 19컬럼 정규화·다운스트림용 | `observed_date` 파티션 재생성 |

> 파싱(parsed)·serving DB·벡터 계층은 미포함. 필요 시 silver 이후 파생물로 추가(CLAUDE.md §10).

## DAG 구조 (`seoul_commerce_daily` · `seoul_commerce_recollect`)

[../seoul_commerce_dag.py](../../seoul_commerce_dag.py). 공통 태스크를 공유하는 **DAG 2개**:
**daily**(전체 수집) · **recollect**(미완료만 재수집, 6h). 흐름은 동일, target 선정만 다르다.

```text
resolve_observed_date ─┐
make_bronze_run_id ────┤
check_api_key (gate) ──┤
(plan_all|find_incomplete)─┴─> ingest_one.expand ──> finalize_run ─> (_RUN 마커/metrics)
```

> **silver 분리**: 이 DAG 라인은 **bronze(원본 수집) 전용**이다. silver 가공은 DAG 오케스트레이션에서
> 분리되어 있고, 가공 로직만 [../../include/silver/](../../include/silver/) 에 보존된다(별도 silver DAG 없음).

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
- **알림 인터페이스**(예외→알림, [common/notify.py](../../include/common/notify.py))는 제공되나 **미와이어링/비활성**
  (기본 no-op) — [../operations/recollect-and-alerts.md](../operations/recollect-and-alerts.md) §2.

태스크는 모두(CLAUDE.md §11): **재시도 안전**(같은 run_id 폴더에 덮어씀) · **관찰 가능**
(마커 + finalize metrics) · **작게 분리**. 중복 제거는 silver 가 `MGTNO` 로.

## 멱등성 & 백필

- 매 실행이 전체 수집 → 별도 스킵/force 없음. 같은 업장 중복은 **silver 가 `MGTNO` 로 제거**.
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
         │  postgres  │        │  trino   │ (dbt/iceberg — commerce 미사용)
         │ (metadata) │        └──────────┘
         └────────────┘
```

- **LocalExecutor** — 별도 워커/브로커(Celery/Redis) 없음. 태스크는 scheduler 프로세스가 실행.
- Postgres 는 **Airflow 메타데이터 전용**(serving DB 없음).
- Trino/Iceberg/dbt 는 호스트의 다른 스모크 라인용 — commerce 파이프라인은 사용하지 않는다.
- 호스트 컴포즈/이미지는 이 번들 밖(별도 리포). 환경/인자: [environments.md](../configuration/environments.md) ·
  [configuration.md](../configuration/configuration.md).
