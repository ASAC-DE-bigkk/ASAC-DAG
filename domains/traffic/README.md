# Traffic 도메인 AI 인덱스

서울 TOPIS `AccInfo` 수집부터 Traffic Silver/Gold 변환, 복구, 신뢰성 보고까지의 실행 코드다.
변경 범위는 `domains/traffic/**`이며 Weather나 다른 도메인 모듈을 import하지 않는다.

## 가장 먼저 읽을 파일

1. 현재 아키텍처 결정: `docs/superpowers/specs/2026-07-16-traffic-landing-materialization-design.md`
2. 5분 raw 수집: `traffic_incident_landing.py`
3. receipt 기반 Bronze 적재: `traffic_incident_bronze.py`
4. exact-parent Flow 적재: `traffic_flow_bronze.py`
5. Incident Silver 변환: `traffic_incident_transform.py`
6. Gold 변환: `traffic_gold_transform.py`
7. durable queue 계약: `traffic_ingest/snapshot_receipt.py`
8. 원천 의미: `docs/source.md`

## DAG entrypoints

| 파일 | 책임 |
|---|---|
| `traffic_incident_landing.py` | 5분마다 TOPIS 원본을 R2에 저장하고 `LANDED` receipt와 raw Asset을 발행한다. Trino를 사용하지 않는다. |
| `traffic_incident_bronze.py` | raw Asset 또는 15분 fallback으로 pending receipt를 순서대로 Iceberg Bronze에 적재한다. 단일 `trino_heavy` task다. |
| `traffic_incident_manual.py` | 운영 스케줄과 분리된 수동 recollect/backfill DAG 두 개를 노출한다. |
| `traffic_flow_bronze.py` | Incident Bronze Asset의 정확한 parent run을 기준으로 TrafficInfo를 수집·적재한다. |
| `traffic_incident_transform.py` | Incident Bronze Asset만 받아 exact publishable snapshot을 고정하고 Silver run/test 후 Silver Asset을 발행한다. |
| `traffic_gold_transform.py` | Silver Asset 또는 compatible Flow Bronze Asset을 받아 exact Silver/Flow와 read-only Citydata snapshot을 고정하고 Gold run/test를 실행한다. |
| `traffic_snapshot_recovery.py` | 특정 publishable snapshot을 격리된 recovery relation으로 검증하는 dev 전용 수동 DAG다. |
| `traffic_reliability_report.py` | landing ledger·receipt backlog·Bronze manifest를 읽어 Discord 신뢰성 리포트를 보낸다. |

`traffic_snapshot_recovery`는 `snapshot_dag_run_id`를 입력받아
`recovery_silver_seoul_traffic_incident`부터 검증하며 canonical Silver/Gold relation은 쓰지 않는다.

DAG entrypoint는 순서와 Airflow wiring만 소유한다. 도메인 로직은 아래 모듈에 둔다.

## 모듈 책임 지도

| 모듈 | 책임 |
|---|---|
| `traffic_ingest/acc_info.py` | TOPIS request/response 파싱과 source-native 필드 |
| `traffic_ingest/landing.py` | pagination, page checkpoint, complete marker, raw object 계약 |
| `traffic_ingest/assets.py` | Incident raw/Bronze와 Flow Bronze Asset URI, metadata 검증, Airflow 2/3 schedule adapter |
| `traffic_ingest/snapshot_receipt.py` | R2 `LANDED`/`MATERIALIZED` terminal receipt와 pending index |
| `traffic_ingest/incident_pipeline.py` | Airflow와 분리된 Incident landing/materialization lifecycle |
| `traffic_ingest/flow_pipeline.py` | exact-parent Flow landing/materialization과 stale Asset 억제 |
| `traffic_ingest/runtime.py` | HTTP·R2 adapter와 landing/manifest 조립 |
| `traffic_ingest/bronze.py` | Traffic Iceberg Bronze DDL, MERGE/검증 SQL |
| `traffic_ingest/run_manifest.py` | STARTED/SUCCESS/FAILED와 publishability 기록 |
| `traffic_ingest/run_ledger.py` | 5분 landing slot의 STARTED/SUCCESS/FAILED 증거 |
| `traffic_ingest/transform_admission.py` | Silver/Gold 입력 identity와 versioned success marker의 skip/run 판정 |
| `traffic_ingest/silver_snapshot_fence.py` | Silver Iceberg snapshot과 compacted-file fingerprint의 외부 rewrite 감지 |
| `traffic_ingest/transform_dag_support.py` | snapshot resolve, admission, marker, dbt phase와 실패 처리의 공용 조립 |
| `traffic_dbt_execution.py` | root dbt 실행, attempt 격리, artifact 보존, 선택 방식 |
| `traffic_dbt_failure.py` | dbt 실패 분류와 안전한 진단 정보 |
| `traffic_lineage.py` | 명시적 opt-in일 때만 DAG OpenLineage selective enable |
| `traffic_ingest/reliability_report.py` | 기존 import를 보존하는 compatibility facade |
| `traffic_ingest/reliability/config.py` | 환경설정, identifier, 상수 |
| `traffic_ingest/reliability/trino_repository.py` | Bronze·manifest read-only 요약 |
| `traffic_ingest/reliability/ledger.py` | landing slot invariant와 연속 실패 구간 |
| `traffic_ingest/reliability/backlog.py` | pending receipt 수와 oldest age |
| `traffic_ingest/reliability/report.py` | 최종 상태와 report dict 조립 |
| `traffic_ingest/reliability/discord.py` | 메시지 formatting과 webhook transport |

## 핵심 실행 흐름

```text
Landing (5m, 1 task) -> R2 raw/checkpoint -> LANDED receipt -> raw Asset
Incident Bronze (1 task) -> pending receipt drain -> MERGE/verify
                         -> MATERIALIZED receipt -> Incident Bronze Asset
                         -> task success callback에서 pending ack
Flow Bronze (2 tasks) -> exact Incident parent -> R2 raw -> MERGE/verify
                      -> parent가 여전히 최신일 때만 Flow Bronze Asset
Silver Transform -> Incident Bronze Asset -> exact Incident pin -> admission
                 -> source/Bronze contract -> Silver run/test -> Silver Asset
                 -> success marker -> lineage/metrics
Gold Transform -> Silver Asset OR compatible Flow Bronze Asset
               -> exact Incident/Flow + read-only Citydata snapshot pin -> admission
               -> Gold run/test -> success marker -> lineage/metrics
Reliability -> landing slot + receipt backlog + Bronze manifest -> Discord
```

Materializer의 빈 fallback 실행은 성공으로 끝나지만 Asset을 발행하지 않는다. 따라서
의도적인 `skipped` terminal task나 `all_done`/`one_failed` 보조 task 없이도 DAG 상태와
데이터 발행 여부가 분리된다. 한 task의 실패가 곧 해당 lifecycle의 실패다.
pending ack는 Asset을 담은 task 성공 메시지가 Airflow supervisor에 수락된 뒤에만 수행한다.
그 사이 실패하면 pending이 남아 다음 Asset/fallback run에서 at-least-once로 재처리된다.

Silver/Gold success marker는 모든 write/test가 성공한 뒤 Airflow Variable에 기록한다. 동일한
input identity와 현재 Silver snapshot/file fingerprint가 모두 일치할 때만 재실행을 skip한다.
marker가 없거나 identity가 다르거나 외부 rewrite로 Silver evidence가 달라지면 기존 멱등
reconciliation을 다시 실행한다. Silver Asset 발행과 marker 기록은 별도 task로 직렬화한다.

dev의 `silver_seoul_traffic_incident`는 R2 automatic compaction을 비활성화한 상태가 운영
전제다. smoke 전 Dashboard에서 이 table 한 개의 설정을 확인하며 catalog 전체나 다른 domain
table 설정은 변경하지 않는다. repo-owned Traffic maintenance와 transform writer는 같은
`trino_traffic_heavy` 1-slot을 사용하고, maintenance는 별도 통제 실행 전까지 pause를 유지한다.

## dbt 선택·manifest·lineage

- 활성 dbt project는 domain-owned monoproject
  `${ASK_SEOUL_DBT_PROJECT_DIR:-/opt/airflow/dbt/domains/traffic_weather}`다.
- Traffic와 Weather를 별도 dbt project로 나누지 않고 domain-owned monoproject 하나를 사용한다.
- DAG는 모델명·raw tag·파일 경로를 나열하지 않고 ASAC-DBT `selectors.yml`의
  `ask_seoul_traffic_*` named selector만 호출한다.
- artifact는 `target/<pipeline>/<run_id>/<task_id>/try<n>/execution/` 아래에 invocation별로 둔다.
- 해당 invocation의 `manifest.json`과 `run_results.json`만 읽으며 공유 target fallback을 쓰지 않는다.
- Airflow DAG lineage는 `traffic_lineage.py`, dbt OpenLineage 실행은 `traffic_dbt_execution.py`가 소유한다.

## 변경 시작점

- API·pagination·checkpoint: `traffic_ingest/landing.py`, `traffic_ingest/acc_info.py`
- DAG 간 전달·재처리: `traffic_ingest/snapshot_receipt.py`, `traffic_ingest/assets.py`
- Bronze schema·적재: `traffic_ingest/bronze.py`, `traffic_ingest/incident_pipeline.py`
- Flow parent 계약: `traffic_ingest/flow_pipeline.py`
- 수집 정합성·publishability: `traffic_ingest/run_manifest.py`
- dbt selector·실행·artifact: `traffic_incident_transform.py`, `traffic_gold_transform.py`, `traffic_dbt_execution.py`
- transform admission·snapshot fence: `traffic_ingest/transform_admission.py`, `traffic_ingest/silver_snapshot_fence.py`, `traffic_ingest/transform_dag_support.py`
- 알림·신뢰성: `traffic_ingest/reliability/`
- 회귀 검증: `tests/test_traffic_*.py`

## 문서 주의

`docs/superpowers/**`와 `docs/retrospectives/**`는 당시 결정·검증 기록이다. 이전 모델 목록이나
pre-monoproject 경로가 남아 있을 수 있으므로 현재 동작의 기준으로 사용하지 않는다.
현재 계약의 우선순위는 실제 코드와 이 README, `docs/source.md`, 그다음 보존 문서 순이다.
