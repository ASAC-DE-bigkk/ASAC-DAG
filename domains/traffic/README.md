# Traffic 도메인 AI 인덱스

서울 TOPIS `AccInfo` 수집부터 Traffic Silver/Gold 변환, 복구, 신뢰성 보고까지의 실행 코드다.
변경 범위는 `domains/traffic/**`이며 Weather나 다른 도메인 모듈을 import하지 않는다.

## 가장 먼저 읽을 파일

1. 수집 흐름: `traffic_incident_bronze.py`
2. 변환 흐름과 dbt tag: `traffic_incident_transform.py`
3. landing 계약: `traffic_ingest/landing.py`
4. 수집 manifest 계약: `traffic_ingest/run_manifest.py`
5. dbt 실행·artifact 계약: `traffic_dbt_execution.py`
6. 원천 의미: `docs/source.md`
7. 상세 Bronze 배경: `docs/bronze-pipeline-reference.md`

## DAG entrypoints

| 파일 | 책임 |
|---|---|
| `traffic_incident_bronze.py` | TOPIS 수집, raw checkpoint, Bronze 적재·검증, run manifest 상태 전이를 조율한다. |
| `traffic_incident_transform.py` | 최신 publishable Bronze run을 고정하고 root dbt project의 Traffic tag를 순서대로 실행한다. |
| `traffic_snapshot_recovery.py` | 특정 publishable snapshot을 격리된 recovery relation으로 검증하는 dev 전용 수동 DAG다. |
| `traffic_reliability_report.py` | Bronze·manifest·Airflow 실패 증거를 읽어 Discord 신뢰성 리포트를 보낸다. |

`traffic_snapshot_recovery`는 `snapshot_dag_run_id`를 입력받아
`recovery_silver_seoul_traffic_incident`부터 검증하며 canonical Silver/Gold relation은 쓰지 않는다.

DAG entrypoint는 순서와 Airflow wiring만 소유한다. 도메인 로직은 아래 모듈에 둔다.

## 모듈 책임 지도

| 모듈 | 책임 |
|---|---|
| `traffic_ingest/acc_info.py` | TOPIS request/response 파싱과 source-native 필드 |
| `traffic_ingest/landing.py` | pagination, page checkpoint, complete marker, raw object 계약 |
| `traffic_ingest/runtime.py` | HTTP·R2 adapter와 landing/manifest 조립 |
| `traffic_ingest/bronze.py` | Traffic Iceberg Bronze DDL, MERGE/검증 SQL |
| `traffic_ingest/run_manifest.py` | STARTED/SUCCESS/FAILED와 publishability 기록 |
| `traffic_dbt_execution.py` | root dbt 실행, attempt 격리, artifact 보존, 선택 방식 |
| `traffic_dbt_failure.py` | dbt 실패 분류와 안전한 진단 정보 |
| `traffic_lineage.py` | 명시적 opt-in일 때만 DAG OpenLineage selective enable |
| `traffic_ingest/reliability_report.py` | 기존 import를 보존하는 compatibility facade |
| `traffic_ingest/reliability/config.py` | 환경설정, identifier, 상수 |
| `traffic_ingest/reliability/trino_repository.py` | Bronze·manifest read-only 요약 |
| `traffic_ingest/reliability/airflow_evidence.py` | scheduled run·R2 Problem 증거와 redaction |
| `traffic_ingest/reliability/report.py` | 최종 상태와 report dict 조립 |
| `traffic_ingest/reliability/discord.py` | 메시지 formatting과 webhook transport |

## 핵심 실행 흐름

```text
Bronze DAG -> TrafficLanding -> R2 raw/checkpoint -> Bronze MERGE/verify
           -> TrafficRunManifest SUCCESS + is_publishable
Transform DAG -> publishable dag_run_id 고정 -> dbt tag run/test
              -> invocation 전용 manifest.json/run_results.json -> lineage/metrics
Reliability DAG -> Trino + Airflow/R2 evidence -> report -> Discord
```

## dbt 선택·manifest·lineage

- 활성 dbt project는 root monoproject `${ASK_SEOUL_DBT_PROJECT_DIR:-/opt/airflow/dbt}`다.
- 도메인별 하위 dbt project를 따로 실행하지 않고 root project 하나만 사용한다.
- DAG는 모델명을 나열하지 않고 `traffic_incident_transform.py`의 `tag:ask_seoul_traffic_*` 선택자를 호출한다.
- artifact는 `target/<pipeline>/<run_id>/<task_id>/try<n>/execution/` 아래에 invocation별로 둔다.
- 해당 invocation의 `manifest.json`과 `run_results.json`만 읽으며 공유 target fallback을 쓰지 않는다.
- Airflow DAG lineage는 `traffic_lineage.py`, dbt OpenLineage 실행은 `traffic_dbt_execution.py`가 소유한다.

## 변경 시작점

- API·pagination·checkpoint: `traffic_ingest/landing.py`, `traffic_ingest/acc_info.py`
- Bronze schema·적재: `traffic_ingest/bronze.py`
- 수집 정합성·publishability: `traffic_ingest/run_manifest.py`
- dbt tag/실행/artifact: `traffic_incident_transform.py`, `traffic_dbt_execution.py`
- 알림·신뢰성: `traffic_ingest/reliability/`
- 회귀 검증: `tests/test_traffic_*.py`

## 문서 주의

`docs/superpowers/**`와 `docs/retrospectives/**`는 당시 결정·검증 기록이다. 이전 모델 목록이나
pre-monoproject 경로가 남아 있을 수 있으므로 현재 동작의 기준으로 사용하지 않는다.
현재 계약의 우선순위는 실제 코드와 이 README, `docs/source.md`, 그다음 보존 문서 순이다.
