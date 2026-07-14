# Traffic 클린 파이프라인 구현 계획

## 범위

`domains/traffic/**`만 변경한다. Weather 또는 다른 domain을 import하지 않는다. Airflow task ID, run/test 분리, callback, 재시도와 실패 분류의 운영 의미는 유지한다.

## 1. dbt 실행 Module

- `traffic_incident_transform.py`와 `traffic_snapshot_recovery.py`는 phase selector와 task dependency만 선언한다.
- domain-owned 실행 Module은 command 조립, `dbt ls` non-empty preflight, target/log path, artifact 검증을 숨긴다.
- `traffic_bronze.seoul_traffic_incident` 및 model/test literal 목록은 DAG에서 제거한다.
- source 40개/axis 7개 exact membership은 root dbt manifest 계약 engine으로 이동한다.
- selector interface: `traffic_transform_contract_gate`, `ask_seoul_traffic_transform_silver`, `ask_seoul_traffic_transform_gold`, recovery phase tags.

## 2. manifest 소유권

- 경로 key는 `dag_id/run_id/task_id/try_number/phase`다.
- `manifest.json`과 `run_results.json`은 같은 invocation만 읽는다.
- selected unique IDs와 결과 status를 terminal artifact에 기록한다.
- dbt dependency 설치는 DAG invocation이 동시에 갱신하지 않는다.

## 3. landing Module

- `traffic_incident_bronze.py`는 수집 순서만 조율한다.
- request redaction, raw object key, payload hash, Iceberg commit, run manifest 원자성을 `traffic_ingest`의 deep Module 뒤로 이동한다.
- 외부 adapter는 TOPIS HTTP, R2, Trino 세 가지이며 테스트는 mock call 횟수가 아니라 landing 결과와 idempotency를 검증한다.

## 4. TDD/검증

1. literal selector 부재, named selector command, invocation path uniqueness를 먼저 실패시키고 구현한다.
2. 기존 task graph와 callback 테스트를 통과시킨다.
3. landing failure/duplicate/retry behavior 테스트를 먼저 작성한 뒤 orchestration을 얇게 만든다.
4. `python -m pytest domains/traffic/tests -q -p no:cacheprovider`와 `compileall`을 실행한다.
5. `git diff --name-only`가 `domains/traffic/**` 밖을 포함하면 실패한다.
6. 커밋, push, PR은 승인 전 실행하지 않는다.
