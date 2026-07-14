# Weather 클린 파이프라인 구현 계획

## 범위

`domains/weather/**`만 변경한다. Traffic 또는 다른 domain을 import하지 않는다. W1/W2 안전 guard, callback graph, teardown provenance, task ID의 운영 의미는 유지한다.

## 1. dbt 실행 Module

- `weather_vilage_fcst_transform.py`는 Silver/Gold/place phase selector와 task dependency만 선언한다.
- domain-owned 실행 Module은 `dbt ls` non-empty preflight, command, invocation target/log path, run result 판정을 숨긴다.
- model/test literal 목록과 고정 `target/run_results.json` 경로를 제거한다.
- selector interface: `ask_seoul_weather_transform_silver`, `ask_seoul_weather_transform_gold_summary`, `ask_seoul_weather_transform_place_mart`.
- test selection은 `--indirect-selection=buildable`을 사용하고 DAG-owned singular test는 dbt tag를 직접 가진다.

## 2. manifest 소유권

- 경로 key는 `dag_id/run_id/task_id/try_number/phase`다.
- phase별 selected unique IDs, manifest checksum, run result status를 terminal artifact에 기록한다.
- OpenLineage emission은 이 invocation artifact만 입력으로 받는다.

## 3. landing Module

- `weather_vilage_fcst_bronze.py`는 KMA 수집 순서만 조율한다.
- service key redaction, raw upload, checkpoint, stale cleanup, Iceberg commit, run manifest 원자성을 `weather_ingest`의 deep Module 뒤로 이동한다.
- HTTP/R2/Iceberg adapter의 실제 behavior를 test surface로 삼는다.

## 4. TDD/검증

1. literal selector 부재, named selector command, invocation path uniqueness를 먼저 실패시키고 구현한다.
2. 기존 W1/W2 및 callback/teardown 테스트를 유지한다.
3. landing retry/checkpoint/idempotency 테스트를 먼저 추가한 뒤 orchestration을 얇게 만든다.
4. `python -m pytest domains/weather/tests -q -p no:cacheprovider`와 `compileall`을 실행한다.
5. `git diff --name-only`가 `domains/weather/**` 밖을 포함하면 실패한다.
6. 커밋, push, PR은 승인 전 실행하지 않는다.
