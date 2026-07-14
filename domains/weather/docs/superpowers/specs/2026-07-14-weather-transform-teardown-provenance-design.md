# Weather transform teardown·artifact provenance 설계

## 목표

`weather_vilage_fcst_transform`의 정상 작업 경로는 한 줄로 유지하면서도 다음 세 가지를 동시에 보장한다.

1. 어떤 dbt 단계가 실패해도 DagRun은 반드시 실패한다.
2. 실패 뒤에도 현재 DagRun이 실제로 생성한 dbt `run_results.json`만 메트릭으로 남긴다.
3. 메트릭 발행 실패는 정상적인 Silver/Gold 변환 결과를 실패로 바꾸지 않는다.

기존 Git 의도는 dbt 단계별 gate·retry·실패 알림 경계를 유지하고, 실패 후 메트릭을 수집하면서 `ALL_DONE` leaf의 false-green을 막는 것이다. PR #348의 callback-only 구조는 자연스러운 business leaf로 DagRun 상태를 정확히 판정하지만, 실패 실행의 메트릭과 current-run artifact provenance를 잃는다.

## 결정

### 1. dbt phase를 run/task/try 단위로 격리한다

Weather의 각 dbt phase는 공통 `dbt_task()` factory와 `run_dbt_phase()` runner를 사용한다. `deps`를 제외한 명령은 다음 경로를 `--target-path`로 사용한다.

```text
/opt/airflow/dbt/domains/weather/target/weather-transform/<run_id>/<task_id>/try<try_number>/
```

`run_id`와 `task_id`는 영문자·숫자·`._=-` 이외 문자를 `-`로 치환한다. 성공 phase는 실제 `run_results.json`이 존재할 때만 그 경로를 return XCom에 기록하고, 실패 phase도 파일이 존재할 때만 `weather_dbt_artifact_path` XCom에 기록한다. `deps`는 artifact 경로와 project vars를 전달하지 않는다.

### 2. 메트릭은 현재 DagRun XCom만 역순으로 해석한다

`_current_run_results_path()`는 dbt phase ID를 실행 순서의 역순으로 조회한다. 각 phase의 성공 return XCom과 실패 artifact XCom을 확인하되 실제 파일이 존재하는 첫 경로만 선택한다.

공유 경로 `/opt/airflow/dbt/domains/weather/target/run_results.json`은 암묵적 fallback으로 사용하지 않는다. 현재 실행 artifact가 없으면 메트릭은 `skipped=True`를 반환하며 이전 실행이나 다른 Weather DAG의 파일을 읽지 않는다.

### 3. 메트릭 task를 non-gating teardown finalizer로 둔다

정상 변환 체인의 마지막 `dbt_test_place_mart` 뒤에 `publish_dbt_run_metrics`를 하나만 연결하고 다음과 같이 표시한다.

```python
publish_dbt_metrics.as_teardown(on_failure_fail_dagrun=False)
```

Airflow 3.2.2에서 `as_teardown()`은 `ALL_DONE_SETUP_SUCCESS`를 설정한다. 직접 연결된 setup이 없으면 모든 upstream 완료만 기다리므로 중간 dbt 실패로 마지막 work task가 `upstream_failed`가 된 경우에도 실행된다. `on_failure_fail_dagrun=False`인 teardown은 DagRun 상태 계산에서 제외되므로 `dbt_test_place_mart`가 effective leaf로 남는다.

```text
validate → dbt phases → dbt_test_place_mart → publish_dbt_run_metrics(teardown)
                        └─ effective leaf      └─ 상태 판정 제외
```

`ONE_FAILED` watcher와 task-to-watcher fan-out은 만들지 않는다. DAG success callback에서도 메트릭을 발행하지 않는다.

### 4. 실패 알림과 상태 판정을 분리한다

- 각 dbt task의 기존 `[notify_weather_transform_failure, record_weather_problem]` callback을 유지한다.
- validation task의 `record_weather_problem` callback을 유지한다.
- metrics teardown에는 `record_weather_problem`을 둔다.
- callback은 관측용이며 DagRun 상태를 보정하지 않는다.

## 실패 행렬

| Work 상태 | Metrics teardown 상태 | 기대 DagRun | 기대 메트릭 |
| --- | --- | --- | --- |
| 성공 | 성공 | 성공 | current-run 최신 artifact 발행 |
| 실패/upstream_failed | 성공 | 실패 | current-run 마지막 artifact 발행 |
| 성공 | 실패 | 성공 | teardown task 실패 기록, 변환 성공 유지 |
| 실패/upstream_failed | 실패 | 실패 | 원래 work 실패 유지 |
| dbt artifact 생성 전 실패 | skipped result | 실패 | stale fallback 없이 0건 |

## 범위

- 변경: Weather transform DAG, 해당 DAG unit test, Weather 설계·계획 문서
- 유지: task ID, dbt selector, W2 canonical revision vars, dev-only target, Bronze Asset schedule, retry 횟수, `trino_heavy` pool, task-level 실패 알림
- 제외: Traffic DAG, dbt model/source/test, R2/Trino 데이터 쓰기, prod 실행, backfill
- 사용자의 명시 지시에 따라 GitHub issue는 생성하지 않는다.

## 검증 기준

- 기존 Weather domain tests가 통과한다.
- 새 테스트가 per-run artifact 경로, `deps` 예외, 성공·실패 XCom, stale shared fallback 금지, teardown topology를 검증한다.
- 실제 Airflow 3.2.2 컨테이너에서 DAG import와 effective-leaf 실패 행렬을 검증한다.
- Python compile, `git diff --check`, 변경 범위와 secret 부재를 확인한다.
