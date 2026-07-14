# Weather Transform Teardown·Artifact Provenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Weather transform의 직렬 business graph를 유지하면서 실패 후 current-run dbt 메트릭을 수집하고 DagRun false-green과 stale artifact 적재를 함께 방지한다.

**Architecture:** 모든 dbt phase를 `run_dbt_phase()`와 `dbt_task()`로 실행해 run/task/try별 target에 artifact를 남긴다. 마지막 dbt test 뒤의 metrics operator는 `on_failure_fail_dagrun=False` teardown으로 실행하고, 현재 DagRun의 성공·실패 XCom에서 존재하는 최신 artifact만 선택한다.

**Tech Stack:** Python 3.11, Apache Airflow 3.2.2, dbt-core 1.10, pytest

## Global Constraints

- `domains/weather/**`만 수정한다.
- 기존 task ID, dbt selector, W2 canonical revision vars, dev-only target, Bronze Asset schedule를 유지한다.
- dbt task는 `retries=1`, 기존 retry delay, `trino_heavy` pool, 기존 task-level failure callbacks를 유지한다.
- `deps`에는 `--target-path`와 project `--vars`를 전달하지 않는다.
- 암묵적 metrics publication은 current-run XCom만 사용하고 공유 `target/run_results.json`으로 fallback하지 않는다.
- metrics는 `on_failure_fail_dagrun=False` teardown이며 work 실패를 가리거나 metrics 실패로 work 성공을 뒤집지 않는다.
- `ONE_FAILED` watcher와 DAG success metrics callback을 함께 두지 않는다.
- 사용자의 명시 지시에 따라 issue를 생성하지 않고 PR base는 `dev`로 한다.
- secret, 생성된 dbt target, pytest cache, bytecode, Airflow runtime 파일을 커밋하지 않는다.

---

### Task 1: Weather dbt artifact provenance

**Files:**
- Modify: `domains/weather/tests/test_weather_transform_dbt_selection.py`
- Modify: `domains/weather/weather_vilage_fcst_transform.py`

**Interfaces:**
- Produces: `_artifact_path(run_id, task_id, try_number) -> str`
- Produces: `run_dbt_phase(*, dbt_args, include_project_vars=True, **context) -> dict[str, str | None]`
- Produces: `dbt_task(task_id, dbt_args, *, include_project_vars=True) -> PythonOperator`
- Produces: `_current_run_results_path(**context) -> str | None`
- XCom key: `WEATHER_DBT_ARTIFACT_XCOM_KEY = "weather_dbt_artifact_path"`

- [ ] **Step 1: artifact와 runner 계약의 RED 테스트를 작성한다.**

  다음 동작을 각각 독립 테스트로 고정한다.

  ```python
  assert module._artifact_path(
      run_id="manual__2026-07-14T01:02:03+00:00",
      task_id="dbt/test",
      try_number=2,
  ).endswith(
      "weather-transform/manual__2026-07-14T01-02-03-00-00/dbt-test/try2/run_results.json"
  )
  ```

  `subprocess.run`의 complete result만 최소 대체해 다음 command 계약도 확인한다.

  ```python
  assert "--target-path" not in deps_command
  assert "--vars" not in deps_command
  assert model_command[model_command.index("--target") + 1] == "dev"
  assert model_command[model_command.index("--target-path") + 1] == str(expected.parent)
  assert json.loads(model_command[model_command.index("--vars") + 1]) == {
      "weather_w2_canonical_revision_date": "2025-04-01"
  }
  ```

- [ ] **Step 2: RED를 확인한다.**

  Run:

  ```powershell
  $env:PYTHONDONTWRITEBYTECODE='1'
  python -m pytest -p no:cacheprovider domains/weather/tests/test_weather_transform_dbt_selection.py -q
  ```

  Expected: `_artifact_path`, `run_dbt_phase`, `dbt_task`, current-run resolver가 없어 실패한다.

- [ ] **Step 3: 최소 runner와 factory를 구현한다.**

  구현의 중심 계약은 다음과 같다.

  ```python
  def run_dbt_phase(*, dbt_args: str, include_project_vars: bool = True, **context):
      ti = context["ti"]
      artifact_path = _artifact_path(
          run_id=context.get("run_id"),
          task_id=getattr(ti, "task_id", None),
          try_number=getattr(ti, "try_number", None),
      )
      command = [DBT_BIN, *shlex.split(dbt_args), "--target", target, "--no-use-colors"]
      if include_project_vars:
          command.extend(["--vars", json.dumps(WEATHER_DBT_CONTRACT_VARS, separators=(",", ":"))])
      if shlex.split(dbt_args)[0] != "deps":
          command.extend(["--target-path", str(Path(artifact_path).parent)])
      completed = subprocess.run(
          command, cwd=DBT_PROJECT, env=env, check=False,
          capture_output=True, text=True,
      )
  ```

  stdout/stderr를 task log로 전달한다. `run_results.json`이 존재하면 성공 return XCom 또는 실패 전 `WEATHER_DBT_ARTIFACT_XCOM_KEY`에 경로를 남기고, non-zero return code는 `AirflowException`으로 다시 던져 기존 retry를 유지한다. `deps` 성공 결과의 `artifact_path`는 `None`이다.

- [ ] **Step 4: current-run resolver의 RED 테스트를 작성하고 확인한다.**

  임시 파일을 사용해 다음을 검증한다.

  - 가장 진행된 성공 artifact가 선택된다.
  - 뒤 phase에 파일 없는 경로가 있으면 앞 phase의 실제 파일을 선택한다.
  - 실패 XCom artifact도 선택한다.
  - current-run XCom이 없으면 공유 target 파일이 존재해도 `None`이다.

- [ ] **Step 5: resolver와 publisher를 구현하고 GREEN을 확인한다.**

  `_current_run_results_path()`는 `DBT_PHASE_TASK_IDS`를 역순으로 순회하고 `os.path.exists()`를 만족하는 성공·실패 XCom 경로만 반환한다. `publish_dbt_run_metrics(run_results_path=None, **context)`는 명시 경로가 없을 때 이 resolver만 사용한다.

  Run: Step 2 command
  Expected: provenance 관련 테스트를 포함해 전부 통과한다.

- [ ] **Step 6: task-scoped 검토 후 커밋한다.**

  ```powershell
  git add domains/weather/tests/test_weather_transform_dbt_selection.py domains/weather/weather_vilage_fcst_transform.py
  git commit -m "fix(weather): dbt artifact provenance를 실행별로 격리한다"
  ```

### Task 2: non-gating metrics teardown

**Files:**
- Modify: `domains/weather/tests/test_weather_transform_dbt_selection.py`
- Modify: `domains/weather/weather_vilage_fcst_transform.py`

**Interfaces:**
- Consumes: `publish_dbt_run_metrics(**context)` from Task 1
- Produces DAG task: `publish_dbt_run_metrics` with `is_teardown=True`

- [ ] **Step 1: teardown topology RED 테스트를 작성한다.**

  Fake operator의 `as_teardown()`이 실제 Airflow 속성을 모델링하게 하고 다음을 검증한다.

  ```python
  metrics = dag.task_dict["publish_dbt_run_metrics"]
  assert metrics.is_teardown is True
  assert metrics.on_failure_fail_dagrun is False
  assert metrics.kwargs["trigger_rule"] == "all_done_setup_success"
  assert dag.task_dict["dbt_test_place_mart"].downstream_task_ids == {
      "publish_dbt_run_metrics"
  }
  assert "fail_transform_if_upstream_failed" not in dag.task_ids
  assert "on_success_callback" not in dag.kwargs
  ```

- [ ] **Step 2: RED를 확인한다.**

  Run: Task 1 Step 2 command
  Expected: callback-only topology가 남아 있어 metrics task assertion이 실패한다.

- [ ] **Step 3: metrics operator를 teardown으로 구현한다.**

  ```python
  publish_dbt_metrics = PythonOperator(
      task_id="publish_dbt_run_metrics",
      python_callable=publish_dbt_run_metrics,
      on_failure_callback=record_weather_problem,
  ).as_teardown(on_failure_fail_dagrun=False)

  dbt_test_place_mart >> publish_dbt_metrics
  ```

  `publish_weather_transform_success_metrics`와 DAG `on_success_callback`을 제거한다. 별도 setup과 watcher를 추가하지 않는다.

- [ ] **Step 4: GREEN과 Airflow 3.2.2 상태 행렬을 확인한다.**

  focused pytest를 통과시킨 뒤 실행 중인 Airflow 3.2.2 컨테이너에서 DAG를 import한다. effective leaf가 `dbt_test_place_mart`인지 확인하고 다음 결과를 검증한다.

  ```text
  work failed + metrics success -> DagRun failed
  work success + metrics failed -> DagRun success
  all success -> DagRun success
  ```

- [ ] **Step 5: task-scoped 검토 후 커밋한다.**

  ```powershell
  git add domains/weather/tests/test_weather_transform_dbt_selection.py domains/weather/weather_vilage_fcst_transform.py
  git commit -m "fix(weather): metrics를 non-gating teardown으로 실행한다"
  ```

### Task 3: 전체 검증과 PR 준비

**Files:**
- Verify: `domains/weather/weather_vilage_fcst_transform.py`
- Verify: `domains/weather/tests/test_weather_transform_dbt_selection.py`
- Verify: `domains/weather/docs/superpowers/specs/2026-07-14-weather-transform-teardown-provenance-design.md`
- Verify: `domains/weather/docs/superpowers/plans/2026-07-14-weather-transform-teardown-provenance.md`

**Interfaces:**
- Consumes: Task 1·2의 최종 DAG와 테스트 계약
- Produces: `dev` 대상 draft PR 검증 증적

- [ ] **Step 1: Weather domain tests를 실행한다.**

  ```powershell
  $env:PYTHONDONTWRITEBYTECODE='1'
  python -m pytest -p no:cacheprovider domains/weather/tests -q
  ```

- [ ] **Step 2: compile과 컨테이너 DAG import를 실행한다.**

  ```powershell
  python -m py_compile domains/weather/weather_vilage_fcst_transform.py
  docker compose exec -T airflow-scheduler airflow dags list-import-errors --output json
  ```

- [ ] **Step 3: scope와 diff를 검토한다.**

  ```powershell
  git diff --check origin/dev...HEAD
  git diff --name-only origin/dev...HEAD
  git status --short --branch
  ```

  Expected: 위 네 Weather 경로와 task review에서 필요한 Weather 문서만 포함하며 secret·generated artifact가 없다.

- [ ] **Step 4: 전체 branch 코드리뷰를 받고 Critical·Important 항목을 해소한다.**

- [ ] **Step 5: 명시 경로만 push하고 `dev` 대상 draft PR을 만든다.**

  PR 본문은 공용 template의 섹션을 유지하고 이슈를 생략한 사용자 지시, 변경 요약, 검증 결과, 데이터 무영향, artifact 경로 계약을 UTF-8 body file로 기록한다.
