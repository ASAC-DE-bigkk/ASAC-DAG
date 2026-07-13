# Traffic dbt deps target-path 회귀 수정 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `dbt_deps`가 지원하지 않는 artifact option으로 실패하지 않게 하고, 다른 dbt phase의 artifact 격리를 유지한다.

**Architecture:** `run_dbt_phase`가 `dbt_args`로 전달받은 첫 dbt subcommand가 `deps`인지 판별한다. `deps`에는 `--target-path`를 추가하지 않고, 나머지 phase에는 기존의 per-run artifact path를 유지한다.

**Tech Stack:** Python 3.11, Airflow PythonOperator, dbt-core 1.10.22, pytest, Docker Compose.

## Global Constraints

- dev Airflow/dbt/Trino 환경만 사용한다.
- pinned snapshot 변수와 failure classification 동작은 변경하지 않는다.
- `deps` 외의 phase에는 run/task/try별 artifact 경로를 계속 전달한다.

---

### Task 1: dbt phase별 artifact option 계약

**Files:**
- Modify: `domains/traffic/tests/test_traffic_transform_dbt_selection.py`
- Modify: `domains/traffic/traffic_incident_transform.py`

**Interfaces:**
- Consumes: `run_dbt_phase(dbt_args, snapshot_task_id, silver_persisted, **context)`
- Produces: `subprocess.run`에 전달되는 dbt command list

- [ ] **Step 1: Write the failing test**

```python
def test_dbt_deps_omits_target_path_but_model_phases_keep_isolated_artifacts(monkeypatch):
    # Capture the commands built by run_dbt_phase for deps and run.
    assert "--target-path" not in deps_command
    assert "--target-path" in run_command
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_dbt_selection.py -q`

Expected: FAIL because the existing runner includes `--target-path` for `deps`.

- [ ] **Step 3: Write minimal implementation**

```python
if shlex.split(dbt_args)[0] != "deps":
    command.extend(["--target-path", str(Path(artifact_path).parent)])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest domains/traffic/tests/test_traffic_transform_dbt_selection.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add domains/traffic/traffic_incident_transform.py domains/traffic/tests/test_traffic_transform_dbt_selection.py domains/traffic/docs/superpowers
git commit -m "fix(traffic): omit target path from dbt deps"
```

### Task 2: dev runtime 검증과 운영 회복

**Files:**
- No additional tracked files

**Interfaces:**
- Consumes: updated bind-mounted DAG file and existing dev dbt project
- Produces: successful `dbt deps` and successful `traffic_incident_transform` recovery run

- [ ] **Step 1: Validate exact CLI surface**

Run: `dbt deps --target dev --no-use-colors --vars '{"traffic_snapshot_dag_run_id": "<resolved-id>"}'`

Expected: packages install successfully without `--target-path`.

- [ ] **Step 2: Run regression suite and import check**

Run: `python -m pytest domains/traffic/tests -q` and `python -m compileall -q domains/traffic/traffic_incident_transform.py`.

Expected: all selected tests pass and compile succeeds.

- [ ] **Step 3: Trigger a dev transform run after merge**

Expected: `dbt_deps` and downstream dbt phase tasks succeed; Silver/Gold are refreshed from the run's pinned latest publishable snapshot.
