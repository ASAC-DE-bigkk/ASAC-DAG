# Traffic Gold Reconciliation Fence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Traffic Gold run과 exact full-history Gold test 사이에 Bronze write가 끼지 않도록 Airflow pool priority fence를 추가한다.

**Architecture:** 기존 `trino_heavy` 1-slot pool과 `DbtPhaseSpec.pin_critical` 계약을 재사용한다. `dbt_run_gold`는 priority 1로 유지하고 `dbt_test_gold`만 priority 10으로 올려, Gold test가 끝날 때까지 Weather/Traffic의 priority 1 Trino task를 약 2~3분 대기시킨다. DBT 모델·selector·test SQL과 full-history 의미는 변경하지 않는다.

**Tech Stack:** Python 3.11, Apache Airflow 3.2.2, pytest, Ruff, dbt Core 1.10.22, dbt-trino 1.10.2, Trino 482, Docker Compose, GitHub PR

## Global Constraints

- production code 변경은 `domains/traffic/**`에만 둔다.
- 실행·탐색·검증 대상은 Weather와 Traffic만 허용한다.
- Commerce, 시티데이터, Transit, 문화정보 파이프라인은 파싱·실행·수정·백필하지 않는다.
- dev catalog/schema만 사용하며 prod/shared schema, full refresh, 대량 backfill을 사용하지 않는다.
- ASAC-DBT의 Gold 모델·selector·test SQL은 변경하지 않는다.
- exact full-history reconciliation, canonical grain, dedup, event/ingest time, idempotency를 낮추지 않는다.
- `.env`와 secret 원문은 읽거나 출력하지 않는다. compose 실행에서는 기존 `.env` 경로만 참조한다.
- `.omc`, `.omx`, `__pycache__`, `.pytest_cache`를 stage·commit·push하지 않는다.
- 원래 dirty root와 사용자 WIP는 수정하거나 정리하지 않는다.
- W2 recovery와 Iceberg maintenance는 별도 설계·검증 전까지 paused 상태로 유지한다.
- 새 issue를 만들지 않는다. PR은 `Closes #420`으로 연결하고 merge 직후 closed 상태를 확인한다.
- ASAC-DAG #419와 ASAC-DBT #117은 미해결 acceptance criteria가 있으므로 이번 작업에서 닫지 않는다.

## File Map

- Modify: `domains/traffic/tests/test_traffic_transform_dag.py:23-40` — Airflow task의 pool·absolute priority 계약을 검증한다.
- Modify: `domains/traffic/tests/test_traffic_transform_contract.py:511-527` — `DBT_PHASE_SPECS`의 snapshot/critical 속성을 exact inventory로 고정한다.
- Modify: `domains/traffic/traffic_ingest/transform_specs.py:79-86` — `dbt_test_gold`를 critical pin phase로 선언한다.
- Read only: `domains/traffic/traffic_incident_transform.py:247-264` — `pin_critical=True`를 `priority_weight=10`으로 변환하는 기존 operator factory다.
- Read only: `.airflowignore` — Weather/Traffic parser allowlist다.
- Runtime record: `C:/Users/Dell3571/Desktop/Projects/ask-seoul-sample-worktrees/deploy-root-dev-20260717/LessonRun.md` — merge 후 dev run 증거를 기록한다.

---

### Task 1: TDD로 Gold test priority 계약 고정

**Files:**
- Modify: `domains/traffic/tests/test_traffic_transform_dag.py:23-40`
- Modify: `domains/traffic/tests/test_traffic_transform_contract.py:511-527`
- Modify: `domains/traffic/traffic_ingest/transform_specs.py:79-86`

**Interfaces:**
- Consumes: `DbtPhaseSpec.pin_critical: bool`, `traffic_incident_transform.PIN_CRITICAL_PRIORITY == 10`, `dbt_task(spec)`의 `priority_weight=(10 if spec.pin_critical else 1)` 매핑
- Produces: `dbt_test_gold.snapshot_required is True`, `dbt_test_gold.pin_critical is True`, Airflow task `dbt_test_gold.priority_weight == 10`, `dbt_run_gold.priority_weight == 1`

- [ ] **Step 1: Airflow task priority 기대값을 먼저 변경한다**

`domains/traffic/tests/test_traffic_transform_dag.py`의 critical task set을 다음처럼 바꾼다.

```python
    critical_task_ids = {
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_test_gold",
    }

    for task_id in module.DBT_PHASE_TASK_IDS:
        task = module.dag.task_dict[task_id]
        assert task.kwargs["pool"] == module.TRINO_HEAVY_POOL
        assert task.kwargs["weight_rule"] == "absolute"
        expected_priority = (
            module.PIN_CRITICAL_PRIORITY
            if task_id in critical_task_ids
            else 1
        )
        assert task.kwargs["priority_weight"] == expected_priority
```

- [ ] **Step 2: phase inventory 기대값을 먼저 변경한다**

`domains/traffic/tests/test_traffic_transform_contract.py`의 exact inventory에서 Gold 항목을 다음처럼 둔다.

```python
        "dbt_run_gold": (True, False),
        "dbt_test_gold": (True, True),
```

- [ ] **Step 3: targeted test가 RED인지 확인한다**

Run:

```powershell
python -m pytest `
  domains/traffic/tests/test_traffic_transform_dag.py::test_traffic_transform_trino_tasks_do_not_inflate_pool_priority_from_chain `
  domains/traffic/tests/test_traffic_transform_contract.py::test_traffic_transform_bootstraps_asac_axes_before_silver `
  -q
```

Expected: 두 test가 현재 `dbt_test_gold`의 priority 1 및 `(True, False)` 때문에 실패한다. collection/import 오류는 허용하지 않는다.

- [ ] **Step 4: 최소 production 변경을 적용한다**

`domains/traffic/traffic_ingest/transform_specs.py`의 `dbt_test_gold` spec을 다음처럼 바꾼다.

```python
    DbtPhaseSpec(
        "dbt_test_gold",
        "test",
        "ask_seoul_traffic_transform_gold",
        silver_persisted=True,
        fresh_parse=True,
        snapshot_required=True,
        pin_critical=True,
    ),
```

`dbt_run_gold` spec과 `traffic_incident_transform.py` operator factory는 변경하지 않는다.

- [ ] **Step 5: targeted test가 GREEN인지 확인한다**

Run:

```powershell
python -m pytest `
  domains/traffic/tests/test_traffic_transform_dag.py::test_traffic_transform_trino_tasks_do_not_inflate_pool_priority_from_chain `
  domains/traffic/tests/test_traffic_transform_contract.py::test_traffic_transform_bootstraps_asac_axes_before_silver `
  -q
```

Expected: `2 passed`.

- [ ] **Step 6: scoped diff를 리뷰한다**

Run:

```powershell
git diff --check
git diff -- `
  domains/traffic/traffic_ingest/transform_specs.py `
  domains/traffic/tests/test_traffic_transform_dag.py `
  domains/traffic/tests/test_traffic_transform_contract.py
```

Expected: production diff는 `pin_critical=True` 한 줄이고, 두 test diff는 정확히 priority/inventory 기대값만 바꾼다.

- [ ] **Step 7: 구현을 커밋한다**

Run:

```powershell
git add -- `
  domains/traffic/traffic_ingest/transform_specs.py `
  domains/traffic/tests/test_traffic_transform_dag.py `
  domains/traffic/tests/test_traffic_transform_contract.py
git diff --cached --check
git commit -m "fix(traffic): fence exact Gold reconciliation test"
```

Expected: design/plan commit 다음에 code/test commit 하나가 생기며 금지 경로는 staged 목록에 없다.

---

### Task 2: Weather/Traffic 정적 회귀와 parser 경계 검증

**Files:**
- Verify: `domains/traffic/**`
- Verify: `domains/weather/**`
- Verify: `.airflowignore`

**Interfaces:**
- Consumes: Task 1의 `DBT_PHASE_SPECS` 및 DAG task graph
- Produces: PR 전에 사용할 Python·lint·compile·parser allowlist 증거

- [ ] **Step 1: Weather/Traffic 전체 Python suite를 실행한다**

Run:

```powershell
python -m pytest domains/traffic/tests domains/weather/tests -q
```

Expected: 기준선과 동일하게 `604 passed, 1 skipped`; 실패 0.

- [ ] **Step 2: Ruff와 Python compile을 실행한다**

Run:

```powershell
python -m ruff check domains/traffic domains/weather
python -m compileall -q domains/traffic domains/weather
```

Expected: 두 명령 모두 exit code 0.

- [ ] **Step 3: 변경 범위와 금지 산출물을 검사한다**

Run:

```powershell
$changed = @(git diff --name-only origin/dev...HEAD)
$changed
if ($changed | Where-Object { $_ -notlike 'domains/traffic/*' }) {
    throw 'Change escaped domains/traffic'
}
if ($changed | Where-Object {
    $_ -match '(^|/)(\.omc|\.omx|__pycache__|\.pytest_cache)(/|$)'
}) {
    throw 'Forbidden artifact is tracked'
}
git diff --check origin/dev...HEAD
```

Expected: 설계·계획·production·test 파일이 모두 `domains/traffic/**` 아래에 있고 금지 산출물은 0개다.

- [ ] **Step 4: `.airflowignore` allowlist를 기계적으로 검증한다**

Run:

```powershell
$guard = Get-Content -LiteralPath '.airflowignore' -Encoding utf8 -Raw
$required = @(
    '/*',
    '!/domains/',
    '!/domains/weather/',
    '!/domains/weather/**',
    '!/domains/traffic/',
    '!/domains/traffic/**'
)
foreach ($line in $required) {
    if (-not $guard.Contains($line)) { throw "Missing allowlist rule: $line" }
}
```

Expected: Weather/Traffic allowlist 여섯 규칙이 모두 존재한다.

- [ ] **Step 5: Airflow 3.2.2 one-off container에서 DagBag import를 확인한다**

Run:

```powershell
$wt = 'C:\Users\Dell3571\Desktop\Projects\ask-seoul-worktrees\dags-420-traffic-gold-reconciliation-fence'
$envFile = 'C:\Users\Dell3571\Desktop\Projects\ask-seoul-sample\.env'
docker run --rm `
  --env-file $envFile `
  -e AIRFLOW__CORE__LOAD_EXAMPLES=false `
  -e AIRFLOW__CORE__DAG_IGNORE_FILE_SYNTAX=glob `
  -e PYTHONPATH=/opt/airflow/dags:/opt/airflow/plugins `
  -v "${wt}:/opt/airflow/dags:ro" `
  -v "${wt}\plugins:/opt/airflow/plugins:ro" `
  --entrypoint python `
  elt-infra-airflow:local `
  -c "from pathlib import Path; from airflow.models import DagBag; b=DagBag('/opt/airflow/dags', include_examples=False, safe_mode=True); roots=(Path('/opt/airflow/dags/domains/weather'),Path('/opt/airflow/dags/domains/traffic')); assert not b.import_errors,b.import_errors; assert len(b.dag_ids)==17,b.dag_ids; assert all(any(Path(d.fileloc).is_relative_to(r) for r in roots) for d in b.dags.values()),[(i,d.fileloc) for i,d in b.dags.items()]; print('allowed_dags=17 import_errors=0')"
```

Expected: 실제 운영과 같은 Airflow 3.2.2/glob parser에서 17개 DAG, import error 0이며 모든 `fileloc`이 `domains/weather/**` 또는 `domains/traffic/**`다. host Airflow 2.11.2/regexp DagBag은 이 저장소의 검증기로 사용하지 않는다.

---

### Task 3: branch publish, PR merge, issue lifecycle 처리

**Files:**
- Read: `C:/Users/Dell3571/Desktop/Projects/asac-org-github/.github/PULL_REQUEST_TEMPLATE.md`
- GitHub target: `ASAC-DE-bigkk/ASAC-DAG`, base `dev`

**Interfaces:**
- Consumes: Task 1 code/test commit과 Task 2 검증 증거
- Produces: merged ASAC-DAG `dev` commit 및 closed ASAC-DAG #420

- [ ] **Step 1: 공용 PR template을 UTF-8로 읽고 필수 섹션을 확인한다**

Run:

```powershell
Get-Content -LiteralPath `
  'C:\Users\Dell3571\Desktop\Projects\asac-org-github\.github\PULL_REQUEST_TEMPLATE.md' `
  -Encoding utf8
```

Expected: 연결 이슈, 작업 범위, 변경 요약, dev 검증, 데이터 영향, contract, prod 영향, 보안, 리뷰 포인트 섹션을 확인한다.

- [ ] **Step 2: branch가 최신 `origin/dev` 기반이고 clean인지 확인한다**

Run:

```powershell
git fetch origin dev
git merge-base --is-ancestor origin/dev HEAD
git status --short
git log --oneline origin/dev..HEAD
```

Expected: ancestry check exit code 0, worktree clean, design/plan commit과 code/test commit만 표시된다. `origin/dev`가 새로 전진했다면 rebase하지 말고 변경 이력을 다시 검토한 뒤 안전한 반영 방법을 결정한다.

- [ ] **Step 3: branch를 push한다**

Run:

```powershell
git push -u origin feat/420-traffic-gold-reconciliation-fence
```

Expected: remote branch가 생성되고 local upstream이 연결된다.

- [ ] **Step 4: `dev` 대상 ready PR을 생성한다**

PR 제목:

```text
fix(traffic): fence exact Gold reconciliation test
```

PR 본문 필수 내용:

```markdown
## 연결 이슈

- Closes #420

## 변경 요약

- `dbt_run_gold`는 priority 1로 유지합니다.
- `dbt_test_gold`만 `trino_heavy` priority 10으로 올려 Bronze write interleave를 막습니다.
- ASAC-DBT exact full-history model/test/selector는 변경하지 않습니다.

## dev 검증 결과

- Weather/Traffic Python: 604 passed, 1 skipped
- Ruff / compileall / diff check: PASS
- Airflow DagBag: import error 0, Weather/Traffic allowlist만 발견
- runtime Gold 171/171은 merge 후 최신 dev 재배포에서 확인

## 데이터 영향

- table/schema/grain/event time/ingest time 변경 없음
- Traffic/Weather raw landing 및 Bronze 적재 로직 변경 없음
- Gold test 동안 `trino_heavy` priority 1 task가 약 2~3분 대기 가능

## 보안 및 범위

- dev only; prod/shared/full refresh/backfill 없음
- `.env`, secret, `.omc`, `.omx`, `__pycache__`, `.pytest_cache` 미포함
```

Expected: PR base=`dev`, head=`feat/420-traffic-gold-reconciliation-fence`, draft=false, 본문의 한글이 정상 표시된다.

- [ ] **Step 5: PR diff와 checks를 확인한 뒤 merge한다**

확인 항목:

```text
changed paths == domains/traffic/**
dbt_run_gold priority == 1
dbt_test_gold priority == 10
DBT SQL/model/test/selector diff == 0
required checks == success
mergeable == true
```

Expected: 위 조건이 모두 충족된 뒤 repository 기본 merge 방식으로 PR을 merge한다.

- [ ] **Step 6: #420 종료를 확인한다**

Expected: merge 직후 `ASAC-DE-bigkk/ASAC-DAG#420`이 `closed/completed`다. 자동 종료되지 않았으면 merge PR URL과 merge SHA를 comment로 남기고 `completed`로 직접 닫는다. post-merge smoke가 실패하면 같은 issue를 즉시 reopen한다.

---

### Task 4: clean dev runtime 재배포와 parser 검증

**Files:**
- Deploy root: `C:/Users/Dell3571/Desktop/Projects/ask-seoul-sample-worktrees/deploy-root-dev-20260717`
- DAGS submodule: deploy root의 `dags/`
- DBT submodule: deploy root의 `dbt/`
- Compose: `docker-compose.yml`, `docker-compose.dev-deploy.override.yml`

**Interfaces:**
- Consumes: Task 3의 merged ASAC-DAG `origin/dev`, 기존 ASAC-DBT `origin/dev`
- Produces: merged exact SHA가 read-only DAG mount에 반영된 healthy dev Airflow/Trino/Postgres runtime

- [ ] **Step 1: clean deploy root의 submodule ref를 최신 dev로 맞춘다**

Run:

```powershell
$deploy = 'C:\Users\Dell3571\Desktop\Projects\ask-seoul-sample-worktrees\deploy-root-dev-20260717'
git -C "$deploy\dags" fetch origin dev
git -C "$deploy\dags" checkout --detach origin/dev
git -C "$deploy\dbt" fetch origin dev
git -C "$deploy\dbt" checkout --detach origin/dev
git -C "$deploy\dags" rev-parse HEAD
git -C "$deploy\dbt" rev-parse HEAD
```

Expected: DAGS SHA는 Task 3 merge commit이고 DBT SHA는 최신 `origin/dev`다. dirty original root는 건드리지 않는다.

- [ ] **Step 2: dev compose stack을 재배포한다**

Run:

```powershell
$deploy = 'C:\Users\Dell3571\Desktop\Projects\ask-seoul-sample-worktrees\deploy-root-dev-20260717'
$envFile = 'C:\Users\Dell3571\Desktop\Projects\ask-seoul-sample\.env'
docker compose `
  --project-directory $deploy `
  --env-file $envFile `
  -f "$deploy\docker-compose.yml" `
  -f "$deploy\docker-compose.dev-deploy.override.yml" `
  up -d --build
```

Expected: secret 값은 출력하지 않고 Airflow, Trino, Postgres가 정상 기동한다.

- [ ] **Step 3: container와 mount를 확인한다**

Run:

```powershell
docker compose `
  --project-directory $deploy `
  --env-file $envFile `
  -f "$deploy\docker-compose.yml" `
  -f "$deploy\docker-compose.dev-deploy.override.yml" `
  ps
docker inspect elt-infra-airflow-scheduler-1 `
  --format '{{range .Mounts}}{{println .Source "->" .Destination .RW}}{{end}}'
```

Expected: required container는 healthy/running이고 scheduler의 `/opt/airflow/dags` source는 clean deploy root `dags`, `RW=false`다.

- [ ] **Step 4: container parser allowlist와 DagBag을 검증한다**

Run:

```powershell
docker compose `
  --project-directory $deploy `
  --env-file $envFile `
  -f "$deploy\docker-compose.yml" `
  -f "$deploy\docker-compose.dev-deploy.override.yml" `
  exec -T airflow-scheduler python -c `
  "from pathlib import Path; from airflow.models import DagBag; b=DagBag('/opt/airflow/dags', include_examples=False); roots=(Path('/opt/airflow/dags/domains/weather'),Path('/opt/airflow/dags/domains/traffic')); assert not b.import_errors, b.import_errors; assert all(any(Path(d.fileloc).is_relative_to(r) for r in roots) for d in b.dags.values()), [(i,d.fileloc) for i,d in b.dags.items()]; print(len(b.dag_ids))"
```

Expected: import error 0이며 Weather/Traffic 소유 DAG만 존재한다.

- [ ] **Step 5: DBT parse를 실행한다**

Run:

```powershell
docker compose `
  --project-directory $deploy `
  --env-file $envFile `
  -f "$deploy\docker-compose.yml" `
  -f "$deploy\docker-compose.dev-deploy.override.yml" `
  exec -T airflow-scheduler bash -lc `
  'cd /opt/airflow/dbt/elt_smoke && dbt parse --target dev --no-partial-parse'
```

Expected: dbt parse PASS. DBT relation을 실행하거나 다른 도메인 pipeline을 trigger하지 않는다.

---

### Task 5: 실제 Asset-triggered Traffic Gold fence 검증

**Files:**
- Runtime DAG: `traffic_incident_transform`
- Runtime source DAGs: Traffic Incident landing/Bronze, Traffic Flow Bronze
- Runtime record: clean deploy root `LessonRun.md`

**Interfaces:**
- Consumes: Task 4 runtime의 `traffic_incident_transform` 및 shared `trino_heavy` 1-slot pool
- Produces: Gold run/test 성공, exact test 171/171, Bronze interleave 0의 dev 증거

- [ ] **Step 1: 실행 전 source readiness와 pause 상태를 확인한다**

확인 항목:

```text
Traffic Incident landing/Bronze latest terminal run == success
Traffic Flow Bronze latest terminal run == success 또는 optional Flow 계약상 허용 상태
traffic_incident_transform active runs == 0
W2 recovery == paused
ask_seoul_iceberg_maintenance == paused
```

Expected: source가 unhealthy하면 transform을 unpause하지 않고 raw/Bronze 원인부터 해결한다.

- [ ] **Step 2: Traffic transform만 unpause한다**

Run:

```powershell
docker compose `
  --project-directory $deploy `
  --env-file $envFile `
  -f "$deploy\docker-compose.yml" `
  -f "$deploy\docker-compose.dev-deploy.override.yml" `
  exec -T airflow-scheduler airflow dags unpause traffic_incident_transform
```

Expected: 다음 정상 Incident/Flow Bronze Asset event가 transform run을 시작한다. 다른 paused transform/recovery/maintenance DAG는 활성화하지 않는다.

- [ ] **Step 3: 새 Asset-triggered run의 phase 순서를 관찰한다**

필수 성공 순서:

```text
preflight
resolve_traffic_snapshot_run priority 10
dbt_run_silver priority 10
dbt_test_silver priority 10
dbt_run_gold priority 1
dbt_test_gold priority 10
publish_dbt_run_metrics
```

Expected: resolver/Silver pin contract가 유지되고 Gold run/test가 각각 별도 task다.

- [ ] **Step 4: Gold fence의 시간 증거를 수집한다**

다음 timestamp를 UTC로 기록한다.

```text
dbt_run_gold end_date
dbt_test_gold start_date
dbt_test_gold end_date
같은 구간에 runnable이었던 Incident/Flow/Weather trino_heavy priority 1 task의 start_date
```

Acceptance:

```text
dbt_test_gold starts before any waiting priority 1 Bronze/heavy task
no priority 1 Bronze/heavy task starts after dbt_run_gold end and before dbt_test_gold end
```

Expected: 추가 대기는 실측 Gold test 시간인 약 2~3분 범위다. scheduler가 priority fence를 지키지 않으면 run을 성공으로 간주하지 않고 #420을 reopen한다.

- [ ] **Step 5: Gold exact validation과 DAG terminal 상태를 확인한다**

Acceptance:

```text
dbt_run_gold == success
dbt_test_gold == success
Gold tests == 171 passed / 171 total
assert_gold_traffic_incident_collection_coverage_5m_reconciles failures == 0
traffic_incident_transform DAG run == success
publish_dbt_run_metrics == success
```

Expected: full-history test를 skip, filter, tolerance 처리하지 않고 모두 통과한다.

- [ ] **Step 6: 실패 경로를 처리한다**

Gold run/test가 실패하면 다음을 수행한다.

```text
traffic_incident_transform를 다시 pause
raw landing과 Bronze는 계속 유지
run ID, failed task, Trino query/error, callback 결과 기록
#420 reopen
Gold test만 재시도하지 않고 dbt_run_gold부터 downstream을 재실행
```

성공하면 transform은 unpaused 상태로 유지한다.

- [ ] **Step 7: `LessonRun.md`에 dev 증거를 기록한다**

기록 필드:

```text
DAGS merge SHA / DBT dev SHA
Airflow DAG run ID
resolved Incident run ID / optional Flow run ID
각 dbt phase 상태와 start/end/duration
Gold tests 171/171
Bronze interleave count 0
영향 catalog/schema/table 및 row count
Discord failure notification 발생 여부
Weather/Traffic 이외 DAG 실행 0
```

Expected: secret 값과 webhook URL은 기록하지 않는다.

---

### Task 6: Traffic fence 완료 판정 후 전체 운영 목표로 복귀

**Files:**
- Read: merged PR, #420, Airflow run evidence, `LessonRun.md`

**Interfaces:**
- Consumes: Task 3 issue state와 Task 5 runtime evidence
- Produces: Traffic Gold race 하위 작업의 명시적 완료/미완료 판정

- [ ] **Step 1: requirement-by-requirement 완료 감사를 수행한다**

검증표:

```text
dbt_run_gold priority 1: code + DagBag task kwargs
dbt_test_gold priority 10: code + DagBag task kwargs
full-history SQL unchanged: ASAC-DBT diff 0
Bronze interleave 0: task timestamps
Gold 171/171: dbt run_results/log
DAG success: Airflow terminal state
forbidden domain run 0: parsed DAG inventory + run inventory
#420 closed: GitHub issue state
```

Expected: 간접 증거나 단순 test green만으로 runtime 요구를 대신하지 않는다.

- [ ] **Step 2: 다음 운영 단계로 이동한다**

Traffic fence가 모두 증명되면 기존 전체 목표 plan에서 다음 순서로 진행한다.

```text
Weather transform small Asset smoke
Weather/Traffic source freshness
09:00 KST 24-hour reliability report
failure-only immediate Discord notification delivery
24-hour continuous run observation
```

W2 recovery와 maintenance는 각각 별도 안전 설계와 acceptance를 만족하기 전까지 paused 상태를 유지한다.
