# Traffic prod materializer schedule Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** prod에서 명시한 Traffic materializer cron만 허용하고 맥미니 Weather/Traffic 정규 DAG schedule 설정을 운영자가 그대로 적용할 수 있게 한다.

**Architecture:** `materializer_schedule()`에 새 canonical env key를 최우선 opt-in으로 추가한다. 기존 dev 기본값과 legacy key는 보존하되 prod에서는 canonical key가 없으면 계속 `None`을 반환한다. downstream Asset 연결은 코드 변경 없이 운영 문서에서 cron 비설정 계약으로 고정한다.

**Tech Stack:** Python 3.11, Apache Airflow schedule/Asset, pytest, Markdown

## Global Constraints

- Issue는 ASAC-DAG [#606](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/606)이다.
- base는 `origin/dev`이며 branch는 `feat/606-traffic-prod-materializer-schedule`이다.
- 수정 범위는 `domains/traffic/**`뿐이다.
- materializer는 cron-only bounded drain을 유지하고 Raw Asset trigger를 복원하지 않는다.
- prod 기본 schedule은 `None`이며 canonical key를 명시한 경우에만 활성화한다.
- `.env*`, credential, 실제 prod DAG 상태 및 prod data는 변경하지 않는다.
- commit, push, PR은 사용자 명시 승인 전에는 실행하지 않는다.

---

### Task 1: prod canonical materializer schedule 계약

**Files:**
- Modify: `domains/traffic/tests/test_traffic_assets.py`
- Modify: `domains/traffic/traffic_ingest/assets.py`

**Interfaces:**
- Consumes: `materializer_schedule(*, env: Mapping[str, str], airflow_version: str | None = None, schedule_factory: Callable[..., object] | None = None)`
- Produces: `ASK_SEOUL_TRAFFIC_MATERIALIZER_DAG_SCHEDULE` canonical env 계약과 `str | None` schedule

- [ ] **Step 1: prod 실패 테스트 작성**

```python
def test_materializer_schedule_allows_explicit_canonical_prod_cron():
    from traffic_ingest import assets

    schedule = assets.materializer_schedule(
        env={
            "ASK_SEOUL_TARGET": "prod",
            "ASK_SEOUL_TRAFFIC_MATERIALIZER_DAG_SCHEDULE": "*/15 * * * *",
        }
    )

    assert schedule == "*/15 * * * *"
```

- [ ] **Step 2: RED 확인**

Run:

```powershell
python -m pytest domains/traffic/tests/test_traffic_assets.py::test_materializer_schedule_allows_explicit_canonical_prod_cron -q
```

Expected: 현재 구현이 prod에서 무조건 `None`을 반환하므로 assertion failure.

- [ ] **Step 3: prod opt-in과 회귀 경계 테스트 작성**

```python
def test_materializer_schedule_keeps_prod_dormant_without_canonical_cron():
    from traffic_ingest import assets

    assert assets.materializer_schedule(env={"ASK_SEOUL_TARGET": "prod"}) is None
    assert (
        assets.materializer_schedule(
            env={
                "ASK_SEOUL_TARGET": "prod",
                "ASK_SEOUL_TRAFFIC_MATERIALIZER_FALLBACK_SCHEDULE": "*/10 * * * *",
            }
        )
        is None
    )


def test_materializer_schedule_canonical_key_precedes_dev_legacy_key():
    from traffic_ingest import assets

    schedule = assets.materializer_schedule(
        env={
            "ASK_SEOUL_TARGET": "dev",
            "ASK_SEOUL_TRAFFIC_MATERIALIZER_DAG_SCHEDULE": "7,22,37,52 * * * *",
            "ASK_SEOUL_TRAFFIC_MATERIALIZER_FALLBACK_SCHEDULE": "*/10 * * * *",
        }
    )

    assert schedule == "7,22,37,52 * * * *"
```

- [ ] **Step 4: 최소 구현**

```python
canonical_schedule_env = "ASK_SEOUL_TRAFFIC_MATERIALIZER_DAG_SCHEDULE"
if canonical_schedule_env in env:
    return env[canonical_schedule_env] or None
if env.get("ASK_SEOUL_TARGET", env.get("DBT_TARGET", "prod")) != "dev":
    return None
return env.get(
    "ASK_SEOUL_TRAFFIC_MATERIALIZER_FALLBACK_SCHEDULE",
    "*/15 * * * *",
) or None
```

- [ ] **Step 5: 관련 단위 테스트 통과 확인**

Run:

```powershell
python -m pytest domains/traffic/tests/test_traffic_assets.py -q
```

Expected: 모든 테스트 PASS.

### Task 2: 맥미니 prod schedule handoff

**Files:**
- Create: `domains/traffic/docs/prod-weather-traffic-schedule-handoff.md`
- Verify: `domains/weather/weather_ingest/bronze_dag_support.py`
- Verify: `domains/weather/weather_vilage_fcst_transform.py`
- Verify: `domains/weather/weather_w2_canonical_transform.py`
- Verify: `domains/traffic/traffic_incident_landing.py`
- Verify: `domains/traffic/traffic_incident_bronze.py`
- Verify: `domains/traffic/traffic_flow_bronze.py`
- Verify: `domains/traffic/traffic_incident_transform.py`
- Verify: `domains/traffic/traffic_flow_transform.py`
- Verify: `domains/traffic/traffic_gold_transform.py`
- Verify: `domains/traffic/traffic_serving_export.py`

**Interfaces:**
- Consumes: 코드에 선언된 cron/Asset schedule과 canonical materializer key
- Produces: 비밀값을 제외한 맥미니 복사 가능 설정 블록, DAG별 cron/Asset/manual matrix, unpause 순서

- [ ] **Step 1: 운영 설정 문서 작성**

문서는 정확히 다음 비밀값 없는 설정을 제공한다.

```dotenv
ASK_SEOUL_TARGET=prod
DBT_TARGET=prod
ASK_SEOUL_KMA_DAG_SCHEDULE="20 2,5,8,11,14,17,20,23 * * *"
ASK_SEOUL_TRAFFIC_DAG_SCHEDULE="*/5 * * * *"
ASK_SEOUL_TRAFFIC_MATERIALIZER_DAG_SCHEDULE="*/15 * * * *"
ASK_SEOUL_TRAFFIC_MATERIALIZER_BATCH_SIZE=24
```

Weather transform override 2개와 Traffic transform override는 설정하지 않는다고 명시한다.
정규 체인의 루트 3개만 cron이고 downstream은 Asset, Weather serving은 manual이라고 표로
구분한다.

- [ ] **Step 2: downstream-first unpause 순서 문서화**

Traffic은 serving → Gold → Flow transform → Incident transform → Flow Bronze →
Incident Bronze → Incident landing 순서로 unpause한다. Weather는 두 transform을 먼저
unpause하고 Weather Bronze를 마지막에 unpause한다. Weather serving은 별도 게시 승인
전까지 manual/paused로 둔다.

- [ ] **Step 3: 문서와 코드 key 일치 확인**

Run:

```powershell
rg -n "ASK_SEOUL_(KMA_DAG_SCHEDULE|TRAFFIC_DAG_SCHEDULE|TRAFFIC_MATERIALIZER_DAG_SCHEDULE|TRAFFIC_MATERIALIZER_BATCH_SIZE|WEATHER_TRANSFORM_DAG_SCHEDULE|WEATHER_W2_CANONICAL_TRANSFORM_DAG_SCHEDULE)" domains/traffic domains/weather
```

Expected: 문서 key와 코드/test key가 동일하고 canonical materializer key가 구현·테스트에 존재한다.

### Task 3: 범위·회귀 검증

**Files:**
- Verify: `domains/traffic/**`
- Verify read-only: `domains/weather/**`

**Interfaces:**
- Consumes: Task 1과 Task 2의 코드·문서
- Produces: 다른 도메인 비영향 및 cron/Asset 계약 검증 결과

- [ ] **Step 1: Traffic 전체 테스트**

Run:

```powershell
python -m pytest domains/traffic/tests -q
```

Expected: 모든 Traffic 테스트 PASS.

- [ ] **Step 2: Weather schedule 관련 회귀 테스트**

Run:

```powershell
python -m pytest domains/weather/tests/test_weather_transform_dag.py domains/weather/tests/test_weather_w2_canonical_transform_dag.py -q
```

Expected: 지정한 Weather Asset transform 테스트가 모두 PASS.

- [ ] **Step 3: compile과 diff 품질 확인**

Run:

```powershell
python -m compileall -q domains/traffic
git diff --check
git status --short
```

Expected: compile error 0, whitespace error 0, 변경 파일은 `domains/traffic/**`뿐이다.

- [ ] **Step 4: 사용자 승인 후에만 commit**

승인을 받은 경우에만 다음 파일을 명시 stage한다.

```powershell
git add domains/traffic/traffic_ingest/assets.py domains/traffic/tests/test_traffic_assets.py domains/traffic/docs/prod-weather-traffic-schedule-handoff.md domains/traffic/docs/superpowers/specs/2026-07-30-traffic-prod-materializer-schedule-design.md domains/traffic/docs/superpowers/plans/2026-07-30-traffic-prod-materializer-schedule.md
git commit -m "fix(traffic): allow explicit prod materializer schedule"
```
