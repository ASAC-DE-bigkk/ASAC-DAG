<!-- shared-domains: all -->
# 도메인 산출물 경계 하네스 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 도메인별 산출물은 `domains/<domain>/`에, 둘 이상 도메인이 함께 소유한 산출물만 `common/` 또는 `docs/cross-domain/`에 두도록 자동 검증한다.

**Architecture:** 순수 Python 정책 모듈이 경로와 `shared-domains` 소유자 표기를 검증한다. 같은 모듈의 CLI와 pytest가 현재 브랜치의 변경 파일을 검사하므로, 루트 제품 스크립트·일반 `docs/` 파일의 재도입은 테스트 단계에서 실패한다.

**Tech Stack:** Python 3.14, pytest, Git changed-path inspection.

## Global Constraints

- Weather·Traffic 비용 측정의 읽기 전용 계약, dbt `compile` 전용 동작, Airflow/DML/DDL 비실행 조건을 바꾸지 않는다.
- `common/`과 `docs/cross-domain/` 파일은 `shared-domains: weather,traffic` 또는 `shared-domains: all`을 파일 첫 부분에 명시한다.
- 도메인 전용 코드·테스트·문서는 반드시 `domains/<domain>/` 아래에 둔다.
- 루트 `scripts/`에 제품 산출물을 새로 만들지 않는다.
- 커밋·push·PR은 별도 Gate B 승인 전에는 수행하지 않는다.

---

### Task 1: 경로·공동 소유권 정책을 테스트로 고정

**Files:**
- Create: `common/tests/test_domain_boundary.py`
- Create: `common/domain_boundary.py`

**Interfaces:**
- Produces: `validate_paths(root: Path, paths: Iterable[str]) -> list[str]`
- Produces: `changed_paths(root: Path, base_ref: str) -> list[str]`
- Produces: `python3 common/domain_boundary.py [--base-ref origin/dev] [--path <path>]`

- [x] **Step 1: Write the failing test**

```python
def test_validate_paths_rejects_root_scripts_and_unowned_common_files(tmp_path):
    assert validate_paths(tmp_path, ["scripts/new_pipeline.py"])
    assert validate_paths(tmp_path, ["common/new_pipeline.py"])
```

- [x] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest common/tests/test_domain_boundary.py -q`

Expected: FAIL because `common.domain_boundary` does not exist.

- [x] **Step 3: Write minimal implementation**

```python
def validate_paths(root: Path, paths: Iterable[str]) -> list[str]:
    violations = []
    for raw_path in paths:
        relative_path = _relative_path(raw_path)
        parts = relative_path.parts
        if len(parts) >= 3 and parts[0] == "domains" and parts[1]:
            continue
        if parts and parts[0] == "common":
            violation = _shared_owner_violation(root, relative_path)
        elif len(parts) >= 4 and parts[:2] == ("docs", "cross-domain") and parts[2]:
            violation = _shared_owner_violation(root, relative_path)
        else:
            violation = f"{relative_path}: product artifacts must live under domains/<domain>/, common/, or docs/cross-domain/<scope>/"
        if violation is not None:
            violations.append(violation)
    return violations
```

- [x] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest common/tests/test_domain_boundary.py -q`

Expected: PASS for allowed weather/traffic common files and FAIL diagnostics for forbidden paths.

### Task 2: Weather·Traffic 공동 벤치마크와 기록을 공동 경로로 이동

**Files:**
- Delete: `scripts/benchmark_weather_traffic_cost_proxy.py`
- Delete: `scripts/tests/test_benchmark_weather_traffic_cost_proxy.py`
- Create: `common/benchmarks/weather_traffic_cost_proxy.py`
- Create: `common/tests/test_weather_traffic_cost_proxy.py`
- Move: `docs/2026-07-13-weather-traffic-cost-proxy-comparison.md` to `docs/cross-domain/weather-traffic/2026-07-13-weather-traffic-cost-proxy-comparison.md`
- Move: `docs/2026-07-13-weather-traffic-cost-proxy-retrospective.md` to `docs/cross-domain/weather-traffic/2026-07-13-weather-traffic-cost-proxy-retrospective.md`
- Modify: `common/trino_query_metrics.py`
- Modify: `common/tests/test_trino_query_metrics.py`

**Interfaces:**
- Consumes: `common.trino_query_metrics.TelemetryCursor`, `collect_iceberg_fingerprint`, `sql_string`
- Produces: `common/benchmarks/weather_traffic_cost_proxy.py` with unchanged `collect_bundle`, `compare_bundles`, and CLI behavior.

- [x] **Step 1: Write the failing relocation test**

```python
def test_weather_traffic_artifacts_are_marked_shared_and_leave_no_root_product_files():
    assert (ROOT / "common/benchmarks/weather_traffic_cost_proxy.py").is_file()
    assert not (ROOT / "scripts/benchmark_weather_traffic_cost_proxy.py").exists()
```

- [x] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest common/tests/test_domain_boundary.py::test_weather_traffic_artifacts_are_marked_shared_and_leave_no_root_product_files -q`

Expected: FAIL because the benchmark remains under root `scripts/`.

- [x] **Step 3: Move and minimally adapt the code**

```python
# common/benchmarks/weather_traffic_cost_proxy.py
# shared-domains: weather,traffic
DAGS_ROOT = Path(__file__).resolve().parents[2]
```

Move its pure-function test into `common/tests/`, update its dynamic import path, and add matching ownership markers to the benchmark, telemetry module, tests, and both cross-domain Markdown records.

- [x] **Step 4: Run targeted tests and the CLI guard**

Run: `python3 -m pytest common/tests/test_trino_query_metrics.py common/tests/test_weather_traffic_cost_proxy.py common/tests/test_domain_boundary.py -q`

Run: `python3 common/domain_boundary.py --base-ref origin/dev`

Expected: all targeted tests PASS and CLI prints `PASS` without a root `scripts/` or unowned shared-artifact violation.

### Task 3: Verify the boundary without changing pipeline execution

**Files:**
- Verify: all Task 1–2 files only

**Interfaces:**
- Consumes: changed paths relative to `origin/dev`
- Produces: a deterministic PASS/FAIL boundary result suitable for preflight or CI.

- [x] **Step 1: Check file locations and ownership headers**

Run: `git diff --check`

Run: `git diff --name-status origin/dev`

Expected: no root product `scripts/` additions; every new `common/` and `docs/cross-domain/` artifact has a valid ownership marker.

- [x] **Step 2: Run focused regression suite**

Run: `python3 -m pytest common/tests/test_trino_query_metrics.py common/tests/test_weather_traffic_cost_proxy.py common/tests/test_domain_boundary.py -q`

Expected: PASS.

- [x] **Step 3: Run safe static checks**

Run: `python3 -m compileall -q common`

Run: `git diff --check`

Expected: PASS; no Docker, Airflow, dbt runtime, DML, DDL, or production action is invoked.

- [x] **Step 4: Stop before integration mutation**

Do not commit, push, open a PR, or change a submodule pointer until the user reviews Gate B evidence.
