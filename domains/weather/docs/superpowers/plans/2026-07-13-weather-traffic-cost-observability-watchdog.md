# Weather·Traffic Cost-Proxy Measurement and Watchdog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a repeatable, dev-only cost-proxy benchmark and low-cost Weather/Traffic freshness watchdogs that prove before/after impact without relying on Cloudflare billing data.

**Architecture:** A small ASAC-DAG query-telemetry module captures completed Trino query statistics immediately from `system.runtime.queries`, with cursor statistics as a clearly marked fallback. A manual, read-only benchmark script compiles the two selected dbt models and executes `EXPLAIN ANALYZE` against a fixed Iceberg fingerprint; it also runs the existing reliability report queries through the same telemetry wrapper. Existing Weather/Traffic report DAGs become cadence-aware, two-threshold watchdogs with persisted Discord status-transition suppression, while dbt source-freshness thresholds align with those SLOs.

**Tech Stack:** Python 3, Airflow 3 PythonOperator/BashOperator, `trino.dbapi`, Trino System connector, Iceberg metadata tables, dbt Core + dbt-trino, Docker Compose, pytest, YAML.

## Global Constraints

- Work in three independent repositories: root coordination, `dags` (ASAC-DAG), and `dbt` (ASAC-DBT). Do not create a cross-repository commit and do not update a root submodule pointer automatically.
- Preserve every current local WIP change. Before editing a tracked file, compare the worktree diff and use an isolated worktree at execution time; do not reset, clean, stash-drop, full-refresh, or rewrite history.
- Dev only. Do not run production writes, raw collection, backfill, recollect, destructive full-refresh, or automatic repair. Docker is currently unavailable locally, so all runtime execution remains `NOT RUN` until a user-approved dev runtime is healthy.
- Do not change Weather/Traffic model grain, data semantics, incremental lookback, canonical-grid rules, or repair behavior. ASAC-DBT #117 (Traffic repair) and #165 (Weather late repair) remain independent owners.
- The benchmark executes compiled model **SELECT** SQL through `EXPLAIN ANALYZE`; it never materializes a model. A write benchmark requires a separately approved isolated source/target schema design.
- Treat unavailable query metrics as `null` plus an explicit reason, never as zero. A before/after comparison is invalid if source-table fingerprints differ.
- The primary cost proxies are `physical_input_bytes` and `cpu_time_ms`; wall-clock time alone never proves cost reduction. Trino keeps only recent query history, so telemetry must be collected immediately after a query finishes.
- Watchdog query cost is measured. Its expected daily `physical_input_bytes` and `cpu_time_ms` must each remain at or below 5% of the same metric’s expected daily transform workload (median transform run × scheduled runs/day); never add bytes and milliseconds together. Otherwise tighten its scan or cadence.
- A retained optimization must preserve the existing data-contract tests and reduce `physical_input_bytes` or `cpu_time_ms` by at least 15% at the suite median, unless a documented spill-elimination exception is approved.
- Gate A is required before code or GitHub state changes in `dags` or `dbt`; Gate B is required before any commit, push, or PR. This plan supplies the exact scope and validation evidence needed for those gates.

---

## Repository Split and File Structure

| Repository | File | Responsibility |
| --- | --- | --- |
| root | `.gitignore` | Ignore generated benchmark JSON/Markdown while retaining the artifact directory marker. |
| root | `docker-compose.yml` | Mount host `artifacts/` into Airflow containers for visible, local benchmark output. |
| root | `artifacts/benchmarks/.gitkeep` | Preserve the empty artifact directory without committing generated measurements. |
| root | `docs/benchmarks/README.md` | Give the exact dev benchmark/preflight/compare commands and interpretation rules. |
| ASAC-DAG | `domains/weather/weather_ingest/trino_query_metrics.py` | Version-tolerant Trino telemetry, Iceberg snapshot/file fingerprints, and a cursor wrapper. |
| ASAC-DAG | `domains/weather/tests/test_trino_query_metrics.py` | Unit tests for query-ID capture, dynamic columns, unavailable fallback, and fingerprints. |
| ASAC-DAG | `domains/weather/weather_ingest/weather_traffic_cost_proxy.py` | Read-only `collect` and `compare` benchmark CLI. |
| ASAC-DAG | `domains/weather/tests/test_weather_traffic_cost_proxy.py` | Tests for model compilation selection, repeat aggregation, comparable-run rejection, and Markdown rendering. |
| ASAC-DAG | `domains/weather/weather_vilage_fcst_transform.py` | Publish existing dbt `run_results.json` metrics after the transform graph finishes. |
| ASAC-DAG | `domains/traffic/traffic_incident_transform.py` | Publish existing dbt `run_results.json` metrics after the transform graph finishes. |
| ASAC-DAG | corresponding transform tests | Prove the metrics task uses `ALL_DONE` and is downstream of the terminal test. |
| ASAC-DAG | `domains/weather/weather_ingest/reliability_report.py` | Weather two-tier freshness, publishability, late-repair-pending marker, partition-bounded report SQL. |
| ASAC-DAG | `domains/traffic/traffic_ingest/reliability_report.py` | Traffic two-tier freshness, publishability, explicit normal-zero handling, and logical date bounds. |
| ASAC-DAG | `domains/weather/weather_reliability_report.py` | Weather schedule, status-transition notification, task-level metrics. |
| ASAC-DAG | `domains/traffic/traffic_reliability_report.py` | Traffic schedule, status-transition notification, task-level metrics. |
| ASAC-DAG | corresponding reliability tests | Prove thresholds, status precedence, safe notification behavior, schedules, and SQL bounds. |
| ASAC-DAG | `domains/weather/tests/test_weather_reliability_dag.py` | Isolated Airflow/Variable tests for Weather notification transition behavior. |
| ASAC-DAG | `domains/traffic/tests/test_traffic_reliability_dag.py` | Isolated Airflow/Variable tests for Traffic notification transition behavior. |
| ASAC-DBT | `domains/weather/models/sources.yml` | Align Weather source freshness to 4h warning / 6h error. |
| ASAC-DBT | `domains/traffic/models/sources.yml` | Align Traffic manifest freshness to 15m warning / 30m error. |
| root | `docs/benchmarks/YYYY-MM-DD-weather-traffic-cost-proxy.md` | Commit only the human-readable comparison after an approved dev run. |
| root | `retrospective-2026-07-09.md` | Append the approved baseline/after table without overwriting existing WIP. |

## Explicit Non-Overlaps

- Do not touch `dbt/domains/traffic/models/silver/silver_seoul_traffic_incident.sql` or the late-data repair contract owned by ASAC-DBT #117.
- Do not touch Weather canonical Gold/repair files owned by ASAC-DBT #165.
- Do not add a shared all-domain SLO framework; the only shared code is the minimal read-only Trino telemetry client used solely by the two in-scope domains.
- Do not create the Traffic Bronze partition migration in this plan. Current Traffic Bronze/audit tables are not physically partitioned, so a date predicate may reduce logical work but must not be presented as physical-scan savings. A partition migration becomes a separate issue only if the baseline proves it is the dominant cost.

## Task 1: Prepare a Safe Benchmark Artifact Surface (root repository)

**Files:**

- Modify: `.gitignore`
- Modify: `docker-compose.yml:28-31`
- Create: `artifacts/benchmarks/.gitkeep`
- Create: `docs/benchmarks/README.md`

**Interfaces:**

- Consumes: Airflow’s existing `/opt/airflow/dags:ro` and `/opt/airflow/dbt` mounts.
- Produces: writable `/opt/airflow/artifacts/benchmarks` inside Airflow containers, backed by the local root `artifacts/benchmarks` directory.

- [ ] **Step 1: Add the artifact ignore rules before creating any benchmark output.**

  Add these exact lines after the existing dbt target ignore rules:

  ```gitignore
  artifacts/benchmarks/*.json
  artifacts/benchmarks/*.md
  !artifacts/benchmarks/.gitkeep
  ```

- [ ] **Step 2: Add a placeholder marker and mount only the artifact directory.**

  Create `artifacts/benchmarks/.gitkeep` as an empty file. In the shared Airflow volume list, keep the existing mounts and add exactly one line:

  ```yaml
      - ./artifacts:/opt/airflow/artifacts
  ```

  The resulting volume block must remain:

  ```yaml
    volumes:
      - ./dags:/opt/airflow/dags:ro
      - ./dbt:/opt/airflow/dbt
      - ./artifacts:/opt/airflow/artifacts
      - airflow_logs:/opt/airflow/logs
  ```

- [ ] **Step 3: Write the operational README with these exact commands and safety notes.**

  `docs/benchmarks/README.md` must include:

  ```markdown
  # Weather·Traffic cost-proxy benchmarks

  Run only against an approved dev runtime. The benchmark compiles dbt SQL and
  executes `EXPLAIN ANALYZE`; it does not collect APIs or materialize dbt models.

  ```bash
  docker compose config --quiet
  docker compose exec airflow-scheduler python \
    /opt/airflow/dags/domains/weather/weather_ingest/weather_traffic_cost_proxy.py collect \
    --label before --repeat 3 \
    --output /opt/airflow/artifacts/benchmarks/before.json
  ```

  ```bash
  docker compose exec airflow-scheduler python \
    /opt/airflow/dags/domains/weather/weather_ingest/weather_traffic_cost_proxy.py compare \
    --before /opt/airflow/artifacts/benchmarks/before.json \
    --after /opt/airflow/artifacts/benchmarks/after.json \
    --output /opt/airflow/artifacts/benchmarks/before-after.md
  ```

  A comparison whose `comparable` field is `false` is evidence only; do not use
  its percentages to claim a cost reduction. `unavailable` means the runtime did
  not expose that metric and is never equivalent to zero.
  ```

- [ ] **Step 4: Verify only configuration and file hygiene; do not start Docker.**

  Run:

  ```bash
  docker compose config --quiet
  git diff --check -- .gitignore docker-compose.yml artifacts/benchmarks/.gitkeep docs/benchmarks/README.md
  ```

  Expected: both commands exit `0`. This does not require a running Docker daemon.

- [ ] **Step 5: Hold the root change for Gate B rather than committing it early.**

  At Gate B, stage only these files:

  ```bash
  git add .gitignore docker-compose.yml artifacts/benchmarks/.gitkeep docs/benchmarks/README.md
  git commit -m "chore: add weather traffic benchmark artifact surface"
  ```

  Do not execute this step before explicit Gate B approval.

### Task 2: Add Version-Tolerant Trino Query Telemetry (ASAC-DAG)

**Files:**

- Create: `dags/domains/weather/weather_ingest/trino_query_metrics.py`
- Create: `dags/domains/weather/tests/test_trino_query_metrics.py`

**Interfaces:**

- Consumes: a completed `trino.dbapi.Cursor`, a separate metadata cursor, and a qualified Iceberg table name.
- Produces: JSON-serializable dictionaries with `metric_source`, `query_id`, all requested metric keys, and `unavailable_metrics`.
- Does not write to Trino, R2, Airflow metadata, or Iceberg.

- [ ] **Step 1: Write failing tests for immediate query-ID capture and dynamic System connector columns.**

  In `dags/domains/weather/tests/test_trino_query_metrics.py`, use fake cursors with deterministic `stats`, `execute`, `fetchone`, and `fetchall` methods. Add these tests:

  ```python
  def test_query_id_from_cursor_reads_trino_camel_case_stats():
      assert query_id_from_cursor(FakeWorkCursor(stats={"queryId": "q_123"})) == "q_123"


  def test_collect_query_metrics_keeps_unavailable_metrics_null():
      cursor = FakeMetadataCursor(
          describe_rows=[("query_id", "varchar"), ("cpu_time_ms", "bigint")],
          result_row=("q_123", 17),
      )
      result = collect_query_metrics(cursor, "q_123", fallback_stats={})

      assert result["metric_source"] == "system.runtime.queries"
      assert result["query_id"] == "q_123"
      assert result["cpu_time_ms"] == 17
      assert result["physical_input_bytes"] is None
      assert "physical_input_bytes" in result["unavailable_metrics"]


  def test_collect_query_metrics_marks_missing_history_as_unavailable_not_zero():
      cursor = FakeMetadataCursor(describe_rows=[("query_id", "varchar")], result_row=None)
      result = collect_query_metrics(cursor, "q_missing", fallback_stats={"processedBytes": 22})

      assert result["metric_source"] == "cursor.stats"
      assert result["input_bytes"] == 22
      assert result["physical_input_bytes"] is None
      assert "query_not_found_in_system_runtime" in result["unavailable_reasons"]
  ```

  Add a fingerprint test that proves Iceberg metadata, not a full table `count(*)`, is used:

  ```python
  def test_collect_iceberg_fingerprint_uses_snapshot_and_files_metadata():
      cursor = FakeMetadataCursor(
          rows=[(123, "2026-07-13T00:00:00Z"), (4, 8192, 400)],
      )
      fingerprint = collect_iceberg_fingerprint(cursor, "iceberg_dev.demo.bronze_table")

      assert fingerprint == {
          "table": "iceberg_dev.demo.bronze_table",
          "snapshot_id": 123,
          "snapshot_committed_at": "2026-07-13T00:00:00Z",
          "file_count": 4,
          "file_bytes": 8192,
          "record_count": 400,
      }
      assert '"bronze_table$snapshots"' in cursor.statements[0]
      assert '"bronze_table$files"' in cursor.statements[1]
      assert "count(*) FROM iceberg_dev.demo.bronze_table" not in " ".join(cursor.statements)
  ```

- [ ] **Step 2: Run the focused tests and confirm they fail because the module does not exist.**

  Run from the `dags` repository:

  ```bash
  python3 -m pytest domains/weather/tests/test_trino_query_metrics.py -q
  ```

  Expected: collection failure or `ModuleNotFoundError` for `weather_ingest.trino_query_metrics`.

- [ ] **Step 3: Implement the narrow telemetry module.**

  Create `domains/weather/weather_ingest/trino_query_metrics.py` with the following stable contract. Keep values JSON-safe and retain absent fields as `None`.

  ```python
  from __future__ import annotations

  from collections.abc import Mapping
  from typing import Any


  METRIC_COLUMNS = (
      "query_id", "state", "queued_time_ms", "analysis_time_ms",
      "distributed_planning_time_ms", "cpu_time_ms", "wall_time_ms",
      "peak_user_memory_bytes", "input_rows", "input_bytes", "output_rows",
      "output_bytes", "physical_input_bytes", "physical_written_bytes",
      "spilled_bytes",
  )

  CURSOR_STAT_MAP = {
      "queryId": "query_id",
      "state": "state",
      "queuedTimeMillis": "queued_time_ms",
      "cpuTimeMillis": "cpu_time_ms",
      "wallTimeMillis": "wall_time_ms",
      "peakMemoryBytes": "peak_user_memory_bytes",
      "processedRows": "input_rows",
      "processedBytes": "input_bytes",
  }


  def sql_string(value: str) -> str:
      return "'" + value.replace("'", "''") + "'"


  def query_id_from_cursor(cursor: Any) -> str | None:
      stats = getattr(cursor, "stats", None) or {}
      query_id = stats.get("queryId") or stats.get("query_id")
      return str(query_id) if query_id else None


  def _blank(query_id: str) -> dict[str, Any]:
      result = {name: None for name in METRIC_COLUMNS}
      result.update(
          query_id=query_id,
          metric_source="unavailable",
          unavailable_metrics=list(METRIC_COLUMNS[1:]),
          unavailable_reasons=[],
      )
      return result


  def _runtime_columns(cursor: Any) -> set[str]:
      cursor.execute("DESCRIBE system.runtime.queries")
      return {str(row[0]) for row in cursor.fetchall()}


  def collect_query_metrics(
      cursor: Any,
      query_id: str,
      *,
      fallback_stats: Mapping[str, Any] | None = None,
  ) -> dict[str, Any]:
      result = _blank(query_id)
      available = _runtime_columns(cursor)
      selected = [name for name in METRIC_COLUMNS if name in available]
      if "query_id" in selected:
          cursor.execute(
              "SELECT " + ", ".join(selected)
              + " FROM system.runtime.queries WHERE query_id = "
              + sql_string(query_id)
          )
          row = cursor.fetchone()
          if row is not None:
              result.update(dict(zip(selected, row, strict=True)))
              result["metric_source"] = "system.runtime.queries"
              result["unavailable_metrics"] = [
                  name for name in METRIC_COLUMNS[1:] if result[name] is None
              ]
              return result
          result["unavailable_reasons"].append("query_not_found_in_system_runtime")

      for source_name, target_name in CURSOR_STAT_MAP.items():
          value = (fallback_stats or {}).get(source_name)
          if value is not None:
              result[target_name] = value
      if any(result[name] is not None for name in METRIC_COLUMNS[1:]):
          result["metric_source"] = "cursor.stats"
      else:
          result["unavailable_reasons"].append("no_cursor_stats_available")
      result["unavailable_metrics"] = [
          name for name in METRIC_COLUMNS[1:] if result[name] is None
      ]
      return result


  def collect_iceberg_fingerprint(cursor: Any, qualified_table: str) -> dict[str, Any]:
      catalog, schema, table = qualified_table.split(".", 2)
      metadata_table = f'{catalog}.{schema}."{table}$snapshots"'
      cursor.execute(
          "SELECT snapshot_id, committed_at FROM " + metadata_table
          + " ORDER BY committed_at DESC LIMIT 1"
      )
      snapshot = cursor.fetchone() or (None, None)
      files_table = f'{catalog}.{schema}."{table}$files"'
      cursor.execute(
          "SELECT count(*), coalesce(sum(file_size_in_bytes), 0), "
          "coalesce(sum(record_count), 0) FROM " + files_table
      )
      files = cursor.fetchone() or (0, 0, 0)
      return {
          "table": qualified_table,
          "snapshot_id": snapshot[0],
          "snapshot_committed_at": str(snapshot[1]) if snapshot[1] is not None else None,
          "file_count": int(files[0] or 0),
          "file_bytes": int(files[1] or 0),
          "record_count": int(files[2] or 0),
      }
  ```

  Add a `TelemetryCursor` wrapper in the same module. Its `execute`, `fetchone`, and `fetchall` methods must proxy to the workload cursor; after each fetch, it must call `collect_query_metrics` with a separate metadata cursor and append one result to a public `records` list. If no query ID exists, append a record with `metric_source="unavailable"` and reason `missing_query_id`.

- [ ] **Step 4: Run the telemetry tests and static compilation.**

  ```bash
  python3 -m pytest domains/weather/tests/test_trino_query_metrics.py -q
  python3 -m compileall domains/weather/weather_ingest/trino_query_metrics.py
  ```

  Expected: all tests pass and compileall reports the module without syntax errors.

- [ ] **Step 5: At Gate B, commit only the telemetry module and test in ASAC-DAG.**

  ```bash
  git add domains/weather/weather_ingest/trino_query_metrics.py domains/weather/tests/test_trino_query_metrics.py
  git commit -m "feat: collect trino query cost metrics"
  ```

### Task 3: Build the Read-Only Weather/Traffic Benchmark CLI (ASAC-DAG)

**Files:**

- Create: `dags/domains/weather/weather_ingest/weather_traffic_cost_proxy.py`
- Create: `dags/domains/weather/tests/test_weather_traffic_cost_proxy.py`

**Interfaces:**

- Consumes: `weather_ingest.trino_query_metrics`, `/home/airflow/dbt-venv/bin/dbt`, the mounted Weather/Traffic dbt projects, and an explicit dev schema.
- Produces: a JSON benchmark bundle with per-query telemetry and fingerprints; a Markdown comparison only when fingerprints match.
- CLI contract:

  ```text
  collect --label {before,after} --repeat N --output PATH
  compare --before PATH --after PATH --output PATH
  ```

- [ ] **Step 1: Write failing pure-function tests for bundle comparison.**

  Add tests that call `compare_bundles(before, after)` directly:

  ```python
  def test_compare_bundles_rejects_different_fingerprints():
      before = {"fingerprint": {"weather": {"snapshot_id": 1}}, "suites": []}
      after = {"fingerprint": {"weather": {"snapshot_id": 2}}, "suites": []}

      comparison = compare_bundles(before, after)

      assert comparison["comparable"] is False
      assert comparison["reason"] == "fingerprint_mismatch"
      assert comparison["metrics"] == []


  def test_compare_bundles_uses_median_and_never_coerces_missing_to_zero():
      before = bundle_with_runs([100, 110, 120], physical_input=[1000, None, 1200])
      after = bundle_with_runs([80, 90, 100], physical_input=[800, None, 900])

      comparison = compare_bundles(before, after)

      assert comparison["comparable"] is True
      assert comparison["metrics"]["wall_time_ms"]["before_median"] == 110
      assert comparison["metrics"]["wall_time_ms"]["after_median"] == 90
      assert comparison["metrics"]["physical_input_bytes"]["status"] == "unavailable"
  ```

  Add an artifact-render test:

  ```python
  def test_render_comparison_markdown_discloses_proxy_and_non_comparable_state():
      rendered = render_comparison_markdown({"comparable": False, "reason": "fingerprint_mismatch", "metrics": {}})
      assert "비용 대리 지표" in rendered
      assert "비교 불가" in rendered
  ```

- [ ] **Step 2: Run the tests and confirm import failure.**

  ```bash
  python3 -m pytest domains/weather/tests/test_weather_traffic_cost_proxy.py -q
  ```

  Expected: import failure because the benchmark script does not yet exist.

- [ ] **Step 3: Implement the benchmark cases and fixed fingerprint contract.**

  Define exactly these cases, each using the source schema in `ASK_SEOUL_SCHEMA` (default `ask_seoul`):

  ```python
  CASES = {
      "weather_silver": {
          "domain": "weather",
          "project": "/opt/airflow/dbt/domains/weather",
          "model": "silver_kma_vilage_fcst",
          "source_tables": [
              "bronze_kma_vilage_fcst",
              "bronze_collection_run_manifest",
          ],
      },
      "traffic_silver": {
          "domain": "traffic",
          "project": "/opt/airflow/dbt/domains/traffic",
          "model": "silver_seoul_traffic_incident",
          "source_tables": [
              "bronze_seoul_traffic_incident",
              "bronze_seoul_traffic_incident_request_audit",
              "bronze_collection_run_manifest",
          ],
      },
      "weather_watchdog": {"domain": "weather", "report": "weather"},
      "traffic_watchdog": {"domain": "traffic", "report": "traffic"},
  }
  ```

  For each dbt case, run one compile before repetition:

  ```python
  subprocess.run(
      [DBT_BIN, "compile", "--select", case["model"], "--target", "dev", "--no-use-colors"],
      cwd=case["project"],
      env={**os.environ, "DBT_PROJECT_DIR": case["project"], "DBT_PROFILES_DIR": case["project"]},
      check=True,
      text=True,
  )
  ```

  Read the matching `resource_type == "model"` and `name == case["model"]` node’s `compiled_code` from `target/manifest.json`. Execute only:

  ```python
  workload_cursor.execute("EXPLAIN ANALYZE " + compiled_code)
  workload_cursor.fetchall()
  ```

  Use `TelemetryCursor` when calling `build_weather_reliability_report` and `build_traffic_reliability_report`; do not invoke the Discord send functions from the benchmark.

  Before and after each suite, obtain Iceberg fingerprints for its declared source tables. Set `suite["comparable_within_run"] = False` if either fingerprint changes during the repeats.

- [ ] **Step 4: Implement comparison with a strict metric policy.**

  `compare_bundles` must:

  ```python
  def median_or_none(values: list[int | float | None]) -> int | float | None:
      usable = sorted(value for value in values if value is not None)
      if len(usable) != len(values) or not usable:
          return None
      middle = len(usable) // 2
      return usable[middle] if len(usable) % 2 else (usable[middle - 1] + usable[middle]) / 2
  ```

  - reject different source fingerprints with `comparable=False`;
  - show min/median/max for each metric when all repeated values are available;
  - set the metric’s status to `unavailable` when even one repeat lacks it;
  - calculate percentage change only for comparable, non-zero baseline medians;
  - render a Korean Markdown table containing `physical_input_bytes`, `cpu_time_ms`, `physical_written_bytes`, `spilled_bytes`, `peak_user_memory_bytes`, and `wall_time_ms`.

- [ ] **Step 5: Verify script behavior without a live Trino runtime.**

  ```bash
  python3 -m pytest domains/weather/tests/test_weather_traffic_cost_proxy.py -q
  python3 -m compileall domains/weather/weather_ingest/weather_traffic_cost_proxy.py
  python3 domains/weather/weather_ingest/weather_traffic_cost_proxy.py --help
  ```

  Expected: unit tests and compilation pass; help prints `collect` and `compare`. Do not run `collect` until the Docker/Trino preflight in Task 8 succeeds.

- [ ] **Step 6: At Gate B, commit the script and test in ASAC-DAG.**

  ```bash
  git add domains/weather/weather_ingest/weather_traffic_cost_proxy.py domains/weather/tests/test_weather_traffic_cost_proxy.py
  git commit -m "feat: add weather traffic cost proxy benchmark"
  ```

### Task 4: Publish Existing dbt Run Results for Normal Transform Runs (ASAC-DAG)

**Files:**

- Modify: `dags/domains/weather/weather_vilage_fcst_transform.py:10-200`
- Modify: `dags/domains/weather/tests/test_weather_transform_dbt_selection.py`
- Modify: `dags/domains/traffic/traffic_incident_transform.py:10-158`
- Modify: `dags/domains/traffic/tests/test_traffic_transform_dbt_selection.py`

**Interfaces:**

- Consumes: existing `common.runmetrics.dump_dbt_run_results(path, domain, target=target)` and each dbt project’s `target/run_results.json`.
- Produces: one `publish_dbt_run_metrics` task per transform DAG, executed even if the final dbt task failed.
- Protects: existing dbt failure semantics; metrics publication must not turn a failing dbt task into a passing DAG run.

- [ ] **Step 1: Extend the transform tests with an `ALL_DONE` metrics-task expectation.**

  Add fake `TriggerRule` support and assert the final DAG edge:

  ```python
  def test_weather_transform_publishes_dbt_run_metrics_after_terminal_test():
      module = load_transform_module()
      task = module.dag.task_dict["publish_dbt_run_metrics"]

      assert task.kwargs["trigger_rule"] == "all_done"
      assert module.dag.task_dict["dbt_test_place_mart"].downstream_task_ids == {"publish_dbt_run_metrics"}


  def test_traffic_transform_publishes_dbt_run_metrics_after_terminal_test():
      module = load_transform_module()
      task = module.dag.task_dict["publish_dbt_run_metrics"]

      assert task.kwargs["trigger_rule"] == "all_done"
      assert module.dag.task_dict["dbt_test_gold"].downstream_task_ids == {"publish_dbt_run_metrics"}
  ```

- [ ] **Step 2: Run the two tests to prove the task is absent.**

  ```bash
  python3 -m pytest \
    domains/weather/tests/test_weather_transform_dbt_selection.py::test_weather_transform_publishes_dbt_run_metrics_after_terminal_test \
    domains/traffic/tests/test_traffic_transform_dbt_selection.py::test_traffic_transform_publishes_dbt_run_metrics_after_terminal_test -q
  ```

  Expected: failure because `publish_dbt_run_metrics` does not exist.

- [ ] **Step 3: Add the same bounded helper to each DAG.**

  Add these imports/constants next to the existing transform constants, replacing `DOMAIN` with the appropriate string:

  ```python
  from airflow.utils.trigger_rule import TriggerRule
  from common.runmetrics import dump_dbt_run_results  # noqa: E402

  RUN_RESULTS_PATH = os.path.join(DBT_PROJECT, "target", "run_results.json")
  DOMAIN = "weather"


  def publish_dbt_run_metrics(run_results_path: str = RUN_RESULTS_PATH, **context) -> dict:
      if not os.path.exists(run_results_path):
          print(f"run_results.json 없음 — 메트릭 적재 skip: {run_results_path}")
          return {"rows": 0, "skipped": True}
      target = (context.get("params") or {}).get("target")
      records = dump_dbt_run_results(run_results_path, domain=DOMAIN, target=target)
      print(f"dbt 실행 메트릭 적재: {len(records)} records (domain={DOMAIN}, target={target})")
      return {"rows": len(records), "skipped": False}
  ```

  Add this operator after the current terminal dbt test:

  ```python
  publish_dbt_metrics = PythonOperator(
      task_id="publish_dbt_run_metrics",
      python_callable=publish_dbt_run_metrics,
      trigger_rule=TriggerRule.ALL_DONE,
      on_failure_callback=record_weather_problem,
  )
  ```

  Use `record_traffic_problem` in Traffic. Wire exactly one final edge:

  ```python
  dbt_test_place_mart >> publish_dbt_metrics  # Weather
  dbt_test_gold >> publish_dbt_metrics        # Traffic
  ```

  Do not alter existing dbt selector order or the current WIP runtime-guard nodes.

- [ ] **Step 4: Run the targeted tests, Python compilation, and diff inspection.**

  ```bash
  python3 -m pytest \
    domains/weather/tests/test_weather_transform_dbt_selection.py \
    domains/traffic/tests/test_traffic_transform_dbt_selection.py -q
  python3 -m compileall \
    domains/weather/weather_vilage_fcst_transform.py \
    domains/traffic/traffic_incident_transform.py
  git diff --check
  ```

  Expected: tests pass; Airflow import remains `NOT RUN` until an approved dev runtime is available.

- [ ] **Step 5: At Gate B, commit only the four ASAC-DAG transform files.**

  ```bash
  git add \
    domains/weather/weather_vilage_fcst_transform.py \
    domains/weather/tests/test_weather_transform_dbt_selection.py \
    domains/traffic/traffic_incident_transform.py \
    domains/traffic/tests/test_traffic_transform_dbt_selection.py
  git commit -m "feat: publish weather traffic dbt run metrics"
  ```

### Task 5: Make the Weather Reliability Report a Low-Cost Two-Tier Watchdog (ASAC-DAG)

**Files:**

- Modify: `dags/domains/weather/weather_ingest/reliability_report.py`
- Modify: `dags/domains/weather/weather_reliability_report.py`
- Modify: `dags/domains/weather/tests/test_weather_reliability_report.py`
- Create: `dags/domains/weather/tests/test_weather_reliability_dag.py`

**Interfaces:**

- Consumes: existing Weather Bronze table, shared manifest, Airflow Variable state, and existing Discord helper.
- Produces: `PASS`, `WARN`, or `FAIL`; distinct `freshness_status`, `publishability_ok`, `coverage_ok`, and `late_publishability` fields.
- Protects: KMA grid coverage, raw-page evidence, no automatic recollect/backfill, and no service-key/webhook exposure.

- [ ] **Step 1: Add failing tests for Weather threshold precedence and partition-safe SQL.**

  Add these tests to the existing report test module:

  ```python
  def test_weather_report_warns_after_four_hours_but_before_six(monkeypatch):
      monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
      cursor = RecordingCursor(rows=[
          (8, 640, 640, 512000, 80, 80, 8, "20260702", "0800", datetime(2026, 7, 2, 4, 30, tzinfo=timezone.utc)),
      ])

      result = report.build_weather_reliability_report(
          cursor=cursor,
          detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
      )

      assert result["status"] == "WARN"
      assert result["weather"]["freshness_status"] == "WARN"


  def test_weather_report_fails_after_six_hours(monkeypatch):
      monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
      cursor = RecordingCursor(rows=[
          (8, 640, 640, 512000, 80, 80, 8, "20260702", "0800", datetime(2026, 7, 2, 2, 59, tzinfo=timezone.utc)),
      ])

      result = report.build_weather_reliability_report(
          cursor=cursor,
          detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
      )

      assert result["status"] == "FAIL"
      assert result["weather"]["freshness_status"] == "FAIL"


  def test_weather_summary_uses_load_date_and_collected_at_bounds(monkeypatch):
      monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
      cursor = RecordingCursor(rows=[(8, 640, 640, 512000, 80, 80, 8, "20260702", "0800", datetime(2026, 7, 2, 8, 20, tzinfo=timezone.utc))])

      report.build_weather_reliability_report(cursor=cursor, detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc))

      assert "load_date >= '2026-06-30'" in cursor.statements[0]
      assert "collected_at >= TIMESTAMP '2026-07-01 09:00:00.000000'" in cursor.statements[0]
  ```

  Add a schedule test asserting `"0 * * * *"` when target is dev and a webhook is configured, and `None` for prod even when a schedule environment variable exists.

  Create `test_weather_reliability_dag.py` with the existing transform-test fake-DAG import pattern. Stub `airflow.DAG`, `PythonOperator`, `airflow.models.Variable`, `common.errors.airflow.problem_failure_callback`, and `common.runmetrics.track`. Add these exact behavior tests:

  ```python
  def test_weather_notifies_only_when_status_changes():
      state = {"value": "PASS"}

      changed = module.should_notify_status_change(
          "WARN",
          get=lambda *_args, **_kwargs: state["value"],
          set=lambda _key, value: state.update(value=value),
      )

      assert changed is True
      assert state["value"] == "WARN"
      assert module.should_notify_status_change(
          "WARN",
          get=lambda *_args, **_kwargs: state["value"],
          set=lambda _key, value: state.update(value=value),
      ) is False


  def test_weather_notifies_fail_open_when_variable_access_fails():
      assert module.should_notify_status_change(
          "FAIL",
          get=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("state unavailable")),
          set=lambda *_args, **_kwargs: None,
      ) is True
  ```

- [ ] **Step 2: Run the new Weather tests and confirm failure.**

  ```bash
  python3 -m pytest domains/weather/tests/test_weather_reliability_report.py -q
  ```

  Expected: the new WARN/SLO and schedule assertions fail against the current binary freshness implementation.

- [ ] **Step 3: Implement two-tier Weather freshness and deterministic query bounds.**

  Replace the single `freshness_minutes` config field with:

  ```python
  @dataclass(frozen=True)
  class WeatherReportConfig:
      catalog: str
      schema: str
      lookback_hours: int
      expected_kma_grids: int
      freshness_warn_minutes: int
      freshness_error_minutes: int
  ```

  Use these defaults in `report_config`:

  ```python
  freshness_warn_minutes=int(env.get("ASK_SEOUL_REPORT_WEATHER_FRESHNESS_WARN_MINUTES", "240")),
  freshness_error_minutes=int(env.get("ASK_SEOUL_REPORT_WEATHER_FRESHNESS_ERROR_MINUTES", "360")),
  ```

  Add exactly this severity helper:

  ```python
  def freshness_status(age_minutes: int | None, warn_minutes: int, error_minutes: int) -> str:
      if age_minutes is None or age_minutes > error_minutes:
          return "FAIL"
      if age_minutes > warn_minutes:
          return "WARN"
      return "PASS"
  ```

  Compute the time cutoff from `detected_at`, not from a database clock:

  ```python
  def _weather_cutoffs(config: WeatherReportConfig, detected_at: datetime) -> tuple[datetime, str]:
      cutoff = detected_at.astimezone(timezone.utc) - timedelta(hours=config.lookback_hours)
      partition_days = max(1, math.ceil(config.lookback_hours / 24) + 1)
      load_date_floor = (detected_at.astimezone(KST).date() - timedelta(days=partition_days)).isoformat()
      return cutoff, load_date_floor
  ```

  Use both predicates in the Bronze summary query:

  ```python
  cutoff, load_date_floor = _weather_cutoffs(config, detected_at)
  ```

  Render the SQL predicates with the already-defined helpers as:

  ```python
  f"AND load_date >= {_sql_string(load_date_floor)}"
  f"AND collected_at >= {_sql_timestamp_utc(cutoff)}"
  ```

  Keep the existing coverage calculation unchanged. Set `weather["freshness_status"]`, `freshness_warn_minutes`, and `freshness_error_minutes`; combine status as `FAIL` for failed coverage/freshness, `WARN` for a warning freshness state, otherwise `PASS`.

  Extend the manifest summary to expose `last_success_at` and `last_publishable_at`. Report `publishability_ok` only when a latest successful publishable run exists. Add the truthful, non-scoring field:

  ```python
  "late_publishability": {
      "status": "NOT_EVALUATED",
      "reason": "bounded late-repair contract is owned by ASAC-DBT #165",
  }
  ```

- [ ] **Step 4: Make notifications transition-based and failure-safe.**

  In `weather_reliability_report.py`, import `Variable`, `TriggerRule` is not required, and `track`:

  ```python
  from airflow.models import Variable
  from common.runmetrics import track  # noqa: E402

  STATUS_VARIABLE = "ask_seoul.weather.bronze_reliability.status"


  def should_notify_status_change(status: str, *, get=Variable.get, set=Variable.set) -> bool:
      try:
          previous = get(STATUS_VARIABLE, default_var="UNKNOWN")
          if previous == status:
              return False
          set(STATUS_VARIABLE, status)
          return True
      except Exception:  # state tracking must never suppress an alert
          return True


  @track(layer="bronze", domain="weather")
  def collect_and_notify(**context) -> dict:
      report = build_weather_reliability_report()
      status_changed = should_notify_status_change(report["status"])
      report["discord_sent"] = send_discord_message(format_weather_discord_message(report)) if status_changed else False
      report["notification_reason"] = "status_changed" if status_changed else "status_unchanged"
      report["dag_run_id"] = context["run_id"]
      return report
  ```

  Wrap `Variable.get`/`Variable.set` in a local `try/except Exception` that returns `True` on state-store failure; this preserves alert delivery instead of failing or suppressing it. Add `DISCORD_YELLOW = 16776960`, render a warning state as yellow, and add the distinct publishability and late-repair-pending lines to the message.

  Change the default dev-with-webhook schedule to `"0 * * * *"`. Return `None` immediately for any non-dev target, including when a schedule override is present.

- [ ] **Step 5: Run focused tests and verify no secret regressions.**

  ```bash
  python3 -m pytest domains/weather/tests/test_weather_reliability_report.py -q
  python3 -m pytest domains/weather/tests/test_weather_reliability_dag.py -q
  python3 -m compileall \
    domains/weather/weather_ingest/reliability_report.py \
    domains/weather/weather_reliability_report.py
  git diff --check
  ```

  Expected: all tests pass. Airflow import remains `NOT RUN` until an approved dev environment exists.

- [ ] **Step 6: At Gate B, commit only the Weather watchdog files.**

  ```bash
  git add \
    domains/weather/weather_ingest/reliability_report.py \
    domains/weather/weather_reliability_report.py \
    domains/weather/tests/test_weather_reliability_report.py \
    domains/weather/tests/test_weather_reliability_dag.py
  git commit -m "feat: harden weather freshness watchdog"
  ```

### Task 6: Make the Traffic Reliability Report a Low-Cost Two-Tier Watchdog (ASAC-DAG)

**Files:**

- Modify: `dags/domains/traffic/traffic_ingest/reliability_report.py`
- Modify: `dags/domains/traffic/traffic_reliability_report.py`
- Modify: `dags/domains/traffic/tests/test_traffic_reliability_report.py`
- Create: `dags/domains/traffic/tests/test_traffic_reliability_dag.py`

**Interfaces:**

- Consumes: Traffic audit rows, shared manifest, Airflow Variable state, existing Discord helper.
- Produces: `PASS`, `WARN`, or `FAIL` without treating a valid zero-row response as availability failure.
- Protects: TOPIS normal zero rows, request-audit evidence, source GRS80 data, and the ASAC-DBT #117 repair boundary.

- [ ] **Step 1: Add failing tests for 15m/30m severity, zero-row safety, and prod schedule blocking.**

  Add tests equivalent to these:

  ```python
  def test_traffic_report_warns_after_fifteen_minutes(monkeypatch):
      monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
      cursor = RecordingCursor(rows=[(1, 0, 0, 0, 1, datetime(2026, 7, 2, 8, 44, tzinfo=timezone.utc))])

      result = report.build_traffic_reliability_report(
          cursor=cursor,
          detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
      )

      assert result["status"] == "WARN"
      assert result["traffic"]["freshness_status"] == "WARN"
      assert result["traffic"]["zero_row_success_count"] == 1
      assert result["traffic"]["coverage_ok"] is True


  def test_traffic_report_fails_after_thirty_minutes(monkeypatch):
      monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
      cursor = RecordingCursor(rows=[(1, 0, 0, 0, 1, datetime(2026, 7, 2, 8, 29, tzinfo=timezone.utc))])

      result = report.build_traffic_reliability_report(
          cursor=cursor,
          detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc),
      )

      assert result["status"] == "FAIL"
      assert result["traffic"]["freshness_status"] == "FAIL"


  def test_traffic_report_has_logical_load_date_bound_without_claiming_partition_pruning(monkeypatch):
      monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
      cursor = RecordingCursor(rows=[(1, 25, 25, 1000, 0, datetime(2026, 7, 2, 8, 55, tzinfo=timezone.utc))])

      report.build_traffic_reliability_report(cursor=cursor, detected_at=datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc))

      assert "load_date >= '2026-06-30'" in cursor.statements[0]
  ```

  Add schedule assertions for `"*/15 * * * *"` in dev with a webhook and `None` in prod even if `ASK_SEOUL_TRAFFIC_REPORT_DAG_SCHEDULE` is set.

  Create `test_traffic_reliability_dag.py` with its own local fake modules: a `FakeDAG` context manager with `task_dict`, a `FakePythonOperator` that records `task_id`/`python_callable`/kwargs, and a `FakeVariable` exposing static `get`/`set` methods. Register those under `airflow`, `airflow.models`, and `airflow.providers.standard.operators.python`; register a no-op `problem_failure_callback` and identity `track` decorator under `common.errors.airflow` and `common.runmetrics`. Then import `traffic_reliability_report.py` by file path. Add:

  ```python
  def test_traffic_notifies_only_on_status_transition():
      state = {"value": "WARN"}

      assert module.should_notify_status_change(
          "WARN",
          get=lambda *_args, **_kwargs: state["value"],
          set=lambda _key, value: state.update(value=value),
      ) is False
      assert module.should_notify_status_change(
          "FAIL",
          get=lambda *_args, **_kwargs: state["value"],
          set=lambda _key, value: state.update(value=value),
      ) is True
      assert state["value"] == "FAIL"
  ```

- [ ] **Step 2: Run the Traffic test module and confirm failure.**

  ```bash
  python3 -m pytest domains/traffic/tests/test_traffic_reliability_report.py -q
  ```

  Expected: threshold and schedule tests fail before implementation.

- [ ] **Step 3: Implement Traffic severity and retain the valid-zero-row contract.**

  Use the same local `freshness_status` function shape as Task 5, with:

  ```python
  freshness_warn_minutes=int(env.get("ASK_SEOUL_REPORT_TRAFFIC_FRESHNESS_WARN_MINUTES", "15")),
  freshness_error_minutes=int(env.get("ASK_SEOUL_REPORT_TRAFFIC_FRESHNESS_ERROR_MINUTES", "30")),
  ```

  Calculate the collection cutoff and `load_date` floor from `detected_at`, then add both predicates to the audit query. Add a code comment adjacent to the predicate:

  ```python
  # Traffic Bronze/audit are not physically partitioned today. This bounds report semantics
  # but is not asserted to reduce physical input bytes; the benchmark determines that.
  ```

  Preserve this existing coverage rule exactly:

  ```python
  coverage_ok = request_count > 0 and (
      list_total_count == 0 or parsed_row_count >= list_total_count or max_end_index >= list_total_count
  )
  ```

  This is what keeps `INFO-000` normal zero rows valid. Add `freshness_status`, warn/error threshold fields, `last_success_at`, `last_publishable_at`, and `publishability_ok`. Add this explicit non-claim:

  ```python
  "late_publishability": {
      "status": "NOT_EVALUATED",
      "reason": "bounded late-repair contract is owned by ASAC-DBT #117",
  }
  ```

- [ ] **Step 4: Implement Traffic status-transition notifications.**

  Implement the Traffic notification helper with this exact distinct key and fail-open behavior:

  ```python
  STATUS_VARIABLE = "ask_seoul.traffic.bronze_reliability.status"


  def should_notify_status_change(status: str, *, get=Variable.get, set=Variable.set) -> bool:
      try:
          previous = get(STATUS_VARIABLE, default_var="UNKNOWN")
          if previous == status:
              return False
          set(STATUS_VARIABLE, status)
          return True
      except Exception:  # state tracking must never suppress an alert
          return True


  @track(layer="bronze", domain="traffic")
  def collect_and_notify(**context) -> dict:
      report = build_traffic_reliability_report()
      status_changed = should_notify_status_change(report["status"])
      report["discord_sent"] = send_discord_message(format_traffic_discord_message(report)) if status_changed else False
      report["notification_reason"] = "status_changed" if status_changed else "status_unchanged"
      report["dag_run_id"] = context["run_id"]
      return report
  ```

  State-store errors must fail open to message delivery. Render `WARN` with yellow and schedule the dev-with-webhook report at `"*/15 * * * *"`; block all prod schedules.

- [ ] **Step 5: Run Traffic validation.**

  ```bash
  python3 -m pytest domains/traffic/tests/test_traffic_reliability_report.py -q
  python3 -m pytest domains/traffic/tests/test_traffic_reliability_dag.py -q
  python3 -m compileall \
    domains/traffic/traffic_ingest/reliability_report.py \
    domains/traffic/traffic_reliability_report.py
  git diff --check
  ```

  Expected: tests pass. Record Airflow import and live DAG execution as `NOT RUN` until dev preflight succeeds.

- [ ] **Step 6: At Gate B, commit only the Traffic watchdog files.**

  ```bash
  git add \
    domains/traffic/traffic_ingest/reliability_report.py \
    domains/traffic/traffic_reliability_report.py \
    domains/traffic/tests/test_traffic_reliability_report.py \
    domains/traffic/tests/test_traffic_reliability_dag.py
  git commit -m "feat: harden traffic freshness watchdog"
  ```

### Task 7: Align dbt Source Freshness With the Watchdog SLOs (ASAC-DBT)

**Files:**

- Modify: `dbt/domains/weather/models/sources.yml:8-11`
- Modify: `dbt/domains/traffic/models/sources.yml:111-114`

**Interfaces:**

- Consumes: existing `loaded_at_field` contracts (`collected_at` for Weather Bronze, `event_at` for Traffic manifest).
- Produces: dbt failure thresholds that match the Watchdog’s warning/error cadence.
- Protects: Traffic normal-zero availability separation and all existing dbt model/repair contracts.

- [ ] **Step 1: Make the exact YAML threshold edits.**

  In Weather, replace:

  ```yaml
  freshness:
    warn_after: {count: 30, period: hour}
    error_after: {count: 48, period: hour}
  ```

  with:

  ```yaml
  freshness:
    warn_after: {count: 4, period: hour}
    error_after: {count: 6, period: hour}
  ```

  In Traffic’s `collection_run_manifest`, replace:

  ```yaml
  freshness:
    warn_after: {count: 30, period: minute}
    error_after: {count: 2, period: hour}
  ```

  with:

  ```yaml
  freshness:
    warn_after: {count: 15, period: minute}
    error_after: {count: 30, period: minute}
  ```

- [ ] **Step 2: Run non-writing dbt validation first.**

  ```bash
  /home/airflow/dbt-venv/bin/dbt deps --project-dir domains/weather --profiles-dir domains/weather
  /home/airflow/dbt-venv/bin/dbt parse --project-dir domains/weather --profiles-dir domains/weather --target dev
  /home/airflow/dbt-venv/bin/dbt deps --project-dir domains/traffic --profiles-dir domains/traffic
  /home/airflow/dbt-venv/bin/dbt parse --project-dir domains/traffic --profiles-dir domains/traffic --target dev
  git diff --check
  ```

  Expected: `deps`/`parse` succeed without database writes. If the dbt virtualenv is unavailable outside Airflow, record these commands as `NOT RUN`; do not substitute a production profile.

- [ ] **Step 3: Run scoped dev source freshness only under an approved dev runtime.**

  ```bash
  /home/airflow/dbt-venv/bin/dbt source freshness \
    --project-dir domains/weather --profiles-dir domains/weather --target dev --select source:weather_bronze
  /home/airflow/dbt-venv/bin/dbt source freshness \
    --project-dir domains/traffic --profiles-dir domains/traffic --target dev --select source:traffic_bronze.collection_run_manifest
  ```

  Expected: report observed freshness against the new limits. A failure is an operational signal to investigate; do not weaken the threshold or change source data during this task.

- [ ] **Step 4: At Gate B, commit only the two source YAML files.**

  ```bash
  git add domains/weather/models/sources.yml domains/traffic/models/sources.yml
  git commit -m "chore: align weather traffic freshness slo"
  ```

### Task 8: Run the Approved Dev Baseline and Produce the Evidence Bundle

**Files:**

- Create at runtime only: `artifacts/benchmarks/before.json`
- Create at runtime only: `artifacts/benchmarks/after.json`
- Create at runtime only: `artifacts/benchmarks/before-after.md`
- Create after review: `docs/benchmarks/YYYY-MM-DD-weather-traffic-cost-proxy.md`
- Modify after review: `retrospective-2026-07-09.md`

**Interfaces:**

- Consumes: a healthy approved dev Docker/Trino stack, the benchmark CLI, and unchanged Iceberg source snapshots.
- Produces: baseline evidence, comparison status, an explicit top-cost suite ranking, and a retrospective record.

- [ ] **Step 1: Check runtime prerequisites without opening secrets.**

  ```bash
  docker compose config --quiet
  docker compose ps
  docker compose exec trino trino --execute "SELECT version()"
  docker compose exec trino trino --execute "SHOW TABLES FROM system.runtime"
  ```

  Expected: Compose services are healthy, `version()` returns a version, and `queries` appears in `system.runtime`. If Docker is unavailable or any check fails, record `NOT RUN` with the exact non-secret error and stop this task; do not start Docker services or alter credentials.

- [ ] **Step 2: Run three read-only baseline repetitions.**

  ```bash
  docker compose exec airflow-scheduler python \
    /opt/airflow/dags/domains/weather/weather_ingest/weather_traffic_cost_proxy.py collect \
    --label before --repeat 3 \
    --output /opt/airflow/artifacts/benchmarks/before.json
  ```

  Expected: each suite contains three repetitions, source-table fingerprints before/after the suite, per-query metrics, and an explicit `unavailable` state for any metric not exposed by Trino.

- [ ] **Step 3: Rank the top cost candidate without changing its SQL yet.**

  Read `before.json` and rank suites by this lexicographic tuple:

  ```text
  median(physical_input_bytes), median(cpu_time_ms), median(spilled_bytes), median(wall_time_ms)
  ```

  Ignore a metric at a rank position when it is marked `unavailable` for any repeat. Record the winning suite and the exact fingerprints in `docs/benchmarks/YYYY-MM-DD-weather-traffic-cost-proxy.md`; do not make a cost-saving claim yet.

- [ ] **Step 3a: Calculate the daily watchdog overhead metric-by-metric.**

  Use the report-suite medians from `before.json` and these known dev cadences:

  ```text
  weather_watchdog_runs_per_day = 24       # 0 * * * *
  traffic_watchdog_runs_per_day = 96       # */15 * * * *
  weather_transform_runs_per_day = 8       # KMA publish cron: 2,5,8,11,14,17,20,23 KST
  traffic_transform_runs_per_day = 288     # Traffic Bronze: */5 * * * *
  ```

  For each available primary metric, compute separately:

  ```text
  watchdog_daily_metric = median(watchdog metric per run) × watchdog_runs_per_day
  transform_daily_metric = median(transform metric per run) × transform_runs_per_day
  watchdog_ratio = watchdog_daily_metric / transform_daily_metric
  ```

  Record `physical_input_bytes` and `cpu_time_ms` ratios independently. If either ratio is greater than `0.05`, do not enable the proposed watchdog cadence; first reduce the report query scan or schedule and repeat the baseline.

- [ ] **Step 4: Apply only the separately approved data-driven optimization and collect `after`.**

  This plan intentionally stops before a model SQL, partitioning, or repair change. Create a separate repository-scoped issue and implementation plan for the observed winner, then obtain a new Gate A. After that independent change passes its own contract tests, run:

  ```bash
  docker compose exec airflow-scheduler python \
    /opt/airflow/dags/domains/weather/weather_ingest/weather_traffic_cost_proxy.py collect \
    --label after --repeat 3 \
    --output /opt/airflow/artifacts/benchmarks/after.json
  docker compose exec airflow-scheduler python \
    /opt/airflow/dags/domains/weather/weather_ingest/weather_traffic_cost_proxy.py compare \
    --before /opt/airflow/artifacts/benchmarks/before.json \
    --after /opt/airflow/artifacts/benchmarks/after.json \
    --output /opt/airflow/artifacts/benchmarks/before-after.md
  ```

  Expected: `before-after.md` is either an explicit `비교 불가` document or contains the median/range/change table. Do not retain an optimization that fails the 15% primary-proxy threshold without the documented spill exception.

- [ ] **Step 5: Append the retrospective without replacing current WIP.**

  Add a new final section to `retrospective-2026-07-09.md` with this exact table shape:

  ```markdown
  ## 비용 대리 지표 실험 (YYYY-MM-DD)

  | Suite | Fingerprint | 지표 | Before median (min–max) | After median (min–max) | 변화율 | 판정 |
  | --- | --- | --- | --- | --- | --- | --- |
  | weather_silver | before.json의 weather Bronze·manifest snapshot ID | physical_input_bytes | before.json 중앙값(최소–최대) | after.json 중앙값(최소–최대) | compare 결과 변화율 | 유지/되돌림/비교 불가 |

  - 정합성 게이트: 실행한 Weather/Traffic Python 및 dbt selector의 PASS/FAIL/NOT RUN 결과
  - 측정 불가 지표: before.json 또는 after.json이 기록한 metric 이름과 unavailable reason
  - 한계: 실제 Cloudflare 청구액이 아니라 Trino·Iceberg·Airflow 비용 대리 지표다.
  - 남은 리스크: ASAC-DBT #117 Traffic repair, ASAC-DBT #165 Weather late repair, baseline이 지목한 다음 최적화 이슈
  ```

  Preserve every existing paragraph and append only after the existing final separator.

- [ ] **Step 6: Run final evidence checks and prepare Gate B reports separately.**

  ```bash
  git diff --check
  git -C dags diff --check
  git -C dbt diff --check
  ```

  Report per repository: exact files, targeted test results, `NOT RUN` runtime checks, fingerprints, data-contract protection, no production writes, and remaining #117/#165 risk. Do not combine or commit repositories in one change.

## Execution Stop Condition

This plan is complete when:

1. the benchmark tool produces a valid dev baseline bundle or accurately records why the runtime is unavailable;
2. Weather/Traffic watchdogs expose separate freshness, coverage, publishability, normal-zero, and late-repair-pending signals;
3. dbt source freshness thresholds match the documented cadence;
4. baseline results identify a single top cost candidate; and
5. a retrospective-ready evidence template exists.

The later model/table optimization is intentionally not guessed in advance. Its exact SQL, data contract, rollback, and test shape must come from the baseline and be approved as a new repository-scoped plan.
