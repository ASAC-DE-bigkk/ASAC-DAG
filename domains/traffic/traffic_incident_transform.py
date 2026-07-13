"""Airflow DAG: traffic silver/gold transform via dbt.

The bronze DAG stores TOPIS AccInfo raw XML in R2 and publishes verified
Iceberg bronze runs. This transform DAG consumes only publishable bronze runs
through the dbt models and keeps transform retries independent from API calls.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule

# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
DOMAINS_DIR = os.path.dirname(DAG_DIR)
if DOMAINS_DIR not in sys.path:
    sys.path.insert(0, DOMAINS_DIR)
DAGS_ROOT_DIR = os.path.dirname(os.path.dirname(DAG_DIR))
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from weather.bronze_run_manifest import MANIFEST_TABLE, STATUS_SUCCESS  # noqa: E402
from common.discord import COLOR_FAIL, first_notice_for_run, send_embed  # noqa: E402
from common.errors.airflow import problem_failure_callback, problem_from_airflow_context  # noqa: E402
from common.errors.sink import R2ErrorSink  # noqa: E402
from common.runmetrics import dump_dbt_run_results  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from traffic_ingest.common.runtime import sql_string, trino_cursor  # noqa: E402
from traffic_dbt_failure import (  # noqa: E402
    R2RecoveryRecordSink,
    build_failure_notification,
    build_recovery_record,
    classify_dbt_failure,
    load_dbt_results,
    silver_persisted_from_results,
)


KST = ZoneInfo("Asia/Seoul")
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/traffic"
RUN_RESULTS_PATH = os.path.join(DBT_PROJECT, "target", "run_results.json")
DOMAIN = "traffic"
# Bronze runs every five minutes. Keep the hourly transform outside that boundary
# so a run consumes one stable, publishable Bronze snapshot.
TRAFFIC_TRANSFORM_CRON_KST = "12 * * * *"
TRAFFIC_SOURCE_ID = "seoul_traffic_incident"
SNAPSHOT_TASK_ID = "resolve_traffic_snapshot_run"
DBT_FAILURE_XCOM_KEY = "traffic_dbt_failure"
DBT_PHASE_TASK_IDS = (
    "dbt_deps",
    "dbt_source_freshness",
    "dbt_test_traffic_incident_availability",
    "dbt_seed_asac_axes",
    "dbt_run_common_admin_dong_dimension",
    "dbt_test_common_admin_dong_dimension",
    "dbt_run_silver",
    "dbt_test_silver",
    "dbt_run_gold",
    "dbt_test_gold",
)
DBT_RETRY_DELAY = timedelta(minutes=2)
DEFAULT_PARAMS = {
    "target": Param(
        default="dev",
        type="string",
        enum=["dev"],
        description="dbt target profile name (dev only until production rollout).",
    )
}
# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_traffic_problem = problem_failure_callback(domain="traffic")


def transform_schedule() -> str | None:
    if "ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE"] or None
    return TRAFFIC_TRANSFORM_CRON_KST


def resolve_traffic_snapshot_run() -> str:
    """Pin the newest completed Bronze run for every dbt command in this DAG run."""
    cursor, catalog, schema = trino_cursor()
    cursor.execute(
        f"""
        SELECT CAST(dag_run_id AS varchar)
        FROM {catalog}.{schema}.{MANIFEST_TABLE}
        WHERE source_id = {sql_string(TRAFFIC_SOURCE_ID)}
          AND status = {sql_string(STATUS_SUCCESS)}
          AND is_publishable
        ORDER BY CAST(event_at AS timestamp(6)) DESC, CAST(dag_run_id AS varchar) DESC
        LIMIT 1
        """
    )
    row = cursor.fetchone()
    if not row or not row[0]:
        raise RuntimeError("No publishable Seoul traffic Bronze run is available for transform.")
    return str(row[0])


def _artifact_path(*, run_id: str | None, task_id: str | None, try_number: int | None) -> str:
    def safe(value: str | None) -> str:
        return "".join(char if char.isalnum() or char in "._=-" else "-" for char in value or "unknown")

    return str(
        Path(DBT_PROJECT) / "target" / "traffic-transform" / safe(run_id)
        / safe(task_id) / f"try{try_number if try_number is not None else 'unknown'}"
        / "run_results.json"
    )


def run_dbt_phase(*, dbt_args: str, snapshot_task_id: str,
                  silver_persisted: bool, fresh_parse: bool = False,
                  **context) -> dict[str, str]:
    """Run one pinned dbt phase and let Airflow retry infrastructure failures only."""
    ti = context["ti"]
    snapshot_run_id = ti.xcom_pull(task_ids=snapshot_task_id)
    run_id = context.get("run_id")
    task_id = getattr(ti, "task_id", None)
    try_number = getattr(ti, "try_number", None)
    artifact_path = _artifact_path(run_id=run_id, task_id=task_id, try_number=try_number)
    params = context.get("params") or {}
    target = params.get("target", "dev")
    def build_command(current_args: str) -> list[str]:
        command = [
            DBT_BIN,
            *shlex.split(current_args),
            "--target", target,
            "--no-use-colors",
            "--vars", f'{{"traffic_snapshot_dag_run_id": "{snapshot_run_id}"}}',
        ]
        if shlex.split(current_args)[0] != "deps":
            command.extend(["--target-path", str(Path(artifact_path).parent)])
        return command

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = DBT_PROJECT
    env["DBT_PROJECT_DIR"] = DBT_PROJECT
    phase_args = ["parse --no-partial-parse", dbt_args] if fresh_parse else [dbt_args]
    for current_args in phase_args:
        completed = subprocess.run(
            build_command(current_args), cwd=DBT_PROJECT, env=env, check=False,
            capture_output=True, text=True,
        )
        if completed.stdout:
            print(completed.stdout, end="")
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr)
        if completed.returncode != 0:
            break
    if completed.returncode == 0:
        return {"status": "success", "artifact_path": artifact_path}

    results = load_dbt_results(artifact_path)
    failure = classify_dbt_failure(
        returncode=completed.returncode,
        results=results,
        artifact_path=artifact_path,
        command_output=f"{completed.stdout}\n{completed.stderr}",
    )
    record = build_recovery_record(
        failure,
        traffic_snapshot_dag_run_id=str(snapshot_run_id) if snapshot_run_id else None,
        dag_id=getattr(ti, "dag_id", "traffic_incident_transform"),
        task_id=task_id,
        run_id=run_id,
        try_number=try_number if isinstance(try_number, int) else None,
        silver_persisted=silver_persisted_from_results(results, default=silver_persisted),
        occurred_at=datetime.now(timezone.utc),
    )
    ti.xcom_push(key=DBT_FAILURE_XCOM_KEY, value=record)
    message = f"traffic dbt {failure.classification}: artifact={artifact_path}"
    if failure.retryable:
        raise AirflowException(message)
    raise AirflowFailException(message)


def record_traffic_dbt_problem(context) -> None:
    """Persist and notify final classified dbt failures without changing task state."""
    ti = context.get("task_instance") or context.get("ti")
    try:
        record = ti.xcom_pull(task_ids=getattr(ti, "task_id", None), key=DBT_FAILURE_XCOM_KEY)
    except Exception:  # noqa: BLE001 - fall back to the common failure record
        record = None
    if not isinstance(record, dict):
        record_traffic_problem(context)
        return

    try:
        problem = problem_from_airflow_context(context, domain="traffic")
        problem.detail = (
            f"{record.get('failure_classification')}; snapshot="
            f"{record.get('traffic_snapshot_dag_run_id')}; artifact={record.get('dbt_artifact_path')}"
        )
        problem.extensions = {
            name: record.get(name)
            for name in (
                "traffic_snapshot_dag_run_id",
                "dbt_test_names",
                "dbt_failed_row_count",
                "dbt_artifact_path",
                "silver_persisted",
                "failure_classification",
                "recovery_action",
            )
        }
        R2ErrorSink().write(problem)
    except Exception:  # noqa: BLE001 - a record failure must not hide the task failure
        pass
    try:
        R2RecoveryRecordSink().write(record)
    except Exception:  # noqa: BLE001 - a record failure must not hide the task failure
        pass
    try:
        if first_notice_for_run(record.get("dag_id"), record.get("run_id")):
            title, description, footer = build_failure_notification(record)
            send_embed(title, description, color=COLOR_FAIL, footer=footer, domain="traffic")
    except Exception:  # noqa: BLE001 - a notification failure must not hide the task failure
        pass


def dbt_task(task_id: str, dbt_args: str, *, silver_persisted: bool = False,
             fresh_parse: bool = False) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=run_dbt_phase,
        op_kwargs={
            "dbt_args": dbt_args,
            "snapshot_task_id": SNAPSHOT_TASK_ID,
            "silver_persisted": silver_persisted,
            "fresh_parse": fresh_parse,
        },
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        on_failure_callback=record_traffic_dbt_problem,
    )


def _current_run_results_path(**context) -> str | None:
    """Return the latest isolated dbt artifact recorded by this DAG run."""
    ti = context.get("ti") or context.get("task_instance")
    if ti is None:
        return None
    for task_id in reversed(DBT_PHASE_TASK_IDS):
        try:
            result = ti.xcom_pull(task_ids=task_id)
        except Exception:  # noqa: BLE001 - continue to earlier current-run phases
            result = None
        if isinstance(result, dict) and result.get("artifact_path"):
            return str(result["artifact_path"])
        try:
            failure = ti.xcom_pull(task_ids=task_id, key=DBT_FAILURE_XCOM_KEY)
        except Exception:  # noqa: BLE001 - continue to earlier current-run phases
            failure = None
        if isinstance(failure, dict) and failure.get("dbt_artifact_path"):
            return str(failure["dbt_artifact_path"])
    return None


def publish_dbt_run_metrics(run_results_path: str | None = None, **context) -> dict:
    """Persist Traffic dbt metrics while preserving terminal dbt failure semantics."""
    resolved_path = (
        run_results_path
        if run_results_path is not None
        else _current_run_results_path(**context)
    )
    if not resolved_path or not os.path.exists(resolved_path):
        print(f"run_results.json 없음 — 메트릭 적재 skip: {resolved_path}")
        return {"rows": 0, "skipped": True}
    target = (context.get("params") or {}).get("target")
    records = dump_dbt_run_results(resolved_path, domain=DOMAIN, target=target)
    print(f"dbt 실행 메트릭 적재: {len(records)} records (domain={DOMAIN}, target={target})")
    return {"rows": len(records), "skipped": False}


def fail_transform_if_upstream_failed() -> None:
    """Leave a failed DAG leaf whenever a transform task fails."""
    raise AirflowFailException("traffic transform upstream task failed")


with DAG(
    dag_id="traffic_incident_transform",
    description="Transform traffic bronze -> silver/gold via dbt.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=transform_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": DBT_RETRY_DELAY},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "traffic", "transform", "silver", "gold", "dbt"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "traffic", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_traffic_problem,
    )

    dbt_deps = dbt_task("dbt_deps", "deps")

    dbt_source_freshness = dbt_task("dbt_source_freshness", "source freshness")

    dbt_test_traffic_incident_availability = dbt_task(
        "dbt_test_traffic_incident_availability",
        "test --select assert_traffic_incident_row_availability",
    )

    dbt_seed_asac_axes = dbt_task("dbt_seed_asac_axes", "seed --select asac_axes")

    dbt_run_common_admin_dong_dimension = dbt_task(
        "dbt_run_common_admin_dong_dimension",
        "run --select asac_axes.dim_admin_dong",
    )

    dbt_test_common_admin_dong_dimension = dbt_task(
        "dbt_test_common_admin_dong_dimension",
        (
            "test --select asac_axes.dim_admin_dong "
            # Canonical Gold tests that also ref dim_admin_dong are selected
            # indirectly here, before the current run has rebuilt Gold.
            "--exclude "
            "assert_gold_traffic_current_by_admin_dong_hourly_admin_join_reconciles "
            "assert_gold_traffic_current_by_admin_dong_hourly_admin_stamp_exact "
            "assert_gold_traffic_current_by_admin_dong_hourly_fanout_reconciles "
            "assert_gold_traffic_current_by_admin_dong_hourly_hourly_completeness "
            "assert_gold_traffic_current_by_admin_dong_hourly_snapshot_reconciles"
        ),
    )

    resolve_snapshot = PythonOperator(
        task_id=SNAPSHOT_TASK_ID,
        python_callable=resolve_traffic_snapshot_run,
        on_failure_callback=record_traffic_problem,
    )

    dbt_run_silver = dbt_task(
        "dbt_run_silver",
        "run --select silver_seoul_traffic_incident silver_seoul_traffic_incident_current",
    )

    dbt_test_silver = dbt_task(
        "dbt_test_silver",
        (
            "test --select "
            "silver_seoul_traffic_incident "
            "silver_seoul_traffic_incident_current "
            "assert_traffic_current_pinned_publishable_run "
            "assert_silver_traffic_uses_publishable_runs "
            "assert_silver_traffic_location_contract "
            "assert_traffic_audit_covers_latest_total_count "
            "assert_silver_seoul_traffic_incident_grain_unique "
            "assert_silver_traffic_event_at_matches_occurred_at "
            "assert_silver_traffic_wgs84_required_when_source_coordinate_available "
            "assert_silver_traffic_admin_axis_consistent "
            "assert_silver_traffic_admin_axis_coverage "
            "assert_silver_traffic_latest_publishable_record "
            # gold-silver 교차 카운트 테스트는 silver 모델명 셀렉터가 참조 테스트로
            # 끌어오지만, 이 단계에서는 gold가 직전 사이클 상태라 silver 가 갱신된
            # 사이클마다 구조적으로 FAIL 한다. gold 재빌드 후 dbt_test_gold 에서만 돌린다.
            "--exclude assert_gold_traffic_counts_match_silver "
            "assert_gold_traffic_current_by_admin_dong_hourly_fanout_reconciles "
            "assert_gold_traffic_current_by_admin_dong_hourly_snapshot_reconciles"
        ),
        silver_persisted=True,
    )

    dbt_run_gold = dbt_task(
        "dbt_run_gold",
        "run --select gold_traffic_incident_summary "
        "gold_traffic_incident_current_by_admin_dong_hourly",
        silver_persisted=True,
    )

    dbt_test_gold = dbt_task(
        "dbt_test_gold",
        (
            "test --select "
            "gold_traffic_incident_summary "
            "gold_traffic_incident_current_by_admin_dong_hourly "
            "assert_gold_traffic_counts_match_silver "
            "assert_gold_traffic_row_counts_positive "
            "assert_gold_traffic_current_by_admin_dong_hourly_admin_join_reconciles "
            "assert_gold_traffic_current_by_admin_dong_hourly_admin_stamp_exact "
            "assert_gold_traffic_current_by_admin_dong_hourly_fanout_reconciles "
            "assert_gold_traffic_current_by_admin_dong_hourly_grain_unique "
            "assert_gold_traffic_current_by_admin_dong_hourly_hourly_completeness "
            "assert_gold_traffic_current_by_admin_dong_hourly_product_row_id_reproducible "
            "assert_gold_traffic_current_by_admin_dong_hourly_snapshot_reconciles "
            "assert_gold_traffic_current_by_admin_dong_hourly_zero_requires_complete"
        ),
        silver_persisted=True,
        fresh_parse=True,
    )

    publish_dbt_metrics = PythonOperator(
        task_id="publish_dbt_run_metrics",
        python_callable=publish_dbt_run_metrics,
        trigger_rule=TriggerRule.ALL_DONE,
        on_failure_callback=record_traffic_problem,
    )

    propagate_transform_failure = PythonOperator(
        task_id="fail_transform_if_upstream_failed",
        python_callable=fail_transform_if_upstream_failed,
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,
    )

    (
        validate_runtime
        >> resolve_snapshot
        >> dbt_deps
        >> dbt_source_freshness
        >> dbt_test_traffic_incident_availability
        >> dbt_seed_asac_axes
        >> dbt_run_common_admin_dong_dimension
        >> dbt_test_common_admin_dong_dimension
        >> dbt_run_silver
        >> dbt_test_silver
        >> dbt_run_gold
        >> dbt_test_gold
        >> publish_dbt_metrics
    )

    for transform_task in (
        validate_runtime,
        resolve_snapshot,
        dbt_deps,
        dbt_source_freshness,
        dbt_test_traffic_incident_availability,
        dbt_seed_asac_axes,
        dbt_run_common_admin_dong_dimension,
        dbt_test_common_admin_dong_dimension,
        dbt_run_silver,
        dbt_test_silver,
        dbt_run_gold,
        dbt_test_gold,
    ):
        transform_task >> propagate_transform_failure
