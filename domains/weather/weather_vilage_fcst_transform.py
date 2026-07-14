"""Airflow DAG: weather silver/gold transform via dbt.

The bronze DAG stores KMA raw payloads in R2 and publishes verified Iceberg
bronze runs. This transform DAG consumes only publishable bronze runs through
the dbt models and keeps silver/gold retries independent from API collection.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

# 공통 패키지(dags/common)와 Weather 로컬 패키지 import 경로를 초기화한다.
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)

DAGS_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.assets import WEATHER_BRONZE_ASSET  # noqa: E402
from common.runmetrics import dump_dbt_run_results  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from weather_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402


LOGGER = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/weather"
WEATHER_DBT_CONTRACT_VARS = {"weather_w2_canonical_revision_date": "2025-04-01"}
RUN_RESULTS_PATH = os.path.join(DBT_PROJECT, "target", "run_results.json")
WEATHER_DBT_ARTIFACT_XCOM_KEY = "weather_dbt_artifact_path"
DBT_PHASE_TASK_IDS = (
    "dbt_deps",
    "dbt_source_freshness",
    "dbt_seed_asac_axes",
    "dbt_run_common_admin_dong_dimension",
    "dbt_test_common_admin_dong_dimension",
    "dbt_seed_place_mapping",
    "dbt_test_place_mapping_seed",
    "dbt_run_silver",
    "dbt_test_silver",
    "dbt_run_gold",
    "dbt_test_gold",
    "dbt_run_place_mart",
    "dbt_test_place_mart",
)
DBT_RETRY_DELAY = timedelta(minutes=2)
DOMAIN = "weather"
WEATHER_DISCORD_WEBHOOK_ENV = "WEATHER_DISCORD_WEBHOOK_URL"
DISCORD_RED = 15158332
DEFAULT_PARAMS = {
    "target": Param(
        default="dev",
        type="string",
        enum=["dev"],
        description="dbt target profile name (dev only until production rollout).",
    )
}
# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
# dbt transform 은 외부 소스 API 를 호출하지 않으므로 source_system 은 생략한다.
record_weather_problem = problem_failure_callback(domain="weather")


def discord_report_date(context) -> str:
    logical_date = context.get("logical_date")
    if logical_date:
        return logical_date.astimezone(KST).strftime("%Y-%m-%d")
    return datetime.now(KST).strftime("%Y-%m-%d")


def short_text(value: object, limit: int = 130) -> str:
    text = str(value or "N/A")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def transform_stage_name(task_id: str) -> str:
    if "deps" in task_id:
        return "dbt 패키지 설치"
    if "freshness" in task_id:
        return "소스 신선도 검사"
    if "common_admin_dong_dimension" in task_id:
        return "공용 행정동 차원 실행/검증"
    if "seed" in task_id:
        return "시드 적재/검증"
    if "place_mart" in task_id:
        return "place mart run/test"
    if "silver" in task_id:
        return "silver run/test"
    if "gold" in task_id:
        return "gold run/test"
    return "알 수 없음"


def send_weather_discord(title: str, description: str, color: int, footer: str) -> None:
    webhook_url = (os.environ.get(WEATHER_DISCORD_WEBHOOK_ENV) or "").strip()
    if not webhook_url:
        LOGGER.info("[weather notify:noop] %s (webhook url not configured)", title)
        return
    payload = {
        "embeds": [{
            "title": title,
            "description": description[:4096],
            "color": color,
            "footer": {"text": footer[:2048]},
        }]
    }
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": "ask-seoul-airflow/1.0"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception as exc:
        LOGGER.warning("[weather notify] Discord send failed: %s", type(exc).__name__)


def notify_weather_transform_failure(context) -> None:
    # 브론즈 수집 DAG 실패 알림과 같은 채널(WEATHER_DISCORD_WEBHOOK_URL) — 미설정이면 no-op.
    ti = context.get("ti") or context.get("task_instance")
    task_id = getattr(ti, "task_id", "N/A")
    exc = context.get("exception")
    run_id = context.get("run_id", "N/A")
    target = (context.get("params") or {}).get("target", "N/A")
    send_weather_discord(
        f"기상청 transform 실패 - {discord_report_date(context)} (target={target})",
        "\n".join(
            [
                "❌ 변환 상태: 실패",
                f"❌ 실패 단계: {transform_stage_name(task_id)}",
                f"❌ 실패 task: `{task_id}`",
                f"❌ 오류 유형: `{type(exc).__name__ if exc else 'N/A'}`",
                "",
                f"Airflow 로그: {getattr(ti, 'log_url', 'N/A')}",
            ]
        ),
        DISCORD_RED,
        f"dag_id={context['dag'].dag_id} · run_id={short_text(run_id, 180)}",
    )


def transform_schedule() -> str | list[Asset] | None:
    if "ASK_SEOUL_WEATHER_TRANSFORM_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_WEATHER_TRANSFORM_DAG_SCHEDULE"] or None
    return [Asset(WEATHER_BRONZE_ASSET)]


def _artifact_path(*, run_id: str | None, task_id: str | None, try_number: int | None) -> str:
    def safe(value: str | None) -> str:
        return "".join(
            char if char.isascii() and (char.isalnum() or char in "._=-") else "-"
            for char in value or "unknown"
        )

    return str(
        PurePosixPath(DBT_PROJECT.replace("\\", "/"))
        / "target"
        / "weather-transform"
        / safe(run_id)
        / safe(task_id)
        / f"try{try_number if try_number is not None else 'unknown'}"
        / "run_results.json"
    )


def run_dbt_phase(
    *, dbt_args: str, include_project_vars: bool = True, **context
) -> dict[str, str | None]:
    """Run one dbt phase with an artifact path isolated to this task attempt."""
    ti = context["ti"]
    artifact_path = _artifact_path(
        run_id=context.get("run_id"),
        task_id=getattr(ti, "task_id", None),
        try_number=getattr(ti, "try_number", None),
    )
    command_args = shlex.split(dbt_args)
    is_deps = bool(command_args) and command_args[0] == "deps"
    target = (context.get("params") or {}).get("target", "dev")
    command = [
        DBT_BIN,
        *command_args,
        "--target",
        target,
        "--no-use-colors",
    ]
    if include_project_vars and not is_deps:
        command.extend(
            [
                "--vars",
                json.dumps(WEATHER_DBT_CONTRACT_VARS, separators=(",", ":")),
            ]
        )
    if not is_deps:
        command.extend(["--target-path", str(Path(artifact_path).parent)])

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = DBT_PROJECT
    env["DBT_PROJECT_DIR"] = DBT_PROJECT
    artifact_reset_succeeded = False
    try:
        if not is_deps:
            try:
                os.remove(artifact_path)
            except FileNotFoundError:
                pass
            artifact_reset_succeeded = True
        completed = subprocess.run(
            command,
            cwd=DBT_PROJECT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        existing_artifact_path = (
            artifact_path
            if artifact_reset_succeeded and os.path.exists(artifact_path)
            else None
        )
        ti.xcom_push(
            key=WEATHER_DBT_ARTIFACT_XCOM_KEY,
            value=existing_artifact_path,
        )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode != 0:
        raise AirflowException(
            f"weather dbt command failed with exit code {completed.returncode}"
        )
    return {"status": "success", "artifact_path": existing_artifact_path}


def dbt_task(
    task_id: str, dbt_args: str, *, include_project_vars: bool = True
) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=run_dbt_phase,
        op_kwargs={
            "dbt_args": dbt_args,
            "include_project_vars": include_project_vars,
        },
        pool=TRINO_HEAVY_POOL,
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )


def _current_run_results_path(**context) -> str | None:
    """Return the latest existing dbt artifact recorded by this DAG run."""
    ti = context.get("ti") or context.get("task_instance")
    if ti is None:
        return None

    def existing_path(candidate: object) -> str | None:
        if not isinstance(candidate, (str, os.PathLike)):
            return None
        path = os.fspath(candidate)
        if not isinstance(path, str) or not os.path.exists(path):
            return None
        return path

    for task_id in reversed(DBT_PHASE_TASK_IDS):
        try:
            result = ti.xcom_pull(task_ids=task_id)
        except Exception:  # noqa: BLE001 - continue to earlier current-run phases
            result = None
        if isinstance(result, dict):
            path = existing_path(result.get("artifact_path"))
            if path is not None:
                return path

        try:
            failure_path = ti.xcom_pull(
                task_ids=task_id,
                key=WEATHER_DBT_ARTIFACT_XCOM_KEY,
            )
        except Exception:  # noqa: BLE001 - continue to earlier current-run phases
            failure_path = None
        path = existing_path(failure_path)
        if path is not None:
            return path
    return None


def publish_dbt_run_metrics(run_results_path: str | None = None, **context) -> dict:
    """Persist model/test run metrics without changing the dbt contract gate."""
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


with DAG(
    dag_id="weather_vilage_fcst_transform",
    description="Transform weather bronze -> silver/gold via dbt.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=transform_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": DBT_RETRY_DELAY},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "transform", "silver", "gold", "dbt"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "weather", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_weather_problem,
    )

    dbt_deps = dbt_task("dbt_deps", "deps", include_project_vars=False)

    dbt_source_freshness = dbt_task("dbt_source_freshness", "source freshness")

    dbt_seed_asac_axes = dbt_task("dbt_seed_asac_axes", "seed --select asac_axes")

    dbt_run_common_admin_dong_dimension = dbt_task(
        "dbt_run_common_admin_dong_dimension",
        "run --select asac_axes.dim_admin_dong",
    )

    dbt_test_common_admin_dong_dimension = dbt_task(
        "dbt_test_common_admin_dong_dimension",
        "test --select asac_axes.dim_admin_dong",
    )

    dbt_seed_place_mapping = dbt_task(
        "dbt_seed_place_mapping",
        "seed --select weather_place_grid_mapping",
    )

    dbt_test_place_mapping_seed = dbt_task(
        "dbt_test_place_mapping_seed",
        "test --select "
        "weather_place_grid_mapping "
        "assert_weather_place_grid_mapping_major_aliases "
        "assert_weather_place_grid_mapping_within_collected_grid_scope "
        "assert_weather_place_grid_mapping_alias_unique_except_allowed",
    )

    dbt_run_silver = dbt_task(
        "dbt_run_silver",
        "run --select silver_kma_vilage_fcst",
    )

    dbt_test_silver = dbt_task(
        "dbt_test_silver",
        "test --select "
        "silver_kma_vilage_fcst "
        "assert_silver_kma_vilage_fcst_grain_unique "
        "assert_silver_kma_vilage_fcst_grid_coverage "
        "assert_silver_kma_uses_publishable_runs "
        "assert_silver_kma_event_at_matches_forecast_at "
        "--exclude "
        "assert_gold_weather_counts_match_silver",
    )

    dbt_run_gold = dbt_task(
        "dbt_run_gold",
        "run --select gold_weather_forecast_summary",
    )

    dbt_test_gold = dbt_task(
        "dbt_test_gold",
        "test --select "
        "gold_weather_forecast_summary "
        "assert_gold_weather_counts_match_silver "
        "assert_gold_weather_row_counts_positive",
    )

    dbt_run_place_mart = dbt_task(
        "dbt_run_place_mart",
        "run --select "
        "dim_weather_place "
        "silver_weather_forecast_by_admin_dong "
        "gold_weather_forecast_by_place",
    )

    dbt_test_place_mart = dbt_task(
        "dbt_test_place_mart",
        "test --select "
        "dim_weather_place "
        "silver_weather_forecast_by_admin_dong "
        "gold_weather_forecast_by_place "
        "assert_silver_weather_admin_dong_grain_unique "
        "assert_silver_weather_admin_axis_consistent "
        "assert_silver_weather_admin_event_at_matches_forecast_at "
        "assert_gold_weather_forecast_by_place_grain_unique "
        "assert_gold_weather_forecast_by_place_major_coverage "
        "assert_gold_weather_forecast_by_place_admin_axis_consistent "
        "assert_dim_weather_place_admin_axis_consistent "
        "assert_gold_weather_forecast_by_place_event_at_matches_forecast_at "
        "assert_gold_weather_forecast_by_place_latest_silver_record",
    )

    publish_dbt_metrics = PythonOperator(
        task_id="publish_dbt_run_metrics",
        python_callable=publish_dbt_run_metrics,
        on_failure_callback=record_weather_problem,
    ).as_teardown(on_failure_fail_dagrun=False)

    (
        validate_runtime
        >> dbt_deps
        >> dbt_source_freshness
        >> dbt_seed_asac_axes
        >> dbt_run_common_admin_dong_dimension
        >> dbt_test_common_admin_dong_dimension
        >> dbt_seed_place_mapping
        >> dbt_test_place_mapping_seed
        >> dbt_run_silver
        >> dbt_test_silver
        >> dbt_run_gold
        >> dbt_test_gold
        >> dbt_run_place_mart
        >> dbt_test_place_mart
    )

    dbt_test_place_mart >> publish_dbt_metrics
