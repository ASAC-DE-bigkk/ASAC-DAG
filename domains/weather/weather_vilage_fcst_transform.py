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
import sys
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
DAGS_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.assets import WEATHER_BRONZE_ASSET  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402


LOGGER = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/weather"
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


def dbt_command(args: str) -> str:
    project = shlex.quote(DBT_PROJECT)
    return (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"{shlex.quote(DBT_BIN)} {args} --target '{{{{ params.target }}}}' --no-use-colors"
    )


with DAG(
    dag_id="weather_vilage_fcst_transform",
    description="Transform weather bronze -> silver/gold via dbt.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=transform_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "transform", "silver", "gold", "dbt"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "weather", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_weather_problem,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_command("deps"),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=dbt_command("source freshness"),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

    dbt_seed_asac_axes = BashOperator(
        task_id="dbt_seed_asac_axes",
        bash_command=dbt_command("seed --select asac_axes"),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

    dbt_run_common_admin_dong_dimension = BashOperator(
        task_id="dbt_run_common_admin_dong_dimension",
        bash_command=dbt_command("run --select asac_axes.dim_admin_dong"),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

    dbt_test_common_admin_dong_dimension = BashOperator(
        task_id="dbt_test_common_admin_dong_dimension",
        bash_command=dbt_command("test --select asac_axes.dim_admin_dong"),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

    dbt_seed_place_mapping = BashOperator(
        task_id="dbt_seed_place_mapping",
        bash_command=dbt_command("seed --select weather_place_grid_mapping"),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

    dbt_test_place_mapping_seed = BashOperator(
        task_id="dbt_test_place_mapping_seed",
        bash_command=dbt_command(
            "test --select "
            "weather_place_grid_mapping "
            "assert_weather_place_grid_mapping_major_aliases "
            "assert_weather_place_grid_mapping_within_collected_grid_scope "
            "assert_weather_place_grid_mapping_alias_unique_except_allowed"
        ),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=dbt_command("run --select silver_kma_vilage_fcst"),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=dbt_command(
            "test --select "
            "silver_kma_vilage_fcst "
            "assert_silver_kma_vilage_fcst_grain_unique "
            "assert_silver_kma_vilage_fcst_grid_coverage "
            "assert_silver_kma_uses_publishable_runs "
            "assert_silver_kma_event_at_matches_forecast_at "
            "--exclude "
            "assert_gold_weather_counts_match_silver"
        ),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=dbt_command("run --select gold_weather_forecast_summary"),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=dbt_command(
            "test --select "
            "gold_weather_forecast_summary "
            "assert_gold_weather_counts_match_silver "
            "assert_gold_weather_row_counts_positive"
        ),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

    dbt_run_place_mart = BashOperator(
        task_id="dbt_run_place_mart",
        bash_command=dbt_command(
            "run --select "
            "dim_weather_place "
            "silver_weather_forecast_by_admin_dong "
            "gold_weather_forecast_by_place"
        ),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

    dbt_test_place_mart = BashOperator(
        task_id="dbt_test_place_mart",
        bash_command=dbt_command(
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
            "assert_gold_weather_forecast_by_place_latest_silver_record"
        ),
        on_failure_callback=[notify_weather_transform_failure, record_weather_problem],
    )

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
