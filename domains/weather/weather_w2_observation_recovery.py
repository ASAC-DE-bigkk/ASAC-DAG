"""Manual, resumable Weather W2 observation recovery for historical gaps."""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
DAGS_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(DAG_DIR)))
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from common.security import redact, sanitize_log_value  # noqa: E402
from weather_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402
from weather_ingest.common.runtime import trino_cursor  # noqa: E402
from weather_ingest.run_manifest import (  # noqa: E402
    MANIFEST_TABLE,
    SOURCE_ID as KMA_SOURCE_ID,
)
from weather_ingest.w2_recovery import (  # noqa: E402
    DbtPhase,
    LINEAGE_RUN_BUCKET_COUNT,
    checkpoint_payload,
    completed_window_labels,
    final_phase,
    format_timestamp,
    lineage_phase,
    preparation_dbt_vars,
    preparation_phase_plan,
    select_windows_with_publishable_anchors,
    split_repair_windows,
    window_phase_plan,
    window_dbt_vars,
)
import weather_dbt_execution as weather_dbt  # noqa: E402
from weather_dbt_failure import classify_weather_dbt_failure  # noqa: E402


LOGGER = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")
DAG_ID = "weather_w2_observation_recovery"
DBT_PIPELINE = "weather-w2-observation-recovery"
DBT_RETRY_DELAY = timedelta(minutes=2)
CHECKPOINT_PREFIX = "ask_seoul.weather.w2_observation_recovery"
CHECKPOINT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
DEFAULT_PARAMS = {
    "target": Param(
        default="dev",
        type="string",
        enum=["dev"],
        description="dbt target profile name. Historical recovery is dev-only.",
    ),
    "repair_start_at": Param(
        default="2026-07-02 00:00:00.000000",
        type="string",
        description="Inclusive KST start for the historical W2 repair.",
    ),
    "repair_cutoff_at": Param(
        default="2026-07-14 23:59:59.999999",
        type="string",
        description="Inclusive KST cutoff for the historical W2 repair.",
    ),
    "checkpoint_id": Param(
        default="issue-196",
        type="string",
        description="Stable recovery checkpoint identity used to resume the same range.",
    ),
}
record_weather_problem = problem_failure_callback(domain="weather")


def checkpoint_variable_name(checkpoint_id: str) -> str:
    if not CHECKPOINT_ID_PATTERN.fullmatch(checkpoint_id):
        raise ValueError(
            "checkpoint_id must be 1-80 letters, numbers, dot, dash, or underscore"
        )
    return f"{CHECKPOINT_PREFIX}.{checkpoint_id}"


def _safe_execution_output(execution) -> str:
    return "\n".join(
        str(redact(value))
        for attempt in execution.attempts
        for value in (attempt.stdout, attempt.stderr)
        if value
    )


def execute_recovery_phase(
    phase: DbtPhase,
    *,
    target: str,
    variables: dict[str, str],
    context: dict[str, object],
) -> None:
    ti = context.get("ti")
    execution = weather_dbt.execute_dbt_phase(
        dbt_command=phase.command,
        selector=phase.selector,
        threads=1,
        invocation_id=phase.invocation_id,
        pipeline=DBT_PIPELINE,
        run_id=context.get("run_id"),
        task_id=getattr(ti, "task_id", None),
        try_number=getattr(ti, "try_number", None),
        target=target,
        variables=json.dumps(variables, separators=(",", ":")),
    )
    safe_output = _safe_execution_output(execution)
    if safe_output:
        LOGGER.info(
            "[weather-w2-recovery] dbt output=%s",
            sanitize_log_value(safe_output, max_len=16_000),
        )
    completed = execution.completed
    if completed.returncode == 0 and not execution.missing_expected_artifacts:
        return

    failure = classify_weather_dbt_failure(
        dbt_command=phase.command,
        returncode=int(completed.returncode),
        artifact_path=execution.existing_run_results_path,
        missing_expected_artifacts=execution.missing_expected_artifacts,
        command_output=safe_output,
    )
    exception_type = AirflowException if failure.retryable else AirflowFailException
    raise exception_type(
        "weather W2 recovery dbt phase failed: "
        f"classification={failure.classification}; "
        f"exit_code={completed.returncode}"
    )


def _checkpoint_for_windows(variable_name: str, windows) -> set[str]:
    stored = Variable.get(variable_name, default=None, deserialize_json=True)
    return completed_window_labels(stored, windows)


def _save_checkpoint(variable_name: str, windows, completed: set[str]) -> None:
    Variable.set(
        variable_name,
        checkpoint_payload(windows, completed_labels=sorted(completed)),
        serialize_json=True,
    )


def publishable_window_indexes(windows) -> set[int]:
    values = ",\n        ".join(
        "({index}, timestamp '{start_at}', timestamp '{cutoff_at}')".format(
            index=index,
            start_at=format_timestamp(window.start_at),
            cutoff_at=format_timestamp(window.cutoff_at),
        )
        for index, window in enumerate(windows)
    )
    cursor, catalog, schema = trino_cursor()
    manifest_table = f"{catalog}.{schema}.{MANIFEST_TABLE}"
    cursor.execute(
        f"""
        with requested_windows(window_index, start_at, cutoff_at) as (
            values
                {values}
        ),
        manifest_events as (
            select
                cast(source_id as varchar) as source_id,
                cast(dag_run_id as varchar) as dag_run_id,
                cast(dag_id as varchar) as dag_id,
                cast(status as varchar) as manifest_status,
                cast(is_publishable as boolean) as is_publishable,
                cast(event_at as timestamp(6)) + interval '9' hour as event_at
            from {manifest_table}
            where cast(source_id as varchar) = '{KMA_SOURCE_ID}'
        ),
        manifest_before_cutoff as (
            select requested_windows.*, manifest_events.*
            from requested_windows
            inner join manifest_events
                on manifest_events.event_at <= requested_windows.cutoff_at
        ),
        manifest_state_ranked as (
            select
                manifest_before_cutoff.*,
                row_number() over (
                    partition by window_index, source_id, dag_run_id
                    order by event_at desc, dag_id desc
                ) as manifest_row_num
            from manifest_before_cutoff
        )
        select distinct window_index
        from manifest_state_ranked
        where manifest_row_num = 1
          and manifest_status = 'SUCCESS'
          and is_publishable
          and event_at >= start_at
          and event_at <= cutoff_at
        """
    )
    return {int(row[0]) for row in cursor.fetchall()}


def recover_observation_windows(**context) -> dict[str, object]:
    params = context["params"]
    target = str(params["target"])
    if target != "dev":
        raise ValueError("Weather W2 historical recovery is dev-only")

    windows = split_repair_windows(
        str(params["repair_start_at"]),
        str(params["repair_cutoff_at"]),
    )
    now_kst = datetime.now(KST).replace(tzinfo=None)
    if windows[-1].cutoff_at > now_kst:
        raise ValueError("repair_cutoff_at must not be in the future")

    anchor_window_indexes = publishable_window_indexes(windows)
    selected_windows = select_windows_with_publishable_anchors(
        windows, anchor_window_indexes
    )
    if not selected_windows:
        raise ValueError("requested repair range has no publishable manifest anchors")

    variable_name = checkpoint_variable_name(str(params["checkpoint_id"]))
    completed = _checkpoint_for_windows(variable_name, windows)
    preparation_variables = preparation_dbt_vars(
        selected_windows[0], selected_windows[-1]
    )
    for phase in preparation_phase_plan():
        execute_recovery_phase(
            phase,
            target=target,
            variables=preparation_variables,
            context=context,
        )

    window_indexes = {window.label: index for index, window in enumerate(windows)}
    for window in selected_windows:
        if window.label in completed:
            LOGGER.info("[weather-w2-recovery] checkpoint skip window=%s", window.label)
            continue
        window_index = window_indexes[window.label]
        variables = window_dbt_vars(window)
        LOGGER.info("[weather-w2-recovery] recover window=%s", window.label)
        for phase in window_phase_plan(window_index):
            execute_recovery_phase(
                phase,
                target=target,
                variables=variables,
                context=context,
            )
        for lineage_bucket_index in range(LINEAGE_RUN_BUCKET_COUNT):
            lineage_variables = {
                **variables,
                "weather_w2_lineage_run_bucket_count": str(LINEAGE_RUN_BUCKET_COUNT),
                "weather_w2_lineage_run_bucket_index": str(lineage_bucket_index),
            }
            execute_recovery_phase(
                lineage_phase(window_index, lineage_bucket_index),
                target=target,
                variables=lineage_variables,
                context=context,
            )
        completed.add(window.label)
        _save_checkpoint(variable_name, windows, completed)

    execute_recovery_phase(
        final_phase(),
        target=target,
        variables=preparation_variables,
        context=context,
    )
    return {
        "checkpoint_variable": variable_name,
        "completed_windows": len(
            completed.intersection({window.label for window in selected_windows})
        ),
        "total_windows": len(windows),
        "selected_windows": len(selected_windows),
        "skipped_windows": len(windows) - len(selected_windows),
    }


with DAG(
    dag_id=DAG_ID,
    description="Recover Weather W2 historical observations in resumable <=6h KST windows.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "dbt", "recovery", "w2", "manual"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "weather", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_weather_problem,
    )

    recover_windows = PythonOperator(
        task_id="recover_observation_windows",
        python_callable=recover_observation_windows,
        pool=TRINO_HEAVY_POOL,
        pool_slots=1,
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        on_failure_callback=record_weather_problem,
    )

    validate_runtime >> recover_windows
