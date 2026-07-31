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
from common.runtime_guard import (  # noqa: E402
    TARGET_CHOICES,
    default_target,
    validate_dev_runtime,
)
from common.security import redact, sanitize_log_value  # noqa: E402
from weather_ingest.common.runtime import trino_cursor  # noqa: E402
from weather_ingest.run_manifest import (  # noqa: E402
    MANIFEST_TABLE,
    SOURCE_ID as KMA_SOURCE_ID,
)
from weather_ingest.w2_recovery import (  # noqa: E402
    CHECKPOINT_CONTRACT_VERSION,
    DbtPhase,
    LINEAGE_RUN_BUCKET_COUNT,
    RecoveryBaseline,
    RecoveryPins,
    RepairWindow,
    STAGED_RECOVERY_STATE,
    WEATHER_BRIDGE_TABLE,
    WEATHER_CANONICAL_GOLD_TABLE,
    WEATHER_SERVING_CURRENT_TABLE,
    WEATHER_SILVER_GRID_TABLE,
    WINNER_RUN_BUCKET_COUNT,
    YONGSIN_ADMIN_DONG_CODE,
    YONGSIN_NX,
    YONGSIN_NY,
    advance_staged_checkpoint,
    checkpoint_payload,
    complete_staged_window,
    completed_window_labels,
    final_phase,
    format_timestamp,
    lineage_phase,
    load_staged_checkpoint,
    preparation_dbt_vars,
    preparation_phase_plan,
    record_staged_known_gap,
    select_windows_with_publishable_anchors,
    split_repair_windows,
    staged_post_publish_bridge_phase,
    staged_checkpoint_payload,
    staged_final_phase,
    staged_lineage_phase,
    staged_preparation_phase,
    staged_publish_phase,
    staged_window_phase_plan,
    staged_winner_phase,
    window_phase_plan,
    window_dbt_vars,
    winner_phase,
)
import weather_dbt_execution as weather_dbt  # noqa: E402
from weather_dbt_failure import classify_weather_dbt_failure  # noqa: E402


LOGGER = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")
DAG_ID = "weather_w2_observation_recovery"
DBT_PIPELINE = "weather-w2-observation-recovery"
DBT_RETRY_DELAY = timedelta(minutes=2)
# Recovery runs on a dedicated one-slot lane so a long manual backfill cannot
# starve live weather ingestion. Canonical transform must remain paused because
# both DAGs can write the same W2 Gold relation.
TRINO_WEATHER_RECOVERY_HEAVY_POOL = "trino_weather_recovery_heavy"
CHECKPOINT_PREFIX = "ask_seoul.weather.w2_observation_recovery"
CHECKPOINT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
DEFAULT_PARAMS = {
    "target": Param(
        default=default_target(),
        type="string",
        enum=list(TARGET_CHOICES),
        description="dbt target profile name; defaults to the runtime env (#561).",
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
    "recovery_mode": Param(
        default="staged_gold_only",
        type="string",
        enum=["staged_gold_only", "bounded_reconcile"],
        description="Gold-only staged recovery v3 or retained legacy v2 orchestration.",
    ),
}
record_weather_problem = problem_failure_callback(domain="weather")

# ASAC-DAG#480: the W2 serving model requires the shared admin_dong crosswalk
# to be pinned to one Iceberg snapshot via this dbt var. Historical recovery
# resolves and passes the same pin so selector-owned dbt phases can compile.
ADMIN_DONG_CROSSWALK_PIN_VAR = "admin_dong_crosswalk_pin_snapshot_id"


class AdminDongCrosswalkSnapshotUnavailableError(RuntimeError):
    """The shared admin_dong crosswalk Iceberg table has no usable snapshot to pin."""


def resolve_admin_dong_crosswalk_snapshot_id() -> int:
    """Pin the shared admin_dong crosswalk seed to its latest Iceberg snapshot (#480).

    The latest snapshot includes the most recent bridge membership (e.g. Yongsin-dong
    added 2026-07-23), which is exactly what a historical backfill needs.
    """
    from weather_ingest.common.runtime import sql_identifier

    cursor, catalog, _ = trino_cursor()
    schema = sql_identifier(os.environ.get("COMMON_SCHEMA", "common"))
    table = sql_identifier("seoul_admin_dong_crosswalk")
    cursor.execute(
        "SELECT snapshot_id "
        f'FROM {catalog}.{schema}."{table}$snapshots" '
        "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
    )
    row = cursor.fetchone()
    try:
        raw_snapshot_id = row[0]
    except (TypeError, IndexError) as exc:
        raise AdminDongCrosswalkSnapshotUnavailableError(
            "admin_dong crosswalk Iceberg snapshot is unavailable"
        ) from exc
    if (
        isinstance(raw_snapshot_id, bool)
        or not isinstance(raw_snapshot_id, int)
        or raw_snapshot_id <= 0
    ):
        raise AdminDongCrosswalkSnapshotUnavailableError(
            "admin_dong crosswalk Iceberg snapshot ID must be a positive integer"
        )
    return raw_snapshot_id


class WeatherRecoverySnapshotUnavailableError(RuntimeError):
    """A required Weather Iceberg table has no usable snapshot."""


def _weather_dbt_schema() -> str:
    from weather_ingest.common.runtime import sql_identifier

    return sql_identifier(os.environ.get("WEATHER_SCHEMA", "weather"))


def resolve_weather_table_snapshot_id(table_name: str) -> int:
    from weather_ingest.common.runtime import sql_identifier

    cursor, catalog, _ = trino_cursor()
    schema = _weather_dbt_schema()
    table = sql_identifier(table_name)
    cursor.execute(
        "SELECT snapshot_id "
        f'FROM {catalog}.{schema}."{table}$snapshots" '
        "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
    )
    row = cursor.fetchone()
    try:
        snapshot_id = row[0]
    except (TypeError, IndexError) as exc:
        raise WeatherRecoverySnapshotUnavailableError(
            f"Weather Iceberg snapshot is unavailable: {table_name}"
        ) from exc
    if (
        isinstance(snapshot_id, bool)
        or not isinstance(snapshot_id, int)
        or snapshot_id <= 0
    ):
        raise WeatherRecoverySnapshotUnavailableError(
            f"Weather Iceberg snapshot ID must be positive: {table_name}"
        )
    return snapshot_id


def _gold_payload_checksum_expression() -> str:
    return """
        lower(to_hex(checksum(json_format(cast(row(
            cast(product_row_id as varchar),
            cast(admin_dong_code as varchar),
            cast(forecast_at as timestamp(6)),
            cast(category as varchar),
            cast(admin_dong as varchar),
            cast(gu_code as varchar),
            cast(gu as varchar),
            cast(admin_dong_revision_date as date),
            cast(bridge_version as varchar),
            cast(nx as integer),
            cast(ny as integer),
            cast(source_grid_place_id as varchar),
            cast(issued_at as timestamp(6)),
            cast(collected_at as timestamp(6)),
            cast(published_at as timestamp(6)),
            cast(fcst_value_raw as varchar),
            cast(fcst_value_num as double),
            cast(value_representation as varchar),
            cast(value_num as double),
            cast(value_lower_bound as double),
            cast(value_upper_bound as double),
            cast(qualitative_code as varchar),
            cast(forecast_lead_hours as bigint),
            cast(source_id as varchar),
            cast(dag_run_id as varchar),
            cast(raw_object_key as varchar),
            cast(request_id as varchar)
        ) as json)))))
    """.strip()


def _non_target_gold_fingerprint(
    *,
    snapshot_id: int | None = None,
) -> tuple[int, str]:
    from weather_ingest.common.runtime import sql_identifier

    cursor, catalog, _ = trino_cursor()
    schema = _weather_dbt_schema()
    table = sql_identifier(WEATHER_CANONICAL_GOLD_TABLE)
    version = f" FOR VERSION AS OF {snapshot_id}" if snapshot_id is not None else ""
    cursor.execute(
        "SELECT count(*), "
        f"{_gold_payload_checksum_expression()} "
        f'FROM {catalog}.{schema}."{table}"{version} '
        f"WHERE admin_dong_code <> '{YONGSIN_ADMIN_DONG_CODE}'"
    )
    row = cursor.fetchone()
    try:
        row_count = int(row[0])
        checksum = str(row[1])
    except (TypeError, ValueError, IndexError) as exc:
        raise WeatherRecoverySnapshotUnavailableError(
            "non-target Gold fingerprint is unavailable"
        ) from exc
    if row_count < 0 or not checksum or checksum == "None":
        raise WeatherRecoverySnapshotUnavailableError(
            "non-target Gold fingerprint is unusable"
        )
    return row_count, checksum


def resolve_staged_recovery_baseline(pins: RecoveryPins) -> RecoveryBaseline:
    from weather_ingest.common.runtime import sql_identifier

    non_target_row_count, non_target_checksum = _non_target_gold_fingerprint(
        snapshot_id=pins.gold_baseline_snapshot_id
    )
    cursor, catalog, _ = trino_cursor()
    schema = _weather_dbt_schema()
    table = sql_identifier(WEATHER_SILVER_GRID_TABLE)
    cursor.execute(
        "SELECT cast(max(published_at) as varchar) "
        f'FROM {catalog}.{schema}."{table}" '
        f"FOR VERSION AS OF {pins.silver_grid_snapshot_id} "
        f"WHERE nx = {YONGSIN_NX} AND ny = {YONGSIN_NY}"
    )
    row = cursor.fetchone()
    try:
        source_watermark = str(row[0])
    except (TypeError, IndexError) as exc:
        raise WeatherRecoverySnapshotUnavailableError(
            "Yongsin-dong Silver source watermark is unavailable"
        ) from exc
    if not source_watermark or source_watermark == "None":
        raise WeatherRecoverySnapshotUnavailableError(
            "Yongsin-dong Silver source watermark is unusable"
        )
    return RecoveryBaseline(
        non_target_row_count=non_target_row_count,
        non_target_checksum=non_target_checksum,
        source_watermark=source_watermark,
    )


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
    stored_version = (
        stored.get("contract_version") if isinstance(stored, dict) else None
    )
    if stored and stored_version != CHECKPOINT_CONTRACT_VERSION:
        LOGGER.warning(
            "[weather-w2-recovery] invalidating legacy checkpoint name=%s "
            "expected_contract_version=%s stored_contract_version=%r",
            variable_name,
            CHECKPOINT_CONTRACT_VERSION,
            stored_version,
        )
    return completed_window_labels(stored, windows)


def _save_checkpoint(variable_name: str, windows, completed: set[str]) -> None:
    Variable.set(
        variable_name,
        checkpoint_payload(windows, completed_labels=sorted(completed)),
        serialize_json=True,
    )


def _save_staged_checkpoint(variable_name: str, payload: dict[str, object]) -> None:
    Variable.set(variable_name, payload, serialize_json=True)


def _load_or_initialize_staged_checkpoint(
    *,
    variable_name: str,
    checkpoint_id: str,
    windows: list[RepairWindow],
) -> dict[str, object]:
    stored = Variable.get(variable_name, default=None, deserialize_json=True)
    if stored is not None:
        if not isinstance(stored, dict):
            raise ValueError("staged checkpoint must be a JSON object")
        load_staged_checkpoint(stored, windows, checkpoint_id=checkpoint_id)
        return stored

    pins = RecoveryPins(
        admin_dong_crosswalk_snapshot_id=resolve_admin_dong_crosswalk_snapshot_id(),
        weather_bridge_snapshot_id=resolve_weather_table_snapshot_id(
            WEATHER_BRIDGE_TABLE
        ),
        silver_grid_snapshot_id=resolve_weather_table_snapshot_id(
            WEATHER_SILVER_GRID_TABLE
        ),
        gold_baseline_snapshot_id=resolve_weather_table_snapshot_id(
            WEATHER_CANONICAL_GOLD_TABLE
        ),
    )
    anchor_window_indexes = publishable_window_indexes(windows)
    selected_windows = select_windows_with_publishable_anchors(
        windows,
        anchor_window_indexes,
    )
    if not selected_windows:
        raise ValueError("requested repair range has no publishable manifest anchors")
    payload = staged_checkpoint_payload(
        windows,
        selected_labels=[window.label for window in selected_windows],
        completed_labels=[],
        checkpoint_id=checkpoint_id,
        pins=pins,
        baseline=resolve_staged_recovery_baseline(pins),
    )
    _save_staged_checkpoint(variable_name, payload)
    return payload


def _staged_dbt_vars(
    window: RepairWindow,
    payload: dict[str, object],
) -> dict[str, object]:
    pins = payload["pins"]
    if not isinstance(pins, dict):
        raise ValueError("staged checkpoint pins must be a JSON object")
    return {
        **window_dbt_vars(window),
        "weather_w2_recovery_checkpoint_id": payload["checkpoint_id"],
        "weather_w2_recovery_target_admin_dong_code": YONGSIN_ADMIN_DONG_CODE,
        ADMIN_DONG_CROSSWALK_PIN_VAR: pins[
            "admin_dong_crosswalk_snapshot_id"
        ],
        "weather_w2_bridge_pin_snapshot_id": pins[
            "weather_bridge_snapshot_id"
        ],
        "weather_w2_silver_grid_pin_snapshot_id": pins[
            "silver_grid_snapshot_id"
        ],
    }


def verify_non_target_baseline(payload: dict[str, object]) -> None:
    baseline = payload.get("baseline")
    if not isinstance(baseline, dict):
        raise AirflowFailException("staged checkpoint baseline is missing")
    actual_row_count, actual_checksum = _non_target_gold_fingerprint()
    if (
        actual_row_count != baseline.get("non_target_row_count")
        or actual_checksum != baseline.get("non_target_checksum")
    ):
        raise AirflowFailException(
            "non-target Gold fingerprint changed during staged recovery"
        )


def verify_staged_recovery_result(payload: dict[str, object]) -> dict[str, int]:
    from weather_ingest.common.runtime import sql_identifier

    verify_non_target_baseline(payload)
    cursor, catalog, _ = trino_cursor()
    schema = _weather_dbt_schema()
    gold_table = sql_identifier(WEATHER_CANONICAL_GOLD_TABLE)
    serving_table = sql_identifier(WEATHER_SERVING_CURRENT_TABLE)
    cursor.execute(
        "SELECT "
        f"(SELECT count(*) FROM {catalog}.{schema}.\"{gold_table}\" "
        f"WHERE admin_dong_code = '{YONGSIN_ADMIN_DONG_CODE}'), "
        f"(SELECT count(*) FROM {catalog}.{schema}.\"{serving_table}\" "
        f"WHERE admin_dong_code = '{YONGSIN_ADMIN_DONG_CODE}')"
    )
    row = cursor.fetchone()
    try:
        gold_row_count = int(row[0])
        serving_row_count = int(row[1])
    except (TypeError, ValueError, IndexError) as exc:
        raise AirflowFailException(
            "Yongsin-dong recovery verification summary is unavailable"
        ) from exc
    if gold_row_count <= 0:
        raise AirflowFailException("Canonical Gold does not contain Yongsin-dong")
    if serving_row_count <= 0:
        raise AirflowFailException(
            "Serving projection does not contain Yongsin-dong"
        )
    return {
        "yongsin_gold_rows": gold_row_count,
        "yongsin_serving_rows": serving_row_count,
    }


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


def staged_window_source_row_count(
    window: RepairWindow,
    payload: dict[str, object],
) -> int:
    """Count target-grid rows that can join a publishable run at the pinned snapshot."""
    from weather_ingest.common.runtime import sql_identifier

    pins = payload.get("pins")
    if not isinstance(pins, dict):
        raise AirflowFailException("staged checkpoint pins are missing")
    snapshot_id = pins.get("silver_grid_snapshot_id")
    if (
        isinstance(snapshot_id, bool)
        or not isinstance(snapshot_id, int)
        or snapshot_id <= 0
    ):
        raise AirflowFailException("staged Silver grid snapshot pin is invalid")

    cursor, catalog, manifest_schema = trino_cursor()
    weather_schema = _weather_dbt_schema()
    manifest_table = sql_identifier(MANIFEST_TABLE)
    silver_grid_table = sql_identifier(WEATHER_SILVER_GRID_TABLE)
    start_at = format_timestamp(window.start_at)
    cutoff_at = format_timestamp(window.cutoff_at)
    cursor.execute(
        f"""
        with manifest_events as (
            select
                cast(source_id as varchar) as source_id,
                cast(dag_run_id as varchar) as dag_run_id,
                cast(dag_id as varchar) as dag_id,
                cast(status as varchar) as manifest_status,
                cast(is_publishable as boolean) as is_publishable,
                cast(event_at as timestamp(6)) + interval '9' hour as event_at
            from {catalog}.{manifest_schema}."{manifest_table}"
            where cast(source_id as varchar) = '{KMA_SOURCE_ID}'
        ),
        manifest_state_ranked as (
            select
                manifest_events.*,
                row_number() over (
                    partition by source_id, dag_run_id
                    order by event_at desc, dag_id desc
                ) as manifest_row_num
            from manifest_events
            where event_at <= timestamp '{cutoff_at}'
        ),
        eligible_manifest_anchors as (
            select source_id, dag_run_id
            from manifest_state_ranked
            where manifest_row_num = 1
              and manifest_status = 'SUCCESS'
              and is_publishable
              and event_at >= timestamp '{start_at}'
              and event_at <= timestamp '{cutoff_at}'
        )
        select count(*)
        from {catalog}.{weather_schema}."{silver_grid_table}"
             FOR VERSION AS OF {snapshot_id} as grid
        inner join eligible_manifest_anchors as anchor
            on cast(grid.source_id as varchar) = anchor.source_id
           and cast(grid.selected_dag_run_id as varchar) = anchor.dag_run_id
        where cast(grid.published_at as timestamp(6))
              >= timestamp '{start_at}'
          and cast(grid.published_at as timestamp(6))
              <= timestamp '{cutoff_at}'
          and grid.nx = {YONGSIN_NX}
          and grid.ny = {YONGSIN_NY}
        """
    )
    row = cursor.fetchone()
    try:
        row_count = int(row[0])
    except (TypeError, ValueError, IndexError) as exc:
        raise AirflowFailException(
            "staged recovery source-row preflight did not return a count"
        ) from exc
    if row_count < 0:
        raise AirflowFailException(
            "staged recovery source-row preflight returned a negative count"
        )
    return row_count


def _recover_bounded_observation_windows(**context) -> dict[str, object]:
    params = context["params"]
    target = str(params["target"])
    if target != "dev":
        raise ValueError("Weather W2 historical recovery is dev-only")

    try:
        crosswalk_pin_id = resolve_admin_dong_crosswalk_snapshot_id()
    except AdminDongCrosswalkSnapshotUnavailableError as exc:
        raise AirflowFailException(str(exc)) from exc
    crosswalk_vars = {ADMIN_DONG_CROSSWALK_PIN_VAR: crosswalk_pin_id}

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
    preparation_variables = {
        **preparation_dbt_vars(selected_windows[0], selected_windows[-1]),
        **crosswalk_vars,
    }
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
        variables = {**window_dbt_vars(window), **crosswalk_vars}
        LOGGER.info("[weather-w2-recovery] recover window=%s", window.label)
        for phase in window_phase_plan(window_index):
            execute_recovery_phase(
                phase,
                target=target,
                variables=variables,
                context=context,
            )
        for winner_bucket_index in range(WINNER_RUN_BUCKET_COUNT):
            winner_variables = {
                **variables,
                "weather_w2_winner_bucket_count": str(WINNER_RUN_BUCKET_COUNT),
                "weather_w2_winner_bucket_index": str(winner_bucket_index),
            }
            execute_recovery_phase(
                winner_phase(window_index, winner_bucket_index),
                target=target,
                variables=winner_variables,
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


def _recover_staged_observation_windows(**context) -> dict[str, object]:
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

    checkpoint_id = str(params["checkpoint_id"])
    variable_name = checkpoint_variable_name(checkpoint_id)
    try:
        payload = _load_or_initialize_staged_checkpoint(
            variable_name=variable_name,
            checkpoint_id=checkpoint_id,
            windows=windows,
        )
    except (
        AdminDongCrosswalkSnapshotUnavailableError,
        WeatherRecoverySnapshotUnavailableError,
    ) as exc:
        raise AirflowFailException(str(exc)) from exc

    checkpoint = load_staged_checkpoint(
        payload,
        windows,
        checkpoint_id=checkpoint_id,
    )
    if checkpoint.state == "verified":
        return {
            "checkpoint_variable": variable_name,
            "state": "verified",
            "completed_windows": len(checkpoint.completed_labels),
            "selected_windows": len(checkpoint.selected_labels),
            "known_gap_windows": len(checkpoint.known_gap_labels),
        }

    execute_recovery_phase(
        staged_preparation_phase(),
        target=target,
        variables={},
        context=context,
    )
    window_by_label = {window.label: window for window in windows}
    window_index_by_label = {
        window.label: index for index, window in enumerate(windows)
    }

    if checkpoint.state == STAGED_RECOVERY_STATE:
        for label in checkpoint.selected_labels:
            if label in checkpoint.completed_labels or label in checkpoint.known_gap_labels:
                LOGGER.info(
                    "[weather-w2-recovery] staged checkpoint skip window=%s",
                    label,
                )
                continue
            window = window_by_label[label]
            source_row_count = staged_window_source_row_count(window, payload)
            if source_row_count == 0:
                LOGGER.warning(
                    "[weather-w2-recovery] pinned source gap window=%s "
                    "silver_grid_snapshot_id=%s",
                    label,
                    payload["pins"]["silver_grid_snapshot_id"],
                )
                payload = record_staged_known_gap(
                    payload,
                    label,
                    reason="no_target_source_rows_at_pinned_snapshot",
                )
                _save_staged_checkpoint(variable_name, payload)
                continue
            variables = _staged_dbt_vars(window, payload)
            for phase in staged_window_phase_plan(window_index_by_label[label]):
                execute_recovery_phase(
                    phase,
                    target=target,
                    variables=variables,
                    context=context,
                )
            payload = complete_staged_window(payload, label)
            _save_staged_checkpoint(variable_name, payload)

        full_window = RepairWindow(
            start_at=windows[0].start_at,
            cutoff_at=windows[-1].cutoff_at,
        )
        final_variables = _staged_dbt_vars(full_window, payload)
        execute_recovery_phase(
            staged_final_phase(),
            target=target,
            variables=final_variables,
            context=context,
        )
        for bucket_index in range(WINNER_RUN_BUCKET_COUNT):
            execute_recovery_phase(
                staged_winner_phase(bucket_index),
                target=target,
                variables={
                    **final_variables,
                    "weather_w2_winner_bucket_count": str(
                        WINNER_RUN_BUCKET_COUNT
                    ),
                    "weather_w2_winner_bucket_index": str(bucket_index),
                },
                context=context,
            )
        for bucket_index in range(LINEAGE_RUN_BUCKET_COUNT):
            execute_recovery_phase(
                staged_lineage_phase(bucket_index),
                target=target,
                variables={
                    **final_variables,
                    "weather_w2_lineage_run_bucket_count": str(
                        LINEAGE_RUN_BUCKET_COUNT
                    ),
                    "weather_w2_lineage_run_bucket_index": str(bucket_index),
                },
                context=context,
            )
        verify_non_target_baseline(payload)
        payload = advance_staged_checkpoint(
            payload,
            next_state="prepublish_validated",
        )
        _save_staged_checkpoint(variable_name, payload)
        checkpoint = load_staged_checkpoint(
            payload,
            windows,
            checkpoint_id=checkpoint_id,
        )

    full_window = RepairWindow(
        start_at=windows[0].start_at,
        cutoff_at=windows[-1].cutoff_at,
    )
    final_variables = _staged_dbt_vars(full_window, payload)
    if checkpoint.state == "prepublish_validated":
        execute_recovery_phase(
            staged_publish_phase(),
            target=target,
            variables=final_variables,
            context=context,
        )
        payload = advance_staged_checkpoint(payload, next_state="published")
        _save_staged_checkpoint(variable_name, payload)
        checkpoint = load_staged_checkpoint(
            payload,
            windows,
            checkpoint_id=checkpoint_id,
        )

    if checkpoint.state == "published":
        execute_recovery_phase(
            staged_post_publish_bridge_phase(),
            target=target,
            variables=final_variables,
            context=context,
        )
        result_summary = verify_staged_recovery_result(payload)
        payload = advance_staged_checkpoint(payload, next_state="verified")
        _save_staged_checkpoint(variable_name, payload)
    else:
        result_summary = {}

    return {
        "checkpoint_variable": variable_name,
        "state": payload["state"],
        "completed_windows": len(payload["completed_windows"]),
        "selected_windows": len(payload["selected_windows"]),
        "known_gap_windows": len(payload.get("known_gaps", [])),
        **result_summary,
    }


def recover_observation_windows(**context) -> dict[str, object]:
    recovery_mode = str(
        context["params"].get("recovery_mode", "bounded_reconcile")
    )
    if recovery_mode == "staged_gold_only":
        return _recover_staged_observation_windows(**context)
    if recovery_mode == "bounded_reconcile":
        return _recover_bounded_observation_windows(**context)
    raise ValueError(f"unsupported recovery_mode: {recovery_mode}")


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
        pool=TRINO_WEATHER_RECOVERY_HEAVY_POOL,
        pool_slots=1,
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        on_failure_callback=record_weather_problem,
    )

    validate_runtime >> recover_windows
