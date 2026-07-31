"""Traffic dbt failure classification for retry and recovery decisions."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from common.security import redact, refresh_env_secrets


@dataclass(frozen=True)
class DbtFailure:
    """The actionable result of one unsuccessful dbt invocation."""

    classification: str
    retryable: bool
    failed_test_names: list[str]
    failed_row_count: int
    artifact_path: str | None


_TRINO_OR_ADAPTER_MARKERS = (
    "trino",
    "adapter",
)
_INFRASTRUCTURE_MARKERS = (
    "temporary failure in name resolution",
    "name or service not known",
    "getaddrinfo",
    "timed out",
    "timeout",
    "connection refused",
    "connection reset",
    "connection aborted",
)
_CATALOG_AVAILABILITY_MARKERS = (
    "iceberg_catalog_error",
    "failed to list tables",
    "nohttpresponseexception",
)
# 공용 asac_axes seed(예: seoul_admin_dong_crosswalk)는 weather/traffic transform마다
# 재시딩되는 계약 게이트다. weather와 traffic의 recurring transform이 서로 다른
# 스케줄로 같은 공용 축을 동시에 재시딩/읽을 수 있어, 한쪽이 seed를 재적재하는 짧은
# 창에 다른 쪽이 dim_admin_dong 같은 공용 view를 읽다 걸릴 수 있다(ASAC-DAG#480).
# 재시딩 주기 자체는 seed drift 계약 검증이라 건드리지 않고, 이 특정 transient
# 신호만 재시도 대상으로 인식한다.
_SHARED_AXIS_REBUILD_RACE_MARKERS = (
    "invalid_view",
    "does not exist",
)
_UNSAFE_SEGMENT_CHARS = re.compile(r"[^A-Za-z0-9._=-]")


def load_dbt_results(path: str | Path) -> list[dict[str, Any]]:
    """Read dbt's result array, treating a missing/malformed artifact as empty."""
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return []
    results = document.get("results") if isinstance(document, dict) else None
    return (
        [result for result in results if isinstance(result, dict)]
        if isinstance(results, list)
        else []
    )


def silver_persisted_from_results(
    results: Iterable[dict[str, Any]],
    *,
    selected_unique_ids: Iterable[str],
    default: bool,
) -> bool:
    """Report persistence for any model selected by this exact invocation."""
    selected_models = {
        str(unique_id)
        for unique_id in selected_unique_ids
        if str(unique_id).startswith("model.")
    }
    for result in results:
        unique_id = str(result.get("unique_id") or "")
        if unique_id in selected_models and str(result.get("status") or "").lower() in {
            "success",
            "pass",
        }:
            return True
    return default


def classify_dbt_failure(
    *,
    returncode: int,
    results: Iterable[dict[str, Any]],
    artifact_path: str | None,
    command_output: str = "",
) -> DbtFailure:
    """Classify dbt's result artifact without retrying a data-contract failure."""
    failed_test_names: list[str] = []
    failed_row_count = 0
    messages = [command_output]
    has_non_test_error = False

    for result in results:
        status = str(result.get("status") or "").lower()
        unique_id = str(result.get("unique_id") or "")
        messages.append(str(result.get("message") or ""))
        messages.append(str(result.get("adapter_response") or ""))
        if status == "fail" and unique_id.startswith("test."):
            failed_test_names.append(unique_id.rsplit(".", 1)[-1])
            failures = result.get("failures")
            if isinstance(failures, int) and failures > 0:
                failed_row_count += failures
        elif status in {"error", "fail"}:
            has_non_test_error = True

    if failed_test_names and not has_non_test_error:
        return DbtFailure(
            classification="data-contract-violation",
            retryable=False,
            failed_test_names=failed_test_names,
            failed_row_count=failed_row_count,
            artifact_path=artifact_path,
        )

    detail = "\n".join(messages).lower()
    has_trino_or_adapter_error = any(marker in detail for marker in _TRINO_OR_ADAPTER_MARKERS)
    has_infrastructure_error = any(marker in detail for marker in _INFRASTRUCTURE_MARKERS)
    has_catalog_availability_error = all(
        marker in detail for marker in _CATALOG_AVAILABILITY_MARKERS
    )
    has_shared_axis_rebuild_race = all(
        marker in detail for marker in _SHARED_AXIS_REBUILD_RACE_MARKERS
    )
    if has_trino_or_adapter_error and (
        has_infrastructure_error
        or has_catalog_availability_error
        or has_shared_axis_rebuild_race
    ):
        return DbtFailure(
            classification="retryable-infrastructure-error",
            retryable=True,
            failed_test_names=[],
            failed_row_count=0,
            artifact_path=artifact_path,
        )

    return DbtFailure(
        classification="model-execution-failed",
        retryable=False,
        failed_test_names=[],
        failed_row_count=0,
        artifact_path=artifact_path,
    )


def build_recovery_record(
    failure: DbtFailure,
    *,
    traffic_snapshot_dag_run_id: str | None,
    dag_id: str | None,
    task_id: str | None,
    run_id: str | None,
    try_number: int | None,
    silver_persisted: bool,
    occurred_at: datetime,
    traffic_flow_snapshot_dag_run_id: str | None = None,
    traffic_citydata_crowding_snapshot_id: int | None = None,
) -> dict[str, Any]:
    """Build the durable operator record for a failed, pinned dbt invocation."""
    return {
        "schema_version": "v1",
        "occurred_at": occurred_at.astimezone(timezone.utc).isoformat(),
        "domain": "traffic",
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "try_number": try_number,
        "traffic_snapshot_dag_run_id": traffic_snapshot_dag_run_id,
        "traffic_flow_snapshot_dag_run_id": traffic_flow_snapshot_dag_run_id,
        "traffic_citydata_crowding_snapshot_id": traffic_citydata_crowding_snapshot_id,
        "failure_classification": failure.classification,
        "retryable": failure.retryable,
        "dbt_test_names": failure.failed_test_names,
        "dbt_failed_row_count": failure.failed_row_count,
        "dbt_artifact_path": failure.artifact_path,
        "silver_persisted": silver_persisted,
        "recovery_action": (
            "retry-same-snapshot" if failure.retryable else "manual-approval-required"
        ),
    }


def build_failure_notification(record: dict[str, Any]) -> tuple[str, str, str]:
    """Create the compact Discord payload operators need to choose a recovery path."""
    failed_tests = ", ".join(record.get("dbt_test_names") or []) or "n/a"
    description = "\n".join(
        (
            f"**snapshot**: `{record.get('traffic_snapshot_dag_run_id') or 'unknown'}`",
            f"**flow snapshot**: `{record.get('traffic_flow_snapshot_dag_run_id') or 'unknown'}`",
            f"**citydata snapshot**: `{record.get('traffic_citydata_crowding_snapshot_id') or 'unknown'}`",
            f"**dbt test**: `{failed_tests}`",
            f"**failed rows**: {record.get('dbt_failed_row_count', 0)}",
            f"**artifact**: `{record.get('dbt_artifact_path') or 'unknown'}`",
            f"**silver persisted**: {'yes' if record.get('silver_persisted') else 'no'}",
            f"**recovery**: {record.get('recovery_action') or 'unknown'}",
        )
    )
    title = f"🚨 traffic dbt {record.get('failure_classification') or 'failure'}"
    footer = f"run_id={record.get('run_id') or 'unknown'} · task={record.get('task_id') or 'unknown'}"
    return title, description, footer


def _safe_segment(value: Any) -> str:
    if value is None or value == "":
        return "unknown"
    return _UNSAFE_SEGMENT_CHARS.sub("-", str(value))


# ops 존 관측 카테고리(ASK-Seoul#60) — ops/recovery/<domain>/observed_date=…/…
# 구경로(루트 recovery/)는 신규 쓰기 중단. 이 sink 는 write-only 라(코드베이스에
# reader 0) dual-read 가 필요 없다 — errors(#573)·metrics(#573)·reports(#579) 와 동일.
RECOVERY_PREFIX = os.environ.get("ASAC_RECOVERY_PREFIX", "ops/recovery")


class R2RecoveryRecordSink:
    """Store a per-failure recovery record beside the existing R2 Problem objects."""

    def __init__(
        self,
        *,
        prefix: str = RECOVERY_PREFIX,
        put_object: Callable[[str, bytes], None] | None = None,
    ) -> None:
        self.prefix = prefix
        self._put_object = put_object

    def object_key(self, record: dict[str, Any]) -> str:
        occurred = datetime.fromisoformat(str(record["occurred_at"])).astimezone(
            timezone.utc
        )
        date = occurred.date().isoformat()
        # 도메인이 카테고리 바로 다음의 bare 세그먼트(#60 A 구조) — 날짜는 그 아래.
        return (
            f"{self.prefix}/traffic/observed_date={date}"
            f"/dag_id={_safe_segment(record.get('dag_id'))}"
            f"/{_safe_segment(record.get('run_id'))}"
            f"__{_safe_segment(record.get('task_id'))}"
            f"__try{record.get('try_number') if record.get('try_number') is not None else 'unknown'}.json"
        )

    def write(self, record: dict[str, Any]) -> str:
        object_key = self.object_key(record)
        refresh_env_secrets()
        payload = json.dumps(redact(record), ensure_ascii=False, sort_keys=True).encode(
            "utf-8"
        )
        if self._put_object is not None:
            self._put_object(object_key, payload)
        else:
            self._put_r2_object(object_key, payload)
        return object_key

    @staticmethod
    def _put_r2_object(object_key: str, payload: bytes) -> None:
        import boto3

        from traffic_ingest.common.runtime import r2_env

        boto3.client(
            "s3",
            endpoint_url=r2_env("R2_ENDPOINT"),
            aws_access_key_id=r2_env("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=r2_env("R2_SECRET_ACCESS_KEY"),
            region_name="auto",
        ).put_object(
            Bucket=r2_env("R2_BUCKET_NAME"),
            Key=object_key,
            Body=payload,
            ContentType="application/json; charset=utf-8",
        )
