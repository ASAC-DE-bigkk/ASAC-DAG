import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any


TRAFFIC_SILVER_RELATION = "iceberg_dev.traffic.silver_seoul_traffic_incident"
_RELATION_CATALOG, _RELATION_SCHEMA, _RELATION_NAME = TRAFFIC_SILVER_RELATION.split(
    ".", 2
)
_RELATION_NAMESPACE = f"{_RELATION_CATALOG}.{_RELATION_SCHEMA}"
_SNAPSHOTS_RELATION = f'{_RELATION_NAMESPACE}."{_RELATION_NAME}$snapshots"'
_FILES_RELATION = f'{_RELATION_NAMESPACE}."{_RELATION_NAME}$files"'
_ALLOWED_OPERATIONS = frozenset({"append", "overwrite", "replace", "delete"})


class SnapshotFenceTelemetryError(RuntimeError):
    """Iceberg metadata could not be converted into reliable fence evidence."""


class ExternalCompactionRace(RuntimeError):
    """An external rewrite changed Traffic Silver during a guarded phase."""


def _is_timezone_aware_iso_timestamp(value: object) -> bool:
    if (
        not isinstance(value, str)
        or len(value) <= 10
        or value[10] not in {"T", " "}
        or value != value.strip()
    ):
        return False
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


@dataclass(frozen=True)
class SilverSnapshotEvidence:
    snapshot_id: int
    committed_at: str
    operation: str
    compacted_files: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "committed_at": self.committed_at,
            "operation": self.operation,
            "compacted_files": list(self.compacted_files),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "SilverSnapshotEvidence":
        required = {"snapshot_id", "committed_at", "operation", "compacted_files"}
        if not isinstance(value, Mapping) or set(value) != required:
            raise SnapshotFenceTelemetryError("snapshot evidence fields are invalid")

        snapshot_id = value["snapshot_id"]
        committed_at = value["committed_at"]
        operation = value["operation"]
        compacted_files = value["compacted_files"]
        if (
            not isinstance(snapshot_id, int)
            or isinstance(snapshot_id, bool)
            or snapshot_id <= 0
        ):
            raise SnapshotFenceTelemetryError("snapshot id is invalid")
        if not _is_timezone_aware_iso_timestamp(committed_at):
            raise SnapshotFenceTelemetryError("snapshot commit time is invalid")
        if not isinstance(operation, str) or operation not in _ALLOWED_OPERATIONS:
            raise SnapshotFenceTelemetryError("snapshot operation is invalid")
        if not isinstance(compacted_files, list) or any(
            not isinstance(path, str) or not path for path in compacted_files
        ):
            raise SnapshotFenceTelemetryError("compacted file list is invalid")

        return cls(
            snapshot_id=snapshot_id,
            committed_at=committed_at,
            operation=operation,
            compacted_files=tuple(sorted(compacted_files)),
        )


def _trino_connection() -> Any:
    import trino.dbapi

    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=_RELATION_CATALOG,
        schema=_RELATION_SCHEMA,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )


def _snapshot_fields(row: object) -> tuple[object, object, object]:
    if not isinstance(row, (tuple, list)) or len(row) != 3:
        raise SnapshotFenceTelemetryError("latest snapshot telemetry is invalid")
    return row[0], row[1], row[2]


def _file_path(row: object) -> str:
    if not isinstance(row, (tuple, list)) or len(row) != 1:
        raise SnapshotFenceTelemetryError("compacted file telemetry is invalid")
    path = row[0]
    if not isinstance(path, str) or not path:
        raise SnapshotFenceTelemetryError("compacted file telemetry is invalid")
    return path


def collect_silver_snapshot_evidence(
    connection_factory: Callable[[], Any] = _trino_connection,
) -> SilverSnapshotEvidence:
    """Read the fixed dev Traffic Silver metadata relations and close DB resources."""
    connection = connection_factory()
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT snapshot_id, committed_at, operation "
            f"FROM {_SNAPSHOTS_RELATION} "
            "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
        )
        snapshot_id, committed_at, operation = _snapshot_fields(cursor.fetchone())
        if isinstance(committed_at, datetime):
            committed_at = str(committed_at)

        cursor.execute(
            "SELECT file_path "
            f"FROM {_FILES_RELATION} "
            "WHERE content = 0 "
            "AND regexp_like(file_path, '(^|/)compacted-[^/]*$') "
            "ORDER BY file_path"
        )
        compacted_files = [_file_path(row) for row in cursor.fetchall()]
        return SilverSnapshotEvidence.from_dict(
            {
                "snapshot_id": snapshot_id,
                "committed_at": committed_at,
                "operation": operation,
                "compacted_files": compacted_files,
            }
        )
    finally:
        try:
            if cursor is not None:
                cursor.close()
        finally:
            connection.close()


def assert_safe_post_write(
    baseline: SilverSnapshotEvidence,
    current: SilverSnapshotEvidence,
) -> None:
    new_compacted = set(current.compacted_files) - set(baseline.compacted_files)
    if new_compacted:
        raise ExternalCompactionRace("new managed-compaction file appeared")
    if current.snapshot_id != baseline.snapshot_id and current.operation == "replace":
        raise ExternalCompactionRace("unexpected replace snapshot after Silver MERGE")


def assert_snapshot_unchanged(
    expected: SilverSnapshotEvidence,
    current: SilverSnapshotEvidence,
) -> None:
    if current != expected:
        raise ExternalCompactionRace("snapshot changed after Silver MERGE")
