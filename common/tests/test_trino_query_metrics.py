"""Trino 비용 대리 지표 수집 단위 테스트.

실제 Trino/네트워크 없이 System connector의 컬럼 차이와 cursor.stats 폴백을
검증한다. 수집기는 조회 전용이며, 누락한 값은 0이 아니라 ``None``이어야 한다.
"""
from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.trino_query_metrics import (  # noqa: E402
    TelemetryCursor,
    collect_iceberg_fingerprint,
    collect_query_metrics,
    query_id_from_cursor,
)


class FakeWorkCursor:
    def __init__(self, *, stats=None, one=None, many=None):
        self.stats = stats or {}
        self.one = one
        self.many = [] if many is None else many
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.many


class FakeMetadataCursor:
    def __init__(self, *, describe_rows=None, result_row=None, rows=None):
        self.describe_rows = describe_rows or []
        self.result_row = result_row
        self.rows = list(rows or [])
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)

    def fetchall(self):
        if self.statements[-1] == "DESCRIBE system.runtime.queries":
            return self.describe_rows
        return []

    def fetchone(self):
        statement = self.statements[-1]
        if "system.runtime.queries" in statement:
            return self.result_row
        return self.rows.pop(0) if self.rows else None


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


def test_collect_query_metrics_enriches_limited_system_row_with_cursor_stats():
    cursor = FakeMetadataCursor(
        describe_rows=[("query_id", "varchar"), ("state", "varchar")],
        result_row=("q_limited", "FINISHED"),
    )

    result = collect_query_metrics(
        cursor,
        "q_limited",
        fallback_stats={
            "state": "FAILED",
            "queuedTimeMillis": 3,
            "analysisTimeMillis": 4,
            "cpuTimeMillis": 5,
            "elapsedTimeMillis": 6,
            "wallTimeMillis": 0,
            "peakMemoryBytes": 7,
            "processedRows": 8,
            "processedBytes": 9,
            "physicalInputBytes": 10,
            "physicalWrittenBytes": 11,
            "spilledBytes": 12,
        },
    )

    assert result["metric_source"] == "system.runtime.queries+cursor.stats"
    assert result["state"] == "FINISHED"
    assert result["queued_time_ms"] == 3
    assert result["analysis_time_ms"] == 4
    assert result["cpu_time_ms"] == 5
    assert result["wall_time_ms"] == 6
    assert result["peak_user_memory_bytes"] == 7
    assert result["input_rows"] == 8
    assert result["input_bytes"] == 9
    assert result["physical_input_bytes"] == 10
    assert result["physical_written_bytes"] == 11
    assert result["spilled_bytes"] == 12
    assert "distributed_planning_time_ms" in result["unavailable_metrics"]


def test_collect_query_metrics_marks_missing_history_as_unavailable_not_zero():
    cursor = FakeMetadataCursor(describe_rows=[("query_id", "varchar")], result_row=None)

    result = collect_query_metrics(cursor, "q_missing", fallback_stats={"processedBytes": 22})

    assert result["metric_source"] == "cursor.stats"
    assert result["input_bytes"] == 22
    assert result["physical_input_bytes"] is None
    assert "query_not_found_in_system_runtime" in result["unavailable_reasons"]


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


def test_telemetry_cursor_records_metrics_after_fetchall():
    workload = FakeWorkCursor(
        stats={"queryId": "q_wrapped", "processedBytes": 99},
        many=[("result",)],
    )
    metadata = FakeMetadataCursor(
        describe_rows=[("query_id", "varchar"), ("input_bytes", "bigint")],
        result_row=("q_wrapped", 101),
    )
    cursor = TelemetryCursor(workload, metadata)

    cursor.execute("SELECT 1")

    assert cursor.fetchall() == [("result",)]
    assert workload.statements == ["SELECT 1"]
    assert cursor.records == [
        {
            **{name: None for name in (
                "state", "queued_time_ms", "analysis_time_ms", "distributed_planning_time_ms",
                "cpu_time_ms", "wall_time_ms", "peak_user_memory_bytes", "input_rows",
                "output_rows", "output_bytes", "physical_input_bytes", "physical_written_bytes",
                "spilled_bytes",
            )},
            "query_id": "q_wrapped",
            "input_bytes": 101,
            "metric_source": "system.runtime.queries",
            "unavailable_metrics": [
                "state", "queued_time_ms", "analysis_time_ms", "distributed_planning_time_ms",
                "cpu_time_ms", "wall_time_ms", "peak_user_memory_bytes", "input_rows",
                "output_rows", "output_bytes", "physical_input_bytes", "physical_written_bytes",
                "spilled_bytes",
            ],
            "unavailable_reasons": [],
        }
    ]


def test_telemetry_cursor_records_missing_query_id_without_guessing():
    cursor = TelemetryCursor(FakeWorkCursor(one=("result",)), FakeMetadataCursor())

    cursor.execute("SELECT 1")

    assert cursor.fetchone() == ("result",)
    assert cursor.records[0]["metric_source"] == "unavailable"
    assert cursor.records[0]["query_id"] is None
    assert cursor.records[0]["unavailable_reasons"] == ["missing_query_id"]
