"""Common D1 Publisher orchestration.

Runs the Serving Contract v1 Publication as one unit per product:
  Contract Load → Publication Gate → D1 Write → row-count verify → _catalog Upsert
  → API Smoke Test, recording runtime metadata and protecting the last-known-good
  snapshot on failure.

Pure w.r.t. runtime: it depends only on the ``SourceReader`` / ``D1Client`` /
``SmokeTester`` seams, so the whole pipeline is unit-tested with in-memory fakes —
no Trino, no Cloudflare, no prod. The real seams live in ``runtime.py``.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol, Sequence

from common.serving import gate as gatelib
from common.serving.contract import ServingContract
from common.serving.d1_client import Column, D1Client, sqlite_type
from common.serving.gate import (
    STATUS_DEGRADED,
    STATUS_FAILED,
    STATUS_PUBLISHED,
    STATUS_SKIPPED,
    GateDecision,
)


@dataclass
class ReadPlan:
    """What the source will publish this run for one product."""

    columns: list[Column]
    rows: list[dict[str, Any]]
    delete_column: str | None = None  # append: clear this window before insert
    delete_literal: str | None = None


class SourceReader(Protocol):
    def read(self, contract: ServingContract, last_good_max: Any | None) -> ReadPlan: ...


class SmokeTester(Protocol):
    def check(self, model_name: str) -> str: ...


@dataclass
class ProductRecord:
    product_id: str
    model_name: str
    publication_id: str
    source_run_id: str
    published_at: str
    serving_status: str
    reason: str
    source_row_count: int = 0
    published_row_count: int = 0
    d1_row_count: int = 0
    distinct_primary_key_count: int = 0
    null_primary_key_count: int = 0
    published_bytes: int = 0
    freshness: str | None = None
    api_smoke_status: str = "not_evaluated"
    stage: str = "initialized"
    rollback_status: str = "not_needed"


@dataclass
class PublicationReport:
    records: list[ProductRecord] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


class PublicationError(RuntimeError):
    """Raised when any product fails; ``report`` carries per-product metadata."""

    def __init__(self, report: PublicationReport) -> None:
        self.report = report
        super().__init__("; ".join(report.failures))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _freshness(contract: ServingContract, rows: Sequence[dict[str, Any]]) -> str | None:
    if not contract.event_time or not rows:
        return None
    values = [row.get(contract.event_time) for row in rows if row.get(contract.event_time) is not None]
    return str(max(values)) if values else None


def _catalog_row(contract: ServingContract, columns: Sequence[Column], record: ProductRecord) -> dict[str, Any]:
    return {
        "name": contract.model_name,
        "product_id": contract.product_id,
        "external": 1 if contract.external else 0,
        "description": contract.description,
        "product_question": contract.product_question,
        "public_gold": (
            json.dumps(contract.public_gold, ensure_ascii=False, sort_keys=True)
            if contract.public_gold is not None
            else None
        ),
        "mcp_projection": (
            json.dumps(contract.mcp_projection, ensure_ascii=False, sort_keys=True)
            if contract.mcp_projection is not None
            else None
        ),
        "tests": json.dumps(list(contract.tests), ensure_ascii=False),
        "time_axis": contract.event_time,
        "columns": json.dumps([{"name": c, "type": t} for c, t in columns], ensure_ascii=False),
        "row_count": record.published_row_count,
        "serving_status": record.serving_status,
        "publication_id": record.publication_id,
        "source_run_id": record.source_run_id,
        "published_bytes": record.published_bytes,
        "freshness": record.freshness,
        "exported_at": record.published_at,
    }


def _product_meta_rows(
    contract: ServingContract,
    columns: Sequence[Column],
    record: ProductRecord,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """핸드오프 메타 3종(#638 §2.2) 행 — 계약 선언(dbt yml→manifest)에서 그대로 나온다.

    타입은 D1 실물과 같은 SQLite 타입으로 싣고(commerce 관행 동형), 컬럼 설명이 없는
    도메인은 description_ko=NULL 로 컬럼 행 자체는 게시한다(이름·타입·게시본 대조는 유효).
    """
    descriptions = contract.column_descriptions or {}
    columns_rows = [
        {
            "product_id": contract.product_id,
            "table_name": contract.model_name,
            "ordinal": ordinal,
            "column_name": name,
            "type": sqlite_type(trino_type),
            "description_ko": descriptions.get(name) or None,
            "publication_id": record.publication_id,
        }
        for ordinal, (name, trino_type) in enumerate(columns)
    ]
    ext_rows = [
        {
            "product_id": contract.product_id,
            "table_name": contract.model_name,
            "source_model": contract.model_name,
            "grain": contract.grain,
            "primary_key": json.dumps(list(contract.primary_key), ensure_ascii=False),
            "time_axis": contract.event_time,
            "tier": contract.serving_tier,
            "rollup_rule": contract.rollup_rule,
            "publication_id": record.publication_id,
        }
    ]
    pattern_rows = [
        {
            "product_id": contract.product_id,
            "pattern_id": pattern.get("pattern_id"),
            "question_ko": pattern.get("question_ko"),
            "sql": pattern.get("sql"),
            "axes": pattern.get("axes"),
            "requires": json.dumps(pattern.get("requires") or [], ensure_ascii=False),
            "verified_rows": pattern.get("verified_rows"),
            "verified_at": pattern.get("verified_at"),
            "verified_publication_id": pattern.get("verified_publication_id"),
            "allow_empty": 1 if pattern.get("allow_empty") else 0,
            "insight_sample_ko": pattern.get("insight_sample_ko"),
            "publication_id": record.publication_id,
        }
        for pattern in contract.usage_patterns
        if pattern.get("pattern_id") and pattern.get("sql")
        # 한 모델→다제품 선언(commerce geo_grid 관행)과의 동형성: d1_table 명시 시 해당 제품만.
        and pattern.get("d1_table", contract.model_name) == contract.model_name
    ]
    return columns_rows, ext_rows, pattern_rows


def _primary_key_stats(rows: Sequence[dict[str, Any]], primary_key: Sequence[str]) -> tuple[int, int, int]:
    if not primary_key:
        raise ValueError("primary_key is required for publication")
    values = [tuple(row.get(column) for column in primary_key) for row in rows]
    null_count = sum(1 for value in values if any(part is None for part in value))
    return len(rows), len(set(values)), null_count


def _uses_replace_lifecycle(contract: ServingContract) -> bool:
    return contract.publication_mode == "snapshot" or (
        contract.publication_mode == "upsert" and contract.upsert_strategy == "exact_set"
    )


def _write(
    d1: D1Client,
    contract: ServingContract,
    plan: ReadPlan,
    rows: Sequence[dict[str, Any]],
) -> tuple[int, int, int]:
    """Write ``rows`` per publication_mode and return physical D1 PK statistics."""
    mode = contract.publication_mode
    if _uses_replace_lifecycle(contract):
        d1.replace_table(contract.model_name, plan.columns, rows, contract.primary_key)  # staging swap protects last-good
        return d1.primary_key_stats(contract.model_name, contract.primary_key)
    d1.ensure_table(contract.model_name, plan.columns, contract.primary_key)
    if mode == "append":
        if plan.delete_column and plan.delete_literal is not None:
            d1.delete_where_gte(contract.model_name, plan.delete_column, plan.delete_literal)
        d1.insert_rows(contract.model_name, plan.columns, rows, replace=False)
    elif mode == "upsert":
        d1.insert_rows(contract.model_name, plan.columns, rows, replace=True)
    else:
        raise ValueError(f"unknown publication_mode: {mode!r}")
    return d1.primary_key_stats(contract.model_name, contract.primary_key)


def _ledger_row(record: ProductRecord, *, outcome: str) -> dict[str, Any]:
    return {
        "publication_id": record.publication_id,
        "product_id": record.product_id,
        "model_name": record.model_name,
        "source_run_id": record.source_run_id,
        "attempted_at": record.published_at,
        "outcome": outcome,
        "stage": record.stage,
        "source_row_count": record.source_row_count,
        "published_row_count": record.published_row_count,
        "d1_row_count": record.d1_row_count,
        "api_smoke_status": record.api_smoke_status,
        "rollback_status": record.rollback_status,
        "reason": record.reason,
    }


def _append_ledger(d1: D1Client, record: ProductRecord, *, outcome: str) -> None:
    try:
        d1.append_publication_ledger(_ledger_row(record, outcome=outcome))
    except Exception as exc:  # noqa: BLE001 -- ledger failures must surface in the primary task
        raise RuntimeError(f"publication ledger 기록 실패: {type(exc).__name__}: {exc}") from exc


def _restore_snapshot(
    d1: D1Client,
    record: ProductRecord,
    *,
    previous_catalog: dict[str, Any] | None,
    catalog_committed: bool,
) -> None:
    """Restore an activated snapshot and, only if needed, its old catalog row."""

    try:
        d1.restore_replaced_table(record.model_name)
        if catalog_committed:
            if previous_catalog is None:
                d1.delete_catalog_row(record.model_name)
            else:
                d1.upsert_catalog([previous_catalog])
        record.rollback_status = "restored"
    except Exception as exc:  # noqa: BLE001 -- retain the original failure plus compensation failure
        record.rollback_status = f"restore_failed:{type(exc).__name__}"


def _fail_after_write(
    d1: D1Client,
    report: PublicationReport,
    record: ProductRecord,
    contract: ServingContract,
    *,
    message: str,
    previous_catalog: dict[str, Any] | None,
    catalog_committed: bool = False,
) -> None:
    record.serving_status = STATUS_FAILED
    record.reason = message
    if _uses_replace_lifecycle(contract):
        _restore_snapshot(
            d1,
            record,
            previous_catalog=previous_catalog,
            catalog_committed=catalog_committed,
        )
    report.failures.append(f"{contract.model_name}: {message}")
    _append_ledger(d1, record, outcome="failed")
    report.records.append(record)


def publish(
    contracts: Sequence[ServingContract],
    source: SourceReader,
    d1: D1Client,
    smoke: SmokeTester,
    *,
    source_run_id: str,
) -> PublicationReport:
    """Publish each contract as one Publication unit. Raises ``PublicationError`` if any fails."""
    report = PublicationReport()
    for contract in contracts:
        record = ProductRecord(
            product_id=contract.product_id,
            model_name=contract.model_name,
            publication_id=uuid.uuid4().hex,
            source_run_id=source_run_id,
            published_at=_now_iso(),
            serving_status=STATUS_FAILED,
            reason="",
        )

        last_good_count = None
        catalog = d1.catalog_row(contract.model_name)
        if catalog and catalog.get("row_count") is not None:
            last_good_count = int(catalog["row_count"])
        last_good_max = (
            d1.table_max(contract.model_name, contract.event_time)
            if contract.event_time and contract.publication_mode == "append"
            else None
        )

        plan = source.read(contract, last_good_max)
        record.source_row_count = len(plan.rows)
        decision = gatelib.evaluate_gate(contract, len(plan.rows), last_good_count)
        record.reason = decision.reason

        if decision.decision == GateDecision.FAIL:
            record.serving_status = STATUS_FAILED
            record.stage = "gate"
            report.failures.append(f"{contract.model_name}: {decision.reason}")
            _append_ledger(d1, record, outcome="failed")
            report.records.append(record)
            continue

        if decision.decision == GateDecision.SKIP_RETAIN:
            record.serving_status = STATUS_SKIPPED
            record.published_row_count = last_good_count or 0  # last-known-good untouched
            record.stage = "gate"
            _append_ledger(d1, record, outcome="skipped_retained")
            report.records.append(record)
            continue

        rows, degraded = gatelib.apply_reliability(contract, plan.rows)
        record.source_row_count = len(rows)
        source_row_count, source_distinct_count, source_null_count = _primary_key_stats(rows, contract.primary_key)
        if source_row_count != source_distinct_count or source_null_count:
            record.serving_status = STATUS_FAILED
            record.stage = "source_primary_key"
            record.reason = (
                "source primary key validation failed: "
                f"rows={source_row_count} distinct={source_distinct_count} null={source_null_count}"
            )
            report.failures.append(f"{contract.model_name}: {record.reason}")
            _append_ledger(d1, record, outcome="failed")
            report.records.append(record)
            continue
        try:
            record.stage = "write"
            d1_row_count, distinct_primary_key_count, null_primary_key_count = _write(d1, contract, plan, rows)
        except Exception as exc:  # noqa: BLE001 -- record + continue; snapshot last-good is intact
            record.serving_status = STATUS_FAILED
            record.stage = "write"
            record.reason = f"write 실패: {type(exc).__name__}: {exc}"
            report.failures.append(f"{contract.model_name}: {record.reason}")
            _append_ledger(d1, record, outcome="failed")
            report.records.append(record)
            continue

        record.d1_row_count = d1_row_count
        record.distinct_primary_key_count = distinct_primary_key_count
        record.null_primary_key_count = null_primary_key_count
        if (
            d1_row_count != distinct_primary_key_count
            or null_primary_key_count != 0
            or (
                contract.publication_mode in {"snapshot", "upsert"}
                and d1_row_count != source_row_count
            )
        ):
            record.stage = "read_back"
            _fail_after_write(d1, report, record, contract, message=(
                "D1 primary key read-back validation failed: "
                f"source={source_row_count} rows={d1_row_count} "
                f"distinct={distinct_primary_key_count} null={null_primary_key_count}"
            ), previous_catalog=catalog)
            continue

        record.published_row_count = d1_row_count
        record.published_bytes = len(json.dumps(rows, ensure_ascii=False, default=str).encode("utf-8"))
        record.freshness = _freshness(contract, rows)
        record.serving_status = (
            STATUS_DEGRADED if (degraded or decision.serving_status == STATUS_DEGRADED) else STATUS_PUBLISHED
        )
        if contract.external:
            record.stage = "api_smoke"
            record.api_smoke_status = smoke.check(contract.model_name)
            if record.api_smoke_status == "failed":
                _fail_after_write(
                    d1,
                    report,
                    record,
                    contract,
                    message="API smoke test 실패",
                    previous_catalog=catalog,
                )
                continue
            elif record.api_smoke_status not in {"passed", "not_evaluated"}:
                _fail_after_write(
                    d1,
                    report,
                    record,
                    contract,
                    message=f"invalid API smoke status={record.api_smoke_status!r}",
                    previous_catalog=catalog,
                )
                continue

        catalog_committed = False
        try:
            record.stage = "catalog"
            d1.upsert_catalog([_catalog_row(contract, plan.columns, record)])
            catalog_committed = True
            registered_count = d1.catalog_domain_count({contract.model_name})
            if registered_count != 1:
                raise RuntimeError("_catalog 자기검증 실패: 등록 누락 가능")
            # 핸드오프 메타(#638) — 같은 try 안이라 실패 시 스냅샷·_catalog 가 함께 복원된다.
            # 메타 행 자체는 보상하지 않는다(#638 §3 — 제품 단위 신·구 혼재 허용, 타 제품 무영향).
            record.stage = "product_meta"
            columns_rows, ext_rows, pattern_rows = _product_meta_rows(contract, plan.columns, record)
            d1.publish_product_meta(
                contract.product_id, record.publication_id, columns_rows, ext_rows, pattern_rows
            )
        except Exception as exc:  # noqa: BLE001 -- restore snapshot after any post-write catalog/meta failure
            _fail_after_write(
                d1,
                report,
                record,
                contract,
                message=f"{record.stage} 실패: {type(exc).__name__}: {exc}",
                previous_catalog=catalog,
                catalog_committed=catalog_committed,
            )
            continue

        if _uses_replace_lifecycle(contract):
            d1.finalize_replaced_table(contract.model_name)
        record.stage = "completed"
        _append_ledger(d1, record, outcome=record.serving_status)
        report.records.append(record)

    if report.failures:
        raise PublicationError(report)
    return report
