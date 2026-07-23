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

from domains.common.serving import gate as gatelib
from domains.common.serving.contract import ServingContract
from domains.common.serving.d1_client import Column, D1Client
from domains.common.serving.gate import (
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
    def check(self, model_name: str) -> bool: ...


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
    published_bytes: int = 0
    freshness: str | None = None


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


def _write(d1: D1Client, contract: ServingContract, plan: ReadPlan, rows: Sequence[dict[str, Any]]) -> int:
    """Write ``rows`` per publication_mode; return the published row count."""
    mode = contract.publication_mode
    if mode == "snapshot":
        d1.replace_table(contract.model_name, plan.columns, rows)  # staging swap protects last-good
        return len(rows)
    d1.ensure_table(contract.model_name, plan.columns)
    if mode == "append":
        if plan.delete_column and plan.delete_literal is not None:
            d1.delete_where_gte(contract.model_name, plan.delete_column, plan.delete_literal)
        d1.insert_rows(contract.model_name, plan.columns, rows, replace=False)
    elif mode == "upsert":
        d1.insert_rows(contract.model_name, plan.columns, rows, replace=True)
    else:
        raise ValueError(f"unknown publication_mode: {mode!r}")
    return d1.table_row_count(contract.model_name)


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
    registered: dict[str, dict[str, Any]] = {}

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
            report.failures.append(f"{contract.model_name}: {decision.reason}")
            report.records.append(record)
            continue

        if decision.decision == GateDecision.SKIP_RETAIN:
            record.serving_status = STATUS_SKIPPED
            record.published_row_count = last_good_count or 0  # last-known-good untouched
            report.records.append(record)
            continue

        rows, degraded = gatelib.apply_reliability(contract, plan.rows)
        try:
            written = _write(d1, contract, plan, rows)
        except Exception as exc:  # noqa: BLE001 -- record + continue; snapshot last-good is intact
            record.serving_status = STATUS_FAILED
            record.reason = f"write 실패: {type(exc).__name__}: {exc}"
            report.failures.append(f"{contract.model_name}: {record.reason}")
            report.records.append(record)
            continue

        record.published_row_count = written
        record.published_bytes = len(json.dumps(rows, ensure_ascii=False, default=str).encode("utf-8"))
        record.freshness = _freshness(contract, rows)
        record.serving_status = (
            STATUS_DEGRADED if (degraded or decision.serving_status == STATUS_DEGRADED) else STATUS_PUBLISHED
        )
        registered[contract.model_name] = _catalog_row(contract, plan.columns, record)
        report.records.append(record)

    # _catalog upsert (self-domain, published/degraded only) + registration self-check (#477 ③).
    if registered:
        d1.upsert_catalog(list(registered.values()))
        registered_count = d1.catalog_domain_count(set(registered))
        if registered_count != len(registered):
            report.failures.append(
                f"_catalog 자기검증 실패: 등록 {registered_count} != 게시 {len(registered)} (적재됐으나 등록 누락 가능)"
            )

    # API Smoke Test — external published products must be reachable.
    published_status = {STATUS_PUBLISHED, STATUS_DEGRADED}
    status_by_model = {r.model_name: r.serving_status for r in report.records}
    for contract in contracts:
        if contract.external and status_by_model.get(contract.model_name) in published_status:
            if not smoke.check(contract.model_name):
                report.failures.append(f"{contract.model_name}: API smoke test 실패")

    if report.failures:
        raise PublicationError(report)
    return report
