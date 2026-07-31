"""Traffic Bronze batch loading independent of Airflow wiring."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from common.raw_manifest import validate_raw_manifest
from traffic_ingest.acc_info import (
    SOURCE_ID,
    metadata_total_count,
    parse_seoul_acc_info_response,
)
from traffic_ingest.errors import (
    TrafficCompletenessError,
    TrafficSourceSchemaError,
)
from traffic_ingest.landing import TrafficCollectionMode, verify_raw_payload_hash


@dataclass(frozen=True)
class _PreparedTrafficPage:
    rows: list[dict]
    metadata: dict
    request_id: str
    start_index: int
    end_index: int
    raw_object_key: str
    raw_hash: str
    http_status: int
    collected_at: datetime


@dataclass(frozen=True)
class _PreparedTrafficBatch:
    pages: tuple[_PreparedTrafficPage, ...]
    result_code: str
    list_total_count: int
    expected_rows: int
    collection_mode: TrafficCollectionMode
    is_publishable: bool


def _required_value(document: Mapping[str, Any], key: str) -> Any:
    if key not in document or document[key] in (None, ""):
        raise TrafficSourceSchemaError(
            f"Traffic raw object is missing required field: {key}"
        )
    return document[key]


def _integer(value: Any, *, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise TrafficSourceSchemaError(
            f"Traffic Bronze field must be an integer: {field}"
        ) from exc


def _collection_mode(raw_result: Mapping[str, Any]) -> TrafficCollectionMode:
    value = raw_result.get("collection_mode", TrafficCollectionMode.FULL_SNAPSHOT.value)
    try:
        return TrafficCollectionMode(value)
    except ValueError as exc:
        raise TrafficSourceSchemaError(
            f"Unsupported Traffic Bronze collection_mode: {value}"
        ) from exc


def validate_traffic_raw_manifest(
    raw_result: Mapping[str, Any],
    *,
    dag_run_id: str,
    dataset: str,
    download_raw_object,
) -> None:
    manifest_key = str(raw_result.get("manifest_key") or "")
    if not manifest_key:
        raise TrafficCompletenessError(
            "Traffic raw landing manifest is missing; cannot load Bronze rows."
        )
    try:
        document = json.loads(
            download_raw_object(manifest_key, "Traffic raw landing manifest")
        )
        validate_raw_manifest(
            document,
            run_id=dag_run_id,
            dataset=dataset,
            object_keys=[
                str(item["raw_object_key"])
                for item in raw_result.get("raw_objects") or []
            ],
        )
    except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TrafficCompletenessError(
            "Traffic raw landing manifest validation failed"
        ) from exc


def _prepare_page(
    raw_object: Any,
    *,
    download_raw_object,
    list_total_count: int,
) -> _PreparedTrafficPage:
    if not isinstance(raw_object, Mapping):
        raise TrafficSourceSchemaError("Traffic raw object must be a mapping")
    request_id = str(_required_value(raw_object, "request_id"))
    raw_object_key = str(_required_value(raw_object, "raw_object_key"))
    raw_hash = str(_required_value(raw_object, "raw_hash"))
    http_status = _integer(
        _required_value(raw_object, "http_status"), field="http_status"
    )
    start_index = _integer(
        _required_value(raw_object, "start_index"), field="start_index"
    )
    end_index = _integer(_required_value(raw_object, "end_index"), field="end_index")
    recorded_row_count = _integer(
        _required_value(raw_object, "row_count"), field="row_count"
    )
    recorded_total_count = _integer(
        _required_value(raw_object, "total_count"), field="total_count"
    )
    try:
        collected_at = datetime.fromisoformat(
            str(_required_value(raw_object, "collected_at"))
        )
    except ValueError as exc:
        raise TrafficSourceSchemaError(
            "Traffic raw object collected_at must be an ISO-8601 datetime"
        ) from exc

    raw_bytes = download_raw_object(raw_object_key, "Seoul traffic raw payload")
    verify_raw_payload_hash(
        raw_bytes,
        expected_hash=raw_hash,
        raw_object_key=raw_object_key,
    )
    metadata, rows = parse_seoul_acc_info_response(raw_bytes)
    if metadata.get("list_total_count") in (None, ""):
        raise TrafficSourceSchemaError(
            "Seoul AccInfo response is missing list_total_count"
        )
    try:
        parsed_total_count = metadata_total_count(metadata)
    except (TypeError, ValueError) as exc:
        raise TrafficSourceSchemaError(
            "Seoul AccInfo list_total_count must be an integer"
        ) from exc
    if not (parsed_total_count == recorded_total_count == list_total_count):
        raise TrafficCompletenessError(
            "Seoul traffic page metadata mismatch: "
            f"raw_object_key={raw_object_key}, parsed_total_count={parsed_total_count}, "
            f"recorded_total_count={recorded_total_count}, "
            f"list_total_count={list_total_count}"
        )
    if recorded_row_count != len(rows):
        raise TrafficCompletenessError(
            "Seoul traffic page row count mismatch: "
            f"raw_object_key={raw_object_key}, recorded_row_count={recorded_row_count}, "
            f"parsed_row_count={len(rows)}"
        )
    if start_index < 1 or end_index < start_index:
        raise TrafficCompletenessError(
            "Seoul traffic page range is invalid: "
            f"start_index={start_index}, end_index={end_index}"
        )
    expected_page_rows = max(0, min(end_index, list_total_count) - start_index + 1)
    if len(rows) != expected_page_rows:
        raise TrafficCompletenessError(
            "Seoul traffic page incomplete: "
            f"start_index={start_index}, end_index={end_index}, "
            f"expected_rows={expected_page_rows}, parsed_rows={len(rows)}"
        )
    return _PreparedTrafficPage(
        rows=rows,
        metadata=metadata,
        request_id=request_id,
        start_index=start_index,
        end_index=end_index,
        raw_object_key=raw_object_key,
        raw_hash=raw_hash,
        http_status=http_status,
        collected_at=collected_at,
    )


def _validate_page_set(
    pages: tuple[_PreparedTrafficPage, ...],
    *,
    raw_result: Mapping[str, Any],
    collection_mode: TrafficCollectionMode,
    list_total_count: int,
) -> None:
    if (
        collection_mode is not TrafficCollectionMode.WINDOW
        and pages[0].start_index != 1
    ):
        raise TrafficCompletenessError(
            "Traffic full snapshot/backfill page coverage must start at index 1"
        )
    for previous, current in zip(pages, pages[1:]):
        if current.start_index != previous.end_index + 1:
            raise TrafficCompletenessError(
                "Traffic Bronze page ranges must be ordered and contiguous"
            )
    if (
        collection_mode is not TrafficCollectionMode.WINDOW
        and pages[-1].end_index < list_total_count
    ):
        raise TrafficCompletenessError(
            "Traffic Bronze page coverage does not reach list_total_count"
        )
    declared_page_count = _integer(
        raw_result.get("page_count", len(pages)), field="page_count"
    )
    if declared_page_count != len(pages):
        raise TrafficCompletenessError(
            "Traffic Bronze page_count mismatch: "
            f"declared={declared_page_count}, actual={len(pages)}"
        )
    declared_raw_keys = raw_result.get("raw_object_keys")
    actual_raw_keys = [page.raw_object_key for page in pages]
    if declared_raw_keys is not None and list(declared_raw_keys) != actual_raw_keys:
        raise TrafficCompletenessError(
            "Traffic Bronze raw_object_keys do not match prepared pages"
        )
    declared_pages = raw_result.get("pages")
    if declared_pages is not None:
        if not isinstance(declared_pages, list) or len(declared_pages) != len(pages):
            raise TrafficCompletenessError(
                "Traffic Bronze page descriptors do not match prepared pages"
            )
        for declared, prepared in zip(declared_pages, pages):
            expected = {
                "start_index": prepared.start_index,
                "end_index": prepared.end_index,
                "row_count": len(prepared.rows),
                "list_total_count": list_total_count,
                "raw_object_key": prepared.raw_object_key,
            }
            if not isinstance(declared, Mapping) or any(
                declared.get(key) != value for key, value in expected.items()
            ):
                raise TrafficCompletenessError(
                    "Traffic Bronze page descriptor mismatch: "
                    f"raw_object_key={prepared.raw_object_key}"
                )


def _prepare_batch(
    *,
    raw_result: Mapping[str, Any],
    download_raw_object,
) -> _PreparedTrafficBatch:
    raw_objects = raw_result.get("raw_objects") or []
    if not isinstance(raw_objects, list) or not raw_objects:
        raise TrafficCompletenessError(
            "Seoul traffic raw landing result is empty; cannot load bronze rows."
        )
    list_total_count = _integer(
        raw_result.get("list_total_count", 0), field="list_total_count"
    )
    expected_rows = _integer(
        raw_result.get("expected_rows", list_total_count), field="expected_rows"
    )
    collection_mode = _collection_mode(raw_result)
    is_publishable = raw_result.get("is_publishable", True)
    if not isinstance(is_publishable, bool):
        raise TrafficSourceSchemaError(
            "Traffic Bronze is_publishable must be a boolean"
        )
    pages = tuple(
        _prepare_page(
            raw_object,
            download_raw_object=download_raw_object,
            list_total_count=list_total_count,
        )
        for raw_object in raw_objects
    )
    _validate_page_set(
        pages,
        raw_result=raw_result,
        collection_mode=collection_mode,
        list_total_count=list_total_count,
    )
    parsed_rows = sum(len(page.rows) for page in pages)
    declared_parsed_rows = _integer(
        raw_result.get("parsed_rows", parsed_rows), field="parsed_rows"
    )
    if declared_parsed_rows != parsed_rows:
        raise TrafficCompletenessError(
            "Traffic Bronze parsed_rows mismatch: "
            f"declared={declared_parsed_rows}, actual={parsed_rows}"
        )
    first_index = pages[0].start_index
    last_index = pages[-1].end_index
    contract_expected_rows = (
        max(0, min(last_index, list_total_count) - first_index + 1)
        if collection_mode is TrafficCollectionMode.WINDOW
        else list_total_count
    )
    if expected_rows != contract_expected_rows or parsed_rows != expected_rows:
        raise TrafficCompletenessError(
            "Seoul traffic bronze load incomplete: "
            f"list_total_count={list_total_count}, parsed_rows={parsed_rows}, "
            f"expected_rows={expected_rows}, collection_mode={collection_mode.value}, "
            f"requested_end_index={raw_result.get('requested_end_index', 'N/A')}"
        )
    result_codes = {str(page.metadata.get("result_code") or "") for page in pages}
    declared_result_code = str(raw_result.get("result_code") or "")
    if len(result_codes) != 1 or (
        declared_result_code and declared_result_code not in result_codes
    ):
        raise TrafficCompletenessError(
            "Traffic Bronze result_code metadata is inconsistent across pages"
        )
    result_code = result_codes.pop() or declared_result_code or "N/A"
    return _PreparedTrafficBatch(
        pages=pages,
        result_code=result_code,
        list_total_count=list_total_count,
        expected_rows=expected_rows,
        collection_mode=collection_mode,
        is_publishable=is_publishable,
    )


def load_traffic_bronze_batch(
    *,
    raw_result: dict,
    dag_run_id: str,
    cursor_factory,
    create_table,
    download_raw_object,
    insert_rows,
) -> dict:
    raw_objects = raw_result.get("raw_objects") or []
    if not isinstance(raw_objects, list) or not raw_objects:
        raise TrafficCompletenessError(
            "Seoul traffic raw landing result is empty; cannot load bronze rows."
        )
    validate_traffic_raw_manifest(
        raw_result,
        dag_run_id=dag_run_id,
        dataset=SOURCE_ID,
        download_raw_object=download_raw_object,
    )
    prepared = _prepare_batch(
        raw_result=raw_result,
        download_raw_object=download_raw_object,
    )
    cursor, catalog, schema = cursor_factory()
    qualified_table = create_table(cursor, catalog, schema)

    inserted = 0
    for page in prepared.pages:
        inserted += insert_rows(
            cursor=cursor,
            qualified_table=qualified_table,
            rows=page.rows,
            metadata=page.metadata,
            request_id=page.request_id,
            start_index=page.start_index,
            end_index=page.end_index,
            raw_object_key=page.raw_object_key,
            raw_hash=page.raw_hash,
            http_status=page.http_status,
            collected_at=page.collected_at,
            dag_run_id=dag_run_id,
        )

    print(
        f"Inserted {inserted} Seoul traffic rows from {len(prepared.pages)} raw objects "
        f"into {qualified_table}"
    )
    return {
        "source_id": SOURCE_ID,
        "raw_object_keys": [page.raw_object_key for page in prepared.pages],
        "inserted": inserted,
        "result_code": prepared.result_code,
        "list_total_count": prepared.list_total_count,
        "expected_rows": prepared.expected_rows,
        "collection_mode": prepared.collection_mode.value,
        "is_publishable": prepared.is_publishable,
        "page_count": len(prepared.pages),
        "requested_end_index": raw_result.get("requested_end_index"),
        "pages": raw_result.get("pages") or [],
    }


def load_traffic_bronze_batches(
    *,
    raw_results: Mapping[str, dict],
    cursor_factory,
    create_table,
    download_raw_object,
    replace_snapshots,
) -> dict[str, dict]:
    """Validate all receipts first, then publish them in one Trino mutation batch."""
    if not raw_results:
        return {}

    prepared_by_run: dict[str, _PreparedTrafficBatch] = {}
    for dag_run_id, raw_result in raw_results.items():
        if not dag_run_id:
            raise TrafficSourceSchemaError(
                "Traffic Bronze batch requires a non-empty dag_run_id"
            )
        raw_objects = raw_result.get("raw_objects") or []
        if not isinstance(raw_objects, list) or not raw_objects:
            raise TrafficCompletenessError(
                "Seoul traffic raw landing result is empty; cannot load bronze rows."
            )
        validate_traffic_raw_manifest(
            raw_result,
            dag_run_id=dag_run_id,
            dataset=SOURCE_ID,
            download_raw_object=download_raw_object,
        )
        prepared_by_run[dag_run_id] = _prepare_batch(
            raw_result=raw_result,
            download_raw_object=download_raw_object,
        )

    cursor, catalog, schema = cursor_factory()
    qualified_table = create_table(cursor, catalog, schema)
    snapshots = [
        {
            "dag_run_id": dag_run_id,
            "pages": [
                {
                    "rows": page.rows,
                    "metadata": page.metadata,
                    "request_id": page.request_id,
                    "start_index": page.start_index,
                    "end_index": page.end_index,
                    "raw_object_key": page.raw_object_key,
                    "raw_hash": page.raw_hash,
                    "http_status": page.http_status,
                    "collected_at": page.collected_at,
                }
                for page in prepared.pages
            ],
        }
        for dag_run_id, prepared in prepared_by_run.items()
    ]
    inserted_by_run = replace_snapshots(
        cursor=cursor,
        qualified_table=qualified_table,
        snapshots=snapshots,
    )

    results: dict[str, dict] = {}
    for dag_run_id, prepared in prepared_by_run.items():
        raw_result = raw_results[dag_run_id]
        inserted = int(inserted_by_run.get(dag_run_id, 0))
        results[dag_run_id] = {
            "source_id": SOURCE_ID,
            "raw_object_keys": [
                page.raw_object_key for page in prepared.pages
            ],
            "inserted": inserted,
            "result_code": prepared.result_code,
            "list_total_count": prepared.list_total_count,
            "expected_rows": prepared.expected_rows,
            "collection_mode": prepared.collection_mode.value,
            "is_publishable": prepared.is_publishable,
            "page_count": len(prepared.pages),
            "requested_end_index": raw_result.get("requested_end_index"),
            "pages": raw_result.get("pages") or [],
        }
    print(
        f"Inserted {sum(result['inserted'] for result in results.values())} "
        f"Seoul traffic rows from {len(results)} receipts into {qualified_table}"
    )
    return results
