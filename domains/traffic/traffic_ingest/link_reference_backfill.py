"""Bounded, resumable catch-up for TOPIS road-link reference pairs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from traffic_ingest.common.runtime import (
    download_raw_object,
    sql_identifier,
    sql_string,
    trino_cursor,
)
from traffic_ingest.errors import (
    TrafficBronzeConfigurationError,
    TrafficCompletenessError,
)
from traffic_ingest.flow_info import normalize_link_ids
from traffic_ingest.link_reference_bronze import (
    load_traffic_link_reference_batch,
    unresolved_link_reference_ids,
    verify_seoul_traffic_link_reference_runtime,
)
from traffic_ingest.link_reference_info import SOURCE_ID


DEFAULT_BATCH_SIZE = 100
MAX_BATCH_SIZE = 500


@dataclass(frozen=True)
class LinkReferenceBackfillRequest:
    start_after_link_id: str | None
    batch_size: int
    force_refresh: bool
    link_ids: list[str] | None


def _batch_size(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TrafficBronzeConfigurationError(
            "batch_size must be an integer"
        )
    if not 1 <= value <= MAX_BATCH_SIZE:
        raise TrafficBronzeConfigurationError(
            f"batch_size must be between 1 and {MAX_BATCH_SIZE}"
        )
    return value


def parse_backfill_conf(conf: dict[str, object]) -> LinkReferenceBackfillRequest:
    if not isinstance(conf, dict):
        raise TrafficBronzeConfigurationError("backfill conf must be an object")
    batch_size = _batch_size(conf.get("batch_size", DEFAULT_BATCH_SIZE))
    force_refresh = conf.get("force_refresh", False)
    if not isinstance(force_refresh, bool):
        raise TrafficBronzeConfigurationError(
            "force_refresh must be a boolean"
        )

    explicit = conf.get("link_ids")
    link_ids = None if explicit in (None, "") else normalize_link_ids(explicit)
    if link_ids is not None and len(link_ids) > batch_size:
        raise TrafficBronzeConfigurationError("link_ids exceeds batch_size")

    raw_cursor = conf.get("start_after_link_id")
    start_after = (
        None
        if raw_cursor in (None, "")
        else normalize_link_ids([raw_cursor])[0]
    )
    if link_ids is not None and start_after is not None:
        raise TrafficBronzeConfigurationError(
            "start_after_link_id cannot be combined with link_ids"
        )
    return LinkReferenceBackfillRequest(
        start_after_link_id=start_after,
        batch_size=batch_size,
        force_refresh=force_refresh,
        link_ids=link_ids,
    )


def build_backfill_link_sql(
    *,
    catalog: str,
    schema: str,
    start_after_link_id: str | None,
    batch_size: int,
) -> str:
    safe_catalog = sql_identifier(catalog)
    safe_schema = sql_identifier(schema)
    safe_batch_size = _batch_size(batch_size)
    normalized_cursor = (
        None
        if start_after_link_id in (None, "")
        else normalize_link_ids([start_after_link_id])[0]
    )
    cursor_predicate = (
        ""
        if normalized_cursor is None
        else f"WHERE link_id > {sql_string(normalized_cursor)}"
    )
    qualified = f"{safe_catalog}.{safe_schema}"
    return f"""
        WITH link_universe AS (
            SELECT cast(link_id AS varchar) AS link_id
            FROM {qualified}.bronze_seoul_traffic_incident
            WHERE link_id IS NOT NULL
              AND trim(cast(link_id AS varchar)) <> ''
            UNION
            SELECT cast(link_id AS varchar) AS link_id
            FROM {qualified}.bronze_seoul_traffic_flow
            WHERE link_id IS NOT NULL
              AND trim(cast(link_id AS varchar)) <> ''
        )
        SELECT link_id
        FROM link_universe
        {cursor_predicate}
        ORDER BY link_id
        LIMIT {safe_batch_size}
    """


def resolve_backfill_link_ids(
    conf: dict[str, object],
    *,
    cursor_factory=trino_cursor,
) -> list[str]:
    request = parse_backfill_conf(conf)
    if request.link_ids is not None:
        return list(request.link_ids)
    cursor, catalog, schema = cursor_factory()
    cursor.execute(
        build_backfill_link_sql(
            catalog=catalog,
            schema=schema,
            start_after_link_id=request.start_after_link_id,
            batch_size=request.batch_size,
        )
    )
    return normalize_link_ids([row[0] for row in cursor.fetchall()])[
        : request.batch_size
    ]


def _empty_landing_result() -> dict[str, object]:
    return {
        "source_id": SOURCE_ID,
        "requested_link_ids": [],
        "raw_objects": [],
        "raw_object_keys": [],
        "parsed_rows": 0,
        "expected_raw_objects": 0,
        "is_publishable": True,
        "manifest_key": None,
        "landing_load_date": None,
    }


def land_backfill_batch(
    *,
    conf: dict[str, object],
    dag_run_id: str,
    landing=None,
    cursor_factory=trino_cursor,
    unresolved_link_ids: Callable[[list[str]], list[str]] = (
        unresolved_link_reference_ids
    ),
) -> dict[str, object]:
    request = parse_backfill_conf(conf)
    candidates = resolve_backfill_link_ids(
        conf,
        cursor_factory=cursor_factory,
    )
    to_fetch = (
        candidates
        if request.force_refresh
        else unresolved_link_ids(candidates)
    )
    if to_fetch:
        if landing is None:
            from traffic_ingest.link_reference_ingest import (
                build_traffic_link_reference_landing,
            )

            landing = build_traffic_link_reference_landing()
        result = dict(
            landing.collect(
                link_ids=to_fetch,
                dag_run_id=dag_run_id,
                landing_load_date=None,
            )
        )
    else:
        result = _empty_landing_result()
    result.update(
        {
            "requested_link_ids": candidates,
            "unresolved_link_ids": to_fetch,
            "last_processed_link_id": candidates[-1] if candidates else None,
            "processed_link_count": len(candidates),
            "remaining_unknown": len(candidates) == request.batch_size,
        }
    )
    return result


def _load_batch(
    *, raw_result: dict[str, Any], dag_run_id: str
) -> dict[str, Any]:
    return load_traffic_link_reference_batch(
        raw_result=raw_result,
        dag_run_id=dag_run_id,
        cursor_factory=trino_cursor,
        download_raw_object=download_raw_object,
    )


def _verify_runtime(
    *, dag_run_id: str, load_result: dict[str, Any]
) -> dict[str, int]:
    return verify_seoul_traffic_link_reference_runtime(
        dag_run_id=dag_run_id,
        expected_info_rows=int(load_result.get("inserted_info", 0)),
        expected_vertex_rows=int(load_result.get("inserted_vertices", 0)),
        expected_raw_objects=int(load_result.get("audit_rows", 0)),
        cursor_factory=trino_cursor,
    )


def materialize_backfill_batch(
    *,
    raw_result: dict[str, object],
    dag_run_id: str,
    load_batch: Callable[..., dict[str, Any]] = _load_batch,
    verify_runtime: Callable[..., dict[str, int]] = _verify_runtime,
    unresolved_link_ids: Callable[[list[str]], list[str]] = (
        unresolved_link_reference_ids
    ),
) -> dict[str, object]:
    requested = normalize_link_ids(raw_result.get("requested_link_ids") or [])
    unresolved = normalize_link_ids(raw_result.get("unresolved_link_ids") or [])
    outside_scope = [link_id for link_id in unresolved if link_id not in requested]
    if outside_scope:
        raise TrafficCompletenessError(
            "Traffic link reference backfill contains links outside its request: "
            + ",".join(outside_scope)
        )

    if unresolved:
        load_result = load_batch(
            raw_result=raw_result,
            dag_run_id=dag_run_id,
        )
        verification = verify_runtime(
            dag_run_id=dag_run_id,
            load_result=load_result,
        )
    else:
        load_result = {
            "inserted_info": 0,
            "inserted_vertices": 0,
            "audit_rows": 0,
            "raw_object_keys": [],
        }
        verification = {
            "info_rows": 0,
            "vertex_rows": 0,
            "raw_objects": 0,
            "audit_rows": 0,
        }

    remaining = unresolved_link_ids(requested)
    if remaining:
        raise TrafficCompletenessError(
            "Traffic link reference remains unresolved after backfill: "
            + ",".join(remaining)
        )
    return {
        **load_result,
        "verification": verification,
        "requested_link_ids": requested,
        "last_processed_link_id": raw_result.get("last_processed_link_id"),
        "processed_link_count": int(
            raw_result.get("processed_link_count", len(requested))
        ),
        "remaining_unknown": bool(raw_result.get("remaining_unknown", False)),
    }


__all__ = [
    "LinkReferenceBackfillRequest",
    "build_backfill_link_sql",
    "land_backfill_batch",
    "materialize_backfill_batch",
    "parse_backfill_conf",
    "resolve_backfill_link_ids",
]
