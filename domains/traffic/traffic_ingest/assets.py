"""Traffic-owned Airflow Asset contracts and metadata validation."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from datetime import datetime
from zoneinfo import ZoneInfo

import airflow
from airflow.sdk import Asset, AssetAlias

from common.assets import TRAFFIC_BRONZE_ASSET


KST = ZoneInfo("Asia/Seoul")
TRAFFIC_INCIDENT_RAW_ASSET = "r2://traffic/incident/raw-snapshot"
TRAFFIC_INCIDENT_BRONZE_ASSET = TRAFFIC_BRONZE_ASSET
TRAFFIC_INCIDENT_SILVER_ASSET = "iceberg://traffic/incident/silver"
TRAFFIC_FLOW_BRONZE_ASSET = "iceberg://traffic/flow/bronze"
TRAFFIC_FLOW_SILVER_ASSET = "iceberg://traffic/flow/silver"
TRAFFIC_GOLD_PUBLICATION_READY_ASSET = "iceberg://traffic/gold/publication-ready"

TRAFFIC_INCIDENT_RAW_ASSET_REF = Asset(TRAFFIC_INCIDENT_RAW_ASSET)
TRAFFIC_INCIDENT_BRONZE_ASSET_REF = Asset(TRAFFIC_INCIDENT_BRONZE_ASSET)
TRAFFIC_INCIDENT_SILVER_ASSET_REF = Asset(TRAFFIC_INCIDENT_SILVER_ASSET)
TRAFFIC_FLOW_BRONZE_ASSET_REF = Asset(TRAFFIC_FLOW_BRONZE_ASSET)
TRAFFIC_FLOW_SILVER_ASSET_REF = Asset(TRAFFIC_FLOW_SILVER_ASSET)
TRAFFIC_GOLD_PUBLICATION_READY_ASSET_REF = Asset(TRAFFIC_GOLD_PUBLICATION_READY_ASSET)
TRAFFIC_INCIDENT_MATERIALIZED_ALIAS = AssetAlias("traffic_incident_materialized")
TRAFFIC_INCIDENT_SILVER_MATERIALIZED_ALIAS = AssetAlias(
    "traffic_incident_silver_materialized"
)
TRAFFIC_FLOW_MATERIALIZED_ALIAS = AssetAlias("traffic_flow_materialized")
TRAFFIC_FLOW_SILVER_MATERIALIZED_ALIAS = AssetAlias(
    "traffic_flow_silver_materialized"
)

INCIDENT_BRONZE_FIELDS = frozenset(
    {
        "source_id",
        "bronze_run_id",
        "bronze_dag_run_id",
        "event_at",
        "load_date",
        "row_count",
        "payload_hash",
        "is_publishable",
    }
)
FLOW_BRONZE_FIELDS = frozenset(
    {
        "source_id",
        "flow_run_id",
        "flow_dag_run_id",
        "parent_incident_run_id",
        "event_at",
        "load_date",
        "row_count",
        "payload_hash",
        "is_publishable",
    }
)
FLOW_SILVER_FIELDS = frozenset(
    {
        "source_id",
        "flow_run_id",
        "flow_dag_run_id",
        "parent_incident_run_id",
        "event_at",
        "is_publishable",
        "contract",
    }
)
LEGACY_INCIDENT_BRONZE_TASK_IDS = frozenset(
    {"verify_seoul_traffic_bronze_runtime"}
)


class TrafficAssetContractError(ValueError):
    """An Asset event does not satisfy the Traffic snapshot contract."""


def _asset_uri(value: object) -> str:
    return str(getattr(value, "uri", None) or value)


def _event_time(metadata: Mapping[str, object]) -> datetime:
    try:
        value = datetime.fromisoformat(str(metadata["event_at"]).replace("Z", "+00:00"))
    except (KeyError, TypeError, ValueError) as exc:
        raise TrafficAssetContractError("Traffic Asset event timestamp is malformed") from exc
    if value.tzinfo is None:
        raise TrafficAssetContractError("Traffic Asset event timestamp must include timezone")
    return value


def _matching_asset_events(
    context: Mapping[str, object], *, asset_uri: str
) -> list[object]:
    triggering = context.get("triggering_asset_events") or {}
    if not isinstance(triggering, Mapping):
        raise TrafficAssetContractError("Traffic triggering Asset events are malformed")

    events: list[object] = []
    for asset_key, asset_events in triggering.items():
        if _asset_uri(asset_key) != asset_uri:
            continue
        if isinstance(asset_events, (list, tuple)):
            events.extend(asset_events)
        else:
            events.append(asset_events)
    return events


def _asset_event_timestamp(event: object) -> datetime:
    timestamp = getattr(event, "timestamp", None)
    if timestamp is None:
        metadata = getattr(event, "extra", None)
        if not isinstance(metadata, Mapping):
            raise TrafficAssetContractError(
                "Traffic triggering Asset event timestamp is unavailable"
            )
        return _event_time(metadata)
    if not isinstance(timestamp, datetime) or timestamp.tzinfo is None:
        raise TrafficAssetContractError(
            "Traffic triggering Asset event timestamp is malformed"
        )
    return timestamp


def _is_legacy_incident_bronze_event(event: object) -> bool:
    metadata = getattr(event, "extra", None)
    return (
        isinstance(metadata, Mapping)
        and not metadata
        and getattr(event, "source_dag_id", None) == "traffic_incident_bronze"
        and getattr(event, "source_task_id", None)
        in LEGACY_INCIDENT_BRONZE_TASK_IDS
    )


def _validated_events(
    context: Mapping[str, object],
    *,
    asset_uri: str,
    required_fields: frozenset[str],
    source_id: str,
    run_field: str,
    duplicate_run_field: str,
    ignore_event: Callable[[object], bool] | None = None,
) -> list[dict[str, object]]:
    events = _matching_asset_events(context, asset_uri=asset_uri)
    if ignore_event is not None:
        events = [event for event in events if not ignore_event(event)]

    validated: list[dict[str, object]] = []
    for event in events:
        metadata = getattr(event, "extra", None)
        if not isinstance(metadata, Mapping) or not required_fields <= metadata.keys():
            raise TrafficAssetContractError("Traffic Asset event metadata is incomplete")
        run_id = str(metadata.get(run_field) or "")
        if (
            metadata.get("source_id") != source_id
            or not run_id
            or str(metadata.get(duplicate_run_field) or "") != run_id
            or metadata.get("is_publishable") is not True
        ):
            raise TrafficAssetContractError("Traffic Asset event identity is invalid")
        _event_time(metadata)
        validated.append(dict(metadata))
    return sorted(validated, key=lambda item: (_event_time(item), str(item[run_field])))


def incident_bronze_events(context: Mapping[str, object]) -> list[dict[str, object]]:
    return _validated_events(
        context,
        asset_uri=TRAFFIC_INCIDENT_BRONZE_ASSET,
        required_fields=INCIDENT_BRONZE_FIELDS,
        source_id="seoul_traffic_incident",
        run_field="bronze_dag_run_id",
        duplicate_run_field="bronze_run_id",
        ignore_event=_is_legacy_incident_bronze_event,
    )


def latest_incident_bronze_event(
    context: Mapping[str, object],
) -> dict[str, object] | None:
    """Select the newest triggering event before applying the strict contract."""

    events = _matching_asset_events(
        context,
        asset_uri=TRAFFIC_INCIDENT_BRONZE_ASSET,
    )
    events = [event for event in events if not _is_legacy_incident_bronze_event(event)]
    if not events:
        return None
    _, latest = max(
        enumerate(events),
        key=lambda item: (_asset_event_timestamp(item[1]), item[0]),
    )
    return incident_bronze_events(
        {
            "triggering_asset_events": {
                TRAFFIC_INCIDENT_BRONZE_ASSET: [latest],
            }
        }
    )[0]


def flow_bronze_events(context: Mapping[str, object]) -> list[dict[str, object]]:
    return _validated_events(
        context,
        asset_uri=TRAFFIC_FLOW_BRONZE_ASSET,
        required_fields=FLOW_BRONZE_FIELDS,
        source_id="seoul_traffic_flow",
        run_field="flow_dag_run_id",
        duplicate_run_field="flow_run_id",
    )


def flow_silver_events(context: Mapping[str, object]) -> list[dict[str, object]]:
    events = _validated_events(
        context,
        asset_uri=TRAFFIC_FLOW_SILVER_ASSET,
        required_fields=FLOW_SILVER_FIELDS,
        source_id="seoul_traffic_flow",
        run_field="flow_dag_run_id",
        duplicate_run_field="flow_run_id",
    )
    if any(
        set(event) != FLOW_SILVER_FIELDS
        or event["contract"] != "traffic_flow_silver.v1"
        for event in events
    ):
        raise TrafficAssetContractError("Traffic Flow Silver Asset metadata is invalid")
    return events


def _airflow_three_schedule(*, cron: str, asset: Asset, timezone: ZoneInfo):
    from airflow.timetables.assets import AssetOrTimeSchedule
    from airflow.timetables.trigger import CronTriggerTimetable

    return AssetOrTimeSchedule(
        # Airflow parses timezone names into its serializable Pendulum type.
        timetable=CronTriggerTimetable(cron, timezone=str(timezone)),
        assets=asset,
    )


def _major_version(value: str) -> int:
    try:
        return int(value.split(".", 1)[0])
    except (TypeError, ValueError) as exc:
        raise TrafficAssetContractError(f"Unsupported Airflow version: {value}") from exc


def schedule_asset(
    asset_uri: str,
    *,
    airflow_version: str | None = None,
):
    """Return the schedule-native Asset type for the installed Airflow core."""

    version = airflow_version or airflow.__version__
    if _major_version(version) >= 3:
        return Asset(asset_uri)
    from airflow.datasets import Dataset

    return Dataset(asset_uri)


def materializer_schedule(
    *,
    env: Mapping[str, str] = os.environ,
    airflow_version: str | None = None,
    schedule_factory: Callable[..., object] | None = None,
):
    """Return a cron-only drain schedule from explicit override or shared default.

    Raw Asset events are deliberately not a second scheduling path: the 5-minute
    landing cadence is drained in bounded groups by this 15-minute materializer.
    """

    canonical_schedule_env = "ASK_SEOUL_TRAFFIC_MATERIALIZER_DAG_SCHEDULE"
    if canonical_schedule_env in env:
        return env[canonical_schedule_env] or None
    if env.get("ASK_SEOUL_TARGET", env.get("DBT_TARGET", "prod")) != "dev":
        return "*/15 * * * *"
    del airflow_version, schedule_factory
    return env.get(
        "ASK_SEOUL_TRAFFIC_MATERIALIZER_FALLBACK_SCHEDULE",
        "*/15 * * * *",
    ) or None


def publish_through_alias(
    context: Mapping[str, object],
    *,
    alias: AssetAlias,
    asset: Asset,
    metadata: dict[str, object],
) -> None:
    outlet_events = context.get("outlet_events")
    if outlet_events is None:
        raise TrafficAssetContractError("Traffic outlet Asset event is unavailable")
    outlet_events[alias].add(asset, extra=metadata)


__all__ = [
    "FLOW_BRONZE_FIELDS",
    "FLOW_SILVER_FIELDS",
    "INCIDENT_BRONZE_FIELDS",
    "KST",
    "TRAFFIC_FLOW_BRONZE_ASSET",
    "TRAFFIC_FLOW_BRONZE_ASSET_REF",
    "TRAFFIC_FLOW_MATERIALIZED_ALIAS",
    "TRAFFIC_FLOW_SILVER_ASSET",
    "TRAFFIC_FLOW_SILVER_ASSET_REF",
    "TRAFFIC_GOLD_PUBLICATION_READY_ASSET",
    "TRAFFIC_GOLD_PUBLICATION_READY_ASSET_REF",
    "TRAFFIC_FLOW_SILVER_MATERIALIZED_ALIAS",
    "TRAFFIC_INCIDENT_BRONZE_ASSET",
    "TRAFFIC_INCIDENT_BRONZE_ASSET_REF",
    "TRAFFIC_INCIDENT_MATERIALIZED_ALIAS",
    "TRAFFIC_INCIDENT_RAW_ASSET",
    "TRAFFIC_INCIDENT_RAW_ASSET_REF",
    "TRAFFIC_INCIDENT_SILVER_ASSET",
    "TRAFFIC_INCIDENT_SILVER_ASSET_REF",
    "TRAFFIC_INCIDENT_SILVER_MATERIALIZED_ALIAS",
    "TrafficAssetContractError",
    "flow_bronze_events",
    "flow_silver_events",
    "incident_bronze_events",
    "latest_incident_bronze_event",
    "materializer_schedule",
    "publish_through_alias",
    "schedule_asset",
]
